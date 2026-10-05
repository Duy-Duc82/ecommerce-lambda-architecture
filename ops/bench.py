"""Benchmark runner — Phase 9 plan section 8 (P2-01).

Runs on the host, like the drills: it drives Docker, and it reaches the
stack through the published ports of an isolated project. ``mp bench``
passes ``-EnvFile env/bench.env``; a bench never runs in ``mp-live``.

Each scenario starts from a fresh project (``down --volumes``, then ``up``),
so one run cannot warm the next, and writes one JSON file under
``data/ops/bench/``. That file always carries:

- ``environment``: hardware, Docker, versions, the commit, and whether the
  tree was dirty;
- ``dataset``: ``replayed_fixture`` — every observation here is the frozen
  fixture replayed (plan decision D4), never a marketplace observation;
- the parameters, the generator seed, every run, and a summary of the runs
  (median, min, max). A single run is never reported as a result unless
  ``--repeat 1`` was asked for, and the file says so.

Scenarios:

- ``crawl``: the crawler against the stub, at the deployed politeness delay
  and again at 0. Only the first describes the crawler as deployed; the
  second is its parse-and-publish capacity against a local source.
- ``ingest``: N observations sent at full speed; Silver records per second
  until the sink's lag is 0, and Silver write latency from object
  ``LastModified`` (one-second resolution) minus ``produced_at``.
- ``speed``: fixed send rates for a fixed time each; Spark's progress, batch
  durations and the per-batch latency the speed audit records.
- ``batch``: the batch over the Silver ``ingest`` leaves, at each size.
- ``speed-cost``: what one speed micro-batch costs. A backlog is loaded with
  the speed query down, then the query drains it at a fixed batch size K
  (``maxOffsetsPerTrigger``) and a zero trigger, one K after another. Per K:
  batch duration and where it went (Spark's phases, then clients, compute,
  Kafka, Elasticsearch, Redis); over all K, the fixed and per-row cost of a
  batch and the saturated throughput. Independent of the load generator's
  own ceiling, which the ``speed`` scenario hits near 550/s.
- ``speed-soak``: one rate held for a long time at a given trigger. The run is
  cut into windows; drift between the first and the last window, failed
  batches and whether the backlog drained say whether it stayed stable.

``speed-cost`` and ``speed-soak`` are not part of ``all``.

``resources`` is not a scenario of its own: ``docker stats`` is sampled every
five seconds alongside each run, and summarised per container.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "data" / "ops" / "bench"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "docker-compose.yml")]
LIVE_PROJECT = "mp-live"
DATASET = "replayed_fixture"
SCENARIOS = ("crawl", "ingest", "speed", "batch")
EXTRA_SCENARIOS = ("speed-cost", "speed-soak")
CORE = ("kafka", "minio", "redis", "postgres-dw", "elasticsearch")


class BenchFailed(RuntimeError):
    pass


# -- pure summaries ----------------------------------------------------------

def percentile(values: Sequence[float], q: float) -> float | None:
    """Nearest rank, as the speed audit computes its own."""
    ordered = sorted(v for v in values if v is not None)
    if not ordered:
        return None
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def spread(values: Iterable[float | None]) -> dict[str, float | None]:
    """Median, min and max of one metric across repeated runs."""
    present = [v for v in values if v is not None]
    if not present:
        return {"median": None, "min": None, "max": None, "runs": 0}
    return {"median": statistics.median(present), "min": min(present), "max": max(present), "runs": len(present)}


def summarise_runs(runs: list[dict[str, Any]]) -> dict[str, dict[str, float | None]]:
    """Every numeric metric of the runs, summarised across them."""
    keys = sorted({k for run in runs for k, v in run.get("metrics", {}).items() if isinstance(v, (int, float))})
    return {k: spread(run["metrics"].get(k) for run in runs) for k in keys}


def crawl_metrics(attempts: list[dict[str, Any]]) -> dict[str, Any]:
    """From ``audit.crawl_request_attempt`` rows of one run."""
    if not attempts:
        raise BenchFailed("the crawl run recorded no attempt")
    started = min(a["started_at"] for a in attempts)
    completed = max(a["completed_at"] for a in attempts)
    seconds = max((completed - started).total_seconds(), 1e-9)
    # PARTIAL is a crawled page some of whose rows were rejected: every stub
    # page is, by its deliberately invalid fixture row.
    completed = [a for a in attempts if a["status"] in ("SUCCEEDED", "PARTIAL")]
    latencies = [a["latency_ms"] for a in attempts if a.get("latency_ms") is not None]
    parsed = sum(a["parsed_count"] for a in attempts)
    raw_bytes = sum(a["raw_bytes"] for a in attempts)
    return {
        "requests": len(attempts), "completed": len(completed),
        "partial": sum(a["status"] == "PARTIAL" for a in attempts),
        "failed": sum(a["status"] == "FAILED" for a in attempts), "seconds": round(seconds, 3),
        "requests_per_second": len(attempts) / seconds, "parsed_per_second": parsed / seconds,
        "bytes_per_second": raw_bytes / seconds, "parsed": parsed,
        "latency_p50_ms": percentile(latencies, 0.50), "latency_p95_ms": percentile(latencies, 0.95),
        "latency_p99_ms": percentile(latencies, 0.99),
    }


def silver_latency_metrics(samples: list[tuple[datetime, datetime]]) -> dict[str, Any]:
    """``(LastModified, produced_at)`` pairs, in milliseconds. LastModified has
    one-second resolution, so these are accurate to a second at best."""
    values = [(written - produced).total_seconds() * 1000 for written, produced in samples]
    return {"silver_latency_p50_ms": percentile(values, 0.50), "silver_latency_p95_ms": percentile(values, 0.95),
            "silver_latency_max_ms": max(values) if values else None, "silver_latency_samples": len(values),
            "silver_latency_resolution_ms": 1000}


def speed_metrics(batches: list[dict[str, Any]], progress: list[dict[str, Any]], *, sent: int,
                  send_seconds: float, drain_seconds: float | None) -> dict[str, Any]:
    """From ``marketplace_speed_batch`` and ``marketplace_stream_progress`` rows
    of one rate step.

    Latency percentiles do not compose across batches, so the step reports
    the median of the batches' p50 and the worst batch's p95 and max, each
    weighted by nothing and named for what it is."""
    with_rows = [b for b in batches if (b.get("input_rows") or 0) > 0]
    input_rows = sum(b.get("input_rows") or 0 for b in batches)
    if with_rows:
        first = min(b["started_at"] for b in with_rows)
        last = max(b["completed_at"] for b in with_rows)
        busy = max((last - first).total_seconds(), 1e-9)
    else:
        busy = None
    durations = [p["trigger_execution_ms"] for p in progress if (p.get("num_input_rows") or 0) > 0]
    return {
        "sent": sent, "offered_rate": sent / send_seconds if send_seconds > 0 else None,
        "input_rows": input_rows, "batches_with_rows": len(with_rows),
        "processed_rows_per_second": input_rows / busy if busy else None,
        "batch_duration_p50_ms": percentile(durations, 0.50), "batch_duration_p95_ms": percentile(durations, 0.95),
        "latency_batch_p50_median_ms": spread(b.get("latency_p50_ms") for b in with_rows)["median"],
        "latency_batch_p95_max_ms": max((b["latency_p95_ms"] for b in with_rows if b.get("latency_p95_ms") is not None), default=None),
        "latency_max_ms": max((b["latency_max_ms"] for b in with_rows if b.get("latency_max_ms") is not None), default=None),
        "drain_seconds": drain_seconds, "kept_up": drain_seconds is not None,
    }


def resource_summary(samples: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    """``docker stats`` samples, per container: mean and max CPU %, max memory."""
    by_name: dict[str, list[dict[str, Any]]] = {}
    for sample in samples:
        by_name.setdefault(sample["name"], []).append(sample)
    return {name: {"cpu_mean_pct": round(statistics.mean(s["cpu_pct"] for s in rows), 2),
                   "cpu_max_pct": max(s["cpu_pct"] for s in rows),
                   "mem_max_mib": max(s["mem_mib"] for s in rows), "samples": len(rows)}
            for name, rows in sorted(by_name.items())}


STAGE_COLUMNS = ("stage_clients_ms", "stage_collect_ms", "stage_kafka_ms", "stage_es_ms", "stage_redis_ms")
SPARK_PHASES = ("add_batch_ms", "query_planning_ms", "latest_offset_ms", "get_batch_ms", "wal_commit_ms",
                "commit_offsets_ms")


def fit_line(points: Sequence[tuple[float, float]]) -> dict[str, float | None]:
    """Least squares ``y = fixed + per_unit * x``, with R squared."""
    if len(points) < 2 or len({x for x, _ in points}) < 2:
        return {"fixed": None, "per_unit": None, "r2": None, "points": len(points)}
    n = len(points)
    mx, my = sum(x for x, _ in points) / n, sum(y for _, y in points) / n
    sxx = sum((x - mx) ** 2 for x, _ in points)
    slope = sum((x - mx) * (y - my) for x, y in points) / sxx
    fixed = my - slope * mx
    total = sum((y - my) ** 2 for _, y in points)
    residual = sum((y - fixed - slope * x) ** 2 for x, y in points)
    return {"fixed": fixed, "per_unit": slope, "r2": 1 - residual / total if total else None, "points": n}


def cost_step_metrics(batches: list[dict[str, Any]], *, size: int) -> dict[str, Any]:
    """One batch size: Spark progress joined to the speed audit, warm batches only.

    ``overhead_ms`` is the part of the trigger outside ``addBatch`` (offsets,
    planning, the write-ahead log and commit); ``audit_other_ms`` is the part
    of ``addBatch`` no stage column accounts for: the audit writes and the
    hand-over between Spark and Python."""
    with_rows = [b for b in batches if (b.get("records") or 0) > 0]
    if not with_rows:
        raise BenchFailed(f"speed-cost: no warm batch with rows at K={size}")
    duration = [b["trigger_execution_ms"] for b in with_rows]
    rows = sum(b["records"] for b in with_rows)
    step: dict[str, Any] = {
        "size": size, "batches": len(with_rows),
        "records_p50": percentile([b["records"] for b in with_rows], 0.50),
        "duration_p50_ms": percentile(duration, 0.50), "duration_p95_ms": percentile(duration, 0.95),
        "duration_max_ms": max(duration),
        "rows_per_second": rows / (sum(duration) / 1000) if sum(duration) else None,
        "overhead_p50_ms": percentile([b["trigger_execution_ms"] - (b.get("add_batch_ms") or 0) for b in with_rows], 0.50),
        "state_rows_max": max((b.get("state_rows_total") or 0 for b in with_rows), default=None),
    }
    for column in (*SPARK_PHASES, *STAGE_COLUMNS):
        step[f"{column.removeprefix('stage_')}_p50"] = percentile([b.get(column) for b in with_rows], 0.50)
    staged = [b for b in with_rows if all(b.get(c) is not None for c in STAGE_COLUMNS) and b.get("add_batch_ms") is not None]
    step["audit_other_ms_p50"] = percentile([b["add_batch_ms"] - sum(b[c] for c in STAGE_COLUMNS) for b in staged], 0.50)
    return step


def capacity_at_trigger(fixed_ms: float | None, per_row_ms: float | None, trigger_seconds: float) -> float | None:
    """Rows per second the speed query keeps up with when a batch must finish
    within one trigger: ``fixed + K * per_row <= trigger``, so ``K / trigger``."""
    if fixed_ms is None or not per_row_ms or per_row_ms <= 0:
        return None
    rows = (trigger_seconds * 1000 - fixed_ms) / per_row_ms
    return max(rows, 0.0) / trigger_seconds


def cost_metrics(steps: list[dict[str, Any]], batches: list[dict[str, Any]],
                 triggers: Sequence[float] = (2, 5, 10, 30)) -> dict[str, Any]:
    """One fit over the steps' medians: duration against records.

    Medians, not every batch: Spark times a batch by the wall clock, and a
    step of the Docker VM's clock (seen: +30 s) lands whole in one batch's
    duration while the stage columns, timed on the monotonic clock, do not
    move. One such batch would otherwise drag the fit."""
    fit = fit_line([(s["records_p50"], s["duration_p50_ms"]) for s in steps])
    collect = fit_line([(s["records_p50"], s["collect_ms_p50"]) for s in steps if s.get("collect_ms_p50") is not None])
    metrics: dict[str, Any] = {
        "fixed_cost_ms": fit["fixed"], "per_row_cost_ms": fit["per_unit"], "fit_r2": fit["r2"],
        "fit_steps": fit["points"], "fit_batches": len(batches), "collect_fixed_ms": collect["fixed"], "collect_per_row_ms": collect["per_unit"],
        "saturated_rows_per_second": max((s["rows_per_second"] or 0 for s in steps), default=None),
        "steps": steps,
    }
    for trigger in triggers:
        metrics[f"capacity_at_trigger_{trigger:g}s_rows_per_second"] = capacity_at_trigger(
            fit["fixed"], fit["per_unit"], trigger)
    for step in steps:
        for key in ("duration_p50_ms", "duration_p95_ms", "rows_per_second", "add_batch_ms_p50", "overhead_p50_ms",
                    "clients_ms_p50", "collect_ms_p50", "kafka_ms_p50", "es_ms_p50", "redis_ms_p50", "audit_other_ms_p50"):
            metrics[f"k{step['size']}_{key}"] = step.get(key)
    return metrics


def _window_of(at: datetime, start: datetime, window_seconds: int) -> int:
    return int((at - start).total_seconds() // window_seconds)


def soak_metrics(batches: list[dict[str, Any]], progress: list[dict[str, Any]], resources: list[dict[str, Any]], *,
                 start: datetime, window_seconds: int, trigger_seconds: float, sent: int, send_seconds: float,
                 drain_seconds: float | None, failed_batches: int, speed_container: str) -> dict[str, Any]:
    """A long run cut into windows of ``window_seconds``.

    Stable means: it drained, no batch failed, and neither the batch duration
    p50 nor the speed container's memory grew by more than 25 % from the first
    full window to the last. The thresholds are stated in the result."""
    windows: dict[int, dict[str, list]] = {}
    for p in progress:
        if (p.get("num_input_rows") or 0) > 0 and p.get("trigger_execution_ms") is not None:
            w = windows.setdefault(_window_of(p["progress_at"], start, window_seconds), {"d": [], "rows": [], "lat50": [],
                                                                                         "lat95": [], "mem": []})
            w["d"].append(p["trigger_execution_ms"])
    # Rows from the audit: Spark's numInputRows counts each record twice (see COST_SQL).
    for b in batches:
        if (b.get("input_rows") or 0) > 0 and b.get("latency_p50_ms") is not None and b.get("completed_at"):
            w = windows.get(_window_of(b["completed_at"], start, window_seconds))
            if w is not None:
                w["rows"].append(b["input_rows"])
                w["lat50"].append(b["latency_p50_ms"])
                w["lat95"].append(b["latency_p95_ms"])
    for r in resources:
        if r["name"] == speed_container and r.get("at"):
            w = windows.get(_window_of(r["at"], start, window_seconds))
            if w is not None:
                w["mem"].append(r["mem_mib"])
    rows = []
    for index in sorted(windows):
        w = windows[index]
        rows.append({"window": index, "batches": len(w["d"]), "rows": sum(w["rows"]),
                     "duration_p50_ms": percentile(w["d"], 0.50), "duration_p95_ms": percentile(w["d"], 0.95),
                     "over_trigger": sum(d > trigger_seconds * 1000 for d in w["d"]),
                     "latency_p50_median_ms": spread(w["lat50"])["median"],
                     "latency_p95_max_ms": max(w["lat95"], default=None),
                     "speed_mem_max_mib": max(w["mem"], default=None)})
    # The last window is usually cut short by the end of sending; compare
    # the first and the last *full* ones.
    full = rows[:-1] if len(rows) > 2 else rows

    def drift(key: str) -> float | None:
        if len(full) < 2 or not full[0][key] or full[-1][key] is None:
            return None
        return full[-1][key] / full[0][key] - 1

    durations = [p["trigger_execution_ms"] for p in progress if (p.get("num_input_rows") or 0) > 0]
    latencies = [b["latency_p95_ms"] for b in batches if b.get("latency_p95_ms") is not None]
    duration_drift, memory_drift = drift("duration_p50_ms"), drift("speed_mem_max_mib")
    limit = 0.25
    stable = (drain_seconds is not None and failed_batches == 0
              and (duration_drift is None or duration_drift <= limit)
              and (memory_drift is None or memory_drift <= limit))
    return {
        "sent": sent, "offered_rate": sent / send_seconds if send_seconds > 0 else None,
        "drain_seconds": drain_seconds, "kept_up": drain_seconds is not None, "failed_batches": failed_batches,
        "batches": len(durations), "batch_duration_p50_ms": percentile(durations, 0.50),
        "batch_duration_p95_ms": percentile(durations, 0.95), "batch_duration_max_ms": max(durations, default=None),
        "batches_over_trigger": sum(d > trigger_seconds * 1000 for d in durations),
        "latency_batch_p95_max_ms": max(latencies, default=None),
        "duration_p50_drift": duration_drift, "speed_mem_drift": memory_drift,
        "drift_limit": limit, "stable": stable, "windows": rows,
    }


_UNITS = {"B": 1 / 2**20, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "TiB": 1024**2,
          "kB": 1000 / 2**20, "MB": 1e6 / 2**20, "GB": 1e9 / 2**20}


def parse_stats_line(line: str) -> dict[str, Any] | None:
    """One ``docker stats --format '{{json .}}'`` line."""
    try:
        row = json.loads(line)
        used = row["MemUsage"].split("/")[0].strip()
        number = "".join(ch for ch in used if ch.isdigit() or ch == ".")
        unit = used[len(number):].strip()
        return {"name": row["Name"], "cpu_pct": float(row["CPUPerc"].rstrip("%")),
                "mem_mib": round(float(number) * _UNITS[unit], 1)}
    except (ValueError, KeyError, json.JSONDecodeError):
        return None


def render_report(results: list[dict[str, Any]]) -> str:
    """One Markdown table per scenario, the dataset label in every row.

    Refuses to put two dataset labels in one table: a replayed number must
    never sit unlabelled beside a live one (Brief section 23)."""
    lines: list[str] = []
    for scenario in sorted({r["scenario"] for r in results}):
        rows = [r for r in results if r["scenario"] == scenario]
        labels = {r["dataset"] for r in rows}
        if len(labels) != 1:
            raise BenchFailed(f"{scenario}: results carry several dataset labels {sorted(labels)}; report them apart")
        lines += [f"## {scenario}", "", "| dataset | variant | metric | median | min | max | runs |",
                  "|---|---|---|---:|---:|---:|---:|"]
        for r in rows:
            for metric, s in sorted(r["summary"].items()):
                cells = ["" if s[k] is None else f"{s[k]:.4g}" for k in ("median", "min", "max")]
                lines.append(f"| {r['dataset']} | {r.get('variant', '')} | {metric} | {' | '.join(cells)} | {s['runs']} |")
        lines.append("")
    return "\n".join(lines)


def refuse_live() -> None:
    project = os.environ.get("COMPOSE_PROJECT_NAME", "")
    if project == LIVE_PROJECT or not os.environ.get("MP_CONTAINER_PREFIX"):
        raise BenchFailed("benchmarks run only in an isolated project: use .\\scripts\\mp.ps1 -EnvFile env/bench.env bench ...")


# -- the stack ---------------------------------------------------------------

def _run(args: list[str], *, env: dict | None = None, check: bool = True, timeout: float | None = None):
    result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, env={**os.environ, **(env or {})},
                            timeout=timeout)
    if check and result.returncode != 0:
        raise BenchFailed(f"{' '.join(args)} failed ({result.returncode}): {result.stderr.strip()[-500:]}")
    return result


def container(service: str) -> str:
    return os.environ.get("MP_CONTAINER_PREFIX", "") + service


def environment() -> dict[str, Any]:
    """What the numbers were measured on."""
    info = json.loads(_run(["docker", "info", "--format", "{{json .}}"]).stdout)
    head = _run(["git", "rev-parse", "HEAD"]).stdout.strip()
    dirty = bool(_run(["git", "status", "--porcelain"]).stdout.strip())
    images = {}
    for image in ("ecommerce/marketplace-python:1", "ecommerce/spark-marketplace:4.0.1"):
        out = _run(["docker", "image", "inspect", "-f", "{{.Id}}", image], check=False).stdout.strip()
        images[image] = out or None
    live = _run(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={LIVE_PROJECT}"], check=False)
    return {
        "host_platform": platform.platform(), "host_cpu_count": os.cpu_count(), "host_python": sys.version.split()[0],
        "docker_server": info.get("ServerVersion"), "docker_os": info.get("OperatingSystem"),
        "docker_kernel": info.get("KernelVersion"), "docker_cpus": info.get("NCPU"),
        "docker_mem_bytes": info.get("MemTotal"), "images": images, "git_head": head, "git_dirty": dirty,
        # The live collection stack shares this machine; say so next to every number.
        "live_stack_running_containers": len(live.stdout.split()),
        "measured_at": datetime.now(timezone.utc).isoformat(),
    }


class ResourceSampler:
    def __init__(self, interval: float = 5.0) -> None:
        self.interval, self.samples, self._stop = interval, [], threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self) -> None:
        prefix = os.environ.get("MP_CONTAINER_PREFIX", "")
        while not self._stop.is_set():
            out = _run(["docker", "stats", "--no-stream", "--format", "{{json .}}"], check=False, timeout=60).stdout
            at = datetime.now(timezone.utc)
            for line in out.splitlines():
                row = parse_stats_line(line)
                if row and row["name"].startswith(prefix):
                    self.samples.append({**row, "at": at})
            self._stop.wait(self.interval)

    def __enter__(self) -> "ResourceSampler":
        self._thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._stop.set()
        self._thread.join(timeout=90)


@dataclass
class Stack:
    """A fresh isolated project per run."""

    env: dict[str, str] = field(default_factory=dict)

    def compose(self, *args: str, check: bool = True, timeout: float | None = None):
        return _run(COMPOSE + ["--profile", "*", *args], env=self.env, check=check, timeout=timeout)

    def fresh(self, *services: str) -> None:
        self.compose("down", "--volumes", "--remove-orphans", timeout=600)
        self.compose("up", "-d", "--build", *CORE, *services, timeout=900)
        for name in (*CORE, *services):
            self.wait_healthy(name)
        self.ops("migrate")

    def down(self) -> None:
        self.compose("down", "--volumes", "--remove-orphans", check=False, timeout=600)

    def wait_healthy(self, service: str, timeout: float = 300) -> None:
        deadline = time.monotonic() + timeout
        while True:
            out = _run(["docker", "inspect", "-f", "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                        container(service)], check=False).stdout.strip()
            if out in ("healthy", "running"):
                return
            if time.monotonic() > deadline:
                raise BenchFailed(f"{service} not healthy after {timeout:.0f}s: {out or 'absent'}")
            time.sleep(3)

    def ops(self, *args: str) -> str:
        return _run(COMPOSE + ["--profile", "ops", "run", "--rm", "--no-deps", "ops", "python", "-m", "ops", *args],
                    env=self.env).stdout

    def query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        from common.postgres import postgres_connection_factory

        with postgres_connection_factory()() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]

    def db_now(self) -> datetime:
        return self.query("SELECT now() AS now")[0]["now"]


def wait_until(check: Callable[[], bool], *, timeout: float, what: str, poll: float = 1.0) -> float:
    began = time.monotonic()
    while not check():
        if time.monotonic() - began > timeout:
            raise BenchFailed(f"timed out after {timeout:.0f}s waiting for {what}")
        time.sleep(poll)
    return time.monotonic() - began


# -- scenarios ---------------------------------------------------------------

def _send_load(*, seed: int, count: int, offers: int, rate: float, cadence_seconds: int = 60) -> tuple[int, float]:
    """Fixture load whose rounds all lie in the past, so a batch at `now` sees them."""
    from data_ingestion.marketplace_producer import create_marketplace_producer
    from ops import bench_load

    rounds = math.ceil(count / offers)
    start = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(seconds=rounds * cadence_seconds)
    spec = bench_load.LoadSpec(seed=seed, start=start, count=count, offers=offers, cadence_seconds=cadence_seconds)
    producer = create_marketplace_producer()
    began = time.monotonic()
    try:
        sent = bench_load.send(bench_load.fixture_events(spec, lambda: datetime.now(timezone.utc)), producer,
                               bench_load.Pacer(rate))
    finally:
        producer.close()
    return sent, time.monotonic() - began


def _silver_lag() -> int:
    from config.settings import KAFKA_SILVER_CONSUMER_GROUP
    from config.topics import MARKETPLACE_OBSERVATIONS
    from ops.validate import LiveSources

    return LiveSources().consumer_lag(KAFKA_SILVER_CONSUMER_GROUP, MARKETPLACE_OBSERVATIONS.name)


def _silver_latency_sample(limit: int = 300) -> list[tuple[datetime, datetime]]:
    from common.object_store import _client
    from config.settings import MARKETPLACE_SILVER_DATASET, MINIO_BUCKET_SILVER
    from config.storage import active_profile

    client = _client(active_profile())
    objects = sorted(client.list_objects(MINIO_BUCKET_SILVER, prefix=MARKETPLACE_SILVER_DATASET.strip("/") + "/",
                                         recursive=True), key=lambda o: o.object_name)
    step = max(1, len(objects) // limit)
    pairs = []
    for obj in objects[::step][:limit]:
        response = client.get_object(MINIO_BUCKET_SILVER, obj.object_name)
        try:
            produced = datetime.fromisoformat(json.loads(response.read())["produced_at"].replace("Z", "+00:00"))
        finally:
            response.close()
            response.release_conn()
        pairs.append((obj.last_modified, produced))
    return pairs


def run_ingest(stack: Stack, *, count: int, seed: int) -> dict[str, Any]:
    stack.fresh("silver-sink")
    began = time.monotonic()
    sent, send_seconds = _send_load(seed=seed, count=count, offers=1000, rate=0)
    wait_until(lambda: _silver_lag() == 0, timeout=3600, what="the Silver sink to drain", poll=1)
    total = time.monotonic() - began
    return {"sent": sent, "send_seconds": round(send_seconds, 3), "total_seconds": round(total, 3),
            "produce_per_second": sent / send_seconds, "silver_records_per_second": sent / total,
            **silver_latency_metrics(_silver_latency_sample())}


def run_batch(stack: Stack, *, count: int, seed: int) -> dict[str, Any]:
    ingest = run_ingest(stack, count=count, seed=seed)
    as_of = stack.db_now().astimezone(timezone.utc).replace(microsecond=0)
    run_id = f"bench-{count}-{as_of:%Y%m%dT%H%M%SZ}"
    _run(COMPOSE + ["--profile", "batch", "run", "--rm", "--no-deps", "batch-once", "python3", "-m",
                    "batch_layer.marketplace_warehouse", "--run-id", run_id, "--as-of", as_of.isoformat()],
         env=stack.env, check=False, timeout=7200)
    rows = stack.query("SELECT status, quality_status, started_at, completed_at, silver_rows, gold_rows, dataset_counts "
                       "FROM audit.marketplace_batch_run WHERE run_id = %s", (run_id,))
    if not rows or rows[0]["completed_at"] is None:
        raise BenchFailed(f"batch {run_id} did not finish: {rows}")
    row = rows[0]
    seconds = (row["completed_at"] - row["started_at"]).total_seconds()
    return {"silver_rows": row["silver_rows"], "gold_rows": row["gold_rows"], "seconds": round(seconds, 3),
            "silver_rows_per_second": (row["silver_rows"] or 0) / seconds if seconds else None,
            "status": row["status"], "quality_status": row["quality_status"],
            "ingest_silver_records_per_second": ingest["silver_records_per_second"]}


def run_speed(stack: Stack, *, rate: float, seconds: int, seed: int) -> dict[str, Any]:
    stack.fresh("speed")
    wait_until(lambda: bool(stack.query("SELECT 1 FROM audit.marketplace_stream_progress LIMIT 1")),
               timeout=600, what="the speed query's first micro-batch", poll=5)
    since = stack.db_now()
    sent, send_seconds = _send_load(seed=seed, count=int(rate * seconds), offers=max(1000, int(rate * 60)), rate=rate)

    def applied() -> int:
        row = stack.query("SELECT coalesce(sum(input_rows), 0) AS n FROM audit.marketplace_speed_batch "
                          "WHERE started_at >= %s AND status = 'SUCCEEDED'", (since,))[0]
        return int(row["n"])

    try:
        drain = wait_until(lambda: applied() >= sent, timeout=max(300, seconds * 2), what="the speed layer to catch up",
                           poll=2)
    except BenchFailed:
        drain = None
    batches = stack.query("SELECT * FROM audit.marketplace_speed_batch WHERE started_at >= %s AND status = 'SUCCEEDED'",
                          (since,))
    progress = stack.query("SELECT * FROM audit.marketplace_stream_progress WHERE progress_at >= %s", (since,))
    return speed_metrics(batches, progress, sent=sent, send_seconds=send_seconds, drain_seconds=drain)


# Spark's numInputRows counts the Kafka source once per branch that reads it
# (valid and invalid are unioned back), so it reports twice the records. The
# speed audit's input_rows has one output per record read: that is the size.
COST_SQL = ("SELECT p.batch_id, p.progress_at, b.input_rows AS records, p.trigger_execution_ms, "
            + ", ".join(f"p.{c}" for c in SPARK_PHASES) + ", p.state_rows_total, "
            + ", ".join(f"b.{c}" for c in STAGE_COLUMNS)
            + " FROM audit.marketplace_stream_progress p LEFT JOIN audit.marketplace_speed_batch b"
            " USING (query_name, query_id, batch_id) WHERE p.batch_id > %s AND b.input_rows > 0"
            " ORDER BY p.batch_id")


_TRIGGER_UNITS = {"millisecond": 0.001, "second": 1, "minute": 60}


def trigger_seconds(trigger: str) -> float:
    """``"30 seconds"``, ``"500 milliseconds"``, ``"1 minute"`` -> seconds."""
    number, _, unit = trigger.strip().partition(" ")
    unit = unit.strip().lower().removesuffix("s")
    if unit not in _TRIGGER_UNITS:
        raise ValueError(f"unknown trigger unit in {trigger!r}")
    return float(number) * _TRIGGER_UNITS[unit]


def _wait_first_progress(stack: Stack) -> None:
    wait_until(lambda: bool(stack.query("SELECT 1 FROM audit.marketplace_stream_progress LIMIT 1")),
               timeout=600, what="the speed query's first micro-batch", poll=5)


def run_speed_cost(stack: Stack, *, sizes: list[int], batches: int, warmup: int, seed: int) -> dict[str, Any]:
    """Drain one preloaded backlog at each batch size K in turn.

    The speed service restarts for every K, so the first ``warmup`` batches
    after each start are dropped: the first one is the batch the previous K
    left planned in the write-ahead log, and the JVM and Python workers are
    still cold. The trigger is zero: a batch starts as soon as the last ends,
    so the step measures the batch and never the wait for a trigger."""
    stack.env.update({"MARKETPLACE_STREAM_TRIGGER": "0 seconds"})
    stack.fresh()
    total = sum(sizes) * (warmup + batches + 1)
    # At most 60 one-minute rounds per offer: event times stay within the hour
    # before now, far inside the six-hour stale timeout.
    sent, send_seconds = _send_load(seed=seed, count=total, offers=max(1000, math.ceil(total / 60)), rate=0)
    steps, measured = [], []
    for size in sizes:
        stack.env["MARKETPLACE_SPEED_MAX_OFFSETS_PER_TRIGGER"] = str(size)
        last = stack.query("SELECT coalesce(max(batch_id), -1) AS b FROM audit.marketplace_stream_progress")[0]["b"]
        stack.compose("up", "-d", "--no-deps", "--force-recreate", "speed", timeout=600)
        wanted = warmup + batches
        wait_until(lambda: len(stack.query(COST_SQL, (last,))) >= wanted,
                   timeout=max(900, wanted * (10 + size * 0.01)), what=f"{wanted} batches at K={size}", poll=3)
        stack.compose("stop", "speed", timeout=300)
        warm = stack.query(COST_SQL, (last,))[warmup:wanted]
        steps.append(cost_step_metrics(warm, size=size))
        measured += warm
        print(json.dumps({"event": "bench_cost_step", **steps[-1]}, default=str), flush=True)
    return {"preloaded": sent, "preload_seconds": round(send_seconds, 3), "warmup_batches": warmup,
            **cost_metrics(steps, measured)}


def run_speed_soak(stack: Stack, *, rate: float, seconds: int, trigger: str, window: int, seed: int,
                   sampler: ResourceSampler | None = None) -> dict[str, Any]:
    stack.env["MARKETPLACE_STREAM_TRIGGER"] = trigger
    stack.fresh("speed")
    _wait_first_progress(stack)
    since = stack.db_now()
    start = datetime.now(timezone.utc)
    sent, send_seconds = _send_load(seed=seed, count=int(rate * seconds), offers=max(1000, int(rate * 60)), rate=rate)

    def applied() -> int:
        return int(stack.query("SELECT coalesce(sum(input_rows), 0) AS n FROM audit.marketplace_speed_batch "
                               "WHERE started_at >= %s AND status = 'SUCCEEDED'", (since,))[0]["n"])

    try:
        drain = wait_until(lambda: applied() >= sent, timeout=max(600, seconds), what="the speed layer to catch up",
                           poll=5)
    except BenchFailed:
        drain = None
    batches = stack.query("SELECT * FROM audit.marketplace_speed_batch WHERE started_at >= %s AND status = 'SUCCEEDED'",
                          (since,))
    failed = stack.query("SELECT count(*) AS n FROM audit.marketplace_speed_batch WHERE started_at >= %s "
                         "AND status = 'FAILED'", (since,))[0]["n"]
    progress = stack.query("SELECT * FROM audit.marketplace_stream_progress WHERE progress_at >= %s", (since,))
    return soak_metrics(batches, progress, list(sampler.samples) if sampler else [], start=start,
                        window_seconds=window, trigger_seconds=trigger_seconds(trigger), sent=sent,
                        send_seconds=send_seconds, drain_seconds=drain, failed_batches=int(failed),
                        speed_container=container("speed"))


# Pages crawled in a run: a PARTIAL attempt fetched and parsed its page too.
CRAWLED_SQL = ("SELECT count(DISTINCT task_id) AS n FROM audit.crawl_request_attempt "
               "WHERE started_at >= %s AND status IN ('SUCCEEDED', 'PARTIAL')")


def run_crawl(stack: Stack, *, delay: float, categories: int, pages: int, rows_per_page: int) -> dict[str, Any]:
    stack.env.update({"STUB_LAST_PAGE": str(pages), "STUB_ROWS_PER_PAGE": str(rows_per_page),
                      "CRAWL_REQUEST_DELAY_SECONDS": str(delay), "CRAWL_ACTIVE_CADENCE_MINUTES": "60",
                      "TIKI_LISTING_URL": "http://stub-source:8000/api/personalish/v1/blocks/listings"})
    stack.fresh("stub-source")
    targets = [str(9100 + i) for i in range(categories)]
    args = ["seed", "--tier", "ACTIVE", "--pages", str(pages)]
    for target in targets:
        args += ["--category", target]
    stack.ops(*args)
    since = stack.db_now()
    stack.compose("up", "-d", "--no-deps", "crawl-worker")
    expected = categories * pages

    def done() -> bool:
        return stack.query(CRAWLED_SQL, (since,))[0]["n"] >= expected

    wait_until(done, timeout=max(600, expected * (delay + 5) * 2), what=f"{expected} pages crawled", poll=5)
    stack.compose("stop", "crawl-worker", check=False)
    attempts = stack.query("SELECT status, started_at, completed_at, latency_ms, parsed_count, raw_bytes "
                           "FROM audit.crawl_request_attempt WHERE started_at >= %s", (since,))
    return crawl_metrics(attempts)


# -- the command ---------------------------------------------------------------

def variants(scenario: str, args: argparse.Namespace) -> list[tuple[str, dict[str, Any]]]:
    if scenario == "crawl":
        return [(f"delay={d:g}s", {"delay": d, "categories": args.categories, "pages": args.pages,
                                   "rows_per_page": args.rows_per_page}) for d in args.delays]
    if scenario == "ingest":
        return [(f"n={n}", {"count": n}) for n in args.sizes]
    if scenario == "batch":
        return [(f"n={n}", {"count": n}) for n in args.sizes]
    if scenario == "speed-cost":
        return [(f"k={'-'.join(map(str, args.cost_sizes))}",
                 {"sizes": args.cost_sizes, "batches": args.cost_batches, "warmup": args.cost_warmup})]
    if scenario == "speed-soak":
        return [(f"rate={r:g}/s-trigger={args.trigger}", {"rate": r, "seconds": args.soak_seconds,
                                                         "trigger": args.trigger, "window": args.soak_window})
                for r in args.soak_rates]
    return [(f"rate={r:g}/s", {"rate": r, "seconds": args.seconds}) for r in args.rates]


RUNNERS = {"crawl": run_crawl, "ingest": run_ingest, "batch": run_batch, "speed": run_speed,
           "speed-cost": run_speed_cost, "speed-soak": run_speed_soak}


def run_scenario(scenario: str, args: argparse.Namespace) -> list[Path]:
    refuse_live()
    env = environment()
    RESULTS.mkdir(parents=True, exist_ok=True)
    written = []
    for variant, params in variants(scenario, args):
        runs = []
        for repeat in range(args.repeat):
            stack = Stack()
            run_params = dict(params)
            if scenario in ("ingest", "batch", "speed", *EXTRA_SCENARIOS):
                run_params["seed"] = args.seed + repeat
            started = datetime.now(timezone.utc)
            try:
                with ResourceSampler() as sampler:
                    # The soak cuts the resource trace into its windows.
                    extra = {"sampler": sampler} if scenario == "speed-soak" else {}
                    metrics = RUNNERS[scenario](stack, **run_params, **extra)
                runs.append({"repeat": repeat, "started_at": started.isoformat(), "params": run_params,
                             "metrics": metrics, "resources": resource_summary(sampler.samples)})
                print(json.dumps({"event": "bench_run", "scenario": scenario, "variant": variant, "repeat": repeat,
                                  "metrics": metrics}, default=str), flush=True)
            finally:
                if not args.keep:
                    stack.down()
        result = {"scenario": scenario, "variant": variant, "dataset": DATASET, "environment": env,
                  "params": params, "seed": args.seed, "repeat": args.repeat,
                  "single_run": args.repeat == 1, "runs": runs, "summary": summarise_runs(runs)}
        path = RESULTS / f"{scenario}-{variant.replace('/', 'per').replace('=', '-').replace(' ', '')}-{started:%Y%m%dT%H%M%SZ}.json"
        path.write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        written.append(path)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.bench", description="Phase 9 benchmarks (replayed fixtures)")
    parser.add_argument("scenario", choices=[*SCENARIOS, *EXTRA_SCENARIOS, "all", "report"])
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--sizes", type=int, nargs="+", default=[10_000, 50_000, 200_000])
    parser.add_argument("--rates", type=float, nargs="+", default=[50, 200, 1000])
    parser.add_argument("--seconds", type=int, default=300, help="per speed rate step")
    parser.add_argument("--delays", type=float, nargs="+", default=[2.0, 0.0])
    parser.add_argument("--categories", type=int, default=6)
    parser.add_argument("--pages", type=int, default=20)
    parser.add_argument("--rows-per-page", type=int, default=40)
    parser.add_argument("--cost-sizes", type=int, nargs="+", default=[1, 10, 100, 1000, 5000, 15000],
                        help="speed-cost: batch sizes K (maxOffsetsPerTrigger)")
    parser.add_argument("--cost-batches", type=int, default=8, help="speed-cost: warm batches measured per K")
    parser.add_argument("--cost-warmup", type=int, default=2, help="speed-cost: batches dropped after each restart")
    parser.add_argument("--soak-rates", type=float, nargs="+", default=[200])
    parser.add_argument("--soak-seconds", type=int, default=1800)
    parser.add_argument("--soak-window", type=int, default=300, help="speed-soak: seconds per window")
    parser.add_argument("--trigger", default="30 seconds", help="speed-soak: the speed query's trigger")
    parser.add_argument("--keep", action="store_true", help="leave the last project up for inspection")
    args = parser.parse_args(argv)
    if args.repeat < 1:
        parser.error("--repeat must be at least 1")
    if args.scenario == "report":
        results = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(RESULTS.glob("*.json"))]
        print(render_report(results))
        return 0
    try:
        for scenario in (SCENARIOS if args.scenario == "all" else (args.scenario,)):
            for path in run_scenario(scenario, args):
                print(json.dumps({"event": "bench_written", "path": str(path)}), flush=True)
    except BenchFailed as error:
        print(json.dumps({"event": "bench_failed", "error": str(error)}), flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
