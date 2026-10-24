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

ROOT = Path(__file__).resolve().parents[3]
RESULTS = ROOT / "data" / "ops" / "bench"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "docker-compose.yml")]
LIVE_PROJECT = "mp-live"
DATASET = "replayed_fixture"
SCENARIOS = ("crawl", "ingest", "speed", "batch")
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


def clock_offset_seconds(host_before: datetime, vm: datetime, host_after: datetime) -> float:
    """Docker VM clock minus host clock, from one VM reading bracketed by two
    host readings (the midpoint is the host instant of the VM reading)."""
    midpoint = host_before + (host_after - host_before) / 2
    return (vm - midpoint).total_seconds()


def clock_fields(start_offset: float, end_offset: float, *, step_threshold: float = 1.0) -> dict[str, Any]:
    """The offset applied to a run's cross-clock latencies, and how much it moved.

    The mean of the two readings is applied. A move larger than
    ``step_threshold`` seconds means the VM clock stepped during the run, so
    latencies sampled on either side of the step are off by up to the step."""
    return {"clock_offset_ms": round((start_offset + end_offset) / 2 * 1000, 1),
            "clock_offset_start_ms": round(start_offset * 1000, 1), "clock_offset_end_ms": round(end_offset * 1000, 1),
            "clock_step_suspected": abs(end_offset - start_offset) > step_threshold}


def silver_latency_metrics(samples: list[tuple[datetime, datetime]], *, clock_offset_seconds: float = 0.0) -> dict[str, Any]:
    """``(LastModified, produced_at)`` pairs, in milliseconds. LastModified has
    one-second resolution, so these are accurate to a second at best.

    LastModified is MinIO's, so the Docker VM's clock, while the bench stamps
    ``produced_at`` on the host; ``clock_offset_seconds`` (VM minus host) is
    subtracted to put both on the host's clock."""
    values = [(written - produced).total_seconds() * 1000 - clock_offset_seconds * 1000 for written, produced in samples]
    return {"silver_latency_p50_ms": percentile(values, 0.50), "silver_latency_p95_ms": percentile(values, 0.95),
            "silver_latency_max_ms": max(values) if values else None, "silver_latency_samples": len(values),
            "silver_latency_resolution_ms": 1000}


def speed_metrics(batches: list[dict[str, Any]], progress: list[dict[str, Any]], *, sent: int,
                  send_seconds: float, drain_seconds: float | None, clock_offset_seconds: float = 0.0) -> dict[str, Any]:
    """From ``marketplace_speed_batch`` and ``marketplace_stream_progress`` rows
    of one rate step.

    Latency percentiles do not compose across batches, so the step reports
    the median of the batches' p50 and the worst batch's p95 and max, each
    weighted by nothing and named for what it is.

    The audit's latency is the speed service's completion time (Docker VM
    clock) minus ``produced_at``, which the bench stamps on the host; the VM
    minus host offset is subtracted from each. Batch durations need no
    correction: Spark times both ends on the VM clock, though a clock step
    inside a batch still lands in its duration."""
    shift = clock_offset_seconds * 1000

    def corrected(value):
        return None if value is None else value - shift

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
        "latency_batch_p50_median_ms": corrected(spread(b.get("latency_p50_ms") for b in with_rows)["median"]),
        "latency_batch_p95_max_ms": corrected(max((b["latency_p95_ms"] for b in with_rows if b.get("latency_p95_ms") is not None), default=None)),
        "latency_max_ms": corrected(max((b["latency_max_ms"] for b in with_rows if b.get("latency_max_ms") is not None), default=None)),
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
            variant = r.get("variant", "")
            if r.get("failed"):
                variant += f" (failed after {r.get('completed_runs', len(r.get('runs', [])))} of {r.get('repeat')} runs)"
            for metric, s in sorted(r["summary"].items()):
                cells = ["" if s[k] is None else f"{s[k]:.4g}" for k in ("median", "min", "max")]
                lines.append(f"| {r['dataset']} | {variant} | {metric} | {' | '.join(cells)} | {s['runs']} |")
        gates = {run["metrics"].get("quality_gate") for r in rows for run in r.get("runs", [])} - {None, "passed"}
        if gates:
            lines += ["", f"Quality gate: {', '.join(sorted(gates))} (the replayed load has no crawl audit)."]
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
            for line in out.splitlines():
                row = parse_stats_line(line)
                if row and row["name"].startswith(prefix):
                    self.samples.append(row)
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
        return _run(COMPOSE + ["--profile", "ops", "run", "--rm", "--no-deps", "ops", "python", "-m", "ecomdata.ops", *args],
                    env=self.env).stdout

    def query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        from ecomdata.infra.postgres import postgres_connection_factory

        with postgres_connection_factory()() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]

    def db_now(self) -> datetime:
        return self.query("SELECT now() AS now")[0]["now"]

    def clock_offset(self) -> float:
        """Docker VM clock minus host clock, in seconds. PostgreSQL runs in the
        VM, and every container shares the VM's kernel clock."""
        self.query("SELECT 1")  # connect first, so the bracket times only the query
        before = datetime.now(timezone.utc)
        vm = self.query("SELECT clock_timestamp() AS t")[0]["t"]
        return clock_offset_seconds(before, vm, datetime.now(timezone.utc))


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
    from ecomdata.ingestion.publisher import create_marketplace_producer
    from ecomdata.ops import bench_load

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
    from ecomdata.settings import KAFKA_SILVER_CONSUMER_GROUP
    from ecomdata.domain.topics import MARKETPLACE_OBSERVATIONS
    from ecomdata.ops.validate import LiveSources

    return LiveSources().consumer_lag(KAFKA_SILVER_CONSUMER_GROUP, MARKETPLACE_OBSERVATIONS.name)


def _silver_latency_sample(limit: int = 300) -> list[tuple[datetime, datetime]]:
    from ecomdata.infra.object_store import _client
    from ecomdata.settings import MARKETPLACE_SILVER_DATASET, MINIO_BUCKET_SILVER
    from ecomdata.infra.storage import active_profile

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
    offset_start = stack.clock_offset()
    began = time.monotonic()
    sent, send_seconds = _send_load(seed=seed, count=count, offers=1000, rate=0)
    wait_until(lambda: _silver_lag() == 0, timeout=3600, what="the Silver sink to drain", poll=1)
    total = time.monotonic() - began
    clock = clock_fields(offset_start, stack.clock_offset())
    return {"sent": sent, "send_seconds": round(send_seconds, 3), "total_seconds": round(total, 3),
            "produce_per_second": sent / send_seconds, "silver_records_per_second": sent / total,
            **silver_latency_metrics(_silver_latency_sample(), clock_offset_seconds=clock["clock_offset_ms"] / 1000),
            **clock}


def run_batch(stack: Stack, *, count: int, seed: int) -> dict[str, Any]:
    ingest = run_ingest(stack, count=count, seed=seed)
    as_of = stack.db_now().astimezone(timezone.utc).replace(microsecond=0)
    run_id = f"bench-{count}-{as_of:%Y%m%dT%H%M%SZ}"
    _run(COMPOSE + ["--profile", "batch", "run", "--rm", "--no-deps", "batch-once", "python3", "-m",
                    "ecomdata.processing.batch.warehouse", "--run-id", run_id, "--as-of", as_of.isoformat()],
         env=stack.env, check=False, timeout=7200)
    rows = stack.query("SELECT status, quality_status, started_at, completed_at, silver_rows, gold_rows, dataset_counts "
                       "FROM audit.marketplace_batch_run WHERE run_id = %s", (run_id,))
    failing = [r["check_name"] for r in stack.query(
        "SELECT check_name FROM audit.marketplace_quality_result "
        "WHERE run_id = %s AND severity = 'MANDATORY' AND status <> 'PASS'", (run_id,))]
    return {**batch_outcome(run_id, rows[0] if rows else None, failing),
            "ingest_silver_records_per_second": ingest["silver_records_per_second"]}


# The replayed load comes from the generator, not the crawler, so it leaves no
# crawl attempt to reconcile Silver against, and this mandatory rule fails on
# every replayed batch. Any other failing check, or a crash, fails the run.
REPLAY_EXPECTED_FAILURES = frozenset({"silver_parse_attempt_reconciliation"})


def batch_outcome(run_id: str, row: dict[str, Any] | None, failing_checks: Sequence[str]) -> dict[str, Any]:
    """Metrics of one batch run, or ``BenchFailed`` when its time is not a
    batch's time: the job did not finish, crashed, or failed a quality check
    the replayed dataset does not explain."""
    if row is None or row["completed_at"] is None:
        raise BenchFailed(f"batch {run_id} did not finish: {row}")
    failing = sorted(failing_checks)
    if row["status"] == "SUCCEEDED":
        gate = "passed"
    elif row["status"] == "QUALITY_FAILED" and failing and set(failing) <= REPLAY_EXPECTED_FAILURES:
        gate = "failed_as_expected_on_replayed_data"
    else:
        raise BenchFailed(f"batch {run_id} ended {row['status']} with failing mandatory checks {failing}")
    seconds = (row["completed_at"] - row["started_at"]).total_seconds()
    return {"silver_rows": row["silver_rows"], "gold_rows": row["gold_rows"], "seconds": round(seconds, 3),
            "silver_rows_per_second": (row["silver_rows"] or 0) / seconds if seconds else None,
            "status": row["status"], "quality_status": row["quality_status"], "quality_gate": gate,
            "quality_failing_checks": failing}


def run_speed(stack: Stack, *, rate: float, seconds: int, seed: int) -> dict[str, Any]:
    stack.fresh("speed")
    wait_until(lambda: bool(stack.query("SELECT 1 FROM audit.marketplace_stream_progress LIMIT 1")),
               timeout=600, what="the speed query's first micro-batch", poll=5)
    since = stack.db_now()
    offset_start = stack.clock_offset()
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
    clock = clock_fields(offset_start, stack.clock_offset())
    return {**speed_metrics(batches, progress, sent=sent, send_seconds=send_seconds, drain_seconds=drain,
                            clock_offset_seconds=clock["clock_offset_ms"] / 1000), **clock}


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
    return [(f"rate={r:g}/s", {"rate": r, "seconds": args.seconds}) for r in args.rates]


RUNNERS = {"crawl": run_crawl, "ingest": run_ingest, "batch": run_batch, "speed": run_speed}


def variant_result(*, scenario: str, variant: str, env: dict[str, Any], params: dict[str, Any], seed: int,
                   repeat: int, runs: list[dict[str, Any]], failure: str | None) -> dict[str, Any]:
    """One variant's file. A variant whose repeat failed keeps the runs it
    finished, marked ``failed``; its summary covers only those runs."""
    return {"scenario": scenario, "variant": variant, "dataset": DATASET, "environment": env,
            "params": params, "seed": seed, "repeat": repeat, "single_run": repeat == 1,
            "runs": runs, "completed_runs": len(runs), "failed": failure is not None, "failure": failure,
            "summary": summarise_runs(runs)}


def run_scenario(scenario: str, args: argparse.Namespace) -> tuple[list[Path], list[str]]:
    """Every variant of a scenario. A failed repeat ends its variant, which is
    still written with the runs already done, and the next variant goes on:
    one bad run must not throw away hours of finished ones."""
    refuse_live()
    env = environment()
    RESULTS.mkdir(parents=True, exist_ok=True)
    written, failures = [], []
    for variant, params in variants(scenario, args):
        runs, failure = [], None
        started = datetime.now(timezone.utc)
        for repeat in range(args.repeat):
            stack = Stack()
            run_params = dict(params)
            if scenario in ("ingest", "batch", "speed"):
                run_params["seed"] = args.seed + repeat
            run_started = datetime.now(timezone.utc)
            try:
                with ResourceSampler() as sampler:
                    metrics = RUNNERS[scenario](stack, **run_params)
                runs.append({"repeat": repeat, "started_at": run_started.isoformat(), "params": run_params,
                             "metrics": metrics, "resources": resource_summary(sampler.samples)})
                print(json.dumps({"event": "bench_run", "scenario": scenario, "variant": variant, "repeat": repeat,
                                  "metrics": metrics}, default=str), flush=True)
            except BenchFailed as error:
                failure = f"repeat {repeat}: {error}"
                print(json.dumps({"event": "bench_failed", "scenario": scenario, "variant": variant,
                                  "error": failure}), flush=True)
            finally:
                if not args.keep:
                    stack.down()
            if failure:
                failures.append(f"{scenario} {variant} {failure}")
                break
        result = variant_result(scenario=scenario, variant=variant, env=env, params=params, seed=args.seed,
                                repeat=args.repeat, runs=runs, failure=failure)
        path = RESULTS / f"{scenario}-{variant.replace('/', 'per').replace('=', '-')}-{started:%Y%m%dT%H%M%SZ}.json"
        path.write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        written.append(path)
    return written, failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ecomdata.ops.bench", description="Phase 9 benchmarks (replayed fixtures)")
    parser.add_argument("scenario", choices=[*SCENARIOS, "all", "report"])
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--sizes", type=int, nargs="+", default=[10_000, 50_000, 200_000])
    parser.add_argument("--rates", type=float, nargs="+", default=[50, 200, 1000])
    parser.add_argument("--seconds", type=int, default=300, help="per speed rate step")
    parser.add_argument("--delays", type=float, nargs="+", default=[2.0, 0.0])
    parser.add_argument("--categories", type=int, default=6)
    parser.add_argument("--pages", type=int, default=20)
    parser.add_argument("--rows-per-page", type=int, default=40)
    parser.add_argument("--keep", action="store_true", help="leave the last project up for inspection")
    args = parser.parse_args(argv)
    if args.repeat < 1:
        parser.error("--repeat must be at least 1")
    if args.scenario == "report":
        results = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(RESULTS.glob("*.json"))]
        print(render_report(results))
        return 0
    failures = []
    try:
        for scenario in (SCENARIOS if args.scenario == "all" else (args.scenario,)):
            paths, failed = run_scenario(scenario, args)
            failures += failed
            for path in paths:
                print(json.dumps({"event": "bench_written", "path": str(path)}), flush=True)
    except BenchFailed as error:  # refused or misconfigured before any run
        print(json.dumps({"event": "bench_failed", "error": str(error)}), flush=True)
        return 1
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
