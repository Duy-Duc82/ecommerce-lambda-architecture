"""What each store holds, once a day — Phase 9 plan section 6.3 (feeds P2-04).

One snapshot is a set of ``(component, scope, bytes, objects)`` rows:

- ``minio``: per bucket and top-level prefix, summed from a listing. The
  prefix stops at the first ``key=value`` partition, so a scope is a dataset,
  not a day.
- ``postgres``: per schema, ``pg_total_relation_size`` summed over its
  tables. ``objects`` is the statistics collector's live-row count, an
  estimate, not an exact count.
- ``elasticsearch``: per index, the total store size and the document count.
  Indices whose name starts with ``.`` belong to Elasticsearch and Kibana
  themselves, and are left out.
- ``kafka``: per topic, the disk space its partitions' log directories occupy, read
  from the broker's data volume mounted read-only at ``/kafka-data``. Kafka 4
  dropped the DescribeLogDirs versions kafka-python speaks (KIP-896), so the
  files are the one source left. ``objects`` is the partition count.

A snapshot only reads, so it is allowed on the live stack. It is keyed on
the UTC date: a second snapshot on the same day writes nothing. A store that
cannot be read is reported, and the others are still written.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

COMPONENTS = ("minio", "postgres", "elasticsearch", "kafka")
SCOPE_DEPTH = 2


@dataclass(frozen=True)
class Measure:
    component: str
    scope: str
    bytes: int
    objects: int | None


def minio_scope(bucket: str, key: str) -> str:
    """``bucket/first/second``, cut before the first partition component."""
    parts = []
    for part in key.split("/")[:-1][:SCOPE_DEPTH]:
        if "=" in part:
            break
        parts.append(part)
    return "/".join([bucket, *parts])


def measure_minio(listings: Mapping[str, Iterable[tuple[str, int]]]) -> list[Measure]:
    """``listings`` maps a bucket to its ``(object key, size)`` pairs."""
    totals: dict[str, list[int]] = {}
    for bucket, objects in listings.items():
        for key, size in objects:
            total = totals.setdefault(minio_scope(bucket, key), [0, 0])
            total[0] += int(size or 0)
            total[1] += 1
    return [Measure("minio", scope, b, n) for scope, (b, n) in sorted(totals.items())]


POSTGRES_SQL = """
SELECT n.nspname, coalesce(sum(pg_total_relation_size(c.oid)), 0), coalesce(sum(s.n_live_tup), 0)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE c.relkind IN ('r', 'm', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_toast%'
GROUP BY n.nspname ORDER BY n.nspname
"""


def measure_postgres(rows: Iterable[tuple[str, Any, Any]]) -> list[Measure]:
    return [Measure("postgres", schema, int(size), int(estimate)) for schema, size, estimate in rows]


def measure_elasticsearch(stats: Mapping[str, Any]) -> list[Measure]:
    """``stats`` is the body of ``GET _stats/store,docs``."""
    measures = []
    for index, body in sorted((stats.get("indices") or {}).items()):
        if index.startswith("."):
            continue
        total = body.get("total") or {}
        measures.append(Measure("elasticsearch", index, int((total.get("store") or {}).get("size_in_bytes") or 0),
                                int((total.get("docs") or {}).get("count") or 0)))
    return measures


_PARTITION_DIR = re.compile(r"^(?P<topic>.+)-(?P<partition>\d+)$")


def measure_kafka(partition_dirs: Iterable[tuple[str, int]]) -> list[Measure]:
    """``partition_dirs`` holds ``(directory name, bytes in it)`` for each entry
    of the broker's log dir. A partition directory is ``<topic>-<n>``; anything
    else -- checkpoint files, a partition marked ``.delete`` or ``-future`` --
    is not a live partition and is left out."""
    totals: dict[str, list[int]] = {}
    for name, size in partition_dirs:
        match = _PARTITION_DIR.match(name)
        if not match or "." in name.rsplit("-", 1)[1]:
            continue
        total = totals.setdefault(match.group("topic"), [0, 0])
        total[0] += int(size)
        total[1] += 1
    return [Measure("kafka", topic, b, n) for topic, (b, n) in sorted(totals.items())]


def allocated_bytes(path: Path) -> int:
    """Bytes the file occupies on disk. Kafka preallocates every segment's
    ``.index`` and ``.timeindex`` at 10 MiB as sparse files, so their apparent
    size would count about 20 MiB per partition that the disk does not hold.
    ``st_blocks`` is in 512-byte units on Linux; elsewhere the apparent size
    is the best there is."""
    stat = path.stat()
    blocks = getattr(stat, "st_blocks", None)
    return blocks * 512 if blocks is not None else stat.st_size


def kafka_partition_dirs(root: Path, size: Callable[[Path], int] = allocated_bytes) -> list[tuple[str, int]]:
    return [(entry.name, sum(size(f) for f in entry.rglob("*") if f.is_file()))
            for entry in sorted(root.iterdir()) if entry.is_dir()]


INSERT_SQL = ("INSERT INTO audit.storage_snapshot (snapshot_date, captured_at, component, scope, bytes, objects) "
              "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (snapshot_date, component, scope) DO NOTHING")


def take_snapshot(collectors: Mapping[str, Callable[[], list[Measure]]], connection_factory: Callable[[], Any],
                  *, now: datetime) -> dict[str, Any]:
    """Run every collector, write what they measured, and report both."""
    measures: list[Measure] = []
    failed: dict[str, str] = {}
    for component, collect in collectors.items():
        try:
            measures.extend(collect())
        except Exception as error:  # noqa: BLE001 - one unreadable store must not cost the others' row
            failed[component] = f"{type(error).__name__}: {error}"[:300]
    snapshot_date: date = now.astimezone(timezone.utc).date()
    written = 0
    with connection_factory() as conn, conn.cursor() as cur:
        for m in measures:
            cur.execute(INSERT_SQL, (snapshot_date, now, m.component, m.scope, m.bytes, m.objects))
            written += max(getattr(cur, "rowcount", 0) or 0, 0)
    return {"event": "storage_snapshot", "snapshot_date": snapshot_date.isoformat(), "measured": len(measures),
            "written": written, "failed": failed}


# -- the live stores; nothing below runs in the default suite --

def live_collectors() -> dict[str, Callable[[], list[Measure]]]:
    from config.settings import ES_HOST, MINIO_BUCKETS

    def minio() -> list[Measure]:
        from common.object_store import _client
        from config.storage import active_profile

        client = _client(active_profile())
        listings = {}
        for bucket in sorted(set(MINIO_BUCKETS.values())):
            if client.bucket_exists(bucket):
                listings[bucket] = [(o.object_name, o.size) for o in client.list_objects(bucket, recursive=True)]
        return measure_minio(listings)

    def postgres() -> list[Measure]:
        from common.postgres import postgres_connection_factory

        with postgres_connection_factory()() as conn, conn.cursor() as cur:
            cur.execute(POSTGRES_SQL)
            return measure_postgres(cur.fetchall())

    def elasticsearch() -> list[Measure]:
        from elasticsearch import Elasticsearch

        es = Elasticsearch(ES_HOST)
        try:
            return measure_elasticsearch(dict(es.indices.stats(metric="store,docs")))
        finally:
            es.close()

    def kafka() -> list[Measure]:
        root = Path(os.environ.get("KAFKA_DATA_MOUNT", "/kafka-data"))
        if not root.is_dir():
            raise FileNotFoundError(f"{root} is not mounted; only the ops and es-projector containers mount it")
        return measure_kafka(kafka_partition_dirs(root))

    return {"minio": minio, "postgres": postgres, "elasticsearch": elasticsearch, "kafka": kafka}


class DailySnapshot:
    """At most one snapshot attempt per UTC date per process; the table's key
    makes a second one on the same date (after a restart) write nothing."""

    def __init__(self, take: Callable[[datetime], dict[str, Any]]) -> None:
        self.take = take
        self.last: date | None = None

    def maybe(self, now: datetime) -> dict[str, Any] | None:
        today = now.astimezone(timezone.utc).date()
        if self.last == today:
            return None
        report = self.take(now)
        # A snapshot every store failed is retried on the next pass.
        if report.get("measured"):
            self.last = today
        return report


def run_snapshot(now: datetime | None = None) -> dict[str, Any]:
    from common.postgres import postgres_connection_factory

    report = take_snapshot(live_collectors(), postgres_connection_factory(), now=now or datetime.now(timezone.utc))
    print(json.dumps(report, sort_keys=True), flush=True)
    return report


# -- P2-04: the growth report (Phase 9 plan section 10) ------------------------

SILVER_SCOPE = "ecommerce-silver/marketplace/offer_observations"
# Brief section 5's planning table: monitored offers and cadence over 60 days.
BRIEF_ROWS = ((1_000, 4 * 3600), (1_000, 3600), (5_000, 3600), (5_000, 1800))


def layers(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    """One snapshot's rows folded into the layers the report speaks of."""
    out = {"bronze": 0, "silver": 0, "gold": 0, "quarantine": 0, "cache": 0, "audit": 0,
           "elasticsearch": 0, "kafka": 0, "silver_observations": 0}
    for r in rows:
        c, scope, size = r["component"], r["scope"], int(r["bytes"])
        if c == "minio":
            if scope.startswith("ecommerce-bronze"):
                out["bronze"] += size
            elif scope == SILVER_SCOPE:
                out["silver"] += size
                out["silver_observations"] += int(r["objects"] or 0)
            elif scope.startswith("ecommerce-silver") and "quarantine" in scope:
                out["quarantine"] += size
            elif scope.startswith("ecommerce-gold"):
                out["gold"] += size
        elif c == "postgres" and scope in ("cache", "audit"):
            out[scope] += size
        elif c in ("elasticsearch", "kafka"):
            out[c] += size
    return out


STORED = ("bronze", "silver", "gold", "quarantine", "cache", "audit", "elasticsearch", "kafka")


def growth_report(snapshots: Iterable[Mapping[str, Any]], *, free_bytes: int | None = None) -> dict[str, Any]:
    by_date: dict[Any, list[Mapping[str, Any]]] = {}
    for row in snapshots:
        by_date.setdefault(row["snapshot_date"], []).append(row)
    dates = sorted(by_date)
    if len(dates) < 2:
        raise ValueError(f"a growth report needs snapshots on two dates at least; found {len(dates)}")
    series = {d: layers(by_date[d]) for d in dates}
    first, last = series[dates[0]], series[dates[-1]]
    days = (dates[-1] - dates[0]).days
    observations = last["silver_observations"]
    per_day = {k: (last[k] - first[k]) / days for k in (*STORED, "silver_observations")}
    total_per_day = sum(per_day[k] for k in STORED)
    per_observation = {k: (last[k] / observations if observations else None) for k in STORED}
    bytes_per_observation = sum(v for v in per_observation.values() if v is not None) if observations else None

    def projected_bytes(at_day: int) -> int | None:
        if bytes_per_observation is None:
            return None
        obs = observations + per_day["silver_observations"] * max(at_day - days, 0)
        return round(obs * bytes_per_observation)

    return {
        "report": "storage_growth", "dataset": "live",
        "window": {"first": dates[0].isoformat(), "last": dates[-1].isoformat(), "days": days, "snapshots": len(dates)},
        "per_date": {d.isoformat(): s for d, s in series.items()},
        "growth_bytes_per_day": {k: round(v) for k, v in per_day.items()},
        "growth_total_bytes_per_day": round(total_per_day),
        "bytes_per_observation": {k: (round(v, 1) if v is not None else None) for k, v in per_observation.items()},
        "bytes_per_observation_total": round(bytes_per_observation, 1) if bytes_per_observation else None,
        "ratios": {"bronze_to_silver": round(last["bronze"] / last["silver"], 3) if last["silver"] else None,
                   "silver_to_gold": round(last["silver"] / last["gold"], 3) if last["gold"] else None},
        "projection_note": "a projection from measured bytes per observation, not a measurement",
        "projection_total_bytes": {f"day_{n}": projected_bytes(n) for n in (30, 45, 60)},
        "projection_brief_rows_60_days": [
            {"offers": offers, "cadence_seconds": cadence, "observations": offers * (86400 // cadence) * 60,
             "bytes": round(offers * (86400 // cadence) * 60 * bytes_per_observation) if bytes_per_observation else None}
            for offers, cadence in BRIEF_ROWS],
        "disk": {"free_bytes": free_bytes,
                 "days_until_full": round(free_bytes / total_per_day, 1) if free_bytes and total_per_day > 0 else None,
                 "measured_on": "the Docker volume disk, as seen from the kafka_data mount"},
    }


def growth_markdown(report: Mapping[str, Any]) -> str:
    w = report["window"]
    lines = [f"# storage_growth ({report['dataset']})", "",
             f"Snapshots {w['first']} to {w['last']} ({w['days']} days, {w['snapshots']} snapshots).", "",
             "| layer | bytes/day | bytes/observation |", "|---|---:|---:|"]
    for layer in STORED:
        lines.append(f"| {layer} | {report['growth_bytes_per_day'][layer]} | {report['bytes_per_observation'][layer]} |")
    lines += ["", f"Projection ({report['projection_note']}): " +
              ", ".join(f"{k} {v} B" for k, v in report["projection_total_bytes"].items()), ""]
    return "\n".join(lines) + "\n"


def run_growth_report(out_dir: str) -> int:
    import shutil

    from common.postgres import postgres_connection_factory

    with postgres_connection_factory()() as conn, conn.cursor() as cur:
        cur.execute("SELECT snapshot_date, component, scope, bytes, objects FROM audit.storage_snapshot")
        rows = [dict(zip(("snapshot_date", "component", "scope", "bytes", "objects"), r)) for r in cur.fetchall()]
    mount = Path(os.environ.get("KAFKA_DATA_MOUNT", "/kafka-data"))
    free = shutil.disk_usage(mount).free if mount.is_dir() else None
    try:
        report = growth_report(rows, free_bytes=free)
    except ValueError as error:
        print(json.dumps({"event": "evaluation_refused", "reason": str(error)}), flush=True)
        return 2
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (out / f"storage-{stamp}.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out / f"storage-{stamp}.md").write_text(growth_markdown(report), encoding="utf-8")
    print(json.dumps({"event": "evaluation_written", "kind": "storage", "window": report["window"]}), flush=True)
    return 0
