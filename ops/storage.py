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
