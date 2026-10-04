"""Project operational audit and the DLQ into Elasticsearch — plan section 11.2.

A loop service. Every ``MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS`` it copies
four operational sources into four Elasticsearch indices:

===================================  =================================  ==========================
Target index                         Source                             ``_id``
===================================  =================================  ==========================
``marketplace-source-health-v1``     ``audit.crawl_source_state``       ``marketplace_code``
``marketplace-crawl-attempts-v1``    ``audit.crawl_request_attempt``    ``attempt_id``
``marketplace-speed-batches-v1``     ``audit.marketplace_speed_batch``  ``query_name:query_id:batch_id``
``marketplace-dlq-v1``               the Kafka DLQ topic                ``dlq_id``
===================================  =================================  ==========================

Three properties hold it together:

* **Every ``_id`` is derived from the row's own key**, so a projection is
  idempotent. Re-projecting a row overwrites the document with itself.
* **Each pass re-reads the last ``MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS``.**
  A row that was ``UPDATE``d after it was first projected, and a pass that
  died halfway, are both repaired by the next pass. The watermark is
  in-process state only: on restart the projector resumes from
  ``now - overlap``, and ``--rebuild`` re-indexes everything.
* **It is read-only towards the pipeline.** It takes no lock, writes no
  PostgreSQL row, and the only state it advances is its own DLQ consumer
  offsets — committed after the records are indexed, never before.

Deviation from the plan, deliberate: section 11.2 keys a speed batch
``query_name:batch_id``. A batch's primary key has been
``(query_name, query_id, batch_id)`` since WP2, because batch IDs restart at 0
under a new checkpoint — the very reason the audit table gained ``query_id``.
Keying the document without it would let a replay's batch 0 silently overwrite
the previous run's batch 0, which is the bug WP2 fixed, reintroduced one layer
up. The ``_id`` here matches the primary key.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Protocol, Sequence

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------


class ProjectorSources(Protocol):
    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]: ...
    def redis_get(self, key: str) -> str | None: ...
    def drain_dlq(self, *, from_beginning: bool = False) -> list[dict]: ...
    def commit_dlq(self) -> None: ...


SOURCE_HEALTH_COLUMNS = ("marketplace_code", "consecutive_failures", "opened_until",
                         "last_failure_at", "last_success_at", "updated_at")
SOURCE_HEALTH_SQL = f"SELECT {', '.join(SOURCE_HEALTH_COLUMNS)} FROM audit.crawl_source_state ORDER BY marketplace_code"

CRAWL_ATTEMPT_COLUMNS = ("attempt_id", "crawl_run_id", "task_id", "marketplace_code", "resource_type",
                         "attempt_number", "started_at", "completed_at", "status", "http_status",
                         "latency_ms", "raw_artifact_id", "raw_uri", "raw_bytes", "parsed_count",
                         "rejected_count", "error_kind", "error_message")
# The frontier carries marketplace_code; the attempt does not. (audit.crawl_run
# has no such column either — the Phase 7 bug that two marts were built on.)
_CRAWL_ATTEMPT_SELECT = (
    "SELECT a.attempt_id, a.crawl_run_id, a.task_id, f.marketplace_code, f.resource_type, "
    "a.attempt_number, a.started_at, a.completed_at, a.status, a.http_status, "
    "a.latency_ms, a.raw_artifact_id, a.raw_uri, a.raw_bytes, a.parsed_count, "
    "a.rejected_count, a.error_kind, a.error_message "
    "FROM audit.crawl_request_attempt a JOIN audit.crawl_frontier f ON f.task_id = a.task_id"
)
CRAWL_ATTEMPT_SQL_ALL = _CRAWL_ATTEMPT_SELECT + " ORDER BY a.completed_at, a.attempt_id"
CRAWL_ATTEMPT_SQL_SINCE = _CRAWL_ATTEMPT_SELECT + " WHERE a.completed_at >= %s ORDER BY a.completed_at, a.attempt_id"

SPEED_BATCH_COLUMNS = ("query_name", "query_id", "batch_id", "status", "started_at", "completed_at",
                       "input_rows", "invalid_rows", "applied_rows", "duplicate_rows", "late_rows",
                       "change_rows", "kafka_rows", "es_rows", "redis_rows", "error_message")
_SPEED_BATCH_SELECT = f"SELECT {', '.join(SPEED_BATCH_COLUMNS)} FROM audit.marketplace_speed_batch"
SPEED_BATCH_SQL_ALL = _SPEED_BATCH_SELECT + " ORDER BY started_at, batch_id"
# A RUNNING batch has no completed_at. It is projected anyway, so a stranded
# one is visible; the next pass overwrites that document with its final state.
SPEED_BATCH_SQL_SINCE = _SPEED_BATCH_SELECT + " WHERE completed_at IS NULL OR completed_at >= %s ORDER BY started_at, batch_id"


# ---------------------------------------------------------------------------
# Documents
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Projection:
    """One document and where it goes. ``doc_id`` is derived, never generated."""

    index: str
    doc_id: str
    document: dict


def _instant(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _iso(value: Any) -> str | None:
    moment = _instant(value)
    return None if moment is None else moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _row(columns: Sequence[str], row: Sequence[Any]) -> dict:
    return dict(zip(columns, row))


def source_health_document(row: Sequence[Any], *, last_observation: str | None, now: datetime) -> Projection:
    from config.settings import ES_INDEX_MARKETPLACE_SOURCE_HEALTH

    values = _row(SOURCE_HEALTH_COLUMNS, row)
    opened_until = _instant(values["opened_until"])
    observed = _instant(last_observation)
    document = {
        "marketplace_code": values["marketplace_code"],
        "consecutive_failures": int(values["consecutive_failures"]),
        # audit.crawl_source_state stores no boolean: the circuit is open only
        # while opened_until is still in the future.
        "circuit_open": bool(opened_until is not None and opened_until > now),
        "opened_until": _iso(opened_until),
        "last_failure_at": _iso(values["last_failure_at"]),
        "last_success_at": _iso(values["last_success_at"]),
        "updated_at": _iso(values["updated_at"]),
        "last_observation_at": _iso(observed),
        # now minus the newest observation this source produced, as of this
        # pass. Refreshed every interval, so the panel reads it as "age".
        "freshness_seconds": None if observed is None else int((now - observed).total_seconds()),
        "projected_at": _iso(now),
    }
    return Projection(ES_INDEX_MARKETPLACE_SOURCE_HEALTH, str(values["marketplace_code"]), document)


def crawl_attempt_document(row: Sequence[Any]) -> Projection:
    from config.settings import ES_INDEX_MARKETPLACE_CRAWL_ATTEMPTS

    values = _row(CRAWL_ATTEMPT_COLUMNS, row)
    document = {
        "attempt_id": str(values["attempt_id"]),
        "crawl_run_id": values["crawl_run_id"],
        "task_id": values["task_id"],
        "marketplace_code": values["marketplace_code"],
        "resource_type": values["resource_type"],
        "attempt_number": int(values["attempt_number"]),
        "started_at": _iso(values["started_at"]),
        "completed_at": _iso(values["completed_at"]),
        "status": values["status"],
        "http_status": None if values["http_status"] is None else int(values["http_status"]),
        "latency_ms": None if values["latency_ms"] is None else int(values["latency_ms"]),
        "raw_artifact_id": values["raw_artifact_id"],
        "raw_uri": values["raw_uri"],
        "raw_bytes": int(values["raw_bytes"]),
        "parsed_count": int(values["parsed_count"]),
        "rejected_count": int(values["rejected_count"]),
        "error_kind": values["error_kind"],
        "error_message": values["error_message"],
    }
    return Projection(ES_INDEX_MARKETPLACE_CRAWL_ATTEMPTS, str(values["attempt_id"]), document)


def speed_batch_document(row: Sequence[Any]) -> Projection:
    from config.settings import ES_INDEX_MARKETPLACE_SPEED_BATCHES

    values = _row(SPEED_BATCH_COLUMNS, row)
    started, completed = _instant(values["started_at"]), _instant(values["completed_at"])
    document = {
        "query_name": values["query_name"],
        "query_id": values["query_id"],
        "batch_id": int(values["batch_id"]),
        "status": values["status"],
        "started_at": _iso(started),
        "completed_at": _iso(completed),
        # Micro-batch duration. NOT observation-to-change latency: the change
        # contract carries no processed time, and Phase 8 does not add one.
        "duration_ms": None if completed is None else int((completed - started).total_seconds() * 1000),
        **{name: int(values[name]) for name in (
            "input_rows", "invalid_rows", "applied_rows", "duplicate_rows", "late_rows",
            "change_rows", "kafka_rows", "es_rows", "redis_rows")},
        "error_message": values["error_message"],
    }
    doc_id = f"{values['query_name']}:{values['query_id']}:{values['batch_id']}"
    return Projection(ES_INDEX_MARKETPLACE_SPEED_BATCHES, doc_id, document)


# payload_text is deliberately not projected. It carries up to a megabyte of
# the raw response body; the body belongs in Bronze and in the DLQ topic, not
# in an operational index (Brief section 20, and the same rule the audit
# tables follow). The document carries the coordinates to find it.
DLQ_DOCUMENT_FIELDS = ("dlq_id", "schema_version", "failed_at", "stage", "source_topic",
                       "source_partition", "source_offset", "source_key", "marketplace",
                       "crawl_run_id", "raw_artifact_id", "raw_uri", "error_type", "error_message")


def dlq_document(record: dict) -> Projection:
    from config.settings import ES_INDEX_MARKETPLACE_DLQ

    dlq_id = record.get("dlq_id")
    if not dlq_id:
        raise ValueError("a DLQ record without dlq_id has no deterministic document id")
    document = {name: record.get(name) for name in DLQ_DOCUMENT_FIELDS}
    document["failed_at"] = _iso(record.get("failed_at"))
    for name in ("source_partition", "source_offset"):
        document[name] = None if record.get(name) is None else int(record[name])
    return Projection(ES_INDEX_MARKETPLACE_DLQ, str(dlq_id), document)


def bulk_body(projections: Iterable[Projection]) -> list[dict]:
    """The Elasticsearch bulk operations for a set of projections."""
    body: list[dict] = []
    for projection in projections:
        body.append({"index": {"_index": projection.index, "_id": projection.doc_id}})
        body.append(projection.document)
    return body


# ---------------------------------------------------------------------------
# The pass
# ---------------------------------------------------------------------------


@dataclass
class Projector:
    """One projection pass, and the watermark that survives between passes."""

    sources: ProjectorSources
    bulk: Callable[[list[dict]], None]
    overlap_seconds: int
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    # Set on construction so the first pass re-reads the overlap window, as a
    # restart must: the watermark is in-process state and nothing persists it.
    watermark: datetime | None = None

    def __post_init__(self) -> None:
        if self.overlap_seconds <= 0:
            raise ValueError("overlap_seconds must be positive")
        if self.watermark is None:
            self.watermark = self.clock()

    def _since(self) -> datetime:
        return self.watermark - timedelta(seconds=self.overlap_seconds)

    def project_once(self, *, rebuild: bool = False) -> dict[str, int]:
        now = self.clock()
        since = None if rebuild else self._since()

        health = [
            source_health_document(
                row,
                last_observation=self.sources.redis_get(f"rt:source:{row[0]}:last_observation"),
                now=now,
            )
            for row in self.sources.query(SOURCE_HEALTH_SQL)
        ]
        attempts_rows = (self.sources.query(CRAWL_ATTEMPT_SQL_ALL) if since is None
                         else self.sources.query(CRAWL_ATTEMPT_SQL_SINCE, (since,)))
        attempts = [crawl_attempt_document(row) for row in attempts_rows]
        batch_rows = (self.sources.query(SPEED_BATCH_SQL_ALL) if since is None
                      else self.sources.query(SPEED_BATCH_SQL_SINCE, (since,)))
        batches = [speed_batch_document(row) for row in batch_rows]
        dlq = [dlq_document(record) for record in self.sources.drain_dlq(from_beginning=rebuild)]

        projections = [*health, *attempts, *batches, *dlq]
        if projections:
            self.bulk(bulk_body(projections))
        # Only after the records are in Elasticsearch. A commit before the
        # index would lose a DLQ record on a crash in between.
        if dlq:
            self.sources.commit_dlq()

        # Advance to the newest settled instant this pass actually saw, so the
        # next pass re-reads from there minus the overlap. A pass that saw
        # nothing leaves the watermark where it was rather than skipping
        # forward over rows that had not landed yet.
        seen = [_instant(doc.document["completed_at"]) for doc in (*attempts, *batches)
                if doc.document["completed_at"] is not None]
        if seen:
            self.watermark = max([self.watermark, *seen])
        return {"source_health": len(health), "crawl_attempts": len(attempts),
                "speed_batches": len(batches), "dlq": len(dlq)}


# ---------------------------------------------------------------------------
# Live wiring
# ---------------------------------------------------------------------------


class LiveProjectorSources:
    """PostgreSQL, Redis and the Kafka DLQ topic, built lazily in the container."""

    def __init__(self) -> None:
        self._connection_factory = None
        self._redis = None
        self._consumer = None
        self._rewound = False

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        if self._connection_factory is None:
            from common.postgres import postgres_connection_factory

            self._connection_factory = postgres_connection_factory()
        with self._connection_factory() as conn, conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return list(cur.fetchall())

    def redis_get(self, key: str) -> str | None:
        if self._redis is None:
            from redis import Redis

            from config.settings import REDIS_DB, REDIS_HOST, REDIS_PORT

            self._redis = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        value = self._redis.get(key)
        return value.decode("utf-8") if isinstance(value, bytes) else value

    def _dlq_consumer(self):
        if self._consumer is None:
            from kafka import KafkaConsumer

            from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_DLQ_PROJECTOR_GROUP
            from config.topics import MARKETPLACE_OBSERVATIONS_DLQ

            self._consumer = KafkaConsumer(
                MARKETPLACE_OBSERVATIONS_DLQ.name,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_DLQ_PROJECTOR_GROUP,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
        return self._consumer

    def drain_dlq(self, *, from_beginning: bool = False) -> list[dict]:
        consumer = self._dlq_consumer()
        if from_beginning and not self._rewound:
            # seek needs an assignment, and the group gets one only after a poll.
            consumer.poll(timeout_ms=2000)
            consumer.seek_to_beginning()
            self._rewound = True
        records: list[dict] = []
        while True:
            batch = consumer.poll(timeout_ms=1000)
            if not batch:
                return records
            records.extend(json.loads(record.value) for messages in batch.values() for record in messages)

    def commit_dlq(self) -> None:
        if self._consumer is not None:
            self._consumer.commit()

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
        if self._redis is not None:
            self._redis.close()


def elasticsearch_bulk(es: Any) -> Callable[[list[dict]], None]:
    """A bulk writer that raises on an item failure instead of counting it green."""

    def write(operations: list[dict]) -> None:
        result = es.bulk(operations=operations)
        failed = [detail for item in result.get("items", []) for detail in item.values()
                  if detail.get("error") or detail.get("status", 200) >= 300]
        if result.get("errors") or failed:
            error = (failed[0].get("error") if failed else None) or {}
            reason = f"{error.get('type', '')}: {error.get('reason', '')}" if isinstance(error, dict) else str(error)
            raise RuntimeError(f"Elasticsearch item failure ({len(failed)} item(s)): {reason}"[:1000])

    return write


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Project marketplace audit and DLQ into Elasticsearch")
    parser.add_argument("--once", action="store_true", help="one pass, then exit")
    parser.add_argument("--rebuild", action="store_true", help="re-index everything on the first pass")
    parser.add_argument("--interval", type=float, default=None,
                        help="seconds between passes (default MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS)")
    args = parser.parse_args(argv)

    from elasticsearch import Elasticsearch

    from common.heartbeat import heartbeat
    from common.lifecycle import StopSignal, install_signal_handlers
    from config.settings import (
        ES_HOST, MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS, MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS,
        SERVICE_HEARTBEAT_FILE,
    )
    from display.kibana.marketplace_index_templates import ensure_projector_indices, install_index_templates

    interval = MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS if args.interval is None else args.interval
    stop = StopSignal()
    install_signal_handlers(stop)
    beat = heartbeat(SERVICE_HEARTBEAT_FILE)
    es = Elasticsearch(ES_HOST)
    sources = LiveProjectorSources()
    # The templates must exist before the first document creates the index,
    # and the indices before a dashboard panel asks Elasticsearch about them.
    install_index_templates(es)
    ensure_projector_indices(es)
    projector = Projector(sources=sources, bulk=elasticsearch_bulk(es),
                          overlap_seconds=MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS)
    rebuild = args.rebuild
    try:
        while True:
            try:
                counts = projector.project_once(rebuild=rebuild)
                print(json.dumps({"event": "projected", **counts}, sort_keys=True), flush=True)
                rebuild = False
            except Exception as error:
                if args.once:
                    raise
                # The overlap means the next pass re-reads whatever this one
                # missed, so an outage costs freshness, never a document.
                print(json.dumps({"event": "projection_failed",
                                  "error": f"{type(error).__name__}: {error}"[:500]}, sort_keys=True), flush=True)
            beat()
            if args.once or stop.wait(interval):
                break
    finally:
        sources.close()
        es.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
