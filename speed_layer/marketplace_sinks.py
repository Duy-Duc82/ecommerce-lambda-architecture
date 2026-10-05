"""Idempotent marketplace speed projections and micro-batch audit."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable
import json
import math
import time

from config.marketplace_wire import marketplace_change_from_wire
from config.settings import (
    ES_INDEX_MARKETPLACE_CHANGES, ES_INDEX_MARKETPLACE_OFFERS,
    REDIS_MARKETPLACE_OFFER_TTL_SECONDS, REDIS_MARKETPLACE_RECENT_CHANGES_MAX,
    MARKETPLACE_SPEED_QUERY_NAME,
)
from data_ingestion.marketplace_change_producer import publish_change
from speed_layer.marketplace_change_rules import offer_state_from_json


@dataclass(frozen=True)
class SpeedOutput:
    output_kind: str
    marketplace: str | None = None
    offer_id: str | None = None
    observation_id: str | None = None
    change_id: str | None = None
    event_time: datetime | None = None
    state_json: str | None = None
    change_json: str | None = None
    source_topic: str | None = None
    source_partition: int | None = None
    source_offset: int | None = None
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class BatchCounts:
    input_rows: int = 0
    invalid_rows: int = 0
    applied_rows: int = 0
    duplicate_rows: int = 0
    late_rows: int = 0
    change_rows: int = 0
    kafka_rows: int = 0
    es_rows: int = 0
    redis_rows: int = 0


@dataclass(frozen=True)
class BatchLatency:
    """Processing latency of one micro-batch, in milliseconds (Phase 9 plan 6.1).

    Each value is the batch's completion, with every sink written, minus an
    applied observation's ``produced_at``: the instant the crawler sent it to
    Kafka, which kafka-python also stamps as the record's CreateTime. It
    therefore includes the time the record waited for the next trigger.
    """
    p50_ms: int
    p95_ms: int
    max_ms: int


@dataclass(frozen=True)
class BatchStages:
    """Where one micro-batch's time went, in milliseconds (speed-cost benchmark).

    ``clients_ms`` opens the Kafka, Elasticsearch and Redis clients and
    ``collect_ms`` is Spark computing the batch, the stateful comparison
    included, and handing it to the driver; both are measured by
    ``write_marketplace_batch``. The three sink stages are measured here.
    The audit writes themselves are the rest of Spark's ``addBatch``.
    """
    clients_ms: int | None = None
    collect_ms: int | None = None
    kafka_ms: int | None = None
    es_ms: int | None = None
    redis_ms: int | None = None


def _elapsed_ms(began: float) -> int:
    return round((time.monotonic() - began) * 1000)


def _nearest_rank(ordered: list[int], q: float) -> int:
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def batch_latency(produced: Iterable[datetime], completed_at: datetime) -> BatchLatency | None:
    """Exact nearest-rank percentiles; None when nothing was applied."""
    values = sorted(round((completed_at - instant).total_seconds() * 1000) for instant in produced)
    if not values:
        return None
    return BatchLatency(_nearest_rank(values, 0.50), _nearest_rank(values, 0.95), values[-1])


def _epoch_micros(dt: datetime) -> int:
    delta = dt.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


class MarketplaceSpeedAudit:
    def __init__(self, connection_factory: Callable[[], Any]): self.connection_factory = connection_factory

    # A batch is (query_name, query_id, batch_id). Batch IDs restart at 0 under
    # a new checkpoint, so without the query id Spark keeps in that checkpoint
    # a replay's batches would be mistaken for the old run's and skipped.
    def begin_batch(self, *, query_name: str, query_id: str, batch_id: int, started_at: datetime) -> str:
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("SELECT status FROM audit.marketplace_speed_batch WHERE query_name=%s AND query_id=%s AND batch_id=%s", (query_name, query_id, batch_id))
            row = cur.fetchone() if hasattr(cur, "fetchone") else None
            if row and row[0] == "SUCCEEDED": return "SKIP"
            cur.execute("""INSERT INTO audit.marketplace_speed_batch(query_name,query_id,batch_id,status,started_at)
                VALUES (%s,%s,%s,'RUNNING',%s) ON CONFLICT (query_name,query_id,batch_id) DO UPDATE SET status='RUNNING',started_at=EXCLUDED.started_at,completed_at=NULL,error_message=NULL""", (query_name, query_id, batch_id, started_at))
        return "RUN"

    def mark_succeeded(self, *, query_name: str, query_id: str, batch_id: int, completed_at: datetime, counts: BatchCounts,
                       latency: BatchLatency | None = None, stages: BatchStages | None = None) -> None:
        self._update(query_name, query_id, batch_id, "SUCCEEDED", completed_at, counts, None, latency, stages)

    def mark_failed(self, *, query_name: str, query_id: str, batch_id: int, completed_at: datetime, error: Exception) -> None:
        self._update(query_name, query_id, batch_id, "FAILED", completed_at, None, str(error)[:2000], None, None)

    def _update(self, query_name, query_id, batch_id, status, completed_at, counts, error, latency, stages):
        counts = counts or BatchCounts()
        stages = stages or BatchStages()
        p50, p95, top = (latency.p50_ms, latency.p95_ms, latency.max_ms) if latency else (None, None, None)
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("""UPDATE audit.marketplace_speed_batch SET status=%s,completed_at=%s,
                input_rows=%s,invalid_rows=%s,applied_rows=%s,duplicate_rows=%s,late_rows=%s,
                change_rows=%s,kafka_rows=%s,es_rows=%s,redis_rows=%s,error_message=%s,
                stage_clients_ms=%s,stage_collect_ms=%s,stage_kafka_ms=%s,stage_es_ms=%s,stage_redis_ms=%s,
                latency_p50_ms=%s,latency_p95_ms=%s,latency_max_ms=%s
                WHERE query_name=%s AND query_id=%s AND batch_id=%s""", (status, completed_at, counts.input_rows, counts.invalid_rows, counts.applied_rows, counts.duplicate_rows, counts.late_rows, counts.change_rows, counts.kafka_rows, counts.es_rows, counts.redis_rows, error, stages.clients_ms, stages.collect_ms, stages.kafka_ms, stages.es_ms, stages.redis_ms, p50, p95, top, query_name, query_id, batch_id))


def _change_document(change: dict) -> dict:
    """The Elasticsearch projection of one change event.

    ``previous_value`` and ``current_value`` hold a scalar for most change
    types but the whole offer for NEW_OFFER. One Elasticsearch field cannot be
    an object and a scalar, so an object value is stored as its canonical
    JSON string. The Kafka event keeps its own shape; only this projection
    changes.
    """
    from config.marketplace_wire import canonical_json

    doc = dict(change)
    for field in ("previous_value", "current_value"):
        if isinstance(doc.get(field), (dict, list)):
            doc[field] = canonical_json(doc[field])
    return doc


class MarketplaceSpeedSinks:
    def __init__(self, *, producer: Any, es: Any, redis: Any, audit: Any):
        self.producer, self.es, self.redis, self.audit = producer, es, redis, audit

    def write_batch(self, outputs: Iterable[SpeedOutput], batch_id: int, *, query_id: str,
                    stages: BatchStages | None = None) -> BatchCounts:
        # Required, with no default: a missing id is exactly how a replay's
        # batches were once taken for the old run's and skipped.
        if not isinstance(query_id, str) or not query_id.strip(): raise ValueError("query_id is required to identify a micro-batch")
        rows = list(outputs)
        started = datetime.now(timezone.utc)
        query_name = MARKETPLACE_SPEED_QUERY_NAME
        if self.audit.begin_batch(query_name=query_name, query_id=query_id, batch_id=batch_id, started_at=started) == "SKIP": return BatchCounts()
        counts = BatchCounts(input_rows=len(rows), invalid_rows=sum(r.output_kind == "INVALID" for r in rows), applied_rows=sum(r.output_kind == "STATE" for r in rows), duplicate_rows=sum(r.output_kind == "DUPLICATE" for r in rows), late_rows=sum(r.output_kind == "LATE" for r in rows))
        try:
            began = time.monotonic()
            changes = [marketplace_change_from_wire(json.loads(r.change_json)) for r in rows if r.output_kind == "CHANGE" and r.change_json]
            for event in changes: publish_change(self.producer, event)
            if hasattr(self.producer, "flush"): self.producer.flush()
            kafka_ms, began = _elapsed_ms(began), time.monotonic()
            es_actions = []
            change_rows = [r for r in rows if r.output_kind == "CHANGE" and r.change_json]
            for row, event in zip(change_rows, changes):
                doc = _change_document(json.loads(row.change_json))
                es_actions.append({"index": {"_index": ES_INDEX_MARKETPLACE_CHANGES, "_id": event.event_id}})
                es_actions.append(doc)
            state_rows = [r for r in rows if r.output_kind == "STATE" and r.state_json]
            states = [offer_state_from_json(row.state_json) for row in state_rows]
            for row, state in zip(state_rows, states):
                es_actions.append({"index": {"_index": ES_INDEX_MARKETPLACE_OFFERS, "_id": state.offer_id}})
                es_actions.append(json.loads(row.state_json))
            if es_actions:
                try:
                    result = self.es.bulk(operations=es_actions) if hasattr(self.es, "bulk") else self.es.bulk(es_actions)
                except TypeError:
                    result = self.es.bulk(es_actions)
                failed = [detail for item in result.get("items", []) for detail in item.values() if detail.get("error") or detail.get("status", 200) >= 300]
                if result.get("errors") or failed:
                    # The first reason is enough to tell a mapping conflict
                    # from an outage in the audit row and the log.
                    error = (failed[0].get("error") if failed else None) or {}
                    reason = f"{error.get('type', '')}: {error.get('reason', '')}" if isinstance(error, dict) else str(error)
                    raise RuntimeError(f"Elasticsearch item failure ({len(failed)} item(s)): {reason}"[:1000])
            es_ms, began = _elapsed_ms(began), time.monotonic()
            pipe = self.redis.pipeline() if hasattr(self.redis, "pipeline") else self.redis
            for row, event in zip(change_rows, changes):
                wire = json.loads(row.change_json)
                pipe.zadd("rt:changes:recent", {event.event_id: _epoch_micros(event.detected_at)})
                pipe.setex(f"rt:change:{event.event_id}", REDIS_MARKETPLACE_OFFER_TTL_SECONDS, json.dumps(wire, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            for row, state in zip(state_rows, states):
                payload = json.loads(row.state_json)
                pipe.hset(f"rt:offer:{state.offer_id}", mapping={"state_json": json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")), "offer_id": state.offer_id, "marketplace": state.marketplace, "observation_id": state.observation_id})
                pipe.expire(f"rt:offer:{state.offer_id}", REDIS_MARKETPLACE_OFFER_TTL_SECONDS)
                source_key = f"rt:source:{state.marketplace}:last_observation"
                previous = self.redis.get(source_key) if hasattr(self.redis, "get") else None
                if isinstance(previous, bytes): previous = previous.decode("utf-8")
                if previous is None or previous < state.observed_at.isoformat():
                    pipe.set(source_key, state.observed_at.isoformat(), nx=False)
            if changes: pipe.zremrangebyrank("rt:changes:recent", 0, -(REDIS_MARKETPLACE_RECENT_CHANGES_MAX + 1))
            if hasattr(pipe, "execute"): pipe.execute()
            redis_ms = _elapsed_ms(began)
            counts = BatchCounts(**{**counts.__dict__, "input_rows": sum(r.output_kind != "CHANGE" for r in rows), "change_rows": len(changes), "kafka_rows": len(changes), "es_rows": len(changes) + len(states), "redis_rows": len(changes) + len(states)})
            # Measured only now, with every sink written: a batch that fails
            # part-way has no latency to report.
            completed = datetime.now(timezone.utc)
            latency = batch_latency((state.produced_at for state in states), completed)
            measured = BatchStages(**{**(stages or BatchStages()).__dict__, "kafka_ms": kafka_ms, "es_ms": es_ms,
                                      "redis_ms": redis_ms})
            self.audit.mark_succeeded(query_name=query_name, query_id=query_id, batch_id=batch_id, completed_at=completed,
                                      counts=counts, latency=latency, stages=measured)
            return counts
        except Exception as exc:
            try: self.audit.mark_failed(query_name=query_name, query_id=query_id, batch_id=batch_id, completed_at=datetime.now(timezone.utc), error=exc)
            finally:
                for client in (self.producer, self.es, self.redis):
                    if hasattr(client, "close"):
                        try: client.close()
                        except Exception: pass
            raise
