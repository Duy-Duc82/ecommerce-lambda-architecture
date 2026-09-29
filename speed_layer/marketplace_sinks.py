"""Idempotent marketplace speed projections and micro-batch audit."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable
import json

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


def _epoch_micros(dt: datetime) -> int:
    delta = dt.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


class MarketplaceSpeedAudit:
    def __init__(self, connection_factory: Callable[[], Any]): self.connection_factory = connection_factory

    def begin_batch(self, *, query_name: str, batch_id: int, started_at: datetime) -> str:
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("SELECT status FROM audit.marketplace_speed_batch WHERE query_name=%s AND batch_id=%s", (query_name, batch_id))
            row = cur.fetchone() if hasattr(cur, "fetchone") else None
            if row and row[0] == "SUCCEEDED": return "SKIP"
            cur.execute("""INSERT INTO audit.marketplace_speed_batch(query_name,batch_id,status,started_at)
                VALUES (%s,%s,'RUNNING',%s) ON CONFLICT (query_name,batch_id) DO UPDATE SET status='RUNNING',started_at=EXCLUDED.started_at,completed_at=NULL,error_message=NULL""", (query_name, batch_id, started_at))
        return "RUN"

    def mark_succeeded(self, *, query_name: str, batch_id: int, completed_at: datetime, counts: BatchCounts) -> None:
        self._update(query_name, batch_id, "SUCCEEDED", completed_at, counts, None)

    def mark_failed(self, *, query_name: str, batch_id: int, completed_at: datetime, error: Exception) -> None:
        self._update(query_name, batch_id, "FAILED", completed_at, None, str(error)[:2000])

    def _update(self, query_name, batch_id, status, completed_at, counts, error):
        counts = counts or BatchCounts()
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("""UPDATE audit.marketplace_speed_batch SET status=%s,completed_at=%s,
                input_rows=%s,invalid_rows=%s,applied_rows=%s,duplicate_rows=%s,late_rows=%s,
                change_rows=%s,kafka_rows=%s,es_rows=%s,redis_rows=%s,error_message=%s
                WHERE query_name=%s AND batch_id=%s""", (status, completed_at, counts.input_rows, counts.invalid_rows, counts.applied_rows, counts.duplicate_rows, counts.late_rows, counts.change_rows, counts.kafka_rows, counts.es_rows, counts.redis_rows, error, query_name, batch_id))


class MarketplaceSpeedSinks:
    def __init__(self, *, producer: Any, es: Any, redis: Any, audit: Any):
        self.producer, self.es, self.redis, self.audit = producer, es, redis, audit

    def write_batch(self, outputs: Iterable[SpeedOutput], batch_id: int) -> BatchCounts:
        rows = list(outputs)
        started = datetime.now(timezone.utc)
        query_name = MARKETPLACE_SPEED_QUERY_NAME
        if self.audit.begin_batch(query_name=query_name, batch_id=batch_id, started_at=started) == "SKIP": return BatchCounts()
        counts = BatchCounts(input_rows=len(rows), invalid_rows=sum(r.output_kind == "INVALID" for r in rows), applied_rows=sum(r.output_kind == "STATE" for r in rows), duplicate_rows=sum(r.output_kind == "DUPLICATE" for r in rows), late_rows=sum(r.output_kind == "LATE" for r in rows))
        try:
            changes = [marketplace_change_from_wire(json.loads(r.change_json)) for r in rows if r.output_kind == "CHANGE" and r.change_json]
            for event in changes: publish_change(self.producer, event)
            if hasattr(self.producer, "flush"): self.producer.flush()
            es_actions = []
            change_rows = [r for r in rows if r.output_kind == "CHANGE" and r.change_json]
            for row, event in zip(change_rows, changes):
                doc = json.loads(row.change_json)
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
                if result.get("errors") or any((detail.get("error") or detail.get("status", 200) >= 300) for item in result.get("items", []) for detail in item.values()):
                    raise RuntimeError("Elasticsearch item failure")
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
            counts = BatchCounts(**{**counts.__dict__, "input_rows": sum(r.output_kind != "CHANGE" for r in rows), "change_rows": len(changes), "kafka_rows": len(changes), "es_rows": len(changes) + len(states), "redis_rows": len(changes) + len(states)})
            self.audit.mark_succeeded(query_name=query_name, batch_id=batch_id, completed_at=datetime.now(timezone.utc), counts=counts)
            return counts
        except Exception as exc:
            try: self.audit.mark_failed(query_name=query_name, batch_id=batch_id, completed_at=datetime.now(timezone.utc), error=exc)
            finally:
                for client in (self.producer, self.es, self.redis):
                    if hasattr(client, "close"):
                        try: client.close()
                        except Exception: pass
            raise
