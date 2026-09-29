"""Sink and audit tests, items 29-38 of the Phase 5 plan section 13.

Every client here is a fake: no Kafka, Elasticsearch, Redis or PostgreSQL
connection is opened.
"""
import json
from contextlib import contextmanager
from decimal import Decimal

import pytest

from config.marketplace_wire import canonical_json
from config.settings import (
    ES_INDEX_MARKETPLACE_CHANGES,
    ES_INDEX_MARKETPLACE_OFFERS,
    MARKETPLACE_SPEED_QUERY_NAME,
    REDIS_MARKETPLACE_OFFER_TTL_SECONDS,
)
from config.topics import MARKETPLACE_CHANGES
from speed_layer.marketplace_change_rules import (
    detect_observation_changes,
    offer_state_to_json,
)
from speed_layer.marketplace_sinks import (
    BatchCounts,
    MarketplaceSpeedAudit,
    MarketplaceSpeedSinks,
    SpeedOutput,
)
from tests.test_marketplace_change_rules import CONFIG, _event, _later
from tests.test_marketplace_producer import FakeProducer


class FakeElasticsearch:
    def __init__(self, item_status=201, errors=False):
        self.calls = []
        self.item_status = item_status
        self.errors = errors
        self.closed = 0

    def bulk(self, operations=None):
        self.calls.append(operations)
        headers = [op for op in operations if "index" in op and len(op) == 1]
        return {
            "errors": self.errors,
            "items": [{"index": {"status": self.item_status}} for _ in headers],
        }

    def close(self):
        self.closed += 1


class FakePipeline:
    def __init__(self, owner, fail_on_execute=False):
        self.owner = owner
        self.fail_on_execute = fail_on_execute

    def _record(self, *command):
        self.owner.commands.append(command)

    def zadd(self, key, mapping):
        self._record("zadd", key, tuple(sorted(mapping.items())))

    def setex(self, key, ttl, value):
        self._record("setex", key, ttl, value)

    def hset(self, key, mapping=None):
        self._record("hset", key, tuple(sorted(mapping.items())))

    def expire(self, key, ttl):
        self._record("expire", key, ttl)

    def set(self, key, value, nx=False):
        self._record("set", key, value, nx)

    def zremrangebyrank(self, key, start, stop):
        self._record("zremrangebyrank", key, start, stop)

    def execute(self):
        if self.fail_on_execute:
            raise RuntimeError("redis pipeline failed")
        self.owner.executed += 1


class FakeRedis:
    def __init__(self, fail_on_execute=False, stored=None):
        self.commands = []
        self.executed = 0
        self.closed = 0
        self.fail_on_execute = fail_on_execute
        self.stored = stored or {}

    def pipeline(self):
        return FakePipeline(self, self.fail_on_execute)

    def get(self, key):
        return self.stored.get(key)

    def close(self):
        self.closed += 1


class RecordingAudit:
    def __init__(self, begin="RUN"):
        self.begin = begin
        self.calls = []

    def begin_batch(self, *, query_name, batch_id, started_at):
        self.calls.append(("begin", query_name, batch_id))
        return self.begin

    def mark_succeeded(self, *, query_name, batch_id, completed_at, counts):
        self.calls.append(("succeeded", query_name, batch_id, counts))

    def mark_failed(self, *, query_name, batch_id, completed_at, error):
        self.calls.append(("failed", query_name, batch_id, str(error)))

    def kinds(self):
        return [call[0] for call in self.calls]


class FakeCursor:
    def __init__(self, row):
        self.row = row
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.row

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:
    def __init__(self, row):
        self.cur = FakeCursor(row)

    def cursor(self):
        return self.cur

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _connection_factory(row=None):
    connection = FakeConnection(row)

    @contextmanager
    def factory():
        yield connection

    factory.connection = connection
    return factory


def _batch():
    """One NEW_OFFER plus one price change, as the transform would emit them."""
    first = detect_observation_changes(None, _event(), CONFIG)
    second = detect_observation_changes(
        first.next_state, _later(current_price=Decimal("150.00")), CONFIG
    )
    outputs = [
        SpeedOutput(
            output_kind="CHANGE",
            change_id=change.event_id,
            change_json=canonical_json(change),
        )
        for change in (*first.changes, *second.changes)
    ]
    outputs.append(
        SpeedOutput(
            output_kind="STATE",
            offer_id=second.next_state.offer_id,
            state_json=offer_state_to_json(second.next_state),
        )
    )
    outputs.append(SpeedOutput(output_kind="DUPLICATE"))
    changes = (*first.changes, *second.changes)
    return outputs, changes, second.next_state


def _sinks(**overrides):
    parts = {
        "producer": FakeProducer(),
        "es": FakeElasticsearch(),
        "redis": FakeRedis(),
        "audit": RecordingAudit(),
    }
    parts.update(overrides)
    return MarketplaceSpeedSinks(**parts), parts


# 29
def test_kafka_key_and_topic_are_exact():
    outputs, changes, _ = _batch()
    sinks, parts = _sinks()

    sinks.write_batch(outputs, batch_id=1)

    sent = parts["producer"].sent
    assert [row["topic"] for row in sent] == [MARKETPLACE_CHANGES.name] * len(changes)
    assert [row["key"] for row in sent] == [change.offer_id for change in changes]
    assert parts["producer"].flushed == 1


# 30
def test_change_publish_failure_propagates_and_fails_the_audit():
    outputs, _, _ = _batch()
    sinks, parts = _sinks(producer=FakeProducer(error=RuntimeError("broker down")))

    with pytest.raises(RuntimeError, match="broker down"):
        sinks.write_batch(outputs, batch_id=1)

    assert parts["audit"].kinds() == ["begin", "failed"]


# 31
def test_es_ids_are_event_id_and_offer_id_never_random():
    outputs, changes, state = _batch()
    sinks, parts = _sinks()

    sinks.write_batch(outputs, batch_id=1)

    operations = parts["es"].calls[0]
    headers = [op["index"] for op in operations if "index" in op and len(op) == 1]
    change_headers = [h for h in headers if h["_index"] == ES_INDEX_MARKETPLACE_CHANGES]
    offer_headers = [h for h in headers if h["_index"] == ES_INDEX_MARKETPLACE_OFFERS]
    assert [h["_id"] for h in change_headers] == [c.event_id for c in changes]
    assert [h["_id"] for h in offer_headers] == [state.offer_id]


# 32
def test_redis_uses_hashes_and_sorted_sets_with_deterministic_members():
    outputs, changes, state = _batch()
    sinks, parts = _sinks()

    sinks.write_batch(outputs, batch_id=1)

    commands = parts["redis"].commands
    verbs = {command[0] for command in commands}
    assert verbs <= {"zadd", "setex", "hset", "expire", "set", "zremrangebyrank"}
    assert not verbs & {"lpush", "rpush", "sadd"}
    members = [
        member
        for command in commands
        if command[0] == "zadd"
        for member, _ in command[2]
    ]
    assert members == [change.event_id for change in changes]
    hset_keys = [command[1] for command in commands if command[0] == "hset"]
    assert hset_keys == [f"rt:offer:{state.offer_id}"]
    assert ("expire", f"rt:offer:{state.offer_id}", REDIS_MARKETPLACE_OFFER_TTL_SECONDS) in commands
    assert parts["redis"].executed == 1


# 33
def test_replaying_identical_outputs_produces_identical_sink_commands():
    outputs, _, _ = _batch()
    first_sinks, first = _sinks()
    second_sinks, second = _sinks()

    first_sinks.write_batch(outputs, batch_id=1)
    second_sinks.write_batch(outputs, batch_id=1)

    assert first["redis"].commands == second["redis"].commands
    assert first["es"].calls == second["es"].calls
    assert [(row["topic"], row["key"], row["value"]) for row in first["producer"].sent] == [
        (row["topic"], row["key"], row["value"]) for row in second["producer"].sent
    ]


# 34
def test_es_item_failure_prevents_success_audit():
    outputs, _, _ = _batch()
    sinks, parts = _sinks(es=FakeElasticsearch(item_status=409))

    with pytest.raises(RuntimeError, match="Elasticsearch item failure"):
        sinks.write_batch(outputs, batch_id=1)

    assert parts["audit"].kinds() == ["begin", "failed"]
    assert parts["redis"].executed == 0


def test_es_errors_flag_prevents_success_audit():
    outputs, _, _ = _batch()
    sinks, parts = _sinks(es=FakeElasticsearch(errors=True))

    with pytest.raises(RuntimeError, match="Elasticsearch item failure"):
        sinks.write_batch(outputs, batch_id=1)

    assert parts["audit"].kinds() == ["begin", "failed"]


# 35
def test_redis_failure_prevents_success_audit():
    outputs, _, _ = _batch()
    sinks, parts = _sinks(redis=FakeRedis(fail_on_execute=True))

    with pytest.raises(RuntimeError, match="redis pipeline failed"):
        sinks.write_batch(outputs, batch_id=1)

    assert parts["audit"].kinds() == ["begin", "failed"]


def test_a_failed_batch_closes_every_client():
    outputs, _, _ = _batch()
    sinks, parts = _sinks(redis=FakeRedis(fail_on_execute=True))

    with pytest.raises(RuntimeError):
        sinks.write_batch(outputs, batch_id=1)

    assert parts["es"].closed == 1
    assert parts["redis"].closed == 1


# 36
def test_succeeded_batch_audit_causes_a_safe_skip():
    outputs, _, _ = _batch()
    sinks, parts = _sinks(audit=RecordingAudit(begin="SKIP"))

    counts = sinks.write_batch(outputs, batch_id=1)

    assert counts == BatchCounts()
    assert parts["audit"].kinds() == ["begin"]
    assert parts["producer"].sent == []
    assert parts["es"].calls == []
    assert parts["redis"].commands == []


def test_audit_begin_skips_a_succeeded_batch():
    factory = _connection_factory(row=("SUCCEEDED",))
    audit = MarketplaceSpeedAudit(factory)

    decision = audit.begin_batch(
        query_name=MARKETPLACE_SPEED_QUERY_NAME, batch_id=7, started_at=None
    )

    assert decision == "SKIP"
    assert len(factory.connection.cur.executed) == 1


# 37
@pytest.mark.parametrize("row", [None, ("FAILED",), ("RUNNING",)])
def test_failed_or_running_audit_causes_an_idempotent_retry(row):
    factory = _connection_factory(row=row)
    audit = MarketplaceSpeedAudit(factory)

    decision = audit.begin_batch(
        query_name=MARKETPLACE_SPEED_QUERY_NAME, batch_id=7, started_at=None
    )

    insert_sql = factory.connection.cur.executed[1][0]
    assert decision == "RUN"
    assert "ON CONFLICT (query_name,batch_id) DO UPDATE" in insert_sql
    assert "status='RUNNING'" in insert_sql


# 38
def test_audit_errors_are_truncated_and_carry_no_event_body():
    outputs, _, _ = _batch()
    body = outputs[0].change_json
    factory = _connection_factory()
    audit = MarketplaceSpeedAudit(factory)

    audit.mark_failed(
        query_name=MARKETPLACE_SPEED_QUERY_NAME,
        batch_id=7,
        completed_at=None,
        error=RuntimeError(body + "x" * 5000),
    )

    params = factory.connection.cur.executed[0][1]
    message = next(p for p in params if isinstance(p, str) and len(p) > 100)
    assert len(message) == 2000
    assert not any(isinstance(p, str) and p == body for p in params)


def test_audit_success_writes_the_batch_counts():
    factory = _connection_factory()
    audit = MarketplaceSpeedAudit(factory)
    counts = BatchCounts(input_rows=4, change_rows=2, es_rows=3, redis_rows=3)

    audit.mark_succeeded(
        query_name=MARKETPLACE_SPEED_QUERY_NAME,
        batch_id=7,
        completed_at=None,
        counts=counts,
    )

    sql, params = factory.connection.cur.executed[0]
    assert "SUCCEEDED" in params
    assert 4 in params and 2 in params
    assert "error_message=%s" in sql


def test_successful_batch_counts_separate_inputs_from_derived_changes():
    outputs, changes, _ = _batch()
    sinks, parts = _sinks()

    counts = sinks.write_batch(outputs, batch_id=1)

    assert counts.change_rows == len(changes)
    assert counts.kafka_rows == len(changes)
    assert counts.es_rows == len(changes) + 1
    assert counts.redis_rows == len(changes) + 1
    assert counts.input_rows == sum(o.output_kind != "CHANGE" for o in outputs)
    assert counts.applied_rows == 1
    assert counts.duplicate_rows == 1
    assert parts["audit"].kinds() == ["begin", "succeeded"]


def test_change_documents_reach_elasticsearch_as_canonical_wire():
    outputs, changes, _ = _batch()
    sinks, parts = _sinks()

    sinks.write_batch(outputs, batch_id=1)

    operations = parts["es"].calls[0]
    assert operations[1] == json.loads(canonical_json(changes[0]))
