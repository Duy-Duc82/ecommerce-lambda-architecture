"""Sink and audit tests, items 29-38 of the Phase 5 plan section 13.

Every client here is a fake: no Kafka, Elasticsearch, Redis or PostgreSQL
connection is opened.
"""
import json
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
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

# The id Spark keeps in a query's checkpoint; batch IDs are unique only within it.
QUERY_ID = "36f9e1c9-87cc-4f53-97a9-e9f1e14397ec"


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
        self.gets = []

    def pipeline(self):
        return FakePipeline(self, self.fail_on_execute)

    def get(self, key):
        self.gets.append(key)
        return self.stored.get(key)

    def close(self):
        self.closed += 1


class RecordingAudit:
    def __init__(self, begin="RUN"):
        self.begin = begin
        self.calls = []

    def begin_batch(self, *, query_name, query_id, batch_id, started_at):
        self.calls.append(("begin", query_name, batch_id))
        self.query_ids = getattr(self, "query_ids", []) + [query_id]
        return self.begin

    def mark_succeeded(self, *, query_name, query_id, batch_id, completed_at, counts, latency=None, stages=None):
        self.calls.append(("succeeded", query_name, batch_id, counts))
        self.latency, self.completed_at, self.stages = latency, completed_at, stages

    def mark_failed(self, *, query_name, query_id, batch_id, completed_at, error):
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

    sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    sent = parts["producer"].sent
    assert [row["topic"] for row in sent] == [MARKETPLACE_CHANGES.name] * len(changes)
    assert [row["key"] for row in sent] == [change.offer_id for change in changes]
    assert parts["producer"].flushed == 1


# 30
def test_change_publish_failure_propagates_and_fails_the_audit():
    outputs, _, _ = _batch()
    sinks, parts = _sinks(producer=FakeProducer(error=RuntimeError("broker down")))

    with pytest.raises(RuntimeError, match="broker down"):
        sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    assert parts["audit"].kinds() == ["begin", "failed"]


# 31
def test_es_ids_are_event_id_and_offer_id_never_random():
    outputs, changes, state = _batch()
    sinks, parts = _sinks()

    sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

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

    sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

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

    first_sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)
    second_sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

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
        sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    assert parts["audit"].kinds() == ["begin", "failed"]
    assert parts["redis"].executed == 0


def test_es_errors_flag_prevents_success_audit():
    outputs, _, _ = _batch()
    sinks, parts = _sinks(es=FakeElasticsearch(errors=True))

    with pytest.raises(RuntimeError, match="Elasticsearch item failure"):
        sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    assert parts["audit"].kinds() == ["begin", "failed"]


# 35
def test_redis_failure_prevents_success_audit():
    outputs, _, _ = _batch()
    sinks, parts = _sinks(redis=FakeRedis(fail_on_execute=True))

    with pytest.raises(RuntimeError, match="redis pipeline failed"):
        sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    assert parts["audit"].kinds() == ["begin", "failed"]


def test_a_failed_batch_closes_every_client():
    outputs, _, _ = _batch()
    sinks, parts = _sinks(redis=FakeRedis(fail_on_execute=True))

    with pytest.raises(RuntimeError):
        sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    assert parts["es"].closed == 1
    assert parts["redis"].closed == 1


# 36
def test_succeeded_batch_audit_causes_a_safe_skip():
    outputs, _, _ = _batch()
    sinks, parts = _sinks(audit=RecordingAudit(begin="SKIP"))

    counts = sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    assert counts == BatchCounts()
    assert parts["audit"].kinds() == ["begin"]
    assert parts["producer"].sent == []
    assert parts["es"].calls == []
    assert parts["redis"].commands == []


def test_audit_begin_skips_a_succeeded_batch():
    factory = _connection_factory(row=("SUCCEEDED",))
    audit = MarketplaceSpeedAudit(factory)

    decision = audit.begin_batch(
        query_name=MARKETPLACE_SPEED_QUERY_NAME, query_id=QUERY_ID, batch_id=7, started_at=None
    )

    assert decision == "SKIP"
    assert len(factory.connection.cur.executed) == 1


# 37
@pytest.mark.parametrize("row", [None, ("FAILED",), ("RUNNING",)])
def test_failed_or_running_audit_causes_an_idempotent_retry(row):
    factory = _connection_factory(row=row)
    audit = MarketplaceSpeedAudit(factory)

    decision = audit.begin_batch(
        query_name=MARKETPLACE_SPEED_QUERY_NAME, query_id=QUERY_ID, batch_id=7, started_at=None
    )

    insert_sql = factory.connection.cur.executed[1][0]
    assert decision == "RUN"
    assert "ON CONFLICT (query_name,query_id,batch_id) DO UPDATE" in insert_sql
    assert "status='RUNNING'" in insert_sql


# 38
def test_audit_errors_are_truncated_and_carry_no_event_body():
    outputs, _, _ = _batch()
    body = outputs[0].change_json
    factory = _connection_factory()
    audit = MarketplaceSpeedAudit(factory)

    audit.mark_failed(
        query_name=MARKETPLACE_SPEED_QUERY_NAME, query_id=QUERY_ID,
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
        query_name=MARKETPLACE_SPEED_QUERY_NAME, query_id=QUERY_ID,
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

    counts = sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    assert counts.change_rows == len(changes)
    assert counts.kafka_rows == len(changes)
    assert counts.es_rows == len(changes) + 1
    assert counts.redis_rows == len(changes) + 1
    assert counts.input_rows == sum(o.output_kind != "CHANGE" for o in outputs)
    assert counts.applied_rows == 1
    assert counts.duplicate_rows == 1
    assert parts["audit"].kinds() == ["begin", "succeeded"]


def test_change_documents_reach_elasticsearch_as_canonical_wire():
    # The projection may not drift from the Kafka event. The one exception,
    # since Phase 8 WP2: an object-valued previous_value/current_value is
    # stored as its canonical JSON string (see the tests below).
    outputs, changes, _ = _batch()
    sinks, parts = _sinks()

    sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    documents = _change_documents(parts)
    for change, doc in zip(changes, documents):
        wire = json.loads(canonical_json(change))
        expected = {key: canonical_json(value) if key in ("previous_value", "current_value") and isinstance(value, (dict, list)) else value
                    for key, value in wire.items()}
        assert doc == expected
    # A scalar change is the wire, byte for byte.
    assert documents[1] == json.loads(canonical_json(changes[1]))


# ----------------------------------------------------------------------------
# Found by the first real price change through the speed query (Phase 8 WP2,
# 2026-10-02). A NEW_OFFER change carries the whole offer as current_value,
# a PRICE_CHANGED carries a scalar. One Elasticsearch field cannot be both:
# once the first NEW_OFFER mapped current_value as an object, every later
# scalar change was refused and the query died. The fake above accepts any
# document, so the conflict never showed. The Kafka contract keeps both
# shapes; only the Elasticsearch projection changes.
# ----------------------------------------------------------------------------
def _change_documents(parts):
    (operations,) = parts["es"].calls
    pairs = list(zip(operations[::2], operations[1::2]))
    return [doc for header, doc in pairs if header["index"]["_index"] == ES_INDEX_MARKETPLACE_CHANGES]


def test_change_values_reach_elasticsearch_as_scalars_only():
    outputs, changes, _ = _batch()
    sinks, parts = _sinks()

    sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    for doc in _change_documents(parts):
        for field in ("previous_value", "current_value"):
            assert not isinstance(doc[field], (dict, list)), (doc["change_type"], field)


def test_an_object_value_is_kept_as_its_canonical_json():
    outputs, changes, _ = _batch()
    sinks, parts = _sinks()

    sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    new_offer = next(doc for doc in _change_documents(parts) if doc["change_type"] == "NEW_OFFER")
    original = json.loads(canonical_json(changes[0]))["current_value"]
    assert isinstance(original, dict)
    assert new_offer["current_value"] == canonical_json(original)
    assert json.loads(new_offer["current_value"]) == original


def test_an_elasticsearch_item_failure_names_its_reason():
    class RefusingElasticsearch(FakeElasticsearch):
        def bulk(self, operations=None):
            self.calls.append(operations)
            return {"errors": True, "items": [{"index": {"status": 400, "error": {
                "type": "document_parsing_exception", "reason": "object mapping for [current_value]"}}}]}

    outputs, _, _ = _batch()
    sinks, parts = _sinks(es=RefusingElasticsearch())

    with pytest.raises(RuntimeError, match="Elasticsearch item failure.*document_parsing_exception.*current_value"):
        sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)



# ----------------------------------------------------------------------------
# Found by replaying the speed query under a new checkpoint (Phase 8 WP2,
# 2026-10-02). A new checkpoint numbers its batches from 0 again, but the
# audit keyed on (query_name, batch_id) and skipped any SUCCEEDED pair. The
# replay's batch 0 — all sixteen observations — was skipped, its offsets were
# committed, and Elasticsearch, Redis and the change topic never saw it. The
# streaming query id, kept in the checkpoint, is what makes a batch unique.
# ----------------------------------------------------------------------------
def test_the_audit_keys_a_batch_by_its_query_id_as_well_as_its_number():
    factory = _connection_factory(row=None)
    audit = MarketplaceSpeedAudit(factory)

    audit.begin_batch(query_name=MARKETPLACE_SPEED_QUERY_NAME, query_id=QUERY_ID, batch_id=0, started_at=None)

    select_sql, select_params = factory.connection.cur.executed[0]
    assert "query_id=%s" in select_sql
    assert select_params == (MARKETPLACE_SPEED_QUERY_NAME, QUERY_ID, 0)
    insert_sql, insert_params = factory.connection.cur.executed[1]
    assert insert_params[:3] == (MARKETPLACE_SPEED_QUERY_NAME, QUERY_ID, 0)


def test_closing_a_batch_updates_only_that_query_s_row():
    factory = _connection_factory(row=None)
    audit = MarketplaceSpeedAudit(factory)

    audit.mark_succeeded(query_name=MARKETPLACE_SPEED_QUERY_NAME, query_id=QUERY_ID, batch_id=0, completed_at=None, counts=BatchCounts())

    sql, params = factory.connection.cur.executed[0]
    assert "WHERE query_name=%s AND query_id=%s AND batch_id=%s" in sql
    assert params[-3:] == (MARKETPLACE_SPEED_QUERY_NAME, QUERY_ID, 0)


def test_the_sinks_hand_the_audit_the_query_id():
    outputs, _, _ = _batch()
    sinks, parts = _sinks()

    sinks.write_batch(outputs, batch_id=0, query_id=QUERY_ID)

    assert parts["audit"].query_ids == [QUERY_ID]


@pytest.mark.parametrize("query_id", [None, "", "  "])
def test_a_batch_without_a_query_id_is_refused_before_any_write(query_id):
    outputs, _, _ = _batch()
    sinks, parts = _sinks()

    with pytest.raises(ValueError, match="query_id"):
        sinks.write_batch(outputs, batch_id=0, query_id=query_id)
    assert parts["audit"].calls == []


def test_the_migration_puts_query_id_in_the_primary_key():
    from pathlib import Path

    sql = (Path(__file__).parent.parent / "scripts" / "init_postgres.sql").read_text(encoding="utf-8")
    assert "ALTER TABLE audit.marketplace_speed_batch ADD COLUMN IF NOT EXISTS query_id" in sql
    assert "ALTER TABLE audit.marketplace_speed_batch DROP CONSTRAINT IF EXISTS marketplace_speed_batch_pkey" in sql
    assert "ADD CONSTRAINT marketplace_speed_batch_pkey PRIMARY KEY (query_name, query_id, batch_id)" in sql


# -- Phase 9 plan section 6.1: processing latency per micro-batch --

def test_batch_latency_is_exact_nearest_rank_over_produced_at():
    from datetime import datetime, timedelta, timezone

    from speed_layer.marketplace_sinks import BatchLatency, batch_latency

    done = datetime(2026, 10, 4, 12, tzinfo=timezone.utc)
    produced = [done - timedelta(milliseconds=ms) for ms in (40, 10, 30, 20, 100, 50, 60, 70, 80, 90)]

    assert batch_latency(produced, done) == BatchLatency(p50_ms=50, p95_ms=100, max_ms=100)
    assert batch_latency([done - timedelta(seconds=2)], done) == BatchLatency(2000, 2000, 2000)


def test_a_batch_with_nothing_applied_has_no_latency():
    from datetime import datetime, timezone

    from speed_layer.marketplace_sinks import batch_latency

    assert batch_latency([], datetime(2026, 10, 4, tzinfo=timezone.utc)) is None


def test_a_succeeded_batch_reports_latency_from_its_applied_states():
    """Measured against the completion the audit row records, after the sinks."""
    from speed_layer.marketplace_sinks import batch_latency

    outputs, _, state = _batch()
    sinks, parts = _sinks()

    sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    audit = parts["audit"]
    assert audit.latency == batch_latency([state.produced_at], audit.completed_at)
    assert audit.latency is not None


def test_a_failed_batch_reports_no_latency():
    outputs, _, _ = _batch()
    sinks, parts = _sinks(redis=FakeRedis(fail_on_execute=True))

    with pytest.raises(RuntimeError):
        sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    assert parts["audit"].kinds() == ["begin", "failed"]
    assert not hasattr(parts["audit"], "latency")


def test_the_audit_writes_the_latency_columns_and_nulls_them_on_failure():
    from speed_layer.marketplace_sinks import BatchLatency

    factory = _connection_factory()
    audit = MarketplaceSpeedAudit(factory)

    audit.mark_succeeded(query_name=MARKETPLACE_SPEED_QUERY_NAME, query_id=QUERY_ID, batch_id=7, completed_at=None,
                         counts=BatchCounts(), latency=BatchLatency(11, 22, 33))
    audit.mark_failed(query_name=MARKETPLACE_SPEED_QUERY_NAME, query_id=QUERY_ID, batch_id=8, completed_at=None,
                      error=RuntimeError("x"))

    (ok_sql, ok_params), (_, failed_params) = factory.connection.cur.executed
    assert "latency_p50_ms=%s,latency_p95_ms=%s,latency_max_ms=%s" in ok_sql
    assert ok_params[-6:-3] == (11, 22, 33)
    assert failed_params[-6:-3] == (None, None, None)


def test_the_migration_adds_nullable_latency_columns_and_the_progress_table():
    from pathlib import Path

    sql = (Path(__file__).parent.parent / "scripts" / "init_postgres.sql").read_text(encoding="utf-8")
    for column in ("latency_p50_ms", "latency_p95_ms", "latency_max_ms"):
        assert f"ALTER TABLE audit.marketplace_speed_batch ADD COLUMN IF NOT EXISTS {column} BIGINT;" in sql
    assert "CREATE TABLE IF NOT EXISTS audit.marketplace_stream_progress" in sql
    assert "CREATE TABLE IF NOT EXISTS audit.storage_snapshot" in sql


def test_a_succeeded_batch_reports_each_sink_stage_and_keeps_the_caller_s():
    from speed_layer.marketplace_sinks import BatchStages

    outputs, _, _ = _batch()
    sinks, parts = _sinks()

    sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID, stages=BatchStages(clients_ms=7, collect_ms=900))

    stages = parts["audit"].stages
    assert (stages.clients_ms, stages.collect_ms) == (7, 900)
    assert all(isinstance(v, int) and v >= 0 for v in (stages.kafka_ms, stages.es_ms, stages.redis_ms))


def test_the_audit_writes_the_stage_columns_and_nulls_them_on_failure():
    from speed_layer.marketplace_sinks import BatchStages

    factory = _connection_factory()
    audit = MarketplaceSpeedAudit(factory)

    audit.mark_succeeded(query_name=MARKETPLACE_SPEED_QUERY_NAME, query_id=QUERY_ID, batch_id=7, completed_at=None,
                         counts=BatchCounts(), stages=BatchStages(1, 2, 3, 4, 5))
    audit.mark_failed(query_name=MARKETPLACE_SPEED_QUERY_NAME, query_id=QUERY_ID, batch_id=8, completed_at=None,
                      error=RuntimeError("x"))

    (ok_sql, ok_params), (_, failed_params) = factory.connection.cur.executed
    assert "stage_clients_ms=%s,stage_collect_ms=%s,stage_kafka_ms=%s,stage_es_ms=%s,stage_redis_ms=%s" in ok_sql
    assert ok_params[-11:-6] == (1, 2, 3, 4, 5)
    assert failed_params[-11:-6] == (None,) * 5


def test_the_migration_adds_nullable_stage_columns():
    from pathlib import Path

    sql = (Path(__file__).parent.parent / "scripts" / "init_postgres.sql").read_text(encoding="utf-8")
    for column in ("stage_clients_ms", "stage_collect_ms", "stage_kafka_ms", "stage_es_ms", "stage_redis_ms"):
        assert f"ALTER TABLE audit.marketplace_speed_batch ADD COLUMN IF NOT EXISTS {column} BIGINT;" in sql


def _states_observed_at(*seconds):
    """STATE outputs for distinct offers of one marketplace, in the given order."""
    _, _, state = _batch()
    states = [replace(state, offer_id=f"{state.offer_id}-{i}", observed_at=state.observed_at + timedelta(seconds=s))
              for i, s in enumerate(seconds)]
    outputs = [SpeedOutput(output_kind="STATE", offer_id=s.offer_id, state_json=offer_state_to_json(s)) for s in states]
    return outputs, states


def _source_sets(redis):
    return [command for command in redis.commands if command[0] == "set" and command[1].startswith("rt:source:")]


def test_the_source_s_last_observation_is_read_once_per_marketplace_not_per_state():
    outputs, states = _states_observed_at(0, 5, 3)
    sinks, parts = _sinks()

    sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    assert parts["redis"].gets == [f"rt:source:{states[0].marketplace}:last_observation"]


def test_the_source_s_last_observation_is_the_batch_s_latest_not_the_last_queued():
    # Both are newer than what is stored; the older one comes last.
    outputs, states = _states_observed_at(5, 3)
    key = f"rt:source:{states[0].marketplace}:last_observation"
    sinks, parts = _sinks(redis=FakeRedis(stored={key: (states[0].observed_at - timedelta(hours=1)).isoformat()}))

    sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    assert _source_sets(parts["redis"]) == [("set", key, states[0].observed_at.isoformat(), False)]


def test_the_source_s_last_observation_never_moves_back():
    outputs, states = _states_observed_at(0, 5)
    key = f"rt:source:{states[0].marketplace}:last_observation"
    stored = (states[1].observed_at + timedelta(seconds=1)).isoformat()
    sinks, parts = _sinks(redis=FakeRedis(stored={key: stored.encode("utf-8")}))

    sinks.write_batch(outputs, batch_id=1, query_id=QUERY_ID)

    assert _source_sets(parts["redis"]) == []
