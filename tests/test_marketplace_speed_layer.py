"""Stateful transform tests, items 21-28 of the Phase 5 plan section 13."""
import json
import subprocess
import sys
from dataclasses import fields
from datetime import timedelta
from decimal import Decimal

import pytest

from config.marketplace_wire import canonical_json
from config.settings import MARKETPLACE_STREAM_CHECKPOINT_VERSION
from speed_layer.marketplace_change_rules import (
    ChangeRuleConfig,
    detect_observation_changes,
    detect_stale_change,
)
from speed_layer.marketplace_sinks import SpeedOutput
from speed_layer.marketplace_speed_layer import (
    SPEED_OUTPUT_SCHEMA,
    SpeedOutputKind,
    _process_group,
    checkpoint_path,
    decode_observation_stream,
    speed_output_schema,
)
from tests.spark_support import requires_spark
from tests.test_marketplace_change_rules import CONFIG, _event, _later
from tests.test_marketplace_schema import OBSERVED_AT


def _row(event, offset=0):
    observation = event.payload.observation
    return {
        "event_json": canonical_json(event),
        "event_time": observation.observed_at,
        "observation_id": observation.observation_id,
        "offer_id": event.payload.offer.offer_id,
        "source_topic": "marketplace.observations.v1",
        "source_partition": 0,
        "source_offset": offset,
    }


def _kinds(outputs):
    return [output.output_kind for output in outputs]


# 23
def test_grouped_rows_are_processed_in_deterministic_order():
    first = _event()
    second = _later(1, current_price=Decimal("150.00"))
    third = _later(2, current_price=Decimal("70.00"))
    rows = [_row(first, 0), _row(second, 1), _row(third, 2)]

    forward = _process_group(list(rows), config=CONFIG)
    shuffled = _process_group([rows[2], rows[0], rows[1]], config=CONFIG)

    assert [output.observation_id for output in forward] == [
        output.observation_id for output in shuffled
    ]
    assert [output.change_json for output in forward] == [
        output.change_json for output in shuffled
    ]
    assert _kinds(forward) == ["STATE", "CHANGE", "STATE", "CHANGE", "STATE", "CHANGE", "CHANGE"]


def test_same_timestamp_rows_sort_by_observation_id():
    left = _event(raw_sha256="b" * 64)
    right = _event(raw_sha256="c" * 64)
    lower, higher = sorted(
        (left, right), key=lambda event: event.payload.observation.observation_id
    )

    outputs = _process_group([_row(higher, 1), _row(lower, 0)], config=CONFIG)

    # Sorting inside the group is what makes the tie deterministic: the lower
    # id is applied first, so the higher id is still in order and applied too.
    states = [o for o in outputs if o.output_kind == "STATE"]
    assert [state.observation_id for state in states] == [
        lower.payload.observation.observation_id,
        higher.payload.observation.observation_id,
    ]
    assert "LATE" not in _kinds(outputs)
    assert _process_group([_row(lower, 0), _row(higher, 1)], config=CONFIG) == outputs


def test_duplicate_and_late_rows_keep_their_own_output_kinds():
    first = _event()
    rows = [_row(first, 0), _row(first, 1)]

    outputs = _process_group(rows, config=CONFIG)

    assert _kinds(outputs) == ["STATE", "CHANGE", "DUPLICATE"]
    assert outputs[-1].state_json is None
    assert outputs[-1].change_json is None


# 24
def test_output_schema_separates_state_and_change_rows():
    outputs = _process_group(
        [_row(_event(), 0), _row(_later(current_price=Decimal("70.00")), 1)],
        config=CONFIG,
    )

    states = [o for o in outputs if o.output_kind == SpeedOutputKind.STATE.value]
    changes = [o for o in outputs if o.output_kind == SpeedOutputKind.CHANGE.value]
    assert states and changes
    for state in states:
        assert state.state_json is not None
        assert state.change_json is None
        assert state.change_id is None
    for change in changes:
        assert change.change_json is not None
        assert change.state_json is None
        assert change.change_id == json.loads(change.change_json)["event_id"]


def test_every_output_kind_is_a_declared_enum_member():
    outputs = _process_group(
        [_row(_event(), 0), _row(_event(), 1)],
        config=CONFIG,
    )

    declared = {member.value for member in SpeedOutputKind}
    assert declared == {"STATE", "CHANGE", "DUPLICATE", "LATE", "INVALID"}
    assert {output.output_kind for output in outputs} <= declared


def test_state_rows_carry_kafka_lineage():
    outputs = _process_group([_row(_event(), 42)], config=CONFIG)

    for output in outputs:
        assert output.source_topic == "marketplace.observations.v1"
        assert output.source_partition == 0
        assert output.source_offset == 42


# 25
def test_timeout_branch_emits_no_duplicate_stale_event():
    state = _process_group([_row(_event(), 0)], config=CONFIG)
    applied = detect_observation_changes(None, _event(), CONFIG).next_state

    stale_state, first_stale = detect_stale_change(applied, CONFIG)
    _, second_stale = detect_stale_change(stale_state, CONFIG)

    assert state[0].output_kind == "STATE"
    assert first_stale is not None
    assert second_stale is None


def test_a_new_observation_after_a_timeout_reopens_the_stale_window():
    applied = detect_observation_changes(None, _event(), CONFIG).next_state
    stale_state, _ = detect_stale_change(applied, CONFIG)

    revived = _process_group([_row(_later(30), 1)], previous=stale_state, config=CONFIG)
    _, reopened = detect_stale_change(
        detect_observation_changes(stale_state, _later(30), CONFIG).next_state, CONFIG
    )

    assert revived[0].output_kind == "STATE"
    assert reopened is not None
    assert reopened.detected_at == OBSERVED_AT + timedelta(
        seconds=30 + CONFIG.stale_after_seconds
    )


# 26
def test_checkpoint_path_contains_the_configured_version():
    path = checkpoint_path()

    assert path.endswith(MARKETPLACE_STREAM_CHECKPOINT_VERSION)
    assert "marketplace_speed" in path.replace("\\", "/")


# 27
def test_state_and_output_schemas_are_explicit_and_stable():
    schema = speed_output_schema()

    assert schema is SPEED_OUTPUT_SCHEMA
    assert schema.names == [field.name for field in fields(SpeedOutput)]
    assert schema["output_kind"].nullable is False
    assert [field.dataType.simpleString() for field in schema.fields] == [
        "string", "string", "string", "string", "string", "timestamp",
        "string", "string", "string", "bigint", "bigint", "string", "string",
    ]


def test_speed_output_defaults_leave_every_optional_column_null():
    output = SpeedOutput(output_kind="DUPLICATE")

    assert output.state_json is None
    assert output.change_json is None
    assert output.error_type is None


# 28
def test_importing_the_module_does_not_start_spark_or_open_clients():
    probe = (
        "import speed_layer.marketplace_speed_layer as module;"
        "from pyspark.sql import SparkSession;"
        "assert SparkSession._instantiatedSession is None;"
        "assert module.speed_output_schema() is module.SPEED_OUTPUT_SCHEMA;"
        "import sys;"
        "assert 'kafka' not in sys.modules;"
        "assert 'redis' not in sys.modules;"
        "assert 'elasticsearch' not in sys.modules;"
        "assert 'psycopg2' not in sys.modules;"
        "print('CLEAN')"
    )

    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, timeout=300
    )

    assert "CLEAN" in result.stdout, result.stderr


# 21 and 22 need real DataFrames.
@requires_spark
def test_decode_accepts_an_exact_valid_observation_row(spark):
    event = _event()
    raw = spark.createDataFrame(
        [(
            "marketplace.observations.v1", 0, 7,
            event.partition_key, canonical_json(event), None,
        )],
        "source_topic string, source_partition long, source_offset long, "
        "source_key string, value string, kafka_timestamp timestamp",
    )

    valid, invalid = decode_observation_stream(raw)

    assert invalid.count() == 0
    row = valid.collect()[0]
    assert row.observation_id == event.payload.observation.observation_id
    assert row.offer_id == event.payload.offer.offer_id
    assert row.source_offset == 7


@requires_spark
@pytest.mark.parametrize(
    "mutate",
    [
        lambda wire: wire.update(schema_version="marketplace-observation.v2"),
        lambda wire: wire.update(event_type="OFFER_CHANGED"),
        lambda wire: wire.update(partition_key="tiki/p1"),
        lambda wire: wire.update(crawl_run_id="run-other"),
        lambda wire: wire.update(raw_uri="file:///tmp/other.json"),
    ],
)
def test_wrong_schema_version_key_or_lineage_is_excluded_and_counted_invalid(
    spark, mutate
):
    event = _event()
    wire = json.loads(canonical_json(event))
    mutate(wire)
    raw = spark.createDataFrame(
        [(
            "marketplace.observations.v1", 0, 1,
            event.partition_key, json.dumps(wire), None,
        )],
        "source_topic string, source_partition long, source_offset long, "
        "source_key string, value string, kafka_timestamp timestamp",
    )

    valid, invalid = decode_observation_stream(raw)

    assert valid.count() == 0
    assert invalid.count() == 1
    assert invalid.collect()[0].error_type == "OBSERVATION_CONTRACT"


@requires_spark
def test_a_key_that_disagrees_with_the_envelope_is_invalid(spark):
    event = _event()
    raw = spark.createDataFrame(
        [(
            "marketplace.observations.v1", 0, 1,
            "tiki:other", canonical_json(event), None,
        )],
        "source_topic string, source_partition long, source_offset long, "
        "source_key string, value string, kafka_timestamp timestamp",
    )

    valid, invalid = decode_observation_stream(raw)

    assert valid.count() == 0
    assert invalid.count() == 1


# ----------------------------------------------------------------------------
# Found when Phase 8 assembled the query (2026-10-02). Spark's to_json drops
# null fields by default, and the re-serialised event_json then fails the
# strict wire contract, which requires every key. Tiki leaves brand and
# seller_id null on many listings, so each such observation would crash its
# micro-batch. The decode test above checks columns only; this one carries a
# decoded row all the way through the change rules.
# ----------------------------------------------------------------------------
@requires_spark
def test_a_decoded_observation_with_null_optional_fields_reaches_the_change_rules(spark):
    event = _event()
    wire = json.loads(canonical_json(event))
    wire["payload"]["offer"]["brand"] = None
    wire["payload"]["offer"]["seller_id"] = None
    raw = spark.createDataFrame(
        [("marketplace.observations.v1", 0, 3, event.partition_key, json.dumps(wire), None)],
        "source_topic string, source_partition long, source_offset long, "
        "source_key string, value string, kafka_timestamp timestamp",
    )

    valid, invalid = decode_observation_stream(raw)
    (row,) = [r.asDict() for r in valid.collect()]

    assert invalid.count() == 0
    decoded = json.loads(row["event_json"])
    assert decoded["payload"]["offer"]["brand"] is None
    assert decoded["payload"]["offer"]["seller_id"] is None
    outputs = _process_group([row], config=CONFIG)
    assert _kinds(outputs)[0] == "STATE"


# ----------------------------------------------------------------------------
# Found by the first real run of the speed query (Phase 8 WP2, 2026-10-02).
# applyInPandasWithState hands the function an *iterator* of pandas frames
# and expects an iterator back. The function treated its argument as one
# frame, so every micro-batch failed with
# "'generator' object has no attribute 'to_dict'". Only the batch path had
# ever been tested.
# ----------------------------------------------------------------------------
import pandas as pd

from speed_layer.marketplace_change_rules import offer_state_to_json
from speed_layer.marketplace_speed_layer import make_state_function


class FakeGroupState:
    """The slice of pyspark's GroupState the state function uses."""

    def __init__(self, stored=None, *, timed_out=False, now_ms=0):
        self.stored = stored
        self.hasTimedOut = timed_out
        self.now_ms = now_ms
        self.timeouts = []

    @property
    def exists(self):
        return self.stored is not None

    @property
    def get(self):
        return self.stored

    def update(self, value):
        self.stored = value

    def setTimeoutDuration(self, duration_ms):
        self.timeouts.append(duration_ms)

    def getCurrentProcessingTimeMs(self):
        return self.now_ms


def _frames(*groups):
    return iter([pd.DataFrame(rows) for rows in groups])


def _collect(result):
    frames = list(result)
    assert all(isinstance(frame, pd.DataFrame) for frame in frames)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def test_the_state_function_reads_every_frame_it_is_given_and_yields_frames():
    first, second = _event(), _later(1, current_price=Decimal("150.00"))
    state = FakeGroupState()

    out = _collect(make_state_function(CONFIG)(("offer",), _frames([_row(first, 0)], [_row(second, 1)]), state))

    # Both frames were read: two STATE rows plus the price change between them.
    assert list(out["output_kind"]).count("STATE") == 2
    assert "CHANGE" in set(out["output_kind"])
    assert set(out.columns) <= set(SPEED_OUTPUT_SCHEMA.names)
    assert state.exists


def test_a_timed_out_key_yields_its_stale_change_as_a_frame():
    prior = detect_observation_changes(None, _event(), CONFIG).next_state
    stored = (offer_state_to_json(prior), prior.observed_at, prior.observation_id)
    state = FakeGroupState(stored, timed_out=True)

    out = _collect(make_state_function(CONFIG)(("offer",), iter([]), state))

    assert list(out["output_kind"]) == ["CHANGE"]
    assert json.loads(out["change_json"][0])["change_type"] == "OFFER_STALE"


def test_a_call_with_only_a_duplicate_keeps_the_stale_timeout_armed():
    # With ProcessingTimeTimeout a key's timeout must be set again on every
    # call, or it is dropped. A call that applied nothing new used to leave it
    # unset, so that offer could never go stale.
    event = _event()
    prior = detect_observation_changes(None, event, CONFIG).next_state
    stored = (offer_state_to_json(prior), prior.observed_at, prior.observation_id)
    state = FakeGroupState(stored, now_ms=int(prior.observed_at.timestamp() * 1000))

    out = _collect(make_state_function(CONFIG)(("offer",), _frames([_row(event, 0)]), state))

    assert list(out["output_kind"]) == ["DUPLICATE"]
    assert state.timeouts == [CONFIG.stale_after_seconds * 1000]
