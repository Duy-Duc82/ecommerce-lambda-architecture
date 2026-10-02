"""Marketplace speed service, Phase 8 plan section 6.3, item 13.

The query builder is checked against a recording DataStreamWriter, so no
Kafka source is needed. The output union runs on real batch DataFrames.
"""
import json
from types import SimpleNamespace

import pytest

from config.marketplace_wire import canonical_json
from config.settings import MARKETPLACE_SPEED_QUERY_NAME, MARKETPLACE_STREAM_TRIGGER
from speed_layer import marketplace_speed_layer as layer
from speed_layer import marketplace_speed_service as service
from speed_layer.marketplace_speed_layer import SPEED_OUTPUT_SCHEMA
from tests.spark_support import requires_spark
from tests.test_marketplace_change_rules import CONFIG, _event


class RecordingWriter:
    def __init__(self, log):
        self.log = log

    def queryName(self, name):
        self.log["queryName"] = name
        return self

    def option(self, key, value):
        self.log.setdefault("options", {})[key] = value
        return self

    def trigger(self, **kwargs):
        self.log["trigger"] = kwargs
        return self

    def outputMode(self, mode):
        self.log["outputMode"] = mode
        return self

    def foreachBatch(self, function):
        self.log["foreachBatch"] = function
        return self

    def start(self):
        self.log["started"] = True
        return SimpleNamespace(name=self.log.get("queryName"))


def _start(monkeypatch, **kwargs):
    log = {}
    outputs = SimpleNamespace(writeStream=RecordingWriter(log))
    monkeypatch.setattr(service, "speed_outputs", lambda raw, config: outputs)
    query = service.start_query(spark=None, read=lambda spark: "raw", config=CONFIG, **kwargs)
    return query, log


# 13
def test_the_query_carries_its_name_checkpoint_trigger_and_sink(monkeypatch):
    query, log = _start(monkeypatch)

    assert log["queryName"] == MARKETPLACE_SPEED_QUERY_NAME
    assert log["options"]["checkpointLocation"] == layer.checkpoint_path()
    assert log["trigger"] == {"processingTime": MARKETPLACE_STREAM_TRIGGER}
    assert log["outputMode"] == "append"
    assert log["foreachBatch"] is layer.write_marketplace_batch
    assert log["started"] is True
    assert query.name == MARKETPLACE_SPEED_QUERY_NAME


# 13
def test_the_checkpoint_root_can_be_moved_but_keeps_its_version(monkeypatch, tmp_path):
    monkeypatch.setattr(layer, "MARKETPLACE_SPEED_CHECKPOINT_ROOT", str(tmp_path / "ckpt"))

    path = layer.checkpoint_path()

    assert path.replace("\\", "/").endswith(f"ckpt/{layer.MARKETPLACE_STREAM_CHECKPOINT_VERSION}")


def test_the_default_checkpoint_root_is_unchanged(monkeypatch):
    monkeypatch.setattr(layer, "MARKETPLACE_SPEED_CHECKPOINT_ROOT", "")

    assert "marketplace_speed" in layer.checkpoint_path().replace("\\", "/")


def test_a_stop_signal_stops_the_query_and_returns():
    stopped = []

    class Query:
        def __init__(self):
            self.checks = 0

        def awaitTermination(self, timeout):
            self.checks += 1
            return False

        def stop(self):
            stopped.append(True)

    class StopAfterTwoChecks:
        def __init__(self, query):
            self.query = query

        def is_set(self):
            return self.query.checks >= 2

    query = Query()

    service.await_query(query, stop=StopAfterTwoChecks(query), poll_seconds=1)

    assert stopped == [True]


def test_a_query_that_ends_by_itself_is_not_stopped_again():
    class Query:
        def awaitTermination(self, timeout):
            return True

        def stop(self):
            raise AssertionError("already terminated")

    service.await_query(Query(), stop=SimpleNamespace(is_set=lambda: False), poll_seconds=1)


# 13: invalid records reach the sink, or the batch audit could not count them.
@requires_spark
def test_invalid_records_are_carried_alongside_the_changes(spark):
    event = _event()
    bad = json.loads(canonical_json(event))
    bad["schema_version"] = "marketplace-observation.v2"
    raw = spark.createDataFrame(
        [
            ("marketplace.observations.v1", 0, 1, event.partition_key, canonical_json(event), None),
            ("marketplace.observations.v1", 0, 2, event.partition_key, json.dumps(bad), None),
        ],
        "source_topic string, source_partition long, source_offset long, "
        "source_key string, value string, kafka_timestamp timestamp",
    )

    outputs = service.speed_outputs(raw, CONFIG)

    assert outputs.schema == SPEED_OUTPUT_SCHEMA
    kinds = sorted(row.output_kind for row in outputs.collect())
    assert "INVALID" in kinds
    assert "STATE" in kinds
    invalid = [row for row in outputs.collect() if row.output_kind == "INVALID"]
    assert [row.source_offset for row in invalid] == [2]


def test_the_entrypoint_answers_help():
    import subprocess
    import sys

    result = subprocess.run([sys.executable, "-m", "speed_layer.marketplace_speed_service", "--help"],
                            capture_output=True, text=True, timeout=300)

    assert result.returncode == 0, result.stderr
    assert "usage" in result.stdout.lower()


# The micro-batch sink is the speed path's long-lived PostgreSQL user.
def test_each_micro_batch_closes_its_audit_connections(monkeypatch):
    from common import postgres

    built = []
    monkeypatch.setattr(postgres, "postgres_connection_factory", lambda: built.append("closing") or (lambda: None))

    class RecordingSinks:
        def __init__(self, *, producer, es, redis, audit):
            self.audit = audit

        def write_batch(self, outputs, batch_id):
            built.append(("batch", batch_id, len(outputs)))

    import speed_layer.marketplace_sinks as sinks

    monkeypatch.setattr(sinks, "MarketplaceSpeedSinks", RecordingSinks)
    monkeypatch.setattr(layer, "_sink_clients", lambda: (object(), object(), object()))

    layer.write_marketplace_batch(SimpleNamespace(collect=lambda: []), 7)

    assert built == ["closing", ("batch", 7, 0)]


def test_the_default_kafka_connector_matches_the_spark_major_version():
    # Spark 4 is built on Scala 2.13. The old default, _2.12:3.5.1, cannot
    # load on it, so the query would die at start before reading a record.
    import os

    import pyspark

    if "SPARK_KAFKA_PACKAGE" in os.environ:
        pytest.skip("the environment overrides the connector")
    from config.settings import SPARK_KAFKA_PACKAGE

    group, artifact, version = SPARK_KAFKA_PACKAGE.split(":")
    assert group == "org.apache.spark"
    assert artifact == "spark-sql-kafka-0-10_2.13"
    assert version.split(".")[0] == pyspark.__version__.split(".")[0]
