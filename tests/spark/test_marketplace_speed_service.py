"""Marketplace speed service, Phase 8 plan section 6.3, item 13.

The query builder is checked against a recording DataStreamWriter, so no
Kafka source is needed. The output union runs on real batch DataFrames.
"""
import json
from types import SimpleNamespace

import pytest

from ecomdata.domain.wire import canonical_json
from ecomdata.processing.speed import service
from ecomdata.processing.speed import stream as layer
from ecomdata.processing.speed.stream import SPEED_OUTPUT_SCHEMA
from ecomdata.settings import SpeedSettings, load_settings
from tests.spark_support import requires_spark
from tests.unit.domain.test_marketplace_change_rules import CONFIG, _event


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


def _write_batch(batch, batch_id):
    return None


def _start(monkeypatch, **kwargs):
    log = {}
    outputs = SimpleNamespace(writeStream=RecordingWriter(log))
    monkeypatch.setattr(service, "speed_outputs", lambda raw, config: outputs)
    options = {"checkpoint": "ckpt", "write_batch": _write_batch, **kwargs}
    query = service.start_query(spark=None, read=lambda spark: "raw", config=CONFIG, **options)
    return query, log


# 13
def test_the_query_carries_its_name_checkpoint_trigger_and_sink(monkeypatch):
    speed = SpeedSettings()
    query, log = _start(monkeypatch, trigger=speed.trigger, query_name=speed.query_name, checkpoint="ckpt/v1")

    assert log["queryName"] == speed.query_name
    assert log["options"]["checkpointLocation"] == "ckpt/v1"
    assert log["trigger"] == {"processingTime": speed.trigger}
    assert log["outputMode"] == "append"
    assert log["foreachBatch"] is _write_batch
    assert log["started"] is True
    assert query.name == speed.query_name


# 13
def test_the_checkpoint_root_can_be_moved_but_keeps_its_version(tmp_path):
    speed = load_settings({"MARKETPLACE_SPEED_CHECKPOINT_ROOT": str(tmp_path / "ckpt")}).speed

    assert speed.checkpoint_path().replace("\\", "/").endswith(f"ckpt/{speed.checkpoint_version}")


def test_the_default_checkpoint_root_is_unchanged():
    assert "marketplace_speed" in SpeedSettings().checkpoint_path().replace("\\", "/")


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

    result = subprocess.run([sys.executable, "-m", "ecomdata.processing.speed.service", "--help"],
                            capture_output=True, text=True, timeout=300)

    assert result.returncode == 0, result.stderr
    assert "usage" in result.stdout.lower()


# The micro-batch sink is the speed path's long-lived PostgreSQL user.
def _wiring(clients, built=None):
    def factory():
        return None

    if built is not None:
        built.append("closing")
    return layer.SinkWiring(clients=lambda: clients, connection_factory=factory)


def test_each_micro_batch_closes_its_audit_connections(monkeypatch):
    built = []

    class RecordingSinks:
        def __init__(self, *, producer, es, redis, audit, **options):
            self.audit = audit

        def write_batch(self, outputs, batch_id, *, query_id, stages=None):
            built.append(("batch", batch_id, len(outputs), query_id))

    import ecomdata.processing.speed.sinks as sinks

    monkeypatch.setattr(sinks, "MarketplaceSpeedSinks", RecordingSinks)
    write = layer.make_batch_writer(_wiring((object(), object(), object()), built))

    write(_micro_batch("query-uuid"), 7)

    assert built == ["closing", ("batch", 7, 0, "query-uuid")]


class _ClosingClient:
    def __init__(self):
        self.closed = 0

    def close(self):
        self.closed += 1


# One micro-batch every trigger, for days: clients a batch opens must not
# outlive it, whether the batch succeeds, is skipped or never reaches the sink.
@pytest.mark.parametrize("outcome", ["succeeded", "collect_failed"])
def test_each_micro_batch_closes_its_sink_clients(monkeypatch, outcome):
    import ecomdata.processing.speed.sinks as sinks

    clients = (_ClosingClient(), _ClosingClient(), _ClosingClient())

    class SucceedingSinks:
        def __init__(self, **kwargs): pass

        def write_batch(self, outputs, batch_id, *, query_id, stages=None): pass

    monkeypatch.setattr(sinks, "MarketplaceSpeedSinks", SucceedingSinks)
    write = layer.make_batch_writer(_wiring(clients))
    batch = _micro_batch("query-uuid")
    if outcome == "collect_failed":
        def failing_collect(): raise RuntimeError("executor lost")
        batch.collect = failing_collect

    if outcome == "succeeded":
        write(batch, 7)
    else:
        with pytest.raises(RuntimeError, match="executor lost"):
            write(batch, 7)

    assert [client.closed for client in clients] == [1, 1, 1]


def _micro_batch(query_id):
    # foreachBatch's frame: its session's context carries the query id as a
    # local property, set by Spark for every micro-batch.
    context = SimpleNamespace(getLocalProperty=lambda key: query_id if key == "sql.streaming.queryId" else None)
    return SimpleNamespace(collect=lambda: [], sparkSession=SimpleNamespace(sparkContext=context))


def test_a_micro_batch_without_a_query_id_is_refused():
    write = layer.make_batch_writer(_wiring((object(), object(), object())))

    with pytest.raises(ValueError, match="query id"):
        write(_micro_batch(None), 7)


def test_the_default_kafka_connector_matches_the_spark_major_version():
    # Spark 4 is built on Scala 2.13. The old default, _2.12:3.5.1, cannot
    # load on it, so the query would die at start before reading a record.
    import pyspark

    group, artifact, version = load_settings({}).kafka.spark_kafka_package.split(":")
    assert group == "org.apache.spark"
    assert artifact == "spark-sql-kafka-0-10_2.13"
    assert version.split(".")[0] == pyspark.__version__.split(".")[0]


def test_the_speed_session_pins_a_small_shuffle_partition_count():
    # Spark fixes a stateful operator's partition count in the checkpoint on
    # the first run, so this has to be right before a production checkpoint
    # exists. The default, 200, ran 200+ tasks per micro-batch for a few dozen
    # records on the first real run.
    settings = load_settings({})

    conf = service.spark_factory(settings).config()

    assert conf["spark.sql.shuffle.partitions"] == str(settings.spark.speed_shuffle_partitions)
    assert 1 <= settings.spark.speed_shuffle_partitions <= 16
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert conf["spark.jars.packages"] == settings.kafka.spark_kafka_package
