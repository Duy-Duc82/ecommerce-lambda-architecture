"""Silver sink service, Phase 8 plan section 6.2, items 9-12.

A scripted consumer stands in for Kafka. No socket is opened and nothing
sleeps: waits go to a recording stop signal.
"""
import subprocess
import sys
from collections import namedtuple
from types import SimpleNamespace

import pytest

from data_ingestion import marketplace_silver_service as service
from data_ingestion.marketplace_silver_sink import SilverWriteError

TOPIC = "marketplace.observations.v1"


# kafka.structs.TopicPartition is a namedtuple, hashable, and a dict key.
TopicPartition = namedtuple("TopicPartition", ["topic", "partition"])


def _tp(partition):
    return TopicPartition(TOPIC, partition)


def _msg(partition, offset):
    return SimpleNamespace(topic=TOPIC, partition=partition, offset=offset, key=b"tiki:p1", value=b"{}")


class ScriptedConsumer:
    """Returns one scripted poll result per call, then empty polls."""

    def __init__(self, polls):
        self.polls = list(polls)
        self.log = []

    def poll(self, timeout_ms, max_records=None):
        self.log.append(("poll",))
        return self.polls.pop(0) if self.polls else {}

    def commit(self, offsets):
        for tp, meta in offsets.items():
            self.log.append(("commit", tp.partition, meta.offset))

    def seek(self, tp, offset):
        self.log.append(("seek", tp.partition, offset))

    def calls(self, kind):
        return [entry[1:] for entry in self.log if entry[0] == kind]


class RecordingStop:
    def __init__(self, *, stop_after_waits=None):
        self.waits = []
        self.stopped = False
        self.stop_after_waits = stop_after_waits

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def wait(self, seconds):
        self.waits.append(seconds)
        if self.stop_after_waits is not None and len(self.waits) >= self.stop_after_waits:
            self.stopped = True
        return self.stopped


def _run(consumer, process, *, stop=None, max_records=None, **overrides):
    options = {"poll_timeout_ms": 1000, "retry_base_seconds": 2, "retry_max_seconds": 60, "log": lambda line: None}
    options.update(overrides)
    stop = stop or RecordingStop()
    handled = service.run_sink(consumer, process=process, stop=stop, max_records=max_records, **options)
    return handled, stop


# 9
def test_each_offset_is_committed_only_after_its_record_was_processed():
    consumer = ScriptedConsumer([{_tp(0): [_msg(0, 5), _msg(0, 6)]}])
    order = []

    def process(record):
        order.append(("process", record.offset))
        # The commit for an offset must not exist yet while it is processed.
        assert ("commit", 0, record.offset + 1) not in consumer.log

    handled, _ = _run(consumer, process, max_records=2)

    assert handled == 2
    assert order == [("process", 5), ("process", 6)]
    assert consumer.calls("commit") == [(0, 6), (0, 7)]


# 9
def test_the_processor_receives_the_record_coordinates_and_bytes():
    consumer = ScriptedConsumer([{_tp(2): [_msg(2, 41)]}])
    seen = []

    _run(consumer, seen.append, max_records=1)

    (record,) = seen
    assert (record.topic, record.partition, record.offset, record.key, record.value) == (TOPIC, 2, 41, b"tiki:p1", b"{}")


# 10, 11
def test_a_failed_record_is_retried_at_the_same_offset_and_never_skipped():
    consumer = ScriptedConsumer([
        {_tp(0): [_msg(0, 5), _msg(0, 6)]},
        {_tp(0): [_msg(0, 6)]},
    ])
    failures = {6: 1}

    def process(record):
        if failures.get(record.offset, 0):
            failures[record.offset] -= 1
            raise SilverWriteError("MinIO unavailable")

    handled, stop = _run(consumer, process, max_records=2)

    assert handled == 2
    assert consumer.calls("commit") == [(0, 6), (0, 7)]
    assert consumer.calls("seek") == [(0, 6)]
    assert stop.waits == [2]


# 10
def test_a_failure_rewinds_every_partition_to_its_first_unprocessed_offset():
    # poll() already advanced every partition's position past what it
    # returned. Abandoning the batch without rewinding the others would skip
    # their records for good.
    consumer = ScriptedConsumer([{_tp(0): [_msg(0, 5)], _tp(1): [_msg(1, 9), _msg(1, 10)]}])

    def process(record):
        if record.partition == 0:
            raise SilverWriteError("down")

    _run(consumer, process, stop=RecordingStop(stop_after_waits=1))

    assert sorted(consumer.calls("seek")) == [(0, 5), (1, 9)]
    assert consumer.calls("commit") == []


# 10
def test_the_backoff_doubles_up_to_its_cap_and_resets_after_a_success():
    polls = [{_tp(0): [_msg(0, 1)]} for _ in range(6)]
    consumer = ScriptedConsumer(polls)
    attempts = {"count": 0}

    def process(record):
        attempts["count"] += 1
        if attempts["count"] <= 4:
            raise SilverWriteError("down")

    _, stop = _run(consumer, process, max_records=1, retry_base_seconds=2, retry_max_seconds=10)

    assert stop.waits == [2, 4, 8, 10]


def test_a_stop_during_the_backoff_ends_the_service_without_committing():
    consumer = ScriptedConsumer([{_tp(0): [_msg(0, 1)]}])

    def process(record):
        raise SilverWriteError("down")

    handled, _ = _run(consumer, process, stop=RecordingStop(stop_after_waits=1))

    assert handled == 0
    assert consumer.calls("commit") == []


def test_a_stop_between_records_finishes_the_one_in_hand_and_takes_no_other():
    stop = RecordingStop()
    consumer = ScriptedConsumer([{_tp(0): [_msg(0, 1), _msg(0, 2)]}])

    def process(record):
        stop.set()

    handled, _ = _run(consumer, process, stop=stop)

    assert handled == 1
    assert consumer.calls("commit") == [(0, 2)]
    # The record it did not take is rewound, not skipped.
    assert consumer.calls("seek") == [(0, 2)]


def test_the_service_refuses_non_positive_settings():
    for name in ("poll_timeout_ms", "retry_base_seconds", "retry_max_seconds"):
        with pytest.raises(ValueError, match=name):
            _run(ScriptedConsumer([]), lambda record: None, max_records=1, **{name: 0})
    with pytest.raises(ValueError, match="retry_max_seconds"):
        _run(ScriptedConsumer([]), lambda record: None, max_records=1, retry_base_seconds=10, retry_max_seconds=5)


def test_the_consumer_never_auto_commits_and_starts_from_the_earliest_offset(monkeypatch):
    kafka = pytest.importorskip("kafka")
    captured = {}

    class RecordingConsumer:
        def __init__(self, *topics, **configs):
            captured.update(configs, topics=topics)

    monkeypatch.setattr(kafka, "KafkaConsumer", RecordingConsumer)

    service.create_consumer("localhost:9092")

    from kafka.consumer.group import KafkaConsumer

    unknown = set(captured) - {"topics"} - set(KafkaConsumer.DEFAULT_CONFIG)
    assert unknown == set(), f"KafkaConsumer would refuse {sorted(unknown)}"
    assert captured["topics"] == (TOPIC,)
    assert captured["enable_auto_commit"] is False
    assert captured["auto_offset_reset"] == "earliest"
    assert captured["group_id"] == "marketplace-silver-v1"


def test_importing_the_service_opens_no_client():
    probe = (
        "import data_ingestion.marketplace_silver_service, speed_layer.marketplace_speed_service;"
        "import sys;"
        "assert 'kafka' not in sys.modules, 'kafka';"
        "assert 'minio' not in sys.modules, 'minio';"
        "assert 'psycopg2' not in sys.modules, 'psycopg2';"
        "print('CLEAN')"
    )
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, timeout=300)

    assert "CLEAN" in result.stdout, result.stderr


def test_the_entrypoint_answers_help():
    result = subprocess.run([sys.executable, "-m", "data_ingestion.marketplace_silver_service", "--help"],
                            capture_output=True, text=True, timeout=300)

    assert result.returncode == 0, result.stderr
    assert "usage" in result.stdout.lower()


# Phase 8 plan section 7.2: one heartbeat per poll, retrying or not, so a sink
# waiting out a storage outage still reads as alive.
def test_the_sink_beats_once_per_poll_even_while_retrying():
    consumer = ScriptedConsumer([{_tp(0): [_msg(0, 5)]}, {_tp(0): [_msg(0, 5)]}])
    calls, beats = [], []

    def flaky(record):
        calls.append(record.offset)
        if len(calls) == 1:
            raise RuntimeError("minio down")

    _run(consumer, flaky, max_records=1, beat=lambda: beats.append(1))

    assert calls == [5, 5]
    assert len(beats) == len(consumer.calls("poll")) == 2
