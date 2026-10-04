"""Spark streaming progress, one row per micro-batch — Phase 9 plan section 6.1.

Offline: progress arrives as the dicts PySpark 4 builds from Spark's own
JSON, and the database is a recording fake.
"""
from __future__ import annotations

import json
import math
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

from speed_layer import marketplace_speed_service as service

NOW = datetime(2026, 10, 4, 12, 18, tzinfo=timezone.utc)
QUERY_ID = "6f1c1a52-0d6a-4b0e-9a54-2b1f7f0c9d11"


def _progress(batch_id: int, **overrides) -> dict:
    """Shaped like Spark 4.0's StreamingQueryProgress JSON."""
    progress = {
        "id": QUERY_ID, "runId": "0b5d7c0e-1111-2222-3333-444455556666", "name": "marketplace_speed",
        "timestamp": "2026-10-04T12:17:30.001Z", "batchId": batch_id, "batchDuration": 812,
        "numInputRows": 15, "inputRowsPerSecond": 0.5, "processedRowsPerSecond": 18.4,
        "durationMs": {"addBatch": 640, "commitOffsets": 20, "getBatch": 0, "latestOffset": 3,
                       "queryPlanning": 30, "triggerExecution": 812, "walCommit": 25},
        "stateOperators": [{"operatorName": "applyInPandasWithState", "numRowsTotal": 600, "memoryUsedBytes": 123456}],
        "sources": [], "sink": {"description": "ForeachBatchSink"},
    }
    progress.update(overrides)
    return progress


class Cursor:
    def __init__(self, fail: bool = False):
        self.executed, self.fail = [], fail

    def execute(self, sql, params=None):
        if self.fail:
            raise RuntimeError("postgres down")
        self.executed.append((sql, params))


def _factory(cursor: Cursor):
    @contextmanager
    def connection():
        @contextmanager
        def cursor_cm():
            yield cursor
        yield SimpleNamespace(cursor=cursor_cm)
    return connection


def test_a_progress_row_carries_spark_s_numbers_unchanged():
    row = service.progress_row(_progress(7), query_name="marketplace_speed", recorded_at=NOW)

    assert row["query_id"] == QUERY_ID and row["batch_id"] == 7
    assert row["progress_at"] == datetime(2026, 10, 4, 12, 17, 30, 1000, tzinfo=timezone.utc)
    assert row["recorded_at"] == NOW
    assert (row["num_input_rows"], row["input_rows_per_second"], row["processed_rows_per_second"]) == (15, 0.5, 18.4)
    assert (row["batch_duration_ms"], row["trigger_execution_ms"], row["add_batch_ms"]) == (812, 812, 640)
    assert (row["get_batch_ms"], row["latest_offset_ms"], row["query_planning_ms"]) == (0, 3, 30)
    assert (row["wal_commit_ms"], row["commit_offsets_ms"]) == (25, 20)
    assert (row["state_rows_total"], row["state_memory_bytes"]) == (600, 123456)
    assert set(row) == set(service.PROGRESS_COLUMNS)


def test_a_progress_object_is_read_through_its_json():
    """PySpark 4 returns StreamingQueryProgress, whose `json` is Spark's text."""
    wrapped = SimpleNamespace(json=json.dumps(_progress(3)))

    assert service.progress_row(wrapped, query_name="q", recorded_at=NOW)["batch_id"] == 3


def test_a_rate_over_an_empty_interval_is_stored_as_null():
    row = service.progress_row(_progress(1, inputRowsPerSecond=math.nan, processedRowsPerSecond=math.inf),
                               query_name="q", recorded_at=NOW)

    assert row["input_rows_per_second"] is None and row["processed_rows_per_second"] is None


def test_a_stateless_progress_has_no_state_size():
    row = service.progress_row(_progress(1, stateOperators=[]), query_name="q", recorded_at=NOW)

    assert row["state_rows_total"] is None and row["state_memory_bytes"] is None


def test_each_batch_is_inserted_once_across_repeated_polls():
    """recentProgress repeats the last hundred updates on every poll."""
    cursor = Cursor()
    recorder = service.ProgressRecorder(_factory(cursor), query_name="marketplace_speed", clock=lambda: NOW)

    assert recorder.record([_progress(1), _progress(2)]) == 2
    assert recorder.record([_progress(1), _progress(2)]) == 0
    assert recorder.record([_progress(2), _progress(3)]) == 1

    batch_ids = [params[service.PROGRESS_COLUMNS.index("batch_id")] for _, params in cursor.executed]
    assert batch_ids == [1, 2, 3]
    assert all("ON CONFLICT (query_name, query_id, batch_id) DO NOTHING" in sql for sql, _ in cursor.executed)


def test_a_failed_write_is_retried_on_the_next_poll():
    cursor = Cursor(fail=True)
    recorder = service.ProgressRecorder(_factory(cursor), clock=lambda: NOW)
    query = SimpleNamespace(recentProgress=[_progress(1)])

    service.record_progress_safely(recorder, query)   # must not raise
    cursor.fail = False
    service.record_progress_safely(recorder, query)

    assert [params[service.PROGRESS_COLUMNS.index("batch_id")] for _, params in cursor.executed] == [1]


def test_the_wait_loop_records_progress_on_every_poll():
    calls = []

    class Query:
        def __init__(self):
            self.polls = 0

        def awaitTermination(self, timeout):  # noqa: N802 - the PySpark API
            self.polls += 1
            return self.polls > 3

    service.await_query(Query(), stop=SimpleNamespace(is_set=lambda: False), poll_seconds=0,
                        on_poll=lambda: calls.append("poll"))

    assert calls == ["poll"] * 3
