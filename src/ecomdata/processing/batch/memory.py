"""In-memory implementations of the batch ports.

Tests inject these in place of PostgreSQL and object storage, and a run with
``--skip-postgres`` uses :class:`MemoryAudit` so its stages still pass through
the state machine without writing an audit row anywhere.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any

from ecomdata.domain.batch_run import PROGRESS, BatchRunStatus, check_transition
from ecomdata.processing.batch.context import MarketplaceBatchContext
from ecomdata.processing.batch.ports import RunRecord


class RunContextMismatch(ValueError):
    pass


class MemoryAudit:
    def __init__(self) -> None:
        self.runs: dict[str, RunRecord] = {}
        self.history: list[tuple[str, BatchRunStatus]] = []

    def get(self, run_id: str) -> RunRecord | None:
        return self.runs.get(run_id)

    def open_run(
        self, context: MarketplaceBatchContext, now: datetime, *, expected_current_run_id: str | None
    ) -> RunRecord:
        existing = self.runs.get(context.run_id)
        if existing is not None:
            if existing.as_of != context.as_of or existing.silver_uri != context.silver_uri:
                raise RunContextMismatch("the run exists with another context")
            return existing
        record = RunRecord(
            context.run_id,
            context.as_of,
            context.silver_uri,
            BatchRunStatus.CREATED,
            BatchRunStatus.CREATED,
            expected_current_run_id=expected_current_run_id,
        )
        self.runs[context.run_id] = record
        self.history.append((context.run_id, record.status))
        return record

    def advance(self, run_id: str, target: BatchRunStatus, now: datetime, **columns: Any) -> RunRecord:
        record = self.runs[run_id]
        check_transition(record.status, target)
        last = target if target in PROGRESS else record.last_completed_status
        columns.setdefault("held_reason", None)
        record = replace(record, status=target, last_completed_status=last, error_message=None, **columns)
        self.runs[run_id] = record
        self.history.append((run_id, target))
        return record

    def reopen(self, run_id: str, now: datetime) -> RunRecord:
        record = self.runs[run_id]
        if record.status not in (BatchRunStatus.FAILED, BatchRunStatus.QUALITY_FAILED):
            return record
        record = replace(record, status=record.last_completed_status, error_message=None)
        self.runs[run_id] = record
        return record

    def fail(self, run_id: str, now: datetime, error: BaseException) -> None:
        record = self.runs[run_id]
        check_transition(record.status, BatchRunStatus.FAILED)
        self.runs[run_id] = replace(record, status=BatchRunStatus.FAILED, error_message=str(error)[:2000])
        self.history.append((run_id, BatchRunStatus.FAILED))

    def set_columns(self, run_id: str, **columns: Any) -> None:
        self.runs[run_id] = replace(self.runs[run_id], **columns)


class MemoryQuality:
    def __init__(self) -> None:
        self.results: dict[str, list[Any]] = {}
        self.decisions: dict[str, Any] = {}

    def record(self, results: Sequence[Any], *, run_id: str) -> None:
        self.results[run_id] = list(results)

    def latest_decision(self, *, run_id: str) -> Any:
        return self.decisions.get(run_id)


class MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put(self, zone: str, key: str, data: bytes) -> str:
        self.objects[(zone, key)] = data
        return f"memory://{zone}/{key}"

    def get(self, zone: str, key: str) -> bytes | None:
        return self.objects.get((zone, key))
