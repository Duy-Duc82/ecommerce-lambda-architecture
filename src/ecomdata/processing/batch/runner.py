"""Run one stage, several, or all of them over one batch run.

The runner owns the state machine's bookkeeping: it opens the run's audit row,
refuses a stage the run's status does not allow, treats a stage whose milestone
the run already passed as a no-op (unless ``force``), records each stage's
outcome, marks a crashed run FAILED, and raises ``QualityGateFailure`` after a
refused run has been recorded. The batch lock is held for the whole invocation,
so ``--all`` and a single stage take it the same way.
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ecomdata.domain.batch_run import BatchRunStatus as S
from ecomdata.domain.batch_run import reached
from ecomdata.processing.batch.context import MarketplaceBatchContext, MarketplaceBatchResult
from ecomdata.processing.batch.manifest import read_current_manifest
from ecomdata.processing.batch.ports import (
    AuditRepo,
    CachePublisher,
    CrawlAuditReader,
    LakeIO,
    ObjectStore,
    QualityRepo,
    RunRecord,
    SparkProvider,
)
from ecomdata.processing.batch.stages.base import RunOptions, Stage, StageContext, StageNotAllowed, StageResult
from ecomdata.processing.batch.stages.dimensions import DimensionsStage
from ecomdata.processing.batch.stages.facts import FactsStage
from ecomdata.processing.batch.stages.marts import MartsStage
from ecomdata.processing.batch.stages.promote import PromoteStage
from ecomdata.processing.batch.stages.publish import PublishStage
from ecomdata.processing.batch.stages.quality import QualityStage
from ecomdata.processing.batch.stages.silver import SilverStage


class RunNeedsResume(RuntimeError):
    """The run crashed earlier; going on needs an explicit ``--resume``."""


def default_stages() -> tuple[Stage, ...]:
    return (SilverStage(), DimensionsStage(), FactsStage(), MartsStage(), QualityStage(), PublishStage(), PromoteStage())


@dataclass
class BatchDependencies:
    """What a batch run talks to: the composition root injects the real ones, a test the in-memory ones."""

    audit: AuditRepo
    quality: QualityRepo
    store: ObjectStore
    io: LakeIO
    spark_provider: SparkProvider
    crawl_audit: CrawlAuditReader
    publisher: CachePublisher | None = None
    lock: Callable[[], AbstractContextManager[Any]] | None = None
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)


@dataclass
class RunReport:
    record: RunRecord
    stages: list[StageResult] = field(default_factory=list)

    def result(self) -> MarketplaceBatchResult:
        record = self.record
        return MarketplaceBatchResult(
            run_id=record.run_id,
            status=record.status.value,
            silver_rows=record.silver_rows or 0,
            dataset_counts=dict(record.dataset_counts),
            quality_status=record.quality_status or "PASS",
            mandatory_failure_count=record.mandatory_failure_count or 0,
            manifest_uri=record.manifest_uri,
            manifest_promoted=record.manifest_promoted,
            promotion_reason=record.held_reason,
        )


def select_stages(names: Sequence[str] | None, stages: Sequence[Stage] | None = None) -> list[Stage]:
    """The stages to run, in pipeline order; ``None`` is all of them."""
    available = list(stages or default_stages())
    if not names:
        return available
    by_name = {stage.name: stage for stage in available}
    unknown = [name for name in names if name not in by_name]
    if unknown:
        raise ValueError(f"unknown stage(s) {unknown}; stages are {[stage.name for stage in available]}")
    return [stage for stage in available if stage.name in set(names)]


def run_batch(
    context: MarketplaceBatchContext,
    *,
    deps: BatchDependencies,
    stages: Sequence[Stage] | None = None,
    options: RunOptions | None = None,
    resume: bool = False,
) -> RunReport:
    with (deps.lock or nullcontext)():
        return _run(context, deps, list(stages or default_stages()), options or RunOptions(), resume)


def _run(
    context: MarketplaceBatchContext, deps: BatchDependencies, stages: list[Stage], options: RunOptions, resume: bool
) -> RunReport:
    audit, now = deps.audit, deps.clock
    record = audit.get(context.run_id)
    if record is None:
        pointer = read_current_manifest(reader=deps.store.get)
        record = audit.open_run(
            context, now(), expected_current_run_id=None if pointer is None else pointer.run_id
        )
    else:
        record = audit.open_run(context, now(), expected_current_run_id=record.expected_current_run_id)
    if record.status in (S.FAILED, S.QUALITY_FAILED) and resume:
        record = audit.reopen(context.run_id, now())
    elif record.status is S.FAILED:
        raise RunNeedsResume(f"run {context.run_id} failed earlier; pass --resume to continue it")

    report = RunReport(record)
    for stage in stages:
        record = audit.get(context.run_id)
        report.record = record
        done = record.last_completed_status if record.status in (S.HELD, S.QUALITY_FAILED) else record.status
        if reached(done, stage.milestone) and not options.force:
            report.stages.append(StageResult(stage.name, record.status, skipped=True))
            continue
        forced = options.force and reached(done, stage.milestone)
        if not forced and record.status not in stage.allowed_from:
            raise StageNotAllowed(stage.name, record.status, stage.allowed_from)
        ctx = StageContext(
            run=context,
            record=record,
            options=options,
            audit=audit,
            store=deps.store,
            io=deps.io,
            quality=deps.quality,
            spark_provider=deps.spark_provider,
            crawl_audit=deps.crawl_audit,
            publisher=deps.publisher,
            clock=now,
        )
        try:
            outcome = stage.execute(ctx)
            if forced or outcome.status is record.status:
                audit.set_columns(context.run_id, **outcome.columns)
            else:
                for step in outcome.via:
                    audit.advance(context.run_id, step, now())
                audit.advance(context.run_id, outcome.status, now(), **outcome.columns)
        except Exception as error:
            _mark_failed(audit, context.run_id, now, error)
            raise
        record = audit.get(context.run_id)
        report.record = record
        report.stages.append(StageResult(stage.name, record.status, outcome.detail))
        if outcome.failure is not None:
            raise outcome.failure
        if record.status is S.HELD:
            # A held run goes no further in this invocation: the operator decides next.
            break
    return report


def _mark_failed(audit: AuditRepo, run_id: str, now: Callable[[], datetime], error: BaseException) -> None:
    try:
        audit.fail(run_id, now(), error)
    except Exception:
        # The original error is the one to report; a run that cannot be marked stays as it was.
        pass
