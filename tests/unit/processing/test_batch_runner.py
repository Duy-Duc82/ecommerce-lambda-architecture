"""The batch runner and the stages that need no Spark: state machine bookkeeping, publish, promote.

Every collaborator is an in-memory one (``processing/batch/memory.py``) or a recording fake; the Spark
stages (silver, dimensions, marts, quality) are exercised in ``tests/spark``.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime

import pytest

from ecomdata.domain.batch_run import BatchRunStatus as S
from ecomdata.domain.datasets import MART_NAMES
from ecomdata.processing.batch.context import QUALITY_ONLY, GoldWriteResult, MarketplaceBatchContext
from ecomdata.processing.batch.manifest import (
    BACKFILL_REFUSED,
    CURRENT_POINTER_PATH,
    GOLD_ZONE,
    PROMOTION_CONFLICT,
    build_gold_manifest,
    promote_manifest,
    read_current_manifest,
    run_manifest_path,
    serialize_manifest,
)
from ecomdata.processing.batch.memory import MemoryAudit, MemoryQuality, MemoryStore, RunContextMismatch
from ecomdata.processing.batch.quality import QualityDecision, QualityGateFailure
from ecomdata.processing.batch.runner import (
    BatchDependencies,
    RunNeedsResume,
    default_stages,
    run_batch,
    select_stages,
)
from ecomdata.processing.batch.stages.base import Outcome, RunOptions, Stage, StageNotAllowed
from ecomdata.processing.batch.stages.promote import PromoteStage
from ecomdata.processing.batch.stages.publish import PublishStage

AS_OF = datetime(2026, 10, 25, 10, tzinfo=UTC)
LATER = datetime(2026, 10, 25, 11, tzinfo=UTC)


def context(run_id="run-1", as_of=AS_OF):
    return MarketplaceBatchContext(run_id, as_of, "s3a://silver/marketplace", "s3a://gold/marketplace")


class StubStage(Stage):
    def __init__(self, name, allowed, target, *, columns=None, fail=None, failure=None, via=()):
        self.via = via
        self.name = name
        self.allowed_from = frozenset(allowed)
        self.milestone = target
        self.target = target
        self.columns = columns or {}
        self.fail = fail
        self.failure = failure
        self.runs = 0

    def execute(self, ctx):
        self.runs += 1
        if self.fail is not None:
            raise self.fail
        return Outcome(self.target, self.columns, failure=self.failure, via=self.via)


def chain():
    return [
        StubStage("silver", {S.CREATED}, S.SILVER_BUILT),
        StubStage("dimensions", {S.SILVER_BUILT}, S.DIMENSIONS_BUILT),
        StubStage("marts", {S.DIMENSIONS_BUILT}, S.GOLD_WRITTEN),
        StubStage("quality", {S.GOLD_WRITTEN}, S.QUALITY_PASSED),
        StubStage("publish", {S.QUALITY_PASSED, S.HELD}, S.PUBLISHED),
        StubStage("promote", {S.PUBLISHED, S.HELD}, S.PROMOTED),
    ]


class Lock:
    def __init__(self, refuse=False):
        self.log = []
        self.refuse = refuse

    @contextmanager
    def __call__(self):
        if self.refuse:
            raise RuntimeError("another batch holds the lock")
        self.log.append("acquire")
        try:
            yield
        finally:
            self.log.append("release")


def deps(store=None, audit=None, lock=None, publisher=None, io=None):
    return BatchDependencies(
        audit=audit or MemoryAudit(),
        quality=MemoryQuality(),
        store=store or MemoryStore(),
        io=io,
        spark_provider=lambda: None,
        crawl_audit=lambda spark: (None, None),
        publisher=publisher,
        lock=lock,
        clock=lambda: LATER,
    )


def test_all_stages_walk_the_state_machine_in_order():
    d = deps()

    report = run_batch(context(), deps=d, stages=chain())

    assert [status for _, status in d.audit.history] == [
        S.CREATED, S.SILVER_BUILT, S.DIMENSIONS_BUILT, S.GOLD_WRITTEN, S.QUALITY_PASSED, S.PUBLISHED, S.PROMOTED,
    ]  # fmt: skip
    assert report.result().status == "PROMOTED"
    assert [item.stage for item in report.stages] == ["silver", "dimensions", "marts", "quality", "publish", "promote"]


def test_each_stage_runs_alone_and_the_run_keeps_its_place():
    d, stages = deps(), chain()

    run_batch(context(), deps=d, stages=select_stages(["silver"], stages))
    assert d.audit.get("run-1").status is S.SILVER_BUILT
    run_batch(context(), deps=d, stages=select_stages(["dimensions"], stages))
    assert d.audit.get("run-1").status is S.DIMENSIONS_BUILT
    run_batch(context(), deps=d, stages=select_stages(["marts"], stages))
    assert d.audit.get("run-1").status is S.GOLD_WRITTEN


def test_a_stage_the_run_cannot_start_from_is_refused():
    d = deps()

    with pytest.raises(StageNotAllowed, match="quality cannot run on a CREATED run"):
        run_batch(context(), deps=d, stages=select_stages(["quality"], chain()))


def test_running_a_finished_stage_again_is_a_noop():
    d, stages = deps(), chain()
    run_batch(context(), deps=d, stages=stages)

    report = run_batch(context(), deps=d, stages=stages)

    assert [stage.runs for stage in stages] == [1] * 6
    assert all(item.skipped for item in report.stages)


def test_force_runs_the_stage_again_without_moving_the_status():
    d, stages = deps(), chain()
    run_batch(context(), deps=d, stages=stages)

    run_batch(context(), deps=d, stages=select_stages(["marts"], stages), options=RunOptions(force=True))

    assert stages[2].runs == 2
    assert d.audit.get("run-1").status is S.PROMOTED


def test_a_crash_marks_the_run_failed_and_needs_resume():
    d, stages = deps(), chain()
    stages[3].fail = RuntimeError("spark died")

    with pytest.raises(RuntimeError, match="spark died"):
        run_batch(context(), deps=d, stages=stages)
    record = d.audit.get("run-1")
    assert record.status is S.FAILED and "spark died" in record.error_message
    assert record.last_completed_status is S.GOLD_WRITTEN

    with pytest.raises(RunNeedsResume):
        run_batch(context(), deps=d, stages=stages)

    stages[3].fail = None
    run_batch(context(), deps=d, stages=stages, resume=True)
    # Only the stage that crashed (and those after it) ran again.
    assert [stage.runs for stage in stages] == [1, 1, 1, 2, 1, 1]
    assert d.audit.get("run-1").status is S.PROMOTED


def test_a_refused_run_is_recorded_before_the_refusal_propagates():
    d, stages = deps(), chain()
    decision = QualityDecision("run-1", False, "quality-rules.v2", AS_OF, 14, 2, 0, 0)
    stages[3].target = S.QUALITY_FAILED
    stages[3].failure = QualityGateFailure(decision, ())
    stages[3].columns = {"quality_status": "FAIL", "mandatory_failure_count": 2}

    with pytest.raises(QualityGateFailure):
        run_batch(context(), deps=d, stages=stages)

    record = d.audit.get("run-1")
    assert record.status is S.QUALITY_FAILED
    assert (record.quality_status, record.mandatory_failure_count) == ("FAIL", 2)
    assert [stage.runs for stage in stages] == [1, 1, 1, 1, 0, 0]
    # Publication is not reachable from a refused run, and a stage after it is refused, not skipped.
    with pytest.raises(StageNotAllowed):
        run_batch(context(), deps=d, stages=select_stages(["publish"], stages))


def test_a_refused_run_can_be_resumed_to_judge_again():
    d, stages = deps(), chain()
    stages[3].target, stages[3].failure = S.QUALITY_FAILED, QualityGateFailure(
        QualityDecision("run-1", False, "quality-rules.v2", AS_OF, 14, 1, 0, 0), ()
    )
    with pytest.raises(QualityGateFailure):
        run_batch(context(), deps=d, stages=stages)

    stages[3].target, stages[3].failure = S.QUALITY_PASSED, None
    run_batch(context(), deps=d, stages=stages, resume=True)

    assert d.audit.get("run-1").status is S.PROMOTED
    assert [stage.runs for stage in stages] == [1, 1, 1, 2, 1, 1]


def test_a_held_run_goes_no_further_in_this_invocation():
    d, stages = deps(), chain()
    stages[3].target = S.HELD
    stages[3].via = (S.QUALITY_PASSED,)
    stages[3].columns = {"held_reason": QUALITY_ONLY}

    report = run_batch(context(), deps=d, stages=stages)

    assert d.audit.get("run-1").status is S.HELD
    assert d.audit.get("run-1").held_reason == QUALITY_ONLY
    assert [stage.runs for stage in stages] == [1, 1, 1, 1, 0, 0]
    assert report.result().promotion_reason == QUALITY_ONLY


def test_the_run_remembers_the_pointer_it_started_against():
    store = MemoryStore()
    store.put(GOLD_ZONE, CURRENT_POINTER_PATH, serialize_manifest(manifest("run-0", AS_OF)))
    d = deps(store=store)

    run_batch(context("run-1"), deps=d, stages=select_stages(["silver"], chain()))

    assert d.audit.get("run-1").expected_current_run_id == "run-0"


def test_with_no_pointer_the_run_expects_none():
    d = deps()

    run_batch(context(), deps=d, stages=select_stages(["silver"], chain()))

    assert d.audit.get("run-1").expected_current_run_id is None


def test_an_existing_run_of_another_context_is_refused():
    d = deps()
    run_batch(context(), deps=d, stages=select_stages(["silver"], chain()))

    with pytest.raises(RunContextMismatch):
        run_batch(context(as_of=LATER), deps=d, stages=select_stages(["marts"], chain()))


def test_the_lock_covers_the_whole_invocation():
    lock, d = Lock(), None
    d = deps(lock=lock)

    run_batch(context(), deps=d, stages=chain())

    assert lock.log == ["acquire", "release"]


def test_a_run_refused_by_the_lock_starts_nothing():
    d = deps(lock=Lock(refuse=True))
    stages = chain()

    with pytest.raises(RuntimeError, match="lock"):
        run_batch(context(), deps=d, stages=stages)

    assert d.audit.runs == {}
    assert [stage.runs for stage in stages] == [0] * 6


def test_unknown_stage_names_are_refused():
    with pytest.raises(ValueError, match="unknown stage"):
        select_stages(["bogus"])


def test_the_default_pipeline_is_silver_dimensions_marts_quality_publish_promote():
    assert [stage.name for stage in default_stages()] == ["silver", "dimensions", "marts", "quality", "publish", "promote"]
    stages = default_stages()
    # Each stage starts where the previous one ends: the chain has no gap.
    assert [stage.milestone for stage in stages] == [
        S.SILVER_BUILT, S.DIMENSIONS_BUILT, S.GOLD_WRITTEN, S.QUALITY_PASSED, S.PUBLISHED, S.PROMOTED,
    ]  # fmt: skip


# --- publish and promote on in-memory storage ------------------------------------------------------------------


def writes_for(run_id):
    return {name: GoldWriteResult(name, f"s3a://gold/marketplace/runs/run_id={run_id}/{name}", 1) for name in MART_NAMES}


def decision(run_id, passed=True):
    return QualityDecision(run_id, passed, "quality-rules.v2", AS_OF, 14, 0 if passed else 1, 0, 0)


def manifest(run_id, as_of, *, previous=None, passed=True):
    return build_gold_manifest(
        writes_for(run_id), decision(run_id, passed), context(run_id, as_of), previous_run_id=previous
    )


class FakePublisher:
    def __init__(self, log):
        self.log = log

    def stage(self, marts, *, run_id):
        self.log.append(("stage", sorted(marts)))
        return ["staged"]

    def publish(self, staged, *, run_id, published_at, quality, manifest_uri):
        self.log.append(("publish", run_id, manifest_uri, quality.passed))

    def cleanup(self, staged):
        self.log.append(("cleanup",))


class FakeIO:
    def read_gold(self, run, name):
        return f"frame:{name}"


class LoggingStore(MemoryStore):
    def __init__(self, log):
        super().__init__()
        self.log = log

    def put(self, zone, key, data):
        self.log.append(("put", key))
        return super().put(zone, key, data)


def serving(store, run_id, as_of, **kw):
    store.put(GOLD_ZONE, CURRENT_POINTER_PATH, serialize_manifest(manifest(run_id, as_of, **kw)))


def prepared(run_id="run-1", as_of=AS_OF, *, log=None, previous_pointer=None, passed=True):
    """A run at QUALITY_PASSED, its manifest written, ready for publish and promote."""
    log = [] if log is None else log
    store = LoggingStore(log)
    if previous_pointer:
        serving(store, *previous_pointer)
    audit = MemoryAudit()
    ctx = context(run_id, as_of)
    pointer = read_current_manifest(reader=store.get)
    audit.open_run(ctx, LATER, expected_current_run_id=None if pointer is None else pointer.run_id)
    uri = store.put(GOLD_ZONE, run_manifest_path(run_id), serialize_manifest(manifest(run_id, as_of, passed=passed)))
    for status in (S.SILVER_BUILT, S.DIMENSIONS_BUILT, S.GOLD_WRITTEN, S.QUALITY_PASSED):
        audit.advance(run_id, status, LATER, **({"manifest_uri": uri} if status is S.QUALITY_PASSED else {}))
    log.clear()
    d = deps(store=store, audit=audit, publisher=FakePublisher(log), io=FakeIO())
    return d, ctx, log


def test_the_cache_is_published_before_the_pointer_moves():
    d, ctx, log = prepared()

    report = run_batch(ctx, deps=d, stages=[PublishStage(), PromoteStage()])

    kinds = [entry[0] if entry[0] != "put" else entry[1] for entry in log]
    assert kinds.index("publish") < kinds.index(CURRENT_POINTER_PATH)
    assert report.result().status == "PROMOTED"
    assert d.audit.get("run-1").cache_published and d.audit.get("run-1").manifest_promoted
    assert read_current_manifest(reader=d.store.get).run_id == "run-1"
    # The publisher got the quality verdict the manifest recorded and the manifest that admitted the cache.
    assert ("publish", "run-1", d.audit.get("run-1").manifest_uri, True) in log


def test_a_publication_failure_leaves_the_pointer_alone():
    d, ctx, log = prepared()

    def boom(*args, **kwargs):
        raise RuntimeError("postgres down")

    d.publisher.publish = boom

    with pytest.raises(RuntimeError, match="postgres down"):
        run_batch(ctx, deps=d, stages=[PublishStage(), PromoteStage()])

    assert read_current_manifest(reader=d.store.get) is None
    assert d.audit.get("run-1").status is S.FAILED
    # The staged tables are cleaned up even though publishing failed.
    assert ("cleanup",) in log


def test_without_a_cache_the_run_is_still_promoted():
    d, ctx, log = prepared()
    d.publisher = None

    report = run_batch(ctx, deps=d, stages=[PublishStage(), PromoteStage()], options=RunOptions(publish_cache=False))

    assert report.result().status == "PROMOTED"
    assert not d.audit.get("run-1").cache_published


def test_a_refused_backfill_is_held_before_the_cache_is_touched():
    d, ctx, log = prepared("run-old", AS_OF, previous_pointer=("run-new", LATER))

    report = run_batch(ctx, deps=d, stages=[PublishStage(), PromoteStage()])

    record = d.audit.get("run-old")
    assert (record.status, record.held_reason) == (S.HELD, BACKFILL_REFUSED)
    assert not any(entry[0] in ("stage", "publish") for entry in log)
    assert read_current_manifest(reader=d.store.get).run_id == "run-new"
    assert report.result().promotion_reason == BACKFILL_REFUSED


def test_an_allowed_backfill_publishes_then_moves_the_pointer():
    d, ctx, log = prepared("run-old", AS_OF, previous_pointer=("run-new", LATER))
    run_batch(ctx, deps=d, stages=[PublishStage(), PromoteStage()])

    run_batch(ctx, deps=d, stages=[PublishStage(), PromoteStage()], options=RunOptions(allow_backfill=True))

    assert d.audit.get("run-old").status is S.PROMOTED
    assert read_current_manifest(reader=d.store.get).run_id == "run-old"
    assert any(entry[0] == "publish" for entry in log)


def test_a_pointer_that_moved_since_the_run_began_stops_it_before_publication():
    d, ctx, log = prepared("run-2", LATER, previous_pointer=None)
    serving(d.store, "run-other", AS_OF)  # another run promoted after run-2 was created

    run_batch(ctx, deps=d, stages=[PublishStage(), PromoteStage()])

    record = d.audit.get("run-2")
    assert (record.status, record.held_reason) == (S.HELD, PROMOTION_CONFLICT)
    assert not any(entry[0] == "publish" for entry in log)
    assert read_current_manifest(reader=d.store.get).run_id == "run-other"


def test_a_pointer_that_moves_between_publish_and_promote_is_a_held_run_not_a_promotion():
    d, ctx, log = prepared("run-2", LATER)
    run_batch(ctx, deps=d, stages=[PublishStage()])
    serving(d.store, "run-other", AS_OF)

    run_batch(ctx, deps=d, stages=[PromoteStage()])

    record = d.audit.get("run-2")
    assert (record.status, record.held_reason, record.manifest_promoted) == (S.HELD, PROMOTION_CONFLICT, False)
    assert read_current_manifest(reader=d.store.get).run_id == "run-other"


def test_a_pointer_that_already_names_the_run_counts_as_promoted():
    d, ctx, log = prepared("run-1", AS_OF)
    run_batch(ctx, deps=d, stages=[PublishStage()])
    # The promotion's reply was lost: the pointer was written, the audit row was not.
    promote_manifest(
        manifest("run-1", AS_OF), writer=d.store.put, reader=d.store.get, expected_current_run_id=None
    )

    run_batch(ctx, deps=d, stages=[PromoteStage()])

    assert d.audit.get("run-1").status is S.PROMOTED
    assert d.audit.get("run-1").manifest_promoted


def test_quality_only_keeps_a_run_off_the_serving_version_until_it_is_let_through():
    d, ctx, log = prepared()
    d.audit.advance("run-1", S.HELD, LATER, held_reason=QUALITY_ONLY)
    held = RunOptions(quality_only=True)

    run_batch(ctx, deps=d, stages=[PublishStage(), PromoteStage()], options=held)
    assert d.audit.get("run-1").status is S.HELD
    assert not any(entry[0] == "publish" for entry in log)

    run_batch(ctx, deps=d, stages=[PublishStage(), PromoteStage()])
    assert d.audit.get("run-1").status is S.PROMOTED


def test_a_scratch_gold_root_is_held_whatever_the_options_say_about_the_cache():
    d, ctx, log = prepared()
    d.audit.advance("run-1", S.HELD, LATER, held_reason="SCRATCH_GOLD_ROOT")
    scratch = RunOptions(serving_gold_root_uri="s3a://gold/other")

    run_batch(ctx, deps=d, stages=[PublishStage(), PromoteStage()], options=scratch)

    assert d.audit.get("run-1").status is S.HELD
    assert read_current_manifest(reader=d.store.get) is None


def test_the_run_options_know_a_scratch_root_regardless_of_a_trailing_slash():
    ctx = context()
    assert not RunOptions(serving_gold_root_uri="s3a://gold/marketplace/").is_scratch(ctx)
    assert RunOptions(serving_gold_root_uri="s3a://gold/elsewhere").is_scratch(ctx)
    assert not RunOptions().is_scratch(ctx)
