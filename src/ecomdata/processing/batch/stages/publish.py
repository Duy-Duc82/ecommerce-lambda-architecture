"""Stage ``publish``: copy the run's Gold into the PostgreSQL BI cache.

The cache is published before the ``current`` pointer moves (stage ``promote``),
as it was before stages existed: a pointer must never name a Gold run whose
cache was not written. The cache has no notion of time, so the refusal of an
older window is decided here, before anything is published, never discovered
after.
"""
from __future__ import annotations

from ecomdata.domain.batch_run import BatchRunStatus as S
from ecomdata.domain.datasets import MART_NAMES
from ecomdata.processing.batch.context import QUALITY_ONLY, SCRATCH_GOLD_ROOT
from ecomdata.processing.batch.manifest import (
    BACKFILL_REFUSED,
    PROMOTION_CONFLICT,
    parse_manifest,
    promotion_refusal,
    read_current_manifest,
    run_manifest_path,
)
from ecomdata.processing.batch.quality import QualityDecision
from ecomdata.processing.batch.stages.base import Outcome, Stage, StageContext

MANIFEST_ZONE = "gold"


def load_run_manifest(ctx: StageContext):
    payload = ctx.store.get(MANIFEST_ZONE, run_manifest_path(ctx.run.run_id))
    if payload is None:
        raise FileNotFoundError(f"the quality stage left no manifest for run {ctx.run.run_id}")
    return parse_manifest(payload)


def decision_of(manifest) -> QualityDecision:
    """The quality verdict the manifest recorded, as the cache publisher wants it."""
    quality = manifest.quality
    return QualityDecision(
        run_id=manifest.run_id,
        passed=quality["status"] == "PASS",
        rule_version=quality["rule_version"],
        evaluated_at=manifest.as_of,
        mandatory_total=int(quality["mandatory_total"]),
        mandatory_failures=int(quality["mandatory_failures"]),
        advisory_failures=int(quality["advisory_failures"]),
        skipped=int(quality["skipped"]),
    )


def still_held(ctx: StageContext) -> bool:
    """A run held for inspection stays held while the command that held it is still in force."""
    record, options = ctx.record, ctx.options
    if record.status is not S.HELD:
        return False
    if record.held_reason == QUALITY_ONLY:
        return options.quality_only
    return record.held_reason == SCRATCH_GOLD_ROOT and options.is_scratch(ctx.run)


class PublishStage(Stage):
    name = "publish"
    requires = ("gold/marketplace/manifests/<run>/manifest.json", "gold/marketplace/runs/<run>")
    produces = ("cache.marketplace_*",)
    allowed_from = frozenset({S.QUALITY_PASSED, S.HELD})
    milestone = S.PUBLISHED

    def execute(self, ctx: StageContext) -> Outcome:
        run, options = ctx.run, ctx.options
        if still_held(ctx):
            return Outcome(S.HELD, {"held_reason": ctx.record.held_reason})
        manifest = load_run_manifest(ctx)
        previous = read_current_manifest(reader=ctx.store.get)
        if promotion_refusal(manifest, previous, allow_backfill=options.allow_backfill) == BACKFILL_REFUSED:
            return Outcome(S.HELD, {"held_reason": BACKFILL_REFUSED})
        # Checked again before the cache moves: the pointer was read when the run was created, and other
        # runs may have promoted since. A conflict found only at promotion would leave the cache serving
        # this run and the pointer another.
        current_id = None if previous is None else previous.run_id
        if current_id not in (ctx.record.expected_current_run_id, run.run_id):
            return Outcome(S.HELD, {"held_reason": PROMOTION_CONFLICT})

        published = False
        if options.publish_cache:
            publisher = ctx.publisher
            if publisher is None:
                raise RuntimeError("publish_cache is set but no cache publisher was injected")
            marts = {name: ctx.io.read_gold(run, name) for name in MART_NAMES}
            staged = publisher.stage(marts, run_id=run.run_id)
            try:
                publisher.publish(
                    staged,
                    run_id=run.run_id,
                    published_at=run.as_of,
                    quality=decision_of(manifest),
                    manifest_uri=ctx.record.manifest_uri or "",
                )
            finally:
                publisher.cleanup(staged)
            published = True
        return Outcome(S.PUBLISHED, {"cache_published": published, "held_reason": None}, {"cache_published": published})
