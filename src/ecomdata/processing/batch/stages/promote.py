"""Stage ``promote``: move the ``current`` pointer to the run's manifest.

The pointer is replaced by one write of the whole manifest, guarded by a
compare-and-swap on the pointer's run id as it stood when the run was created
(``expected_current_run_id``). A run that cannot take the pointer (it lost a
race, or it is an older window and ``--allow-backfill`` is off) is HELD with the
reason, never PROMOTED: PROMOTED means the pointer names the run.
"""
from __future__ import annotations

from ecomdata.domain.batch_run import BatchRunStatus as S
from ecomdata.processing.batch.manifest import ALREADY_CURRENT, promote_manifest
from ecomdata.processing.batch.stages.base import Outcome, Stage, StageContext
from ecomdata.processing.batch.stages.publish import load_run_manifest, still_held


class PromoteStage(Stage):
    name = "promote"
    requires = ("gold/marketplace/manifests/<run>/manifest.json",)
    produces = ("gold/marketplace/current.json",)
    allowed_from = frozenset({S.PUBLISHED, S.HELD})
    milestone = S.PROMOTED

    def execute(self, ctx: StageContext) -> Outcome:
        if still_held(ctx):
            return Outcome(S.HELD, {"held_reason": ctx.record.held_reason})
        manifest = load_run_manifest(ctx)
        result = promote_manifest(
            manifest,
            writer=ctx.store.put,
            reader=ctx.store.get,
            allow_backfill=ctx.options.allow_backfill,
            expected_current_run_id=self._expected(ctx, manifest.run_id),
        )
        detail = {"promoted": result.promoted, "reason": result.reason, "manifest_uri": result.manifest_uri}
        if result.promoted or result.reason == ALREADY_CURRENT:
            return Outcome(
                S.PROMOTED,
                {"manifest_promoted": True, "manifest_uri": result.manifest_uri or ctx.record.manifest_uri, "held_reason": None},
                detail,
            )
        return Outcome(S.HELD, {"manifest_promoted": False, "held_reason": result.reason}, detail)

    @staticmethod
    def _expected(ctx: StageContext, run_id: str) -> str | None:
        """The pointer's run id the swap expects: the one recorded at creation, or this run (a promotion whose reply was lost)."""
        from ecomdata.processing.batch.manifest import read_current_manifest

        current = read_current_manifest(reader=ctx.store.get)
        if current is not None and current.run_id == run_id:
            return run_id
        return ctx.record.expected_current_run_id
