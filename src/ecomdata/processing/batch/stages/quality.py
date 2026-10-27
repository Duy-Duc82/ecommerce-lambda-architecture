"""Stage ``quality``: judge the run's Gold and star, and write its manifest.

Reads Silver, the crawl audit, the Gold the ``marts`` stage wrote and the star
at the versions this run built, evaluates every registered rule, records the
results, and writes the run manifest with those star versions (also for a
refused run: a refusal leaves a readable record). The run then moves to
QUALITY_PASSED, QUALITY_FAILED (the runner raises ``QualityGateFailure`` after
recording it) or, for a scratch Gold root or ``--quality-only``, HELD.

A refused run has already merged its dimensions and facts into the shared star,
so the star is restored to the versions the ``current`` manifest certified:
nothing that failed the gate stays where the next run, or a reader, would build on it.
"""
from __future__ import annotations

import json

from pyspark.sql import functions as F

from ecomdata.domain.batch_run import BatchRunStatus as S
from ecomdata.domain.datasets import MART_NAMES, STAR_NAMES
from ecomdata.processing.batch.context import QUALITY_ONLY, SCRATCH_GOLD_ROOT, GoldWriteResult
from ecomdata.processing.batch.inputs import deduplicate_observations, observations_as_of
from ecomdata.processing.batch.manifest import build_gold_manifest, read_current_manifest, write_run_manifest
from ecomdata.processing.batch.quality import QualityGateFailure, decide, evaluate_quality_gates
from ecomdata.processing.batch.stages.base import Outcome, Stage, StageContext
from ecomdata.processing.batch.stages.facts import built_star_versions
from ecomdata.processing.batch.stages.marts import load_judged
from ecomdata.processing.batch.stages.marts import summary_key as marts_summary_key
from ecomdata.processing.batch.stages.silver import OBSERVATIONS, QUARANTINE, SILVER_ZONE


class QualityStage(Stage):
    name = "quality"
    requires = ("silver/offer_observations_v2", "gold/marketplace/runs/<run>", "silver/_runs/<run>/marts.json")
    produces = ("gold/marketplace/manifests/<run>/manifest.json",)
    allowed_from = frozenset({S.GOLD_WRITTEN})
    milestone = S.QUALITY_PASSED

    def execute(self, ctx: StageContext) -> Outcome:
        run, options = ctx.run, ctx.options
        judged, attempts, runs = load_judged(ctx)
        observations = deduplicate_observations(judged).cache()
        versions = built_star_versions(ctx)
        try:
            marts = {name: ctx.io.read_gold(run, name) for name in MART_NAMES}
            star = {name: ctx.io.read_dw(run, name, versions.get(name)) for name in STAR_NAMES}
            repeated = self._repeated_observation_ids(ctx)
            results = evaluate_quality_gates(
                observations,
                marts,
                attempts,
                runs,
                run,
                duplicate_observation_ids=repeated,
                accounted_rows=self._accounted_rows(ctx),
                star=star,
            )
            writes = {name: GoldWriteResult(name, run.gold_dataset_uri(name), frame.count()) for name, frame in marts.items()}
        finally:
            observations.unpersist()
        decision = decide(results, run)
        # Its own write, before the decision is acted on: the evidence for a refused run is the point of the gate.
        ctx.quality.record(results, run_id=run.run_id)

        previous = read_current_manifest(reader=ctx.store.get)
        # A pointer that already names this run is not its own predecessor: keep the one it recorded.
        previous_run_id = (
            None if previous is None else previous.previous_run_id if previous.run_id == run.run_id else previous.run_id
        )
        manifest = build_gold_manifest(writes, decision, run, previous_run_id=previous_run_id, dw_versions=versions)
        scratch = options.is_scratch(run)
        # A scratch run's manifest would land beside production's under a run_id that may be production's too.
        manifest_uri = None if scratch else write_run_manifest(manifest, writer=ctx.store.put)
        detail = {
            "passed": decision.passed,
            "mandatory_failures": decision.mandatory_failures,
            "advisory_failures": decision.advisory_failures,
            "skipped": decision.skipped,
        }
        columns = {
            "manifest_uri": manifest_uri,
            "quality_status": "PASS" if decision.passed else "FAIL",
            "mandatory_failure_count": decision.mandatory_failures,
        }
        if not decision.passed:
            # A scratch run's star has its own root and nothing certified it: leave it for inspection.
            if not scratch:
                detail["star_restored"] = self._restore_star(ctx, previous, versions)
            return Outcome(S.QUALITY_FAILED, columns, detail, failure=QualityGateFailure(decision, results))
        if scratch:
            return Outcome(S.HELD, {**columns, "held_reason": SCRATCH_GOLD_ROOT}, detail, via=(S.QUALITY_PASSED,))
        if options.quality_only:
            # Inspect a suspect window without touching the serving version.
            return Outcome(S.HELD, {**columns, "held_reason": QUALITY_ONLY}, detail, via=(S.QUALITY_PASSED,))
        return Outcome(S.QUALITY_PASSED, columns, detail)

    @staticmethod
    def _restore_star(ctx: StageContext, certified, built: dict[str, int | None]) -> dict[str, int | None]:
        """Put every star table back at the version the current manifest certified; table -> new version.

        A table without a certified version (no manifest yet, or one from before
        schema v2) stays as built, recorded as ``None``: there is nothing older
        that was judged good.
        """
        known = {} if certified is None else dict(certified.dw_versions)
        restored: dict[str, int | None] = {}
        for name in STAR_NAMES:
            target = known.get(name)
            if target is None:
                restored[name] = None
            elif built.get(name) == target:
                restored[name] = target
            else:
                restored[name] = ctx.io.restore_dw(ctx.run, name, target)
        return restored

    @staticmethod
    def _accounted_rows(ctx: StageContext):
        """Per crawl run: the listing rows Silver holds, sponsored ones included, plus the quarantined ones."""
        cut = observations_as_of(ctx.io.read_silver(OBSERVATIONS), ctx.run.as_of)
        valid = cut.groupBy("crawl_run_id").agg(
            F.count(F.lit(1)).alias("_silver_rows"), F.max("observed_at").alias("_last_observed_at")
        )
        quarantined = (
            ctx.io.read_silver(QUARANTINE)
            .filter(F.col("fetched_at") <= F.lit(ctx.run.as_of))
            .groupBy("crawl_run_id")
            .agg(F.count(F.lit(1)).alias("_silver_rows"), F.max("fetched_at").alias("_last_observed_at"))
        )
        return (
            valid.unionByName(quarantined)
            .groupBy("crawl_run_id")
            .agg(F.sum("_silver_rows").alias("_silver_rows"), F.max("_last_observed_at").alias("_last_observed_at"))
        )

    @staticmethod
    def _repeated_observation_ids(ctx: StageContext) -> int:
        payload = ctx.store.get(SILVER_ZONE, marts_summary_key(ctx.run.run_id))
        if payload is None:
            raise FileNotFoundError("the marts stage left no summary: cannot count repeated observation ids")
        return int(json.loads(payload)["duplicates_before_dedup"]["observation_id"])
