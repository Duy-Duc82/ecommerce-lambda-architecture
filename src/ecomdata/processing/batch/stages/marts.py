"""Stage ``marts``: Silver v2 to the ten Gold marts of one run.

Reads ``offer_observations_v2`` and the crawl audit as they stood at the run's
``as_of``, drops sponsored rows, counts the repeated ``observation_id`` before
deduplicating (the quality rule ``observation_id_unique`` reads that count), and
writes each mart under ``gold/marketplace/runs/run_id=<id>/``.
"""
from __future__ import annotations

import json

from ecomdata.domain.batch_run import BatchRunStatus as S
from ecomdata.domain.datasets import MART_NAMES
from ecomdata.processing.batch.inputs import (
    crawl_audit_as_of,
    deduplicate_observations,
    duplicate_counts,
    judged_observations,
)
from ecomdata.processing.batch.marts import build_marketplace_marts
from ecomdata.processing.batch.stages.base import Outcome, Stage, StageContext
from ecomdata.processing.batch.stages.silver import OBSERVATIONS, RUNS_DIRECTORY, SILVER_ZONE


def summary_key(run_id: str) -> str:
    return f"marketplace/{RUNS_DIRECTORY}/run_id={run_id}/marts.json"


def load_judged(ctx: StageContext):
    """The deduplicated organic observations of the run and the crawl audit, both cut at ``as_of``."""
    judged = judged_observations(ctx.io.read_silver(OBSERVATIONS), ctx.run.as_of)
    attempts, runs = crawl_audit_as_of(*ctx.crawl_audit(ctx.spark), ctx.run.as_of)
    return judged, attempts, runs


class MartsStage(Stage):
    name = "marts"
    requires = (f"silver/{OBSERVATIONS}",)
    produces = tuple(f"gold/marketplace/runs/<run>/{name}" for name in MART_NAMES) + ("silver/_runs/<run>/marts.json",)
    allowed_from = frozenset({S.DIMENSIONS_BUILT})
    milestone = S.GOLD_WRITTEN

    def execute(self, ctx: StageContext) -> Outcome:
        run = ctx.run
        judged, attempts, runs = load_judged(ctx)
        judged = judged.cache()
        try:
            duplicates = duplicate_counts(judged)
            observations = deduplicate_observations(judged).cache()
            try:
                marts = build_marketplace_marts(observations, attempts, runs, run)
                writes = {name: ctx.io.write_gold(run, name, frame) for name, frame in marts.items()}
                silver_rows = observations.count()
            finally:
                observations.unpersist()
        finally:
            judged.unpersist()
        counts = {name: result.row_count for name, result in writes.items()}
        summary = {
            "run_id": run.run_id,
            "as_of": run.as_of.isoformat(),
            "silver_rows": silver_rows,
            "duplicates_before_dedup": {k: v for k, v in duplicates.items() if k != "rows"},
            "dataset_counts": counts,
        }
        ctx.store.put(SILVER_ZONE, summary_key(run.run_id), json.dumps(summary, sort_keys=True).encode("utf-8"))
        return Outcome(
            S.GOLD_WRITTEN,
            columns={"gold_run_uri": run.gold_run_uri, "silver_rows": silver_rows, "dataset_counts": counts},
            detail=summary,
        )
