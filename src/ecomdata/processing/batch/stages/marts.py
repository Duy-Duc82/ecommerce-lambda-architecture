"""Stage ``marts``: the star to the ten Gold marts of one run.

Reads ``fact_offer_observation`` and its dimensions from ``dw/`` (built by the
``dimensions`` and ``facts`` stages) and the crawl audit as it stood at the
run's ``as_of``, drops sponsored rows, and writes each mart under
``gold/marketplace/runs/run_id=<id>/``. The marts keep their names, columns and
rules: the star's sightings are given back in the canonical columns
(``star_sightings``) and the same builders run on them. Silver is still read for
one number, the repeated ``observation_id`` count before deduplication, which the
quality rule ``observation_id_unique`` reads and the star, one row per id, cannot
show.
"""
from __future__ import annotations

import json

from ecomdata.domain.batch_run import BatchRunStatus as S
from ecomdata.domain.datasets import BRIDGE, DIM, MART_NAMES, datasets_of_kind
from ecomdata.processing.batch.inputs import (
    crawl_audit_as_of,
    duplicate_counts,
    judged_observations,
    observations_as_of,
    organic_only,
)
from ecomdata.processing.batch.marts import build_marketplace_marts
from ecomdata.processing.batch.stages.base import Outcome, Stage, StageContext
from ecomdata.processing.batch.stages.silver import OBSERVATIONS, RUNS_DIRECTORY, SILVER_ZONE
from ecomdata.processing.batch.star_sightings import sightings_from_facts


def summary_key(run_id: str) -> str:
    return f"marketplace/{RUNS_DIRECTORY}/run_id={run_id}/marts.json"


def load_judged(ctx: StageContext):
    """The deduplicated organic observations of the run and the crawl audit, both cut at ``as_of``."""
    judged = judged_observations(ctx.io.read_silver(OBSERVATIONS), ctx.run.as_of)
    attempts, runs = crawl_audit_as_of(*ctx.crawl_audit(ctx.spark), ctx.run.as_of)
    return judged, attempts, runs


class MartsStage(Stage):
    name = "marts"
    requires = (
        f"silver/{OBSERVATIONS}",
        "gold/marketplace/dw/fact_offer_observation",
        "gold/marketplace/dw/<dimensions>",
    )
    produces = tuple(f"gold/marketplace/runs/<run>/{name}" for name in MART_NAMES) + ("silver/_runs/<run>/marts.json",)
    allowed_from = frozenset({S.FACTS_BUILT})
    milestone = S.GOLD_WRITTEN

    def execute(self, ctx: StageContext) -> Outcome:
        run = ctx.run
        judged, attempts, runs = load_judged(ctx)
        judged = judged.cache()
        try:
            duplicates = duplicate_counts(judged)
        finally:
            judged.unpersist()
        dims = {d.name: ctx.io.read_dw(run, d.name) for d in datasets_of_kind(DIM, BRIDGE)}
        fact = observations_as_of(ctx.io.read_dw(run, "fact_offer_observation"), run.as_of)
        observations = organic_only(sightings_from_facts(fact, dims)).cache()
        try:
            marts = build_marketplace_marts(observations, attempts, runs, run)
            writes = {name: ctx.io.write_gold(run, name, frame) for name, frame in marts.items()}
            silver_rows = observations.count()
        finally:
            observations.unpersist()
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
