"""Stage ``facts``: Silver and the dimensions to the star's facts in the lake.

Reads what the ``dimensions`` stage read (all of ``offer_observations_v2``, the
wide tables, ``page_context``; not cut at ``as_of``, for the reason given
there), the built ``dim_product_listing`` from ``dw/`` (the facts take their
``listing_key`` from its versions) and the whole crawl audit. It builds the six
facts (``processing/batch/facts.py``) and merges each into
``gold/marketplace/dw/<table>/``, partitioned by ``date_key``. Like the
dimensions, the facts are rebuilt whole from Silver on every run, so a rerun
on the same Silver leaves them as they were; the run's summary records the
Delta version each table reached.
"""
from __future__ import annotations

import json

from ecomdata.domain.batch_run import BatchRunStatus as S
from ecomdata.domain.datasets import FACT, datasets_of_kind
from ecomdata.domain.sources import SOURCES, SourceRegistry
from ecomdata.processing.batch.facts import FactRules, build_marketplace_facts
from ecomdata.processing.batch.inputs import deduplicate_observations
from ecomdata.processing.batch.stages.base import Outcome, Stage, StageContext
from ecomdata.processing.batch.stages.silver import OBSERVATIONS, PAGE_CONTEXT, RUNS_DIRECTORY, SILVER_ZONE, wide_name
from ecomdata.processing.batch.star_inputs import star_inputs

FACT_NAMES = tuple(d.name for d in datasets_of_kind(FACT))


def summary_key(run_id: str) -> str:
    return f"marketplace/{RUNS_DIRECTORY}/run_id={run_id}/facts.json"


def built_star_versions(ctx: StageContext) -> dict[str, int | None]:
    """The version of every star table this run built, from the dimensions and facts summaries.

    The marts and the quality gate read the star at these versions, never at
    whatever the table holds now, so a rerun of a stage reads the star its run built.
    """
    from ecomdata.processing.batch.stages import dimensions

    versions: dict[str, int | None] = {}
    for key in (dimensions.summary_key(ctx.run.run_id), summary_key(ctx.run.run_id)):
        payload = ctx.store.get(SILVER_ZONE, key)
        if payload is None:
            raise FileNotFoundError(f"the run left no {key}: cannot tell which star it built")
        versions.update({name: table["version"] for name, table in json.loads(payload)["tables"].items()})
    return versions


class FactsStage(Stage):
    name = "facts"
    requires = (
        f"silver/{OBSERVATIONS}",
        f"silver/{PAGE_CONTEXT}",
        "silver/<source>_listing_wide",
        "gold/marketplace/dw/dim_product_listing",
    )
    produces = tuple(f"gold/marketplace/dw/{name}" for name in FACT_NAMES) + ("silver/_runs/<run>/facts.json",)
    allowed_from = frozenset({S.DIMENSIONS_BUILT})
    milestone = S.FACTS_BUILT

    def __init__(self, registry: SourceRegistry = SOURCES) -> None:
        self.registry = registry

    def execute(self, ctx: StageContext) -> Outcome:
        run = ctx.run
        observations = deduplicate_observations(ctx.io.read_silver(OBSERVATIONS))
        observations = observations.cache()
        try:
            present = {row.source_code for row in observations.select("source_code").distinct().collect()}
            codes = [spec.code for spec in self.registry if spec.code in present]
            wide = {code: ctx.io.read_silver(wide_name(code)) for code in codes}
            contexts = ctx.io.read_silver(PAGE_CONTEXT)
            inputs = star_inputs(observations, wide, contexts, self.registry)
            attempts, _ = ctx.crawl_audit(ctx.spark)
            facts = build_marketplace_facts(
                inputs.observations,
                ctx.io.read_dw(run, "dim_product_listing"),
                inputs.listed_counts,
                attempts,
                FactRules.from_context(run),
            )
            writes = {name: ctx.io.merge_dw(run, name, facts[name]) for name in FACT_NAMES}
        finally:
            observations.unpersist()
        summary = {
            "run_id": run.run_id,
            "as_of": run.as_of.isoformat(),
            "sources": codes,
            "tables": {
                name: {"uri": result.uri, "rows": result.row_count, "version": result.version}
                for name, result in writes.items()
            },
        }
        ctx.store.put(SILVER_ZONE, summary_key(run.run_id), json.dumps(summary, sort_keys=True).encode("utf-8"))
        return Outcome(S.FACTS_BUILT, detail=summary)
