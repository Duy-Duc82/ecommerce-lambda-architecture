"""Stage ``dimensions``: Silver to the star's dimensions in the lake.

Reads all of ``offer_observations_v2`` (every sighting, sponsored ones
included: the facts keep them, marked), each source's ``<source>_listing_wide``
and ``page_context``, builds every dimension and the category bridge
(``processing/batch/dimensions.py``) and merges each into
``gold/marketplace/dw/<table>/`` (``LakeIO.merge_dw``). The tables live outside
the run directories and are rebuilt whole from Silver on every run, so a rerun
on the same Silver leaves them as they were; the run's summary records the
Delta version each table reached, which is what lets a later read see exactly
the star this run built (``VERSION AS OF``).

The star is not cut at the run's ``as_of``: it holds everything Silver knows,
and ``as_of`` cuts where a run reads it (the marts). Cutting here would make a
run of an older window delete every row a newer run had added, because the
merge removes the rows its source no longer has.
"""
from __future__ import annotations

import json

from ecomdata.domain.batch_run import BatchRunStatus as S
from ecomdata.domain.datasets import BRIDGE, DIM, datasets_of_kind
from ecomdata.domain.sources import SOURCES, SourceRegistry
from ecomdata.processing.batch.dimensions import build_marketplace_dimensions
from ecomdata.processing.batch.inputs import deduplicate_observations
from ecomdata.processing.batch.stages.base import Outcome, Stage, StageContext
from ecomdata.processing.batch.stages.silver import OBSERVATIONS, PAGE_CONTEXT, RUNS_DIRECTORY, SILVER_ZONE, wide_name
from ecomdata.processing.batch.star_inputs import star_inputs

DIMENSION_NAMES = tuple(d.name for d in datasets_of_kind(DIM, BRIDGE))


def summary_key(run_id: str) -> str:
    return f"marketplace/{RUNS_DIRECTORY}/run_id={run_id}/dimensions.json"


class DimensionsStage(Stage):
    name = "dimensions"
    requires = (f"silver/{OBSERVATIONS}", f"silver/{PAGE_CONTEXT}", "silver/<source>_listing_wide")
    produces = tuple(f"gold/marketplace/dw/{name}" for name in DIMENSION_NAMES) + (
        "silver/_runs/<run>/dimensions.json",
    )
    allowed_from = frozenset({S.SILVER_BUILT})
    milestone = S.DIMENSIONS_BUILT

    def __init__(self, registry: SourceRegistry = SOURCES) -> None:
        self.registry = registry

    def execute(self, ctx: StageContext) -> Outcome:
        run = ctx.run
        observations = deduplicate_observations(ctx.io.read_silver(OBSERVATIONS))
        observations = observations.cache()
        try:
            present = {row.source_code for row in observations.select("source_code").distinct().collect()}
            # The silver stage writes a wide table for every source it saw records of.
            codes = [spec.code for spec in self.registry if spec.code in present]
            wide = {code: ctx.io.read_silver(wide_name(code)) for code in codes}
            contexts = ctx.io.read_silver(PAGE_CONTEXT)
            inputs = star_inputs(observations, wide, contexts, self.registry)
            dims = build_marketplace_dimensions(
                inputs.observations, None, inputs.category_names, inputs.category_paths
            )
            writes = {name: ctx.io.merge_dw(run, name, dims[name]) for name in DIMENSION_NAMES}
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
        return Outcome(S.DIMENSIONS_BUILT, detail=summary)
