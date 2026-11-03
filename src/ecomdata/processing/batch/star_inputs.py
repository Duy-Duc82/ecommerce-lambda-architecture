"""What the star's builders read, assembled from Silver.

``docs/DATA_MODEL.md`` section 4.4. The canonical observations carry the
measures and the shared ids; the descriptive attributes that only some sources
publish (seller badges, SKUs, origin, delivery flags) and the category names
and breadcrumbs come from each source's ``<source>_listing_wide`` table and
from ``page_context``, read by that source's normaliser module
(``listing_attributes``, ``category_names``, ``category_paths``,
``listed_product_counts``).

Every builder takes the observations through :func:`with_listing_columns`, so
a frame without the wide-table columns (an older fixture, a source without a
wide table) reads as "unknown" rather than failing.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecomdata.domain.sources import SOURCES, SourceRegistry
from ecomdata.processing.normalize import common

# Canonical Silver v2 columns the star reads beyond the first, v1 block.
OBSERVATION_EXTRA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("record_id", "string"),
    ("seller_name", "string"),
    ("brand_id", "string"),
    ("sku", "string"),
    ("search_rank", "bigint"),
    ("is_advertisement", "boolean"),
    ("request_target", "string"),
    ("request_page", "int"),
)
ATTRIBUTE_COLUMNS = tuple(c for c in common.LISTING_ATTRIBUTE_COLUMNS if c[0] != "record_id")


def with_listing_columns(observations: DataFrame) -> DataFrame:
    """``observations`` with every column the star reads; one it lacks is added as NULL."""
    present = set(observations.columns)
    missing = [(n, t) for n, t in OBSERVATION_EXTRA_COLUMNS + ATTRIBUTE_COLUMNS if n not in present]
    return observations.select("*", *[F.lit(None).cast(t).alias(n) for n, t in missing])


@dataclass(frozen=True)
class StarInputs:
    observations: DataFrame
    category_names: DataFrame
    category_paths: DataFrame
    listed_counts: DataFrame


def _union(frames: list[DataFrame], spark: SparkSession, columns: tuple[tuple[str, str], ...]) -> DataFrame:
    result = common.empty(spark, columns)
    for frame in frames:
        result = result.unionByName(common.typed(frame, columns))
    return result


def star_inputs(
    observations: DataFrame,
    wide: Mapping[str, DataFrame],
    contexts: DataFrame,
    registry: SourceRegistry = SOURCES,
) -> StarInputs:
    """Observations joined to their listing attributes, and every source's category names and paths.

    ``wide`` maps a source code to its ``<source>_listing_wide`` frame;
    ``contexts`` is Silver's ``page_context``. A source with no wide table
    contributes no attributes.
    """
    spark = observations.sparkSession
    attributes, names, paths, listed = [], [], [], []
    for code, frame in sorted(wide.items()):
        module = registry.normalizer(code)
        source_contexts = contexts.filter(F.col("source_code") == code)
        attributes.append(module.listing_attributes(frame))
        names.append(module.category_names(frame, source_contexts))
        paths.append(module.category_paths(source_contexts))
        listed.append(module.listed_product_counts(source_contexts))
    by_record = _union(attributes, spark, common.LISTING_ATTRIBUTE_COLUMNS)
    base = observations.drop(*[n for n, _ in ATTRIBUTE_COLUMNS if n in observations.columns])
    if "record_id" not in base.columns:
        base = base.withColumn("record_id", F.lit(None).cast("string"))
    enriched = with_listing_columns(base.join(by_record, "record_id", "left"))
    return StarInputs(
        observations=enriched,
        category_names=_union(names, spark, common.CATEGORY_NAME_COLUMNS),
        category_paths=_union(paths, spark, common.CATEGORY_PATH_COLUMNS),
        listed_counts=_union(listed, spark, common.LISTED_COUNT_COLUMNS),
    )
