"""The canonical sightings, read back out of the star.

``docs/PLAN_PHASE10_ON_REFACTOR.md`` D4. The ten marts are built from the
facts: :func:`sightings_from_facts` joins ``fact_offer_observation`` to the
dimensions its keys name and gives back the canonical columns the mart builders
(``processing/batch/marts.py``) read, so each mart keeps its name, columns and
rule. An attribute a dimension holds once (a seller's id, a brand's name, a
category's path) comes from the dimension; one that changes between sightings
(the title) comes from the listing version the sighting fell in.

A key that is the unknown member (``-1``) reads as NULL, not as the member's
label, so an offer without a seller is still an offer without a seller.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecomdata.processing.batch.keys import UNKNOWN_KEY


def _lookup(dim: DataFrame, key: str, **renamed: str) -> DataFrame:
    """``dim`` reduced to its key and the attributes it lends, the unknown member's attributes NULL."""
    lent = {alias: F.when(F.col(key) != UNKNOWN_KEY, F.col(column)).alias(alias) for alias, column in renamed.items()}
    return dim.select(F.col(key).alias(f"_{key}"), *lent.values())


def sightings_from_facts(fact: DataFrame, dims: dict[str, DataFrame]) -> DataFrame:
    """``fact_offer_observation`` with the canonical observation columns the marts read."""
    lookups = (
        ("marketplace_key", _lookup(dims["dim_marketplace"], "marketplace_key", marketplace="marketplace_code", marketplace_id="marketplace_id")),
        (
            "listing_key",
            _lookup(
                dims["dim_product_listing"], "listing_key",
                offer_id="offer_id", platform_listing_id="platform_listing_id", product_title="product_title", source_url="source_url",
            ),
        ),
        ("seller_key", _lookup(dims["dim_seller"], "seller_key", seller_id="seller_id")),
        ("brand_key", _lookup(dims["dim_brand"], "brand_key", brand="brand_name")),
        ("category_key", _lookup(dims["dim_category"], "category_key", category_path="full_path")),
        (
            "listing_flags_key",
            _lookup(dims["dim_listing_flags"], "listing_flags_key", availability="availability", active_status="active_status"),
        ),
    )  # fmt: skip
    sightings = fact
    for key, lookup in lookups:
        sightings = sightings.join(lookup, sightings[key] == lookup[f"_{key}"], "left").drop(f"_{key}")
    return sightings.withColumn("observed_date", F.to_date("observed_at"))
