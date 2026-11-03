"""The quality gate's star rules, on a star built by the real stages and then broken on purpose."""
from __future__ import annotations

import json

import pytest
from pyspark.sql import functions as F

from ecomdata.domain.datasets import STAR_NAMES
from ecomdata.processing.batch.context import MarketplaceBatchContext
from ecomdata.processing.batch.manifest import read_current_manifest
from ecomdata.processing.batch.quality import FAIL, PASS, SKIPPED, STAR_RULES, QualityGateFailure, _evaluate_star
from ecomdata.processing.batch.runner import default_stages, run_batch, select_stages
from ecomdata.processing.batch.stages.base import RunOptions
from tests.spark.lake_support import lines_of
from tests.spark.test_batch_stages import AS_OF, pipeline, rows
from tests.spark.test_normalize import lazada_records, tiki_records
from tests.spark_support import requires_spark

pytestmark = requires_spark


@pytest.fixture(scope="module")
def built(spark):
    context, deps, lake, *_ = pipeline(spark, lines_of([*tiki_records(), *lazada_records()]), parsed=10)
    run_batch(context, deps=deps, stages=select_stages(["silver", "dimensions", "facts"]))
    return context, {name: lake.read_dw(context, name) for name in STAR_NAMES}


def verdicts(star, context):
    return {result.check_name: result for result in _evaluate_star(star, context)}


def test_the_star_the_stages_build_passes_every_star_rule(built):
    context, star = built

    results = verdicts(star, context)

    assert set(results) == set(STAR_RULES)
    assert {name: r.status for name, r in results.items()} == dict.fromkeys(STAR_RULES, PASS)


def test_without_the_star_the_rules_are_skipped(built):
    context, _ = built

    assert {r.status for r in _evaluate_star(None, context)} == {SKIPPED}


def test_a_fact_key_with_no_dimension_row_fails_and_is_named(built):
    context, star = built
    broken = dict(star, fact_offer_observation=star["fact_offer_observation"].withColumn("seller_key", F.lit(424242).cast("bigint")))

    result = verdicts(broken, context)["star_foreign_keys_resolve"]

    assert result.status == FAIL and result.observed_value == star["fact_offer_observation"].count()
    assert "fact_offer_observation.seller_key=424242" in result.failure_sample_json


def test_a_repeated_surrogate_key_fails(built):
    context, star = built
    sellers = star["dim_seller"]
    broken = dict(star, dim_seller=sellers.unionByName(sellers.limit(1)))

    result = verdicts(broken, context)["star_surrogate_keys_unique"]

    assert result.status == FAIL and result.observed_value == 1 and "dim_seller/" in result.failure_sample_json


def test_two_current_versions_of_one_offer_fail(built):
    context, star = built
    listings = star["dim_product_listing"]
    offer = listings.filter(F.col("listing_key") != -1).select("offer_key").first()["offer_key"]
    copy = listings.filter(F.col("offer_key") == offer).withColumn("listing_key", F.col("listing_key") + 1)
    broken = dict(star, dim_product_listing=listings.unionByName(copy))

    result = verdicts(broken, context)["star_listing_versions_consistent"]

    assert result.status == FAIL and result.observed_value == 1 and str(offer) in result.failure_sample_json


def test_the_unknown_member_rate_is_reported(built):
    context, star = built

    result = verdicts(star, context)["star_unknown_member_rate"]

    assert 0.0 <= result.observed_value <= 1.0
    assert {key.split("=")[0] for key in json.loads(result.failure_sample_json)["keys"]} == {
        "brand_key", "category_key", "seller_key",
    }


def test_a_run_that_fails_the_gate_puts_the_star_back_at_the_certified_versions(spark):
    tiki = lines_of(tiki_records())
    context, deps, lake, store, audit, _ = pipeline(spark, tiki)
    options = RunOptions(serving_gold_root_uri="memory://gold")
    run_batch(context, deps=deps, stages=default_stages(), options=options)
    certified = read_current_manifest(reader=store.get)
    assert set(certified.dw_versions) == set(STAR_NAMES)
    good = {name: rows(lake.read_dw(context, name)) for name in STAR_NAMES}

    # The next run brings a new source and a Bronze line copied under another id:
    # its star differs, and observation_id_unique refuses it.
    twins = []
    for line in tiki:
        wire = json.loads(line)
        if wire["record_kind"] == "listing_row":
            wire["record_id"] = "twin-" + wire["record_id"]
            wire["raw_artifact_id"] = "twin-" + wire["raw_artifact_id"]
            twins.append(json.dumps(wire))
    lake.lines = [*tiki, *twins, *lines_of(lazada_records())]
    later = MarketplaceBatchContext("run-2", AS_OF, "memory://silver", "memory://gold")
    with pytest.raises(QualityGateFailure):
        run_batch(later, deps=deps, stages=default_stages(), options=options)

    assert {name: rows(lake.read_dw(later, name)) for name in STAR_NAMES} == good
    assert read_current_manifest(reader=store.get).run_id == context.run_id


def test_a_requested_category_no_page_has_described_yet_is_an_inferred_member(built):
    _, star = built
    attempted = {row.category_key for row in star["fact_crawl_attempt"].select("category_key").collect()}
    categories = star["dim_category"].filter(F.col("category_key").isin(*attempted))

    assert categories.count() == len(attempted)
    inferred = categories.filter(F.col("level").isNull()).collect()
    assert inferred and all(row.parent_key is None and row.full_path == row.category_id for row in inferred)
