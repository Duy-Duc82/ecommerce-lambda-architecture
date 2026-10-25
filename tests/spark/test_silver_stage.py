"""Stage ``silver`` (Bronze records to Silver v2), and AC7: the new path against the pre-ELT one.

The host Spark has no filesystem, so the lake and the object store are in-memory
fakes: the stage logic and its Spark transforms run for real, the Parquet and S3A
I/O is exercised in the Docker stack.
"""
from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from ecomdata.domain.batch_run import BatchRunStatus
from ecomdata.domain.identity import make_seller_id
from ecomdata.domain.observations import (
    Availability,
    create_marketplace_offer,
    create_observation_event,
    create_offer_observation,
)
from ecomdata.domain.sources import TIKI
from ecomdata.domain.wire import canonical_json
from ecomdata.processing.batch.context import MarketplaceBatchContext
from ecomdata.processing.batch.inputs import organic_only
from ecomdata.processing.batch.marts import build_marketplace_marts
from ecomdata.processing.batch.memory import MemoryStore
from ecomdata.processing.batch.stages import silver as silver_stage
from ecomdata.processing.batch.stages.base import RunOptions, StageContext
from ecomdata.processing.batch.stages.silver import SilverStage, duplicate_counts
from ecomdata.processing.normalize import common
from ecomdata.processing.schemas import MARKETPLACE_OBSERVATION_WIRE_SCHEMA
from tests.spark.lake_support import MemoryLake, lines_of
from tests.spark.legacy_path import deduplicate_marketplace_observations, flatten_marketplace_observations
from tests.spark.test_marketplace_marts import _audit
from tests.spark.test_normalize import FETCHED, lazada_records, tiki_records
from tests.spark_support import requires_spark

pytestmark = requires_spark

AS_OF = datetime(2026, 10, 25, 10, tzinfo=UTC)


def run_silver(spark, lines, *, options=None, as_of=AS_OF):
    lake, store = MemoryLake(spark, lines), MemoryStore()
    run = MarketplaceBatchContext("run-1", as_of, "memory://silver", "memory://gold")
    ctx = StageContext(
        run=run, record=None, options=options or RunOptions(), audit=None, store=store, io=lake, quality=None,
        spark_provider=lambda: spark, crawl_audit=None,
    )
    outcome = SilverStage().execute(ctx)
    summary = json.loads(store.objects[("silver", silver_stage.summary_key("run-1"))])
    return outcome, lake, summary


@pytest.fixture(scope="module")
def both(spark):
    return run_silver(spark, lines_of([*tiki_records(), *lazada_records()]))


def test_the_stage_writes_valid_rows_of_both_sources_and_moves_the_run_to_silver_built(both):
    outcome, lake, summary = both

    assert outcome.status is BatchRunStatus.SILVER_BUILT
    observations = lake.written[silver_stage.OBSERVATIONS]
    per_source = {r.source_code: r.n for r in observations.groupBy("source_code").agg(F.count("*").alias("n")).collect()}
    assert per_source == {"tiki": 5, "lazada": 5}
    assert dict(outcome.columns) == {"silver_rows": 10} and summary["silver_rows"] == 10
    assert lake.partitions[silver_stage.OBSERVATIONS] == ("source_code", "observed_date")
    assert summary["quarantined"] == 0 and summary["payload_drift"] == []


def test_the_sponsored_row_stays_in_silver_marked(both):
    _, lake, _ = both
    observations = lake.written[silver_stage.OBSERVATIONS]

    assert observations.filter("is_advertisement and source_code = 'tiki'").count() == 1
    assert organic_only(observations).filter("source_code = 'tiki'").count() == 4


def test_each_source_gets_a_wide_table_and_each_page_a_context_row(both):
    _, lake, summary = both

    assert lake.written[silver_stage.wide_name("tiki")].count() == 5
    assert lake.written[silver_stage.wide_name("lazada")].count() == 5
    assert "payload_json" in lake.written[silver_stage.wide_name("tiki")].columns
    contexts = lake.written[silver_stage.PAGE_CONTEXT]
    assert sorted(r.source_code for r in contexts.collect()) == ["lazada", "tiki"]
    assert summary["page_contexts"] == 2


def test_a_row_that_cannot_be_an_observation_is_quarantined_not_lost(spark):
    broken = [{"name": "no id", "price": 10}, {"id": 6, "price": 7}, {"id": 4001, "name": "neg", "price": -1}]

    _, lake, summary = run_silver(spark, lines_of(tiki_records(extra_rows=broken)))

    assert summary["quarantine_by_reason"] == {
        "missing_listing_id": 1, "missing_or_invalid_price": 1, "missing_title": 1,
    }
    assert lake.written[silver_stage.QUARANTINE].count() == 3
    # Every listing row is accounted for: 5 from the page and 3 broken ones.
    assert summary["silver_rows"] + summary["quarantined"] == 8


def test_a_listing_repeated_on_a_page_is_counted_in_quarantine_and_leaves_ids_unique(spark):
    organic = next(r for r in tiki_records() if r.record_kind == "listing_row" and not r.payload.get("advertisement"))

    _, _, summary = run_silver(spark, lines_of(tiki_records(extra_rows=[dict(organic.payload)])))

    assert summary["quarantine_by_reason"] == {"duplicate_listing_on_page": 1}
    assert summary["silver_rows"] == 5
    assert summary["duplicates_before_dedup"] == {"record_id": 0, "observation_id": 0, "identical_content": 0}


def test_a_replayed_record_is_counted_and_dropped_so_a_rerun_gives_the_same_silver(spark):
    lines = lines_of(tiki_records())

    _, _, summary = run_silver(spark, lines + lines)

    # Every one of the 6 records (5 rows and the page context) was written twice.
    assert summary["silver_rows"] == 5
    assert summary["duplicates_before_dedup"] == {"record_id": 6, "observation_id": 0, "identical_content": 0}


def test_the_same_observation_from_two_records_is_counted_before_dedup_and_not_dropped(spark):
    lines = lines_of(tiki_records())
    twins = []
    for line in lines:
        wire = json.loads(line)
        if wire["record_kind"] == "listing_row":
            # Another artifact, same listing, instant and body: the same observation_id.
            wire["record_id"] = "twin-" + wire["record_id"]
            wire["raw_artifact_id"] = "twin-" + wire["raw_artifact_id"]
            twins.append(json.dumps(wire))

    _, _, summary = run_silver(spark, lines + twins)

    assert summary["silver_rows"] == 10
    assert summary["duplicates_before_dedup"]["observation_id"] == 5
    assert summary["duplicates_before_dedup"]["identical_content"] == 5


def test_duplicate_counts_tell_an_id_with_other_content_from_a_pure_replay(spark):
    frame = spark.createDataFrame(
        [("a", 1), ("a", 1), ("b", 1), ("b", 2), ("c", 3)], "observation_id string, current_price int"
    )

    assert duplicate_counts(frame) == {"rows": 5, "observation_id": 2, "identical_content": 1}


def test_an_unknown_source_and_an_unsupported_envelope_are_quarantined(spark):
    lines = lines_of(tiki_records())
    row = next(line for line in lines if json.loads(line)["record_kind"] == "listing_row")
    foreign = {**json.loads(row), "source_code": "nowhere", "record_id": "foreign-1"}
    future = {**json.loads(row), "schema_version": "marketplace-record.v9", "record_id": "future-1"}

    _, _, summary = run_silver(spark, [*lines, json.dumps(foreign), json.dumps(future)])

    assert summary["quarantine_by_reason"] == {"unknown_source": 1, "unsupported_envelope": 1}
    assert summary["silver_rows"] == 5


def test_drift_is_reported_in_the_run_summary(spark):
    new = [{"id": 11, "name": "n", "price": 1, "brand_new_field": 1}]

    _, _, summary = run_silver(spark, lines_of(tiki_records(extra_rows=new)))

    assert summary["payload_drift"] == [{"source_code": "tiki", "key": "brand_new_field", "occurrences": 1}]


def test_a_run_with_no_records_writes_empty_datasets(spark):
    outcome, lake, summary = run_silver(spark, [])

    assert outcome.status is BatchRunStatus.SILVER_BUILT and summary["silver_rows"] == 0
    assert lake.written[silver_stage.OBSERVATIONS].count() == 0
    assert lake.written[silver_stage.OBSERVATIONS].columns == list(common.CANONICAL_NAMES)


def test_silver_since_rebuilds_only_the_days_from_that_date_on(spark):
    lines = lines_of(tiki_records())

    _, _, after = run_silver(spark, lines, options=RunOptions(silver_since=date(2026, 10, 26)))
    _, _, same_day = run_silver(spark, lines, options=RunOptions(silver_since=date(2026, 10, 25)))

    assert after["silver_rows"] == 0 and same_day["silver_rows"] == 5


def test_records_fetched_after_as_of_are_not_read(spark):
    outcome, _, _ = run_silver(spark, lines_of(tiki_records()), as_of=datetime(2026, 10, 25, 8, tzinfo=UTC))

    assert dict(outcome.columns) == {"silver_rows": 0}


# ---------------------------------------------------------------- AC7: parity
def pre_elt_events():
    """What the pre-ELT Tiki adapter produced from the fixture page, rebuilt from its frozen mapping.

    The adapter (``TikiCrawler._event_from_row``, removed in R3) dropped sponsored
    rows, then built one canonical event per organic row with the domain factories.
    The mapping below is that function's, row by row, so the old path is
    reproduced from the same source page the new path reads.
    """
    listing = [r for r in tiki_records() if r.record_kind == "listing_row"]
    organic = [r for r in listing if not r.payload.get("advertisement")]
    events = []
    for index, record in enumerate(organic):
        row = record.payload
        price = Decimal(str(row["price"]))
        list_price = Decimal(str(row["list_price"])) if row.get("list_price") else None
        original = Decimal(str(row["original_price"])) if row.get("original_price") else None
        if list_price in (None, Decimal(0)):
            list_price = original if original not in (None, Decimal(0)) else None
        seller = make_seller_id("tiki", str(row["seller_id"])) if row.get("seller_id") is not None else None
        offer = create_marketplace_offer(
            marketplace_code="tiki", marketplace_id=TIKI.marketplace_id, platform_listing_id=str(row["id"]),
            seller_id=seller, product_title=row["name"].strip(), brand=(row.get("brand_name") or None),
            category_path=row.get("primary_category_path") or record.request_target,
            source_url=f"https://tiki.vn/{row['url_key'].lstrip('/')}.html", currency="VND",
            first_seen_at=FETCHED, last_seen_at=FETCHED,
        )
        availability = Availability.UNKNOWN
        if row.get("shippable") is False or row.get("availability") in (False, 0):
            availability = Availability.OUT_OF_STOCK
        elif row.get("availability") in (True, 1):
            availability = Availability.IN_STOCK
        observation = create_offer_observation(
            marketplace_code="tiki", platform_listing_id=str(row["id"]), offer_id=offer.offer_id,
            observed_at=FETCHED, fetched_at=FETCHED, current_price=price, raw_uri=record.raw_uri,
            raw_sha256=record.body_sha256, adapter_version=record.adapter_version, crawl_run_id=record.crawl_run_id,
            list_price=list_price,
            discount_amount=Decimal(str(row["discount"])) if row.get("discount") is not None else None,
            discount_percent=Decimal(str(row["discount_rate"])) if row.get("discount_rate") is not None else None,
            rating_value=Decimal(str(row["rating_average"])) if row.get("rating_average") is not None else None,
            rating_scale=Decimal(5) if row.get("rating_average") is not None else None,
            review_count=row.get("review_count"),
            sold_count=(row.get("quantity_sold") or {}).get("value"),
            availability=availability, ranking_position=index + 1,
        )
        events.append(
            create_observation_event(
                marketplace_code="tiki", offer=offer, observation=observation,
                platform_listing_id=str(row["id"]), produced_at=FETCHED,
            )
        )
    return events


def old_path_observations(spark):
    lines = [canonical_json(event) for event in pre_elt_events()]
    wire = (
        spark.createDataFrame([(line,) for line in lines], "value string")
        .select(F.from_json("value", MARKETPLACE_OBSERVATION_WIRE_SCHEMA).alias("w"))
        .select("w.*")
    )
    return deduplicate_marketplace_observations(flatten_marketplace_observations(wire))


def canonical_rows(frame, columns):
    def norm(value):
        if isinstance(value, datetime):
            return value.astimezone(UTC).isoformat() if value.tzinfo else value.isoformat()
        if isinstance(value, float):
            return round(value, 9)
        return value

    return sorted(
        (tuple(norm(row[c]) for c in columns) for row in frame.collect()),
        key=lambda t: tuple("" if v is None else str(v) for v in t),
    )


def test_the_old_path_reproduces_the_fixture_page_it_stands_for(spark):
    old = old_path_observations(spark)

    assert old.count() == 4 and old.filter("current_price <= 0").count() == 0


def test_the_ten_marts_of_the_new_path_equal_those_of_the_pre_elt_path_on_the_tiki_fixture(spark):
    old = old_path_observations(spark).cache()
    _, lake, _ = run_silver(spark, lines_of(tiki_records()))
    new = organic_only(lake.written[silver_stage.OBSERVATIONS])
    attempts, runs = _audit(spark, [("run-1", "tiki", date(2026, 10, 25), "SUCCEEDED", 100, 10, 4, 0, None)])
    context = MarketplaceBatchContext("run-1", AS_OF, "memory://silver", "memory://gold")

    old_marts = build_marketplace_marts(old, attempts, runs, context)
    new_marts = build_marketplace_marts(new, attempts, runs, context)

    assert sorted(old_marts) == sorted(new_marts) and len(old_marts) == 10
    for name in sorted(old_marts):
        columns = old_marts[name].columns
        assert new_marts[name].columns == columns, name
        assert canonical_rows(new_marts[name], columns) == canonical_rows(old_marts[name], columns), name
    assert old_marts["offer_current"].count() == 4


def test_the_observation_rows_agree_on_every_pre_elt_column(spark):
    old = old_path_observations(spark)
    _, lake, _ = run_silver(spark, lines_of(tiki_records()))
    new = organic_only(lake.written[silver_stage.OBSERVATIONS])

    # The v2 rows carry extra columns (seller_name, brand_id, category_ids, sku, search_rank, ...)
    # that the pre-ELT canonical rows never had; on every pre-ELT column the rows are equal.
    assert set(old.columns) <= set(new.columns)
    assert canonical_rows(new, old.columns) == canonical_rows(old, old.columns)
