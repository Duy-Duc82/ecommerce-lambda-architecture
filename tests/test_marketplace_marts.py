"""Temporal mart tests, Phase 6 plan section 15 items 1-23, 25 and 28-32.

These need real DataFrames, so they run on a local Spark session over small
frozen fixtures. No MinIO, PostgreSQL or network is touched.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from batch_layer.marketplace_marts import (
    UNKNOWN_CATEGORY,
    build_category_price_daily,
    build_counter_delta_daily,
    build_counter_transitions,
    build_crawl_reliability_daily,
    build_offer_change_daily,
    build_offer_current,
    build_offer_freshness,
    build_offer_price_history_daily,
    build_seller_current,
    build_source_coverage_daily,
)
from tests.spark_support import requires_spark

pytestmark = requires_spark

DAY = datetime(2026, 9, 4, tzinfo=timezone.utc)
AS_OF = DAY + timedelta(hours=12)

MONEY = "decimal(38,6)"
# Declared as pairs, not one string: a "decimal(38,6)" type carries its own
# comma, so a schema string cannot be split back into field names.
_COLUMNS = [
    ("event_id", "string"), ("schema_version", "string"), ("event_type", "string"),
    ("occurred_at", "timestamp"), ("produced_at", "timestamp"),
    ("marketplace", "string"), ("partition_key", "string"),
    ("crawl_run_id", "string"), ("raw_uri", "string"),
    ("offer_id", "string"), ("marketplace_id", "string"),
    ("platform_listing_id", "string"), ("seller_id", "string"),
    ("product_title", "string"), ("brand", "string"), ("category_path", "string"),
    ("source_url", "string"), ("currency", "string"),
    ("first_seen_at", "timestamp"), ("last_seen_at", "timestamp"),
    ("active_status", "string"),
    ("observation_id", "string"), ("observed_at", "timestamp"),
    ("fetched_at", "timestamp"),
    ("current_price", MONEY), ("list_price", MONEY), ("shipping_price", MONEY),
    ("discount_amount", MONEY), ("discount_percent", MONEY),
    ("rating_value", MONEY), ("rating_scale", MONEY),
    ("rating_count", "bigint"), ("review_count", "bigint"), ("sold_count", "bigint"),
    ("availability", "string"), ("promotion_json", "string"),
    ("ranking_position", "bigint"), ("raw_sha256", "string"),
    ("adapter_version", "string"), ("observed_date", "date"),
]
OBSERVATION_SCHEMA = ", ".join(f"{name} {dtype}" for name, dtype in _COLUMNS)
_FIELDS = [name for name, _ in _COLUMNS]


def observation(offer="offer-1", minute=0, price="100.00", **overrides):
    observed_at = overrides.pop("observed_at", None) or DAY + timedelta(minutes=minute)
    values = {
        "event_id": overrides.get("observation_id", f"obs-{offer}-{minute}"),
        "schema_version": "marketplace-observation.v1",
        "event_type": "OFFER_OBSERVED",
        "occurred_at": observed_at,
        "produced_at": observed_at,
        "marketplace": "tiki",
        "partition_key": f"tiki:{offer}",
        "crawl_run_id": "run-1",
        "raw_uri": "s3a://bronze/raw.json",
        "offer_id": offer,
        "marketplace_id": "marketplace-tiki",
        "platform_listing_id": offer,
        "seller_id": None,
        "product_title": "Fixture product",
        "brand": None,
        "category_path": "1846",
        "source_url": f"https://tiki.vn/{offer}.html",
        "currency": "VND",
        "first_seen_at": DAY,
        "last_seen_at": observed_at,
        "active_status": "ACTIVE",
        "observation_id": f"obs-{offer}-{minute}",
        "observed_at": observed_at,
        "fetched_at": observed_at,
        "current_price": Decimal(price),
        "list_price": None,
        "shipping_price": None,
        "discount_amount": None,
        "discount_percent": None,
        "rating_value": None,
        "rating_scale": None,
        "rating_count": None,
        "review_count": None,
        "sold_count": None,
        "availability": "UNKNOWN",
        "promotion_json": None,
        "ranking_position": None,
        "raw_sha256": "a" * 64,
        "adapter_version": "test-v1",
        "observed_date": observed_at.date(),
    }
    values.update(overrides)
    values["event_id"] = values["observation_id"]
    return tuple(values[name] for name in _FIELDS)


def observations(spark, rows):
    return spark.createDataFrame(list(rows), schema=OBSERVATION_SCHEMA)


def rows_as_dicts(frame, *, sort_by=None):
    records = [row.asDict() for row in frame.collect()]
    if sort_by:
        records.sort(key=lambda record: tuple(str(record[key]) for key in sort_by))
    return records


def naive(moment):
    """Render an instant the way collect() does.

    The SQL session runs in UTC, so date bucketing matches production, but
    PySpark converts a collected TimestampType to a naive datetime in the
    driver's local zone. Comparisons go through here so the assertion states
    the instant, not this machine's offset.
    """
    return moment.astimezone().replace(tzinfo=None)


def one(frame):
    records = frame.collect()
    assert len(records) == 1, f"expected one row, got {len(records)}"
    return records[0].asDict()


# 6, 7, 8
def test_current_offer_chooses_the_latest_ordered_observation(spark):
    frame = observations(
        spark,
        [
            observation(minute=0, price="100.00"),
            observation(minute=5, price="150.00"),
            observation(minute=2, price="120.00"),
        ],
    )

    current = one(build_offer_current(frame))

    assert current["current_price"] == Decimal("150.000000")
    assert current["current_observation_id"] == "obs-offer-1-5"
    assert current["last_seen_at"] == naive(DAY + timedelta(minutes=5))
    assert current["first_seen_at"] == naive(DAY)


def test_current_offer_has_exactly_one_row_per_offer(spark):
    frame = observations(
        spark,
        [
            observation(offer="offer-1", minute=0),
            observation(offer="offer-1", minute=1),
            observation(offer="offer-2", minute=0),
        ],
    )

    current = build_offer_current(frame)

    assert current.count() == 2
    assert current.select("offer_id").distinct().count() == 2


# 5
def test_ordering_is_stable_for_equal_observed_timestamps(spark):
    rows = [
        observation(minute=0, price="100.00", observation_id="obs-b"),
        observation(minute=0, price="200.00", observation_id="obs-a"),
    ]

    forward = one(build_offer_current(observations(spark, rows)))
    reversed_input = one(build_offer_current(observations(spark, list(reversed(rows)))))

    assert forward == reversed_input
    assert forward["current_observation_id"] == "obs-b"


# 9
def test_seller_projection_excludes_null_ids_and_invents_nothing(spark):
    frame = observations(
        spark,
        [
            observation(offer="offer-1", minute=0, seller_id="seller-1"),
            observation(offer="offer-2", minute=3, seller_id="seller-1"),
            observation(offer="offer-3", minute=0, seller_id=None),
        ],
    )

    sellers = build_seller_current(frame)
    record = one(sellers)

    assert set(sellers.columns) == {
        "seller_id", "marketplace", "marketplace_id",
        "first_seen_at", "last_seen_at", "observed_offer_count",
    }
    assert record["seller_id"] == "seller-1"
    assert record["observed_offer_count"] == 2
    assert record["first_seen_at"] == naive(DAY)
    assert record["last_seen_at"] == naive(DAY + timedelta(minutes=3))


# 10, 11
def test_price_daily_first_last_min_max_and_count_are_exact(spark):
    rows = [
        observation(minute=0, price="100.00"),
        observation(minute=5, price="70.00"),
        observation(minute=9, price="130.00"),
    ]

    record = one(build_offer_price_history_daily(observations(spark, rows)))

    assert record["first_price"] == Decimal("100.000000")
    assert record["last_price"] == Decimal("130.000000")
    assert record["min_price"] == Decimal("70.000000")
    assert record["max_price"] == Decimal("130.000000")
    assert record["observation_count"] == 3
    assert record["distinct_price_count"] == 3
    assert record["last_observation_id"] == "obs-offer-1-9"
    assert record["observed_date"] == date(2026, 9, 4)


def test_unordered_input_yields_the_same_price_mart(spark):
    rows = [
        observation(minute=0, price="100.00"),
        observation(minute=5, price="70.00"),
        observation(minute=9, price="130.00"),
    ]

    ordered = one(build_offer_price_history_daily(observations(spark, rows)))
    shuffled = one(
        build_offer_price_history_daily(
            observations(spark, [rows[2], rows[0], rows[1]])
        )
    )

    assert ordered == shuffled


# 12, 13
def test_the_first_observation_is_not_a_transition(spark):
    record = one(build_offer_change_daily(observations(spark, [observation()])))

    assert record["transition_count"] == 0
    assert record["price_change_count"] == 0
    assert record["max_price_drop"] is None


def test_increase_drop_and_magnitudes_are_exact_decimals(spark):
    rows = [
        observation(minute=0, price="100.00"),
        observation(minute=1, price="70.00"),
        observation(minute=2, price="90.00"),
    ]

    record = one(build_offer_change_daily(observations(spark, rows)))

    assert record["transition_count"] == 2
    assert record["price_change_count"] == 2
    assert record["price_drop_count"] == 1
    assert record["price_increase_count"] == 1
    assert record["absolute_price_change_sum"] == Decimal("50.000000")
    assert record["signed_price_change_sum"] == Decimal("-10.000000")
    assert record["max_price_drop"] == Decimal("30.000000")
    assert record["max_price_increase"] == Decimal("20.000000")


# 14
def test_null_safe_rating_and_counter_comparisons(spark):
    rows = [
        observation(minute=0, rating_value=None, rating_count=None, review_count=None),
        observation(
            minute=1,
            rating_value=Decimal("4.5"),
            rating_scale=Decimal("5"),
            rating_count=10,
            review_count=3,
        ),
        observation(
            minute=2,
            rating_value=Decimal("4.5"),
            rating_scale=Decimal("5"),
            rating_count=10,
            review_count=3,
        ),
    ]

    record = one(build_offer_change_daily(observations(spark, rows)))

    assert record["rating_change_count"] == 1
    assert record["counter_change_count"] == 1


# 15
def test_ambiguous_availability_does_not_count_as_a_change(spark):
    rows = [
        observation(minute=0, availability="IN_STOCK"),
        observation(minute=1, availability="UNKNOWN"),
        observation(minute=2, availability="OUT_OF_STOCK"),
    ]

    record = one(build_offer_change_daily(observations(spark, rows)))

    assert record["availability_change_count"] == 0


def test_a_known_availability_transition_counts_once(spark):
    rows = [
        observation(minute=0, availability="IN_STOCK"),
        observation(minute=1, availability="OUT_OF_STOCK"),
    ]

    record = one(build_offer_change_daily(observations(spark, rows)))

    assert record["availability_change_count"] == 1


# 16, 17
@pytest.mark.parametrize(
    "age_seconds, expected",
    [(0, "FRESH"), (3600, "FRESH"), (3601, "STALE")],
)
def test_freshness_boundary_is_inclusive_for_fresh(spark, age_seconds, expected):
    as_of = DAY + timedelta(hours=2)
    observed = as_of - timedelta(seconds=age_seconds)
    frame = observations(spark, [observation(observed_at=observed)])

    record = one(
        build_offer_freshness(
            frame, as_of=as_of, stale_after_seconds=3600, rule_version="fresh.v1"
        )
    )

    assert record["age_seconds"] == age_seconds
    assert record["freshness_status"] == expected
    assert record["stale_after_seconds"] == 3600
    assert record["freshness_rule_version"] == "fresh.v1"


def test_a_future_observation_is_future_and_not_clamped(spark):
    as_of = DAY
    frame = observations(spark, [observation(minute=10)])

    record = one(
        build_offer_freshness(
            frame, as_of=as_of, stale_after_seconds=3600, rule_version="fresh.v1"
        )
    )

    assert record["freshness_status"] == "FUTURE"
    assert record["age_seconds"] == -600


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"stale_after_seconds": 0}, "stale_after_seconds"),
        ({"rule_version": "  "}, "rule_version"),
    ],
)
def test_freshness_rejects_invalid_configuration(spark, kwargs, message):
    frame = observations(spark, [observation()])
    values = {
        "as_of": AS_OF,
        "stale_after_seconds": 3600,
        "rule_version": "fresh.v1",
    }
    values.update(kwargs)

    with pytest.raises(ValueError, match=message):
        build_offer_freshness(frame, **values)


def test_freshness_rejects_a_naive_as_of(spark):
    frame = observations(spark, [observation()])

    with pytest.raises(ValueError, match="as_of"):
        build_offer_freshness(
            frame,
            as_of=datetime(2026, 9, 4, 12),
            stale_after_seconds=3600,
            rule_version="fresh.v1",
        )


# 18, 19
def test_category_mart_separates_marketplace_category_and_currency(spark):
    rows = [
        observation(offer="offer-1", minute=0, price="100.00", category_path="a"),
        observation(offer="offer-2", minute=0, price="200.00", category_path="b"),
        observation(offer="offer-3", minute=0, price="300.00", currency="USD"),
    ]

    records = rows_as_dicts(
        build_category_price_daily(observations(spark, rows)),
        sort_by=["category_path", "currency"],
    )

    assert len(records) == 3
    assert {record["currency"] for record in records} == {"VND", "USD"}
    assert {record["category_path"] for record in records} == {"a", "b", "1846"}


def test_a_null_category_uses_only_the_documented_bucket(spark):
    rows = [
        observation(offer="offer-1", minute=0, price="100.00", category_path=None),
        observation(offer="offer-2", minute=0, price="300.00", category_path=None),
    ]

    record = one(build_category_price_daily(observations(spark, rows)))

    assert record["category_path"] == UNKNOWN_CATEGORY
    assert record["observed_offer_count"] == 2
    assert record["min_price"] == Decimal("100.000000")
    assert record["max_price"] == Decimal("300.000000")
    assert record["median_price"] is not None


# 25, 28, 29, 30
def _transitions(spark, rows, *, max_gap_seconds=86400, rule_version="counter.v1"):
    return build_counter_transitions(
        observations(spark, rows),
        max_gap_seconds=max_gap_seconds,
        rule_version=rule_version,
    )


def _sold(frame):
    return sorted(
        (row.asDict() for row in frame.filter("counter_name = 'sold_count'").collect()),
        key=lambda record: (record["observed_at"], record["observation_id"]),
    )


def test_a_null_counter_creates_missing_value_evidence(spark):
    rows = [
        observation(minute=0, sold_count=None),
        observation(minute=1, sold_count=5),
    ]

    records = _sold(_transitions(spark, rows))

    assert records[0]["invalid_reason"] is None
    assert records[1]["invalid_reason"] == "MISSING_VALUE"
    assert records[1]["valid_delta"] is None


def test_a_non_positive_interval_is_invalid(spark):
    rows = [
        observation(minute=0, sold_count=1, observation_id="obs-a"),
        observation(minute=0, sold_count=4, observation_id="obs-b"),
    ]

    records = _sold(_transitions(spark, rows))

    assert records[1]["invalid_reason"] == "NON_POSITIVE_INTERVAL"
    assert records[1]["valid_delta"] is None


def test_an_excessive_gap_is_invalid(spark):
    rows = [
        observation(minute=0, sold_count=1),
        observation(minute=10, sold_count=4),
    ]

    records = _sold(_transitions(spark, rows, max_gap_seconds=60))

    assert records[1]["invalid_reason"] == "GAP_TOO_LONG"
    assert records[1]["raw_delta"] == 3
    assert records[1]["valid_delta"] is None


def test_a_semantics_version_change_is_invalid(spark):
    rows = [
        observation(minute=0, sold_count=1, adapter_version="test-v1"),
        observation(minute=1, sold_count=4, adapter_version="test-v2"),
    ]

    records = _sold(_transitions(spark, rows))

    assert records[1]["invalid_reason"] == "SEMANTICS_VERSION_CHANGED"


def test_an_unregistered_marketplace_cannot_produce_a_valid_delta(spark):
    rows = [
        observation(minute=0, sold_count=1, marketplace="shopee"),
        observation(minute=1, sold_count=4, marketplace="shopee"),
    ]

    records = _sold(_transitions(spark, rows))

    assert records[1]["invalid_reason"] == "SEMANTICS_UNREGISTERED"


# 29
def test_a_negative_delta_is_retained_and_flagged_never_clamped(spark):
    rows = [
        observation(minute=0, sold_count=10),
        observation(minute=1, sold_count=4),
    ]

    records = _sold(_transitions(spark, rows))

    assert records[1]["invalid_reason"] == "COUNTER_DECREASED"
    assert records[1]["raw_delta"] == -6
    assert records[1]["valid_delta"] is None


# 30
def test_valid_delta_and_hourly_velocity_are_exact(spark):
    rows = [
        observation(minute=0, sold_count=10),
        observation(minute=30, sold_count=40),
    ]

    daily = build_counter_delta_daily(_transitions(spark, rows))
    record = next(
        row.asDict()
        for row in daily.collect()
        if row.counter_name == "sold_count"
    )

    assert record["first_value"] == 10
    assert record["last_value"] == 40
    assert record["valid_delta_sum"] == 30
    assert record["valid_transition_count"] == 1
    assert record["elapsed_seconds_valid"] == 1800
    assert record["velocity_proxy_per_hour"] == pytest.approx(60.0)
    assert record["counter_reset_or_invalid"] is False


# 31
def test_daily_invalid_reason_json_is_sorted_and_deterministic(spark):
    rows = [
        observation(minute=0, sold_count=10),
        observation(minute=1, sold_count=4),
        observation(minute=1, sold_count=4, observation_id="obs-dup"),
        observation(minute=40, sold_count=None),
    ]

    daily = build_counter_delta_daily(_transitions(spark, rows, max_gap_seconds=600))
    record = next(
        row.asDict() for row in daily.collect() if row.counter_name == "sold_count"
    )

    import json

    reasons = json.loads(record["invalid_reasons_json"])
    assert reasons == sorted(reasons)
    assert record["counter_reset_or_invalid"] is True
    assert "COUNTER_DECREASED" in reasons


# 32
def test_the_counter_mart_never_names_a_proxy_as_sale_or_demand(spark):
    rows = [observation(minute=0, sold_count=1), observation(minute=1, sold_count=2)]

    daily = build_counter_delta_daily(_transitions(spark, rows))

    lowered = " ".join(daily.columns).lower()
    assert "sale" not in lowered
    assert "demand" not in lowered
    assert "revenue" not in lowered
    assert "velocity_proxy_per_hour" in daily.columns


# The column sets below are copied from scripts/init_postgres.sql, not invented
# to suit the transforms. The earlier fixtures gave `attempts` a `marketplace`
# column and `runs` a `marketplace` column; neither table has one, so the code
# path that has to resolve a marketplace from the real schema was never
# exercised and shipped broken.
ATTEMPT_SCHEMA = (
    "crawl_run_id string, task_id string, attempt_number int, "
    "started_at timestamp, completed_at timestamp, status string, "
    "http_status int, latency_ms bigint, raw_artifact_id string, raw_uri string, "
    "raw_bytes bigint, parsed_count bigint, rejected_count bigint, error_kind string"
)
RUN_SCHEMA = (
    "crawl_run_id string, marketplace_id string, started_at timestamp, "
    "completed_at timestamp, status string, requested bigint, succeeded bigint, "
    "failed bigint, adapter_version string"
)
FRONTIER_SCHEMA = (
    "task_id string, marketplace_code string, marketplace_id string, "
    "target string, resource_type string, status string"
)


def _audit_frames(spark, attempt_rows):
    """Build audit frames with the real column sets.

    ``attempt_rows`` keeps the readable shape the tests already used:
    ``(crawl_run_id, marketplace_code, request_date, status, latency_ms,
    raw_bytes, parsed_count, rejected_count, error_kind)``.
    """
    attempts, runs, frontier = [], {}, {}
    for index, row in enumerate(attempt_rows):
        run_id, marketplace_code, request_date, status, latency, raw_bytes, parsed, rejected, error_kind = row
        started = datetime(request_date.year, request_date.month, request_date.day, 8, tzinfo=timezone.utc)
        completed = started + timedelta(minutes=3)
        task_id = f"task-{run_id}"
        attempts.append((
            run_id, task_id, index + 1, started, completed, status,
            200 if status != "FAILED" else 500, latency, f"raw-{run_id}-{index}",
            f"file:///bronze/{run_id}-{index}.json", raw_bytes, parsed, rejected, error_kind,
        ))
        runs[run_id] = (run_id, f"marketplace-{marketplace_code}", started, completed, "COMPLETED", 1, 1, 0, "test-v1")
        frontier[task_id] = (task_id, marketplace_code, f"marketplace-{marketplace_code}", "1846", "LISTING_PAGE", "SUCCEEDED")
    return (
        spark.createDataFrame(attempts, schema=ATTEMPT_SCHEMA),
        spark.createDataFrame(list(runs.values()), schema=RUN_SCHEMA),
        spark.createDataFrame(list(frontier.values()), schema=FRONTIER_SCHEMA),
    )


# 20, 21, 22, 23
def _audit(spark, attempt_rows):
    attempts, runs, _frontier = _audit_frames(spark, attempt_rows)
    return attempts, runs


def test_source_coverage_counts_reconcile(spark):
    frame = observations(
        spark,
        [
            observation(offer="offer-1", minute=0),
            observation(offer="offer-2", minute=0),
        ],
    )
    attempts, runs = _audit(
        spark, [("run-1", "tiki", date(2026, 9, 4), "SUCCEEDED", 100, 10, 2, 1, None)]
    )

    record = one(
        build_source_coverage_daily(
            frame,
            attempts,
            runs,
            as_of=AS_OF,
            stale_after_seconds=3600,
            rule_version="fresh.v1",
        )
    )

    assert record["eligible_offer_count"] == 2
    assert record["observed_offer_count"] == 2
    assert record["missing_offer_count"] == 0
    assert record["coverage_rate"] == pytest.approx(1.0)
    assert record["observation_count"] == 2


def test_a_day_without_an_observation_counts_as_missing(spark):
    next_day = DAY + timedelta(days=1)
    frame = observations(
        spark,
        [
            observation(offer="offer-1", minute=0, observation_id="o1-d1"),
            observation(offer="offer-2", minute=0, observation_id="o2-d1"),
            observation(offer="offer-1", observed_at=next_day, observation_id="o1-d2"),
        ],
    )
    attempts, runs = _audit(
        spark, [("run-1", "tiki", date(2026, 9, 4), "SUCCEEDED", 100, 10, 2, 0, None)]
    )

    records = {
        record["observed_date"]: record
        for record in rows_as_dicts(
            build_source_coverage_daily(
                frame,
                attempts,
                runs,
                as_of=next_day + timedelta(hours=12),
                stale_after_seconds=3600,
                rule_version="fresh.v1",
            )
        )
    }

    first_day, second_day = records[date(2026, 9, 4)], records[date(2026, 9, 5)]
    assert first_day["eligible_offer_count"] == 2
    assert first_day["observed_offer_count"] == 2
    assert first_day["missing_offer_count"] == 0
    assert second_day["eligible_offer_count"] == 2
    assert second_day["observed_offer_count"] == 1
    assert second_day["missing_offer_count"] == 1
    assert second_day["coverage_rate"] == pytest.approx(0.5)


def test_parsed_and_rejected_counts_come_from_the_audit(spark):
    frame = observations(spark, [observation(offer="offer-1", minute=0)])
    attempts, runs = _audit(
        spark,
        [
            ("run-1", "tiki", date(2026, 9, 4), "SUCCEEDED", 100, 10, 7, 3, None),
            ("run-1", "tiki", date(2026, 9, 4), "FAILED", 200, 0, 0, 2, "PARSE"),
        ],
    )

    record = one(
        build_source_coverage_daily(
            frame,
            attempts,
            runs,
            as_of=AS_OF,
            stale_after_seconds=3600,
            rule_version="fresh.v1",
        )
    )

    assert record["parsed_count"] == 7
    assert record["rejected_count"] == 5
    assert record["observation_count"] == 1
    assert record["rejection_rate"] == pytest.approx(5 / 12)


def test_a_zero_denominator_rate_is_null(spark):
    frame = observations(spark, [observation(offer="offer-1", minute=0)])
    attempts, runs = _audit(
        spark, [("run-1", "tiki", date(2026, 9, 4), "SUCCEEDED", 100, 10, 0, 0, None)]
    )

    record = one(
        build_source_coverage_daily(
            frame,
            attempts,
            runs,
            as_of=AS_OF,
            stale_after_seconds=3600,
            rule_version="fresh.v1",
        )
    )

    assert record["rejection_rate"] is None


def test_crawl_reliability_aggregation_is_exact(spark):
    attempts, runs = _audit(
        spark,
        [
            ("run-1", "tiki", date(2026, 9, 4), "SUCCEEDED", 100, 500, 5, 0, None),
            ("run-1", "tiki", date(2026, 9, 4), "FAILED", 300, 0, 0, 2, "TRANSPORT"),
            ("run-1", "tiki", date(2026, 9, 4), "PARTIAL", 200, 100, 1, 1, "RATE"),
        ],
    )

    record = one(build_crawl_reliability_daily(attempts, runs))

    assert record["request_count"] == 3
    assert record["succeeded_count"] == 2
    assert record["failed_count"] == 1
    assert record["success_rate"] == pytest.approx(2 / 3)
    assert record["avg_latency_ms"] == pytest.approx(200.0)
    assert record["raw_bytes"] == 600
    assert record["parsed_count"] == 6
    assert record["rejected_count"] == 3
    assert record["transport_error_count"] == 1
    assert record["rate_limited_count"] == 1
