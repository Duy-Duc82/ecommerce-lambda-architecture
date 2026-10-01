"""Quality gate tests, Phase 7 plan section 17 items 17-31.

The registry tests need no Spark. The evaluation tests are marked individually
so the registry contract still gets checked on a runtime that cannot serve
DataFrames.
"""
import json
import subprocess
import sys
from datetime import timedelta
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from batch_layer.marketplace_marts import build_marketplace_marts
from batch_layer.marketplace_quality import (
    FAIL,
    PASS,
    SKIPPED,
    QualityGateFailure,
    decide,
    evaluate_quality_gates,
)
from batch_layer.marketplace_warehouse import MarketplaceBatchContext
from config.quality_rules import (
    ADVISORY,
    GOLD_DATASETS,
    MANDATORY,
    QUALITY_RULES,
    QualityRule,
    mandatory_rule_names,
    quality_rule,
    rules_with_severity,
)
# The observation builder is forty fields wide. Re-declaring it here would make
# two fixtures that drift apart; importing it keeps one definition of what a
# canonical Silver row looks like.
from tests.test_marketplace_marts import DAY, observation, observations
from tests.spark_support import requires_spark


# ----------------------------------------------------------------------------
# Registry. No Spark needed.
# ----------------------------------------------------------------------------

# 17
def test_the_registry_rejects_malformed_rules():
    with pytest.raises(ValueError, match="check_name must match"):
        QualityRule("Not A Name", MANDATORY, "silver", "x")
    with pytest.raises(ValueError, match="severity must be one of"):
        QualityRule("ok_name", "BLOCKING", "silver", "x")
    with pytest.raises(ValueError, match="unknown dataset_name"):
        QualityRule("ok_name", MANDATORY, "offer_currrent", "x")
    with pytest.raises(ValueError, match="expectation must be non-empty"):
        QualityRule("ok_name", MANDATORY, "silver", "   ")


# 17
def test_an_unregistered_check_name_is_an_error_not_a_default():
    with pytest.raises(ValueError, match="unregistered quality check"):
        quality_rule("observation_id_uniqe")


# 17
def test_check_names_are_unique_and_expectations_are_stated():
    names = [rule.check_name for rule in QUALITY_RULES]

    assert len(set(names)) == len(names)
    for rule in QUALITY_RULES:
        assert quality_rule(rule.check_name) is rule
        assert rule.expectation.strip()


# 18
def test_exactly_thirteen_mandatory_and_four_advisory_rules_are_registered():
    mandatory = rules_with_severity(MANDATORY)
    advisory = rules_with_severity(ADVISORY)

    assert len(mandatory) == 13
    assert len(advisory) == 4
    assert len(QUALITY_RULES) == 17
    assert mandatory_rule_names() == tuple(rule.check_name for rule in mandatory)


# 18
def test_the_mandatory_list_matches_the_brief_section_16_checks():
    # Spelled out rather than derived, so renaming a check in the registry
    # cannot quietly rename the contract it is supposed to implement.
    assert mandatory_rule_names() == (
        "raw_artifact_checksum_and_uri_present",
        "silver_observation_raw_lineage_complete",
        "observation_id_unique",
        "offer_key_and_price_complete",
        "price_non_negative",
        "observed_at_within_future_tolerance",
        "currency_valid",
        "silver_parse_attempt_reconciliation",
        "offer_listing_key_unique",
        "offer_current_single_row_per_offer",
        "gold_daily_row_count_reconciles",
        "gold_daily_price_aggregates_reconcile",
        "freshness_rule_version_and_as_of_applied",
    )


# 18
def test_the_registry_gold_dataset_list_matches_the_publisher_allowlist():
    # config cannot import batch_layer, so the ten names are repeated there.
    # This is the test that catches the two copies drifting apart.
    from batch_layer.marketplace_postgres import DATASETS

    assert set(GOLD_DATASETS) == set(DATASETS)


# ----------------------------------------------------------------------------
# Evaluation. These need real DataFrames.
# ----------------------------------------------------------------------------
AS_OF = DAY + timedelta(days=3, hours=12)
ATTEMPT_SCHEMA = (
    "crawl_run_id string, status string, started_at timestamp, completed_at timestamp, "
    "latency_ms bigint, raw_bytes bigint, parsed_count bigint, rejected_count bigint, error_kind string"
)
DAILY = 1440


def context(**overrides):
    # A three-day window with a minimum of one sample lets the tiny fixture
    # actually reach an anomaly verdict, so advisory coverage is not zero.
    fields = {"anomaly_window_days": 3, "anomaly_min_samples": 1}
    fields.update(overrides)
    return MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///gold", **fields)


def attempts(spark, parsed_count, *, run_ids=("run-1",)):
    rows = [(run_id, "SUCCEEDED", DAY, DAY, 120, 2048, parsed_count, 0, None) for run_id in run_ids]
    return spark.createDataFrame(rows, schema=ATTEMPT_SCHEMA)


# The fixture crawl run finished an hour before as_of: settled, and well inside
# any lookback, so the clean fixture is reconciled under the default windows.
SETTLED_AT = AS_OF - timedelta(hours=1)
RUN_SCHEMA = (
    "crawl_run_id string, marketplace string, started_at timestamp, "
    "completed_at timestamp, status string"
)


def runs(spark, *, run_ids=("run-1",), completed_at=SETTLED_AT):
    status = "RUNNING" if completed_at is None else "COMPLETED"
    return spark.createDataFrame(
        [(run_id, "tiki", DAY, completed_at, status) for run_id in run_ids],
        schema=RUN_SCHEMA,
    )


def clean_rows():
    return [
        observation(minute=DAILY * day, price=price)
        for day, price in enumerate(("100.00", "101.00", "102.00"))
    ]


def gates(spark, rows=None, *, parsed_count=None, ctx=None, attempt_frame=None, run_frame=None, tamper=None):
    rows = clean_rows() if rows is None else rows
    ctx = ctx or context()
    frame = observations(spark, rows)
    counted = len(rows) if parsed_count is None else parsed_count
    attempt = attempt_frame if attempt_frame is not None else attempts(spark, counted)
    run_frame = run_frame if run_frame is not None else runs(spark)
    marts = build_marketplace_marts(frame, attempt, run_frame, ctx)
    if tamper:
        marts = tamper(marts)
    return evaluate_quality_gates(frame, marts, attempt, run_frame, ctx), ctx


def status_of(results, check_name):
    return result_of(results, check_name).status


def result_of(results, check_name):
    return next(result for result in results if result.check_name == check_name)


# 19
@requires_spark
def test_every_registered_rule_yields_exactly_one_result(spark):
    results, _ = gates(spark)

    assert len(results) == len(QUALITY_RULES)
    assert [result.check_name for result in results] == [rule.check_name for rule in QUALITY_RULES]


# 19
@requires_spark
def test_a_clean_run_passes_every_check(spark):
    results, ctx = gates(spark)

    assert [result.check_name for result in results if result.status != PASS] == []
    assert decide(results, ctx).passed is True


# 20
@requires_spark
def test_a_broken_checksum_fails_its_check(spark):
    rows = clean_rows() + [observation(minute=DAILY * 4, raw_sha256="not-a-digest")]

    assert status_of(gates(spark, rows)[0], "raw_artifact_checksum_and_uri_present") == FAIL


# 20
@requires_spark
def test_missing_adapter_version_fails_lineage(spark):
    rows = clean_rows() + [observation(minute=DAILY * 4, adapter_version="  ")]

    assert status_of(gates(spark, rows)[0], "silver_observation_raw_lineage_complete") == FAIL


# 20
@requires_spark
def test_a_repeated_observation_id_fails_uniqueness(spark):
    rows = clean_rows() + [observation(minute=0, price="777.00")]

    results, _ = gates(spark, rows)

    assert status_of(results, "observation_id_unique") == FAIL
    assert result_of(results, "observation_id_unique").observed_value == 1.0


# 20
@requires_spark
def test_a_null_price_fails_key_completeness(spark):
    rows = clean_rows() + [observation(minute=DAILY * 4, current_price=None)]

    assert status_of(gates(spark, rows)[0], "offer_key_and_price_complete") == FAIL


# 20
@requires_spark
def test_a_negative_list_price_fails_the_non_negative_check(spark):
    rows = clean_rows() + [observation(minute=DAILY * 4, list_price=Decimal("-1.00"))]

    assert status_of(gates(spark, rows)[0], "price_non_negative") == FAIL


# 20, 21
@requires_spark
def test_the_future_tolerance_boundary_is_inclusive(spark):
    # The tolerance is measured against the row's own fetch, not the run's
    # as_of: the run has already cut everything after as_of away, so a check
    # against as_of could never fire. What it catches is an observation that
    # claims a time after the response that produced it.
    ctx = context()
    fetched = DAY + timedelta(days=2, hours=6)
    edge = fetched + timedelta(seconds=ctx.future_tolerance_seconds)
    at_edge = clean_rows() + [observation(minute=DAILY * 4, observed_at=edge, fetched_at=fetched, produced_at=edge)]
    past_edge = clean_rows() + [
        observation(minute=DAILY * 5, observed_at=edge + timedelta(seconds=1), fetched_at=fetched, produced_at=edge + timedelta(seconds=1))
    ]

    assert status_of(gates(spark, at_edge, ctx=ctx)[0], "observed_at_within_future_tolerance") == PASS
    assert status_of(gates(spark, past_edge, ctx=ctx)[0], "observed_at_within_future_tolerance") == FAIL


# 21
@requires_spark
def test_an_observation_produced_before_it_claims_to_be_observed_fails(spark):
    ctx = context()
    observed = DAY + timedelta(days=2, hours=6)
    skewed = clean_rows() + [
        observation(
            minute=DAILY * 4, observed_at=observed, fetched_at=observed,
            produced_at=observed - timedelta(seconds=ctx.future_tolerance_seconds + 1),
        )
    ]

    assert status_of(gates(spark, skewed, ctx=ctx)[0], "observed_at_within_future_tolerance") == FAIL


# 21
@requires_spark
def test_a_row_later_than_as_of_is_not_a_clock_skew_violation(spark):
    # Such a row is the input's business, not the gate's: the orchestrator cuts
    # it before evaluation. If one is handed in anyway, check 6 judges only
    # whether its own timestamps agree with each other.
    ctx = context()
    later = ctx.as_of + timedelta(days=2)
    rows = clean_rows() + [observation(minute=DAILY * 6, observed_at=later)]

    assert status_of(gates(spark, rows, ctx=ctx)[0], "observed_at_within_future_tolerance") == PASS


# 20, 22
@requires_spark
def test_currency_must_be_a_three_letter_code_inside_the_allowlist(spark):
    unlisted = clean_rows() + [observation(minute=DAILY * 4, currency="XYZ")]
    malformed = clean_rows() + [observation(minute=DAILY * 5, currency="vnd")]

    assert status_of(gates(spark, unlisted)[0], "currency_valid") == FAIL
    assert status_of(gates(spark, malformed)[0], "currency_valid") == FAIL
    # Widening the allowlist is a configuration change, not a code change.
    widened = context(allowed_currencies=("XYZ", "VND"))
    assert status_of(gates(spark, unlisted, ctx=widened)[0], "currency_valid") == PASS


# 20
@requires_spark
def test_a_parsed_count_that_disagrees_with_silver_fails_reconciliation(spark):
    assert status_of(gates(spark, parsed_count=99)[0], "silver_parse_attempt_reconciliation") == FAIL


# 20
@requires_spark
def test_one_listing_key_under_two_offer_ids_fails(spark):
    rows = clean_rows() + [observation(offer="offer-2", minute=DAILY * 4, platform_listing_id="offer-1")]

    assert status_of(gates(spark, rows)[0], "offer_listing_key_unique") == FAIL


# 20
@requires_spark
def test_a_duplicated_offer_current_row_fails(spark):
    def tamper(marts):
        marts["offer_current"] = marts["offer_current"].unionByName(marts["offer_current"])
        return marts

    assert status_of(gates(spark, tamper=tamper)[0], "offer_current_single_row_per_offer") == FAIL


# 20
@requires_spark
def test_a_daily_row_count_that_does_not_reconcile_fails(spark):
    def tamper(marts):
        marts["offer_price_history_daily"] = marts["offer_price_history_daily"].withColumn(
            "observation_count", F.col("observation_count") + F.lit(1)
        )
        return marts

    results, _ = gates(spark, tamper=tamper)

    assert status_of(results, "gold_daily_row_count_reconciles") == FAIL
    # Three offer-days, each inflated by one.
    assert result_of(results, "gold_daily_row_count_reconciles").observed_value == 3.0


# 20, 25
@requires_spark
def test_a_tampered_price_aggregate_is_caught_by_recomputation(spark):
    def tamper(marts):
        marts["offer_price_history_daily"] = marts["offer_price_history_daily"].withColumn(
            "min_price", (F.col("min_price") - F.lit(1)).cast("decimal(38,6)")
        )
        return marts

    assert status_of(gates(spark, tamper=tamper)[0], "gold_daily_price_aggregates_reconcile") == FAIL


# 20
@requires_spark
def test_a_freshness_row_under_another_rule_version_fails(spark):
    def tamper(marts):
        marts["offer_freshness"] = marts["offer_freshness"].withColumn(
            "freshness_rule_version", F.lit("freshness-rules.v0")
        )
        return marts

    assert status_of(gates(spark, tamper=tamper)[0], "freshness_rule_version_and_as_of_applied") == FAIL


# 23
@requires_spark
def test_an_empty_audit_skips_reconciliation_and_the_skip_fails_the_gate(spark):
    empty = spark.createDataFrame([], schema=ATTEMPT_SCHEMA)

    results, ctx = gates(spark, attempt_frame=empty)
    reconciliation = result_of(results, "silver_parse_attempt_reconciliation")
    decision = decide(results, ctx)

    assert reconciliation.status == SKIPPED
    assert json.loads(reconciliation.failure_sample_json) == {"reason": "crawl attempt audit is empty"}
    assert decision.passed is False
    assert decision.skipped >= 1


# 24
@requires_spark
def test_reconciliation_ignores_crawl_runs_absent_from_this_input(spark):
    rows = clean_rows()
    extra = spark.createDataFrame(
        [
            ("run-1", "SUCCEEDED", DAY, DAY, 120, 2048, len(rows), 0, None),
            ("run-2", "SUCCEEDED", DAY, DAY, 120, 2048, 4242, 0, None),
        ],
        schema=ATTEMPT_SCHEMA,
    )

    assert status_of(gates(spark, rows, attempt_frame=extra)[0], "silver_parse_attempt_reconciliation") == PASS


# 24
@requires_spark
def test_a_crawl_run_still_settling_is_not_reconciled_yet(spark):
    # Its observations may still be on their way through Kafka to Silver. A
    # short count here is lag, not loss; the next run reconciles it.
    ctx = context()
    settling = runs(spark, completed_at=ctx.as_of - timedelta(seconds=ctx.reconciliation_settle_seconds - 1))

    results, _ = gates(spark, parsed_count=99, ctx=ctx, run_frame=settling)

    assert status_of(results, "silver_parse_attempt_reconciliation") == PASS


# 24
@requires_spark
def test_a_crawl_run_settled_exactly_at_the_delay_is_reconciled(spark):
    ctx = context()
    settled = runs(spark, completed_at=ctx.as_of - timedelta(seconds=ctx.reconciliation_settle_seconds))

    results, _ = gates(spark, parsed_count=99, ctx=ctx, run_frame=settled)

    assert status_of(results, "silver_parse_attempt_reconciliation") == FAIL


# 24
@requires_spark
def test_an_unfinished_crawl_run_is_not_reconciled(spark):
    results, _ = gates(spark, parsed_count=99, run_frame=runs(spark, completed_at=None))

    assert status_of(results, "silver_parse_attempt_reconciliation") == PASS


# 24
@requires_spark
def test_a_crawl_run_older_than_the_lookback_no_longer_blocks_publication(spark):
    # A discrepancy that was already reported must not freeze publication
    # forever: once the run leaves the lookback it stops being re-judged.
    ctx = context()
    old = runs(spark, completed_at=ctx.as_of - timedelta(seconds=ctx.reconciliation_lookback_seconds + 1))

    results, _ = gates(spark, parsed_count=99, ctx=ctx, run_frame=old)

    assert status_of(results, "silver_parse_attempt_reconciliation") == PASS


# 24
@requires_spark
def test_a_crawl_run_at_the_lookback_edge_is_still_reconciled(spark):
    ctx = context()
    edge = runs(spark, completed_at=ctx.as_of - timedelta(seconds=ctx.reconciliation_lookback_seconds))

    results, _ = gates(spark, parsed_count=99, ctx=ctx, run_frame=edge)

    assert status_of(results, "silver_parse_attempt_reconciliation") == FAIL


# 24
@requires_spark
def test_silver_rows_with_no_audit_run_are_placed_by_their_last_observation(spark):
    ctx = context(reconciliation_lookback_seconds=2 * 86400)
    recent = clean_rows() + [observation(minute=DAILY * 2 + 60, crawl_run_id="ghost")]
    stale = clean_rows() + [
        observation(
            minute=0, observation_id="obs-ghost-old", crawl_run_id="ghost",
            observed_at=ctx.as_of - timedelta(seconds=ctx.reconciliation_lookback_seconds + 60),
        )
    ]
    audited = attempts(spark, len(clean_rows()))

    # No provenance for rows inside the window is a discrepancy...
    recent_results, _ = gates(spark, recent, ctx=ctx, attempt_frame=audited)
    assert status_of(recent_results, "silver_parse_attempt_reconciliation") == FAIL
    assert json.loads(result_of(recent_results, "silver_parse_attempt_reconciliation").failure_sample_json) == {"keys": ["ghost"]}
    # ...but one that has aged out of the lookback is no longer re-judged.
    stale_results, _ = gates(spark, stale, ctx=ctx, attempt_frame=audited)
    assert status_of(stale_results, "silver_parse_attempt_reconciliation") == PASS


# 23, 24
@requires_spark
def test_an_audit_without_run_completion_times_skips_reconciliation(spark):
    bare = spark.createDataFrame([("run-1", "tiki")], schema="crawl_run_id string, marketplace string")

    results, ctx = gates(spark, run_frame=bare)
    reconciliation = result_of(results, "silver_parse_attempt_reconciliation")

    assert reconciliation.status == SKIPPED
    assert json.loads(reconciliation.failure_sample_json) == {"reason": "crawl run audit lacks completed_at"}
    assert decide(results, ctx).passed is False


# 21, 24
@requires_spark
def test_a_backfill_over_a_silver_that_kept_growing_passes_once_cut(spark):
    # The case the cut exists for: Silver holds rows days past this run's
    # as_of, from crawl runs that had not even started yet.
    from batch_layer.marketplace_warehouse import crawl_audit_as_of, observations_as_of

    ctx = context()
    later = ctx.as_of + timedelta(days=2)
    rows = clean_rows() + [observation(minute=DAILY * 6, observed_at=later, crawl_run_id="run-later")]
    audit_attempts = spark.createDataFrame(
        [
            ("run-1", "SUCCEEDED", DAY, DAY, 120, 2048, len(clean_rows()), 0, None),
            ("run-later", "SUCCEEDED", later, later, 120, 2048, 1, 0, None),
        ],
        schema=ATTEMPT_SCHEMA,
    )
    audit_runs = spark.createDataFrame(
        [("run-1", "tiki", DAY, SETTLED_AT, "COMPLETED"), ("run-later", "tiki", later, later, "COMPLETED")],
        schema=RUN_SCHEMA,
    )

    frame = observations_as_of(observations(spark, rows), ctx.as_of)
    cut_attempts, cut_runs = crawl_audit_as_of(audit_attempts, audit_runs, ctx.as_of)
    marts = build_marketplace_marts(frame, cut_attempts, cut_runs, ctx)
    results = evaluate_quality_gates(frame, marts, cut_attempts, cut_runs, ctx)

    assert [result.check_name for result in results if result.status != PASS] == []
    assert frame.count() == len(clean_rows())


# 18
def test_the_rule_version_moved_when_checks_6_and_8_changed_meaning():
    # Verdicts stored under v1 judged a different question. Keeping the old
    # version would make them indistinguishable from verdicts under the new one.
    import os
    from config.settings import MARKETPLACE_QUALITY_RULE_VERSION

    if "MARKETPLACE_QUALITY_RULE_VERSION" in os.environ:
        pytest.skip("the environment overrides the rule version")
    assert MARKETPLACE_QUALITY_RULE_VERSION == "quality-rules.v2"


# 26
@requires_spark
def test_an_advisory_failure_alone_does_not_block_publication(spark):
    # The default minimum sample size over a three-day fixture leaves nothing
    # evaluable, so anomaly coverage fails while every mandatory check holds.
    ctx = context(anomaly_window_days=14, anomaly_min_samples=7)

    results, _ = gates(spark, ctx=ctx)
    decision = decide(results, ctx)

    assert status_of(results, "price_anomaly_evaluation_coverage") == FAIL
    assert decision.advisory_failures == 1
    assert decision.mandatory_failures == 0
    assert decision.passed is True


@requires_spark
def test_a_counter_the_source_never_published_is_not_an_invalid_transition(spark):
    # The fixture leaves every counter null. A marketplace that simply does not
    # expose sold_count must not read as a permanent hundred-percent invalid
    # transition rate; those transitions do not exist, they are not bad.
    rate = result_of(gates(spark)[0], "counter_invalid_transition_rate")

    assert rate.observed_value == 0.0
    assert rate.status == PASS


# 27
@requires_spark
def test_a_missing_mandatory_result_fails_the_decision(spark):
    results, ctx = gates(spark)
    without = tuple(result for result in results if result.check_name != "observation_id_unique")

    assert decide(without, ctx).passed is False
    assert decide(without, ctx).mandatory_failures == 1


# 28
@requires_spark
def test_observed_values_are_numeric_and_rates_stay_in_range(spark):
    results, _ = gates(spark)
    rates = {"counter_invalid_transition_rate", "price_anomaly_evaluation_coverage"}

    for result in results:
        if result.status == SKIPPED:
            continue
        assert isinstance(result.observed_value, float), result.check_name
        if result.check_name in rates:
            assert 0.0 <= result.observed_value <= 1.0, result.check_name


# 29
@requires_spark
def test_failure_samples_are_bounded_sorted_and_carry_identifiers_only(spark):
    rows = clean_rows() + [observation(minute=DAILY * (4 + index), currency="XYZ") for index in range(15)]

    sample = result_of(gates(spark, rows)[0], "currency_valid").failure_sample_json
    keys = json.loads(sample)["keys"]

    assert len(keys) == 10
    assert keys == sorted(keys)
    assert all(key.startswith("obs-") for key in keys)
    for leaked in ("Fixture product", "https://tiki.vn", "a" * 64):
        assert leaked not in sample


# 30
def test_importing_the_quality_module_opens_no_client_or_session():
    probe = (
        "import batch_layer.marketplace_quality as quality;"
        "from pyspark.sql import SparkSession;"
        "assert SparkSession._instantiatedSession is None;"
        "import sys;"
        "assert 'psycopg2' not in sys.modules;"
        "assert 'minio' not in sys.modules;"
        "assert 'redis' not in sys.modules;"
        "print('CLEAN')"
    )

    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, timeout=300)

    assert "CLEAN" in result.stdout, result.stderr


# 31
@requires_spark
def test_evaluation_reads_no_wall_clock(spark):
    results, ctx = gates(spark)

    assert {result.checked_at for result in results} == {ctx.as_of}
    assert decide(results, ctx).evaluated_at == ctx.as_of


@requires_spark
def test_the_gate_failure_names_the_failing_checks(spark):
    results, ctx = gates(spark, parsed_count=99)
    decision = decide(results, ctx)

    error = QualityGateFailure(decision, results)

    assert "silver_parse_attempt_reconciliation" in str(error)
    assert error.decision.passed is False
