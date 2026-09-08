"""Phase 6 GOLD-01/GOLD-04: reader paths, run orchestration and assertions.

Spark-dependent transforms are covered by tests marked ``integration``, which
skip unless MARKETPLACE_SPARK_TESTS is set — pyspark is not a default dependency
of this repo. Everything that decides correctness without a cluster (path
pruning, assertion evaluation, publish gating, clock discipline) is covered
unconditionally.
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from batch_layer.marketplace_gold_contracts import MARKETPLACE_MART_SPECS
from batch_layer.marketplace_gold_job import (
    STAGE_ASSERT,
    STATUS_FAILED,
    STATUS_SUCCEEDED,
    AssertionResult,
    GoldAssertionFailed,
    GoldRunReport,
    default_freshness_thresholds,
    run_marketplace_gold,
)
from batch_layer.marketplace_silver_reader import (
    SILVER_OBSERVATIONS_COMPACTED,
    SILVER_OBSERVATIONS_DATASET,
    SilverReadReport,
    date_range,
    observation_partition_paths,
    quarantine_partition_paths,
)

AS_OF = datetime(2026, 9, 8, 0, 0, tzinfo=timezone.utc)
RULE = "marketplace-gold-rules.v1"
integration = pytest.mark.skipif(
    not os.getenv("MARKETPLACE_SPARK_TESTS"),
    reason="requires a local Spark; set MARKETPLACE_SPARK_TESTS to run",
)


# --- 23: pruned partition paths --------------------------------------------


def test_partition_paths_are_pruned_by_marketplace_and_day():
    paths = observation_partition_paths(
        marketplaces=["tiki"], start_date=date(2026, 9, 6), end_date=date(2026, 9, 8)
    )
    assert len(paths) == 3
    for day in ("2026-09-06", "2026-09-07", "2026-09-08"):
        assert any(f"observed_date={day}" in path for path in paths)
    assert all("marketplace=tiki" in path for path in paths)
    assert all(path.endswith("*.json") for path in paths)


def test_partition_paths_never_scan_the_dataset_root():
    paths = observation_partition_paths(
        marketplaces=["tiki"], start_date=date(2026, 9, 8), end_date=date(2026, 9, 8)
    )
    for path in paths:
        tail = path.split(SILVER_OBSERVATIONS_DATASET)[1]
        assert tail.startswith("/marketplace="), "root scan would list every object ever written"


def test_partition_paths_cover_the_cross_product_of_sources_and_days():
    paths = observation_partition_paths(
        marketplaces=["tiki", "shopee"], start_date=date(2026, 9, 7), end_date=date(2026, 9, 8)
    )
    assert len(paths) == 4


def test_marketplace_codes_are_lowercased():
    paths = observation_partition_paths(
        marketplaces=["TIKI"], start_date=date(2026, 9, 8), end_date=date(2026, 9, 8)
    )
    assert "marketplace=tiki" in paths[0]


def test_blank_or_empty_marketplaces_are_rejected():
    with pytest.raises(ValueError, match="at least one marketplace"):
        observation_partition_paths(marketplaces=[], start_date=date(2026, 9, 8), end_date=date(2026, 9, 8))
    with pytest.raises(ValueError, match="must not be blank"):
        observation_partition_paths(marketplaces=["  "], start_date=date(2026, 9, 8), end_date=date(2026, 9, 8))


def test_reversed_date_range_is_rejected():
    with pytest.raises(ValueError, match="must not precede"):
        date_range(date(2026, 9, 8), date(2026, 9, 1))


def test_single_day_range_is_one_day():
    assert date_range(date(2026, 9, 8), date(2026, 9, 8)) == (date(2026, 9, 8),)


def test_quarantine_paths_are_dated_and_globbed():
    paths = quarantine_partition_paths(start_date=date(2026, 9, 8), end_date=date(2026, 9, 8))
    assert len(paths) == 1
    assert "observed_date=2026-09-08" in paths[0]


def test_compacted_dataset_is_a_new_path_not_the_landing_one():
    assert SILVER_OBSERVATIONS_COMPACTED != SILVER_OBSERVATIONS_DATASET
    assert SILVER_OBSERVATIONS_DATASET in SILVER_OBSERVATIONS_COMPACTED  # same family, new dataset


# --- 24: read report -------------------------------------------------------


def test_read_report_derives_duplicates_removed():
    report = SilverReadReport(paths_requested=30, paths_with_rows=30, rows_read=100, rows_deduplicated=95)
    assert report.duplicates_removed == 5


def test_empty_window_is_a_zero_report_not_an_error():
    report = SilverReadReport(paths_requested=30, paths_with_rows=0, rows_read=0, rows_deduplicated=0)
    assert report.rows_read == 0 and report.duplicates_removed == 0


# --- 33: the job reads no clock inside a transform -------------------------


def test_no_transform_reads_a_clock():
    source = Path("batch_layer/marketplace_gold_job.py").read_text(encoding="utf-8")
    # The only permitted clock is the default for the injectable `clock`
    # argument of run_marketplace_gold, which stamps the run row.
    occurrences = [
        line.strip()
        for line in source.splitlines()
        if "datetime.now(" in line or "current_timestamp" in line or "F.now(" in line
    ]
    assert occurrences == ["clock = clock or (lambda: datetime.now(timezone.utc))"]


def test_builders_take_as_of_as_an_argument():
    import inspect

    from batch_layer import marketplace_gold_job as job

    for name in (
        "build_offer_current",
        "build_offer_freshness",
        "build_price_history_daily",
        "build_offer_change_daily",
        "build_category_price_daily",
        "build_counter_delta_daily",
        "build_source_coverage_daily",
        "build_crawl_reliability_daily",
    ):
        signature = inspect.signature(getattr(job, name))
        assert "as_of" in signature.parameters, f"{name} must take as_of"


def _balanced_call_arguments(source: str, call: str) -> list[str]:
    """Return each `call(...)` argument text, counting parens so nested
    `F.col("x")` calls do not truncate the match the way a regex would."""
    arguments = []
    for match in re.finditer(re.escape(call), source):
        index = match.end()
        depth = 1
        while index < len(source) and depth:
            if source[index] == "(":
                depth += 1
            elif source[index] == ")":
                depth -= 1
            index += 1
        arguments.append(source[match.end() : index - 1])
    return arguments


def test_every_per_offer_window_breaks_ties_on_observation_id():
    source = Path("batch_layer/marketplace_gold_job.py").read_text(encoding="utf-8")
    partitions = _balanced_call_arguments(source, "partitionBy(")
    orders = _balanced_call_arguments(source, "orderBy(")
    assert partitions and orders, "expected window definitions"
    offer_partitions = [text for text in partitions if "offer_id" in text]
    assert offer_partitions, "expected at least one per-offer window"
    for order in orders:
        assert "observation_id" in order, f"window ordered by {order.strip()} is nondeterministic"


def test_no_transform_forward_fills_or_clamps():
    source = Path("batch_layer/marketplace_gold_job.py").read_text(encoding="utf-8")
    for banned in ("last(", "greatest(F.col(\"observed_delta\")", "coalesce(F.col(\"current_price\")"):
        assert banned not in source


def test_the_job_module_imports_without_pyspark():
    import importlib

    with pytest.raises(ImportError):
        importlib.import_module("pyspark")
    module = importlib.import_module("batch_layer.marketplace_gold_job")
    assert hasattr(module, "run_marketplace_gold")


# --- 35-38: assertions ------------------------------------------------------


def _assertion(name, passed):
    return AssertionResult(
        name=name, observed_value="0", expectation="expectation", passed=passed
    )


def test_assertion_status_maps_to_passed():
    assert _assertion("a", True).status == "PASSED"
    assert _assertion("a", False).status == "FAILED"


def test_assertion_row_carries_the_rule_version():
    row = _assertion("offer_current_unique", True).as_row(RULE)
    assert row["rule_version"] == RULE
    assert row["assertion_name"] == "offer_current_unique"
    assert row["status"] == "PASSED"


def test_a_failed_assertion_blocks_publish_at_the_report_level():
    with pytest.raises(ValueError, match="failed assertion must not be published"):
        GoldRunReport(
            gold_run_id="goldrun_x",
            as_of=AS_OF,
            window_days=30,
            rule_version=RULE,
            started_at=AS_OF,
            completed_at=AS_OF,
            observations_read=1,
            observations_deduplicated=1,
            quarantined_rows=0,
            assertions=(_assertion("offer_current_unique", False),),
            published=True,
        )


def test_a_failed_run_must_not_be_published():
    with pytest.raises(ValueError, match="failed run must not be published"):
        GoldRunReport(
            gold_run_id="goldrun_x",
            as_of=AS_OF,
            window_days=30,
            rule_version=RULE,
            started_at=AS_OF,
            completed_at=AS_OF,
            observations_read=1,
            observations_deduplicated=1,
            quarantined_rows=0,
            published=True,
            status=STATUS_FAILED,
            failure_stage=STAGE_ASSERT,
        )


def test_a_failed_run_requires_a_stage():
    with pytest.raises(ValueError, match="failed run requires a failure stage"):
        GoldRunReport(
            gold_run_id="goldrun_x",
            as_of=AS_OF,
            window_days=30,
            rule_version=RULE,
            started_at=AS_OF,
            completed_at=AS_OF,
            observations_read=0,
            observations_deduplicated=0,
            quarantined_rows=0,
            status=STATUS_FAILED,
        )


def test_assertion_failure_carries_every_failing_name():
    error = GoldAssertionFailed(
        [_assertion("offer_current_unique", False), _assertion("daily_counts_reconcile", False)]
    )
    assert "offer_current_unique" in str(error)
    assert "daily_counts_reconcile" in str(error)
    assert len(error.failures) == 2


def test_run_row_reports_skipped_marts():
    report = GoldRunReport(
        gold_run_id="goldrun_x",
        as_of=AS_OF,
        window_days=30,
        rule_version=RULE,
        started_at=AS_OF,
        completed_at=AS_OF,
        observations_read=10,
        observations_deduplicated=9,
        quarantined_rows=2,
        skipped_marts={"crawl_reliability_daily": "audit source unavailable"},
    )
    row = report.run_row()
    assert row["skipped_marts"] == {"crawl_reliability_daily": "audit source unavailable"}
    assert row["status"] == STATUS_SUCCEEDED


# --- 32, 42: guards before any Spark or database work ---------------------


def test_naive_as_of_is_rejected_before_spark_starts():
    with pytest.raises(ValueError, match="timezone-aware"):
        run_marketplace_gold(as_of=datetime(2026, 9, 8), spark=object())


def test_non_positive_window_is_rejected_before_spark_starts():
    with pytest.raises(ValueError, match="positive int"):
        run_marketplace_gold(as_of=AS_OF, window_days=0, spark=object())


def test_freshness_thresholds_come_from_settings_and_are_ordered():
    thresholds = default_freshness_thresholds()
    assert thresholds.stale_after > thresholds.fresh_after


def test_all_eight_marts_are_wired_into_the_run():
    source = Path("batch_layer/marketplace_gold_job.py").read_text(encoding="utf-8")
    for spec in MARKETPLACE_MART_SPECS:
        assert f"{spec.name.upper()}.name" in source or spec.name in source


def test_gold_run_writes_no_latest_pointer_or_manifest():
    source = Path("batch_layer/marketplace_gold_job.py").read_text(encoding="utf-8")
    for banned in ('"latest"', "'latest'", "manifest"):
        assert banned not in source


def test_change_daily_is_not_derived_from_kafka_redis_or_elasticsearch():
    source = Path("batch_layer/marketplace_gold_job.py").read_text(encoding="utf-8")
    offenders = [
        line.strip()
        for line in source.splitlines()
        if re.match(r"^\s*(import|from)\s", line)
        and any(name in line.lower() for name in ("kafka", "redis", "elasticsearch"))
    ]
    assert offenders == []


# --- Spark-backed transform tests -----------------------------------------


@integration
def test_marts_match_their_declared_schemas():
    from batch_layer.marketplace_gold_contracts import MART_BY_NAME

    for spec in MARKETPLACE_MART_SPECS:
        schema = spec.to_spark_schema()
        assert [field.name for field in schema.fields] == list(spec.column_names)
        assert MART_BY_NAME[spec.name] is spec


@integration
def test_two_runs_with_the_same_as_of_produce_identical_row_counts():
    first = run_marketplace_gold(as_of=AS_OF, window_days=7)
    second = run_marketplace_gold(as_of=AS_OF, window_days=7)
    assert first.gold_run_id == second.gold_run_id
    assert first.mart_row_counts == second.mart_row_counts
