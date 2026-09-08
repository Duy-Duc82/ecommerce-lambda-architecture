"""Phase 6 GOLD-03: Gold mart contracts, run identity and paths."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from batch_layer.marketplace_gold_contracts import (
    COLUMN_TYPES,
    MARKETPLACE_MART_SPECS,
    MART_BY_NAME,
    MONEY,
    REQUIRED_COLUMNS,
    Column,
    MartSpec,
    gold_dataset_path,
    make_gold_run_id,
)

AS_OF = datetime(2026, 9, 8, 0, 0, tzinfo=timezone.utc)
RULE = "marketplace-gold-rules.v1"

EXPECTED_MARTS = (
    "offer_current",
    "offer_price_history_daily",
    "offer_change_daily",
    "offer_freshness",
    "category_price_daily",
    "source_coverage_daily",
    "crawl_reliability_daily",
    "counter_delta_daily",
)


# --- 14: the eight declared marts ------------------------------------------


def test_exactly_the_eight_planned_marts_exist():
    assert tuple(spec.name for spec in MARKETPLACE_MART_SPECS) == EXPECTED_MARTS


def test_mart_names_are_unique():
    names = [spec.name for spec in MARKETPLACE_MART_SPECS]
    assert len(names) == len(set(names)) == len(MART_BY_NAME)


@pytest.mark.parametrize("spec", MARKETPLACE_MART_SPECS, ids=lambda s: s.name)
def test_every_mart_carries_run_lineage(spec):
    for required in REQUIRED_COLUMNS:
        assert required in spec.column_names, f"{spec.name} is missing {required}"


@pytest.mark.parametrize("spec", MARKETPLACE_MART_SPECS, ids=lambda s: s.name)
def test_grain_columns_are_present_and_non_nullable(spec):
    for column in spec.grain:
        assert column in spec.column_names
        declared = spec.columns[spec.column_names.index(column)]
        assert declared.nullable is False


@pytest.mark.parametrize("spec", MARKETPLACE_MART_SPECS, ids=lambda s: s.name)
def test_partition_columns_exist_in_the_schema(spec):
    for column in spec.partition_by:
        assert column in spec.column_names


# --- 15: no floating point anywhere ---------------------------------------


def test_the_column_vocabulary_has_no_float_type():
    assert "double" not in COLUMN_TYPES and "float" not in COLUMN_TYPES


def test_a_float_money_column_is_rejected_at_declaration():
    with pytest.raises(ValueError, match="unsupported type"):
        Column("price", "double")


@pytest.mark.parametrize("spec", MARKETPLACE_MART_SPECS, ids=lambda s: s.name)
def test_every_price_like_column_is_declared_money(spec):
    price_like = [
        column.name
        for column in spec.columns
        if any(token in column.name for token in ("price", "delta_percent", "rating_value"))
        and not column.name.endswith("_rows")
        and "changes" not in column.name
        and "drops" not in column.name
        and "count" not in column.name
    ]
    for name in price_like:
        declared = spec.columns[spec.column_names.index(name)]
        assert declared.type == MONEY, f"{spec.name}.{name} is {declared.type}, not money"


# --- 16-17: validation --------------------------------------------------


def _lineage():
    return (
        Column("gold_run_id", "string", nullable=False),
        Column("as_of", "timestamp", nullable=False),
        Column("rule_version", "string", nullable=False),
    )


def test_grain_column_absent_from_schema_fails():
    with pytest.raises(ValueError, match="grain columns absent"):
        MartSpec(name="m", grain=("missing",), columns=(Column("a", "string", nullable=False),) + _lineage())


def test_missing_lineage_column_fails():
    with pytest.raises(ValueError, match="missing required columns"):
        MartSpec(name="m", grain=("a",), columns=(Column("a", "string", nullable=False),))


def test_nullable_grain_column_fails():
    with pytest.raises(ValueError, match="must be non-nullable"):
        MartSpec(name="m", grain=("a",), columns=(Column("a", "string"),) + _lineage())


def test_duplicate_column_fails():
    with pytest.raises(ValueError, match="duplicate columns"):
        MartSpec(
            name="m",
            grain=("a",),
            columns=(Column("a", "string", nullable=False), Column("a", "long")) + _lineage(),
        )


def test_empty_grain_fails():
    with pytest.raises(ValueError, match="grain must not be empty"):
        MartSpec(name="m", grain=(), columns=_lineage())


def test_unknown_partition_column_fails():
    with pytest.raises(ValueError, match="partition columns absent"):
        MartSpec(
            name="m",
            grain=("a",),
            columns=(Column("a", "string", nullable=False),) + _lineage(),
            partition_by=("nope",),
        )


def test_blank_names_are_rejected():
    with pytest.raises(ValueError):
        Column("  ", "string")
    with pytest.raises(ValueError):
        MartSpec(name=" ", grain=("a",), columns=(Column("a", "string", nullable=False),) + _lineage())


# --- 18: run identity ---------------------------------------------------


def test_run_id_is_deterministic():
    assert make_gold_run_id(AS_OF, 30, RULE) == make_gold_run_id(AS_OF, 30, RULE)


def test_run_id_changes_with_rule_version_window_and_instant():
    base = make_gold_run_id(AS_OF, 30, RULE)
    assert make_gold_run_id(AS_OF, 30, "marketplace-gold-rules.v2") != base
    assert make_gold_run_id(AS_OF, 7, RULE) != base
    assert make_gold_run_id(AS_OF.replace(hour=1), 30, RULE) != base


def test_run_id_requires_aware_instant_and_positive_window():
    with pytest.raises(ValueError, match="timezone-aware"):
        make_gold_run_id(datetime(2026, 9, 8), 30, RULE)
    with pytest.raises(ValueError, match="positive int"):
        make_gold_run_id(AS_OF, 0, RULE)
    with pytest.raises(ValueError, match="rule_version"):
        make_gold_run_id(AS_OF, 30, "  ")


# --- 19: paths ----------------------------------------------------------


def test_gold_path_is_run_scoped_and_has_no_latest_pointer():
    run_id = make_gold_run_id(AS_OF, 30, RULE)
    path = gold_dataset_path(MART_BY_NAME["offer_current"], gold_run_id=run_id)
    assert f"gold_run_id={run_id}" in path
    assert "/offer_current/" in path
    assert "latest" not in path and "manifest" not in path


def test_gold_path_is_stable_for_one_run():
    run_id = make_gold_run_id(AS_OF, 30, RULE)
    spec = MART_BY_NAME["counter_delta_daily"]
    assert gold_dataset_path(spec, gold_run_id=run_id) == gold_dataset_path(spec, gold_run_id=run_id)


def test_gold_path_rejects_bad_arguments():
    run_id = make_gold_run_id(AS_OF, 30, RULE)
    with pytest.raises(TypeError):
        gold_dataset_path("offer_current", gold_run_id=run_id)
    with pytest.raises(ValueError):
        gold_dataset_path(MART_BY_NAME["offer_current"], gold_run_id="  ")


def test_contracts_import_without_pyspark_installed():
    import importlib

    with pytest.raises(ImportError):
        importlib.import_module("pyspark")
    module = importlib.import_module("batch_layer.marketplace_gold_contracts")
    assert len(module.MARKETPLACE_MART_SPECS) == 8


def test_to_spark_schema_needs_pyspark_and_says_so():
    with pytest.raises(ImportError):
        MART_BY_NAME["offer_current"].to_spark_schema()


# --- semantics guards ---------------------------------------------------


def test_no_mart_column_is_named_after_sales():
    forbidden = ("sale", "sales", "sold_units", "orders", "revenue", "demand", "units_sold")
    for spec in MARKETPLACE_MART_SPECS:
        offenders = [name for name in spec.column_names if name in forbidden]
        assert offenders == [], f"{spec.name}: {offenders}"


def test_the_public_counter_keeps_its_source_name():
    assert "sold_count" in MART_BY_NAME["offer_current"].column_names


def test_counter_mart_reports_validity_per_reason():
    columns = MART_BY_NAME["counter_delta_daily"].column_names
    for reason in (
        "no_previous_rows",
        "negative_delta_rows",
        "gap_too_long_rows",
        "non_positive_elapsed_rows",
        "invalid_rows",
        "valid_rows",
    ):
        assert reason in columns


def test_quantile_mart_declares_its_method_and_accuracy():
    columns = MART_BY_NAME["category_price_daily"].column_names
    assert "quantile_method" in columns and "quantile_accuracy" in columns


def test_price_history_has_no_mean_column():
    columns = MART_BY_NAME["offer_price_history_daily"].column_names
    assert not any("avg" in name or "mean" in name for name in columns)
    assert "price_close" in columns


def test_reliability_mart_can_record_a_skip_reason():
    assert "skipped_reason" in MART_BY_NAME["crawl_reliability_daily"].column_names
