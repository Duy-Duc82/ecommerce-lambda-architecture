"""Declared schemas and paths for the marketplace Gold marts.

Deviation from the Phase 6 plan, recorded deliberately: the plan declared
``MartSpec.schema`` as a Spark ``StructType``.  Doing that would require pyspark
at import time, which would make the mart contract — the thing most worth
testing — untestable wherever Spark is not installed, including this repo's own
test environment.  So the contract is declared as pure data and converted with
``to_spark_schema()``, which imports pyspark lazily.  The contract is still the
authority: transforms must satisfy it, not define it.

Column types are a closed vocabulary.  ``double`` and ``float`` are absent on
purpose: a money column stored as a float would make the published number
disagree with the Decimal it was derived from, and the disagreement would be
invisible.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote

from common.identity import deterministic_id
from config.settings import (
    MARKETPLACE_DECIMAL_PRECISION,
    MARKETPLACE_DECIMAL_SCALE,
    data_lake_uri,
)

MONEY = "money"
COLUMN_TYPES = ("string", "long", "boolean", "timestamp", "date", MONEY)
GOLD_DATASET_ROOT = "marketplace"

# Every mart row must be traceable to a run, an instant and a rule version.
REQUIRED_COLUMNS = ("gold_run_id", "as_of", "rule_version")


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("column name is required")
        if self.type not in COLUMN_TYPES:
            raise ValueError(
                f"column {self.name}: unsupported type {self.type!r}; "
                f"allowed types are {COLUMN_TYPES}"
            )


@dataclass(frozen=True)
class MartSpec:
    name: str
    grain: tuple[str, ...]
    columns: tuple[Column, ...]
    partition_by: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("mart name is required")
        if not self.grain:
            raise ValueError(f"mart {self.name}: grain must not be empty")
        names = [column.name for column in self.columns]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"mart {self.name}: duplicate columns {duplicates}")
        missing_grain = [column for column in self.grain if column not in names]
        if missing_grain:
            raise ValueError(f"mart {self.name}: grain columns absent from schema {missing_grain}")
        missing_required = [column for column in REQUIRED_COLUMNS if column not in names]
        if missing_required:
            raise ValueError(f"mart {self.name}: missing required columns {missing_required}")
        unknown_partitions = [column for column in self.partition_by if column not in names]
        if unknown_partitions:
            raise ValueError(f"mart {self.name}: partition columns absent {unknown_partitions}")
        for column in self.grain:
            spec = self.columns[names.index(column)]
            if spec.nullable:
                raise ValueError(f"mart {self.name}: grain column {column} must be non-nullable")

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    @property
    def money_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns if column.type == MONEY)

    def to_spark_schema(self):
        """Build the Spark StructType. pyspark is imported lazily."""
        from pyspark.sql.types import (  # type: ignore
            BooleanType,
            DateType,
            DecimalType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        mapping = {
            "string": StringType(),
            "long": LongType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType(),
            "date": DateType(),
            MONEY: DecimalType(MARKETPLACE_DECIMAL_PRECISION, MARKETPLACE_DECIMAL_SCALE),
        }
        return StructType(
            [
                StructField(column.name, mapping[column.type], column.nullable)
                for column in self.columns
            ]
        )


def _lineage() -> tuple[Column, ...]:
    return (
        Column("gold_run_id", "string", nullable=False),
        Column("as_of", "timestamp", nullable=False),
        Column("rule_version", "string", nullable=False),
    )


OFFER_CURRENT = MartSpec(
    name="offer_current",
    grain=("offer_id",),
    columns=(
        Column("offer_id", "string", nullable=False),
        Column("marketplace", "string", nullable=False),
        Column("platform_listing_id", "string", nullable=False),
        Column("seller_id", "string"),
        Column("product_title", "string"),
        Column("brand", "string"),
        Column("category_path", "string"),
        Column("source_url", "string"),
        Column("currency", "string"),
        Column("observation_id", "string", nullable=False),
        Column("observed_at", "timestamp", nullable=False),
        Column("current_price", MONEY),
        Column("list_price", MONEY),
        Column("rating_value", MONEY),
        Column("review_count", "long"),
        Column("sold_count", "long"),
        Column("availability", "string"),
        Column("freshness_status", "string", nullable=False),
        Column("age_minutes", "long", nullable=False),
        Column("raw_uri", "string"),
        *_lineage(),
    ),
    partition_by=("marketplace",),
)

OFFER_PRICE_HISTORY_DAILY = MartSpec(
    name="offer_price_history_daily",
    grain=("offer_id", "observed_date"),
    columns=(
        Column("offer_id", "string", nullable=False),
        Column("observed_date", "date", nullable=False),
        Column("marketplace", "string", nullable=False),
        Column("price_open", MONEY),
        Column("price_close", MONEY),
        Column("price_min", MONEY),
        Column("price_max", MONEY),
        Column("observation_count", "long", nullable=False),
        *_lineage(),
    ),
    partition_by=("marketplace", "observed_date"),
)

OFFER_CHANGE_DAILY = MartSpec(
    name="offer_change_daily",
    grain=("offer_id", "observed_date"),
    columns=(
        Column("offer_id", "string", nullable=False),
        Column("observed_date", "date", nullable=False),
        Column("marketplace", "string", nullable=False),
        Column("price_changes", "long", nullable=False),
        Column("large_price_drops", "long", nullable=False),
        Column("availability_changes", "long", nullable=False),
        Column("availability_transitions_excluded", "long", nullable=False),
        Column("counter_changes", "long", nullable=False),
        Column("max_abs_price_delta", MONEY),
        Column("avg_abs_price_delta", MONEY),
        Column("max_drop_percent", MONEY),
        *_lineage(),
    ),
    partition_by=("marketplace", "observed_date"),
)

OFFER_FRESHNESS = MartSpec(
    name="offer_freshness",
    grain=("offer_id",),
    columns=(
        Column("offer_id", "string", nullable=False),
        Column("marketplace", "string", nullable=False),
        Column("last_observation_at", "timestamp", nullable=False),
        Column("last_observation_id", "string", nullable=False),
        Column("age_minutes", "long", nullable=False),
        Column("freshness_status", "string", nullable=False),
        Column("fresh_threshold_minutes", "long", nullable=False),
        Column("stale_threshold_minutes", "long", nullable=False),
        *_lineage(),
    ),
    partition_by=("marketplace",),
)

CATEGORY_PRICE_DAILY = MartSpec(
    name="category_price_daily",
    grain=("marketplace", "category_path", "observed_date"),
    columns=(
        Column("marketplace", "string", nullable=False),
        Column("category_path", "string", nullable=False),
        Column("observed_date", "date", nullable=False),
        Column("offer_count", "long", nullable=False),
        Column("observation_count", "long", nullable=False),
        Column("price_min", MONEY),
        Column("price_max", MONEY),
        Column("price_p25", MONEY),
        Column("price_median", MONEY),
        Column("price_p75", MONEY),
        # A distribution panel that cannot say how its median was computed is
        # not evidence, so the method travels with the numbers.
        Column("quantile_method", "string", nullable=False),
        Column("quantile_accuracy", "long", nullable=False),
        *_lineage(),
    ),
    partition_by=("marketplace", "observed_date"),
)

SOURCE_COVERAGE_DAILY = MartSpec(
    name="source_coverage_daily",
    grain=("marketplace", "observed_date"),
    columns=(
        Column("marketplace", "string", nullable=False),
        Column("observed_date", "date", nullable=False),
        Column("offers_observed", "long", nullable=False),
        Column("observations", "long", nullable=False),
        Column("offers_missing", "long", nullable=False),
        Column("offers_stale", "long", nullable=False),
        Column("offers_without_seller", "long", nullable=False),
        Column("rows_rejected", "long", nullable=False),
        *_lineage(),
    ),
    partition_by=("marketplace", "observed_date"),
)

CRAWL_RELIABILITY_DAILY = MartSpec(
    name="crawl_reliability_daily",
    grain=("marketplace", "observed_date"),
    columns=(
        Column("marketplace", "string", nullable=False),
        Column("observed_date", "date", nullable=False),
        Column("requests", "long", nullable=False),
        Column("succeeded", "long", nullable=False),
        Column("failed", "long", nullable=False),
        Column("success_rate", MONEY),
        Column("p50_latency_ms", "long"),
        Column("p95_latency_ms", "long"),
        Column("errors_json", "string"),
        # Populated when the Phase 3 audit source is unavailable. An empty
        # mart with a stated reason beats a mart invented from Silver.
        Column("skipped_reason", "string"),
        *_lineage(),
    ),
    partition_by=("marketplace", "observed_date"),
)

COUNTER_DELTA_DAILY = MartSpec(
    name="counter_delta_daily",
    grain=("offer_id", "observed_date"),
    columns=(
        Column("offer_id", "string", nullable=False),
        Column("observed_date", "date", nullable=False),
        Column("marketplace", "string", nullable=False),
        Column("total_valid_delta", "long", nullable=False),
        Column("valid_rows", "long", nullable=False),
        Column("invalid_rows", "long", nullable=False),
        Column("no_previous_rows", "long", nullable=False),
        Column("negative_delta_rows", "long", nullable=False),
        Column("gap_too_long_rows", "long", nullable=False),
        Column("non_positive_elapsed_rows", "long", nullable=False),
        Column("max_velocity_per_hour", MONEY),
        *_lineage(),
    ),
    partition_by=("marketplace", "observed_date"),
)

MARKETPLACE_MART_SPECS: tuple[MartSpec, ...] = (
    OFFER_CURRENT,
    OFFER_PRICE_HISTORY_DAILY,
    OFFER_CHANGE_DAILY,
    OFFER_FRESHNESS,
    CATEGORY_PRICE_DAILY,
    SOURCE_COVERAGE_DAILY,
    CRAWL_RELIABILITY_DAILY,
    COUNTER_DELTA_DAILY,
)

_names = [spec.name for spec in MARKETPLACE_MART_SPECS]
if len(_names) != len(set(_names)):
    raise ValueError("mart names must be unique")

MART_BY_NAME: dict[str, MartSpec] = {spec.name: spec for spec in MARKETPLACE_MART_SPECS}


def make_gold_run_id(as_of: datetime, window_days: int, rule_version: str) -> str:
    """Deterministic run identity.

    Re-running the same logical window targets the same partition and
    overwrites it, instead of accumulating a second copy of the same Gold.
    """
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    if not isinstance(window_days, int) or isinstance(window_days, bool) or window_days < 1:
        raise ValueError("window_days must be a positive int")
    if not isinstance(rule_version, str) or not rule_version.strip():
        raise ValueError("rule_version is required")
    return deterministic_id("goldrun", as_of, window_days, rule_version.strip())


def gold_dataset_path(mart: MartSpec, *, gold_run_id: str) -> str:
    """Run-scoped Gold path.

    There is no ``latest`` pointer and no manifest in this phase: promotion
    belongs to the phase that adds the mandatory quality gates. Advertising
    ungated Gold as current is exactly what that ordering exists to prevent.
    """
    if not isinstance(mart, MartSpec):
        raise TypeError("mart must be a MartSpec")
    if not isinstance(gold_run_id, str) or not gold_run_id.strip():
        raise ValueError("gold_run_id is required")
    dataset = (
        f"{GOLD_DATASET_ROOT}/{quote(mart.name, safe='-_.~')}/"
        f"gold_run_id={quote(gold_run_id.strip(), safe='-_.~')}"
    )
    return data_lake_uri("gold", dataset)
