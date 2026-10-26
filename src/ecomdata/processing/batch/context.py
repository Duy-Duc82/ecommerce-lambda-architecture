"""What a batch run is: its identity, its rule configuration and its outcome types.

``MarketplaceBatchContext`` is the run's identity (id, as_of, where Silver and
Gold live) together with the versioned rules every stage judges by. The batch
bootstrap builds it from the loaded settings; no stage reads settings itself.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import quote

RUN_ID_RE = re.compile(r"[a-zA-Z0-9_-]{1,64}\Z")


@dataclass(frozen=True)
class MarketplaceBatchContext:
    run_id: str
    as_of: datetime
    silver_uri: str
    gold_root_uri: str
    # Defaults are those of BatchSettings and QualitySettings; the batch
    # bootstrap builds the context from the loaded settings.
    freshness_seconds: int = 21600
    freshness_rule_version: str = "freshness-rules.v1"
    counter_rule_version: str = "counter-rules.v1"
    counter_max_gap_seconds: int = 86400
    # The speed layer's change rules, which fact_price_change applies too.
    change_rule_version: str = "speed-rules.v1"
    large_drop_absolute: Decimal = Decimal("100000")
    large_drop_relative: Decimal = Decimal("0.20")
    anomaly_window_days: int = 14
    anomaly_min_samples: int = 7
    anomaly_mad_threshold: Decimal = Decimal("3.5")
    anomaly_iqr_multiplier: Decimal = Decimal("1.5")
    anomaly_rule_version: str = "anomaly-rules.v1"
    quality_rule_version: str = "quality-rules.v2"
    future_tolerance_seconds: int = 300
    reconciliation_settle_seconds: int = 900
    reconciliation_lookback_seconds: int = 172800
    allowed_currencies: tuple[str, ...] = ("USD", "VND")
    percentile_accuracy: int = 10000
    sample_limit: int = 10

    def __post_init__(self) -> None:
        if not RUN_ID_RE.fullmatch(self.run_id):
            raise ValueError("run_id must match [a-zA-Z0-9_-]{1,64}")
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        object.__setattr__(self, "as_of", self.as_of.astimezone(UTC))
        for name in (
            "silver_uri",
            "gold_root_uri",
            "freshness_rule_version",
            "counter_rule_version",
            "change_rule_version",
            "anomaly_rule_version",
            "quality_rule_version",
        ):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must be non-empty")
        for name in (
            "freshness_seconds",
            "counter_max_gap_seconds",
            "anomaly_window_days",
            "anomaly_min_samples",
            "anomaly_mad_threshold",
            "anomaly_iqr_multiplier",
            "future_tolerance_seconds",
            "reconciliation_settle_seconds",
            "reconciliation_lookback_seconds",
            "percentile_accuracy",
            "sample_limit",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.large_drop_absolute < 0:
            raise ValueError("large_drop_absolute must be non-negative")
        if not Decimal("0") <= self.large_drop_relative <= Decimal("1"):
            raise ValueError("large_drop_relative must be between 0 and 1")
        # A minimum above the frame size would make every row report
        # INSUFFICIENT_HISTORY while nothing looked broken.
        if self.anomaly_min_samples > self.anomaly_window_days:
            raise ValueError("anomaly_min_samples cannot exceed anomaly_window_days")
        # An empty reconciliation window would reconcile nothing and pass.
        if self.reconciliation_lookback_seconds <= self.reconciliation_settle_seconds:
            raise ValueError("reconciliation_lookback_seconds must exceed reconciliation_settle_seconds")
        # Normalised and sorted so a manifest built from this context is
        # byte-stable regardless of how the environment spelled the list.
        codes = tuple(sorted({str(code).strip().upper() for code in self.allowed_currencies if str(code).strip()}))
        if not codes:
            raise ValueError("allowed_currencies must list at least one code")
        if any(len(code) != 3 or not code.isalpha() for code in codes):
            raise ValueError("allowed_currencies must hold ISO-4217 style codes")
        object.__setattr__(self, "allowed_currencies", codes)

    @property
    def gold_run_uri(self) -> str:
        """Where this run's marts live: the one place that computes it."""
        return f"{self.gold_root_uri.rstrip('/')}/runs/run_id={quote(self.run_id, safe='')}"

    def gold_dataset_uri(self, dataset_name: str) -> str:
        return f"{self.gold_run_uri}/{dataset_name}"

    @property
    def dw_root_uri(self) -> str:
        """The star: one table per dataset, kept across runs, outside every run directory."""
        return f"{self.gold_root_uri.rstrip('/')}/dw"

    def dw_dataset_uri(self, dataset_name: str) -> str:
        return f"{self.dw_root_uri}/{dataset_name}"


class ConflictingObservationError(ValueError):
    pass


@dataclass(frozen=True)
class GoldWriteResult:
    dataset_name: str
    uri: str
    row_count: int


@dataclass(frozen=True)
class DwWriteResult:
    """A star table after a merge: its rows and, for Delta, the table version the merge left."""

    dataset_name: str
    uri: str
    row_count: int
    version: int | None


# A passing run deliberately kept off the serving version for inspection.
QUALITY_ONLY = "QUALITY_ONLY"
# A run into a Gold root other than the serving one. Manifests and the pointer
# live at one fixed place, so such a run must not write either.
SCRATCH_GOLD_ROOT = "SCRATCH_GOLD_ROOT"


@dataclass(frozen=True)
class MarketplaceBatchResult:
    run_id: str
    status: str
    silver_rows: int
    dataset_counts: Mapping[str, int]
    quality_status: str = "PASS"
    mandatory_failure_count: int = 0
    manifest_uri: str | None = None
    manifest_promoted: bool = False
    promotion_reason: str | None = None
