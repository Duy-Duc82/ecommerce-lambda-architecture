"""Explicit registry for the marketplace data-quality gate.

Phase 7 plan section 7. The mandatory list is the parent Brief's section 16
list, one entry each; the evaluator may not invent a check name, drop one, or
decide a check does not apply. A check that cannot run reports SKIPPED, and a
skipped mandatory check fails the gate — an unreachable audit database must
never read as a green light.

This module holds names and expectations only. It imports nothing from
``batch_layer`` so that the contract can be read, and its spelling checked,
without starting Spark.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

MANDATORY = "MANDATORY"
ADVISORY = "ADVISORY"

SEVERITIES = (MANDATORY, ADVISORY)
CHECK_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")

# The ten Gold datasets, repeated here rather than imported so config stays
# free of batch_layer. A test asserts this matches the publisher's allowlist,
# which is what catches the two drifting apart.
GOLD_DATASETS = (
    "offer_current", "seller_current", "offer_price_history_daily",
    "offer_change_daily", "offer_freshness", "category_price_daily",
    "source_coverage_daily", "crawl_reliability_daily", "counter_delta_daily",
    "price_anomaly_daily",
)
SOURCE_DATASETS = ("silver", "audit")


@dataclass(frozen=True)
class QualityRule:
    check_name: str
    severity: str
    dataset_name: str
    expectation: str

    def __post_init__(self) -> None:
        if not CHECK_NAME_RE.fullmatch(self.check_name):
            raise ValueError(f"check_name must match {CHECK_NAME_RE.pattern}: {self.check_name!r}")
        if self.severity not in SEVERITIES:
            raise ValueError(f"severity must be one of {SEVERITIES}: {self.severity!r}")
        if self.dataset_name not in SOURCE_DATASETS + GOLD_DATASETS:
            raise ValueError(f"unknown dataset_name: {self.dataset_name!r}")
        if not self.expectation.strip():
            raise ValueError(f"expectation must be non-empty: {self.check_name}")


QUALITY_RULES: tuple[QualityRule, ...] = (
    QualityRule(
        "raw_artifact_checksum_and_uri_present", MANDATORY, "silver",
        "every observation carries a non-empty raw_uri and a 64-character hexadecimal raw_sha256",
    ),
    QualityRule(
        "silver_observation_raw_lineage_complete", MANDATORY, "silver",
        "every observation carries crawl_run_id, adapter_version and fetched_at",
    ),
    QualityRule(
        "observation_id_unique", MANDATORY, "silver",
        "the count of distinct observation_id equals the row count",
    ),
    QualityRule(
        "offer_key_and_price_complete", MANDATORY, "silver",
        "offer_id, marketplace, observed_at and current_price are all non-null",
    ),
    QualityRule(
        "price_non_negative", MANDATORY, "silver",
        "current_price is at least zero and list_price is null or at least zero",
    ),
    QualityRule(
        "observed_at_within_future_tolerance", MANDATORY, "silver",
        "observed_at is no later than the run as_of plus the configured future tolerance",
    ),
    QualityRule(
        "currency_valid", MANDATORY, "silver",
        "currency is a three-letter uppercase code inside the configured allowlist",
    ),
    QualityRule(
        "silver_parse_attempt_reconciliation", MANDATORY, "audit",
        "per crawl run present in this input, the Silver observation count equals the audit parsed_count",
    ),
    QualityRule(
        "offer_listing_key_unique", MANDATORY, "silver",
        "no marketplace_id and platform_listing_id pair maps to more than one offer_id",
    ),
    QualityRule(
        "offer_current_single_row_per_offer", MANDATORY, "offer_current",
        "offer_current holds exactly one row per offer_id",
    ),
    QualityRule(
        "gold_daily_row_count_reconciles", MANDATORY, "offer_price_history_daily",
        "the sum of observation_count equals the deduplicated Silver row count",
    ),
    QualityRule(
        "gold_daily_price_aggregates_reconcile", MANDATORY, "offer_price_history_daily",
        "per offer and observed date, min_price and max_price equal a direct recomputation from Silver",
    ),
    QualityRule(
        "freshness_rule_version_and_as_of_applied", MANDATORY, "offer_freshness",
        "every row carries the run as_of and freshness rule version and a non-null freshness status",
    ),
    QualityRule(
        "counter_invalid_transition_rate", ADVISORY, "counter_delta_daily",
        "the share of public-counter transitions marked invalid is reported for review",
    ),
    QualityRule(
        "price_anomaly_evaluation_coverage", ADVISORY, "price_anomaly_daily",
        "the share of offer-days with enough baseline history to be evaluated is reported for review",
    ),
    QualityRule(
        "source_coverage_rate_denominator_present", ADVISORY, "source_coverage_daily",
        "coverage_rate is non-null wherever eligible_offer_count is positive",
    ),
    QualityRule(
        "crawl_reliability_evidence_present", ADVISORY, "crawl_reliability_daily",
        "crawl_reliability_daily is non-empty whenever Silver holds observations",
    ),
)

_BY_NAME = {rule.check_name: rule for rule in QUALITY_RULES}
if len(_BY_NAME) != len(QUALITY_RULES):
    raise ValueError("duplicate quality check name in the registry")


def quality_rule(check_name: str) -> QualityRule:
    """Look one rule up. An unregistered name is a defect, not a fallback."""
    try:
        return _BY_NAME[check_name]
    except KeyError:
        raise ValueError(f"unregistered quality check: {check_name!r}") from None


def rules_with_severity(severity: str) -> tuple[QualityRule, ...]:
    if severity not in SEVERITIES:
        raise ValueError(f"severity must be one of {SEVERITIES}: {severity!r}")
    return tuple(rule for rule in QUALITY_RULES if rule.severity == severity)


def mandatory_rule_names() -> tuple[str, ...]:
    return tuple(rule.check_name for rule in rules_with_severity(MANDATORY))
