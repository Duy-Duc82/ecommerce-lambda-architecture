"""Quality gate tests, Phase 7 plan section 17 items 17-31.

The registry tests need no Spark. The evaluation tests are marked individually
so the registry contract still gets checked on a runtime that cannot serve
DataFrames.
"""
import pytest

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
