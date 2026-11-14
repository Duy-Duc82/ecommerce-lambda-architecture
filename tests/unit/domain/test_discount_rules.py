"""The discount-credibility vocabulary and scoring (ADR-0009), without Spark."""
from __future__ import annotations

from decimal import Decimal

import pytest

from ecomdata.domain import discount_rules as d


def test_every_reason_has_a_weight_and_the_score_is_capped():
    assert set(d.REASON_WEIGHTS) == set(d.REASONS)
    assert d.rule_score(()) == 0
    assert d.rule_score((d.PRICE_ABOVE_LIST,)) == 40
    # Repeating a reason does not count it twice; the cap holds the sum at 100.
    assert d.rule_score((d.CLAIM_EXCEEDS_HISTORY, d.CLAIM_EXCEEDS_HISTORY)) == 50
    assert d.rule_score(d.REASONS) == 100
    with pytest.raises(ValueError, match="unknown discount reasons"):
        d.rule_score(("LOOKS_ODD",))


def test_the_two_history_free_reasons_are_each_questionable_and_together_suspect():
    one = d.rule_score((d.DISCOUNT_OVER_CAP,))
    both = d.rule_score((d.DISCOUNT_OVER_CAP, d.PRICE_ABOVE_LIST))

    assert d.credibility_status(displayed=True, score=one, enough_history=False) == d.STATUS_QUESTIONABLE
    assert d.credibility_status(displayed=True, score=both, enough_history=False) == d.STATUS_SUSPECT


def test_a_contradicted_claim_alone_is_suspect():
    score = d.rule_score((d.CLAIM_EXCEEDS_HISTORY,))
    assert d.credibility_status(displayed=True, score=score, enough_history=True) == d.STATUS_SUSPECT


def test_nothing_displayed_and_nothing_contradicting_is_no_discount_whatever_the_history():
    assert d.credibility_status(displayed=False, score=0, enough_history=False) == d.STATUS_NO_DISCOUNT
    assert d.credibility_status(displayed=False, score=0, enough_history=True) == d.STATUS_NO_DISCOUNT


def test_a_clean_reduction_is_credible_only_once_history_can_vouch():
    assert d.credibility_status(displayed=True, score=0, enough_history=False) == d.STATUS_INSUFFICIENT_HISTORY
    assert d.credibility_status(displayed=True, score=0, enough_history=True) == d.STATUS_CREDIBLE


def test_the_score_range_is_checked():
    with pytest.raises(ValueError, match="score"):
        d.credibility_status(displayed=True, score=101, enough_history=True)


def test_the_config_refuses_a_minimum_no_window_can_reach_and_a_cap_outside_percent():
    d.DiscountRuleConfig()
    with pytest.raises(ValueError, match="min_history_days cannot exceed"):
        d.DiscountRuleConfig(reference_window_days=3, min_history_days=4)
    with pytest.raises(ValueError, match="legal_cap_percent"):
        d.DiscountRuleConfig(legal_cap_percent=Decimal("0"))
    with pytest.raises(ValueError, match="claim_gap_percent"):
        d.DiscountRuleConfig(claim_gap_percent=Decimal("-1"))
    with pytest.raises(ValueError, match="rule_version"):
        d.DiscountRuleConfig(rule_version=" ")
