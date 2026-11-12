"""The discount-credibility rules: vocabulary, weights and thresholds (ADR-0009).

The question the rules answer is "can the discount this page shows be taken at
face value?" They are judged per offer-day against the offer's **own** price
history, never against another offer, and the answer is a graded verdict with
its reasons, never a claim that a seller is dishonest or that a price is wrong.

Two external anchors fix the thresholds, so they are not taste:

- the *reference price* is the lowest price the offer showed in the 30 days
  before the day judged (EU Directive 2019/2161 "Omnibus", art. 6a of
  98/6/EC: a price-reduction announcement must state the lowest prior price
  of at least 30 days);
- a displayed reduction above 50 % of the list price is outside what a
  promotion may normally announce (Vietnam, Nghị định 81/2018/NĐ-CP, điều 7).

This module is pure: constants, a frozen config and small functions on plain
values, so the Spark builder, the tests and a report can all read one source
of truth for what each reason means and how the score is composed.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

DISCOUNT_RULE_VERSION = "discount-rules.v1"

# Reason codes, each one observable from the mart's own columns.
PRICE_ABOVE_LIST = "PRICE_ABOVE_LIST"  # the price is higher than the list price it is "reduced from"
DISCOUNT_OVER_CAP = "DISCOUNT_OVER_CAP"  # the displayed reduction exceeds the legal promotion cap
LIST_PRICE_RAISED = "LIST_PRICE_RAISED"  # the list price is above every list price of the reference window
CLAIM_EXCEEDS_HISTORY = "CLAIM_EXCEEDS_HISTORY"  # displayed reduction minus the real one, against the reference price, is large
NOT_BELOW_REFERENCE = "NOT_BELOW_REFERENCE"  # a reduction is displayed, yet the price is not below the prior lowest

REASONS = (PRICE_ABOVE_LIST, DISCOUNT_OVER_CAP, LIST_PRICE_RAISED, CLAIM_EXCEEDS_HISTORY, NOT_BELOW_REFERENCE)

# The weight each reason adds to the rule score (0..100, capped). The two
# reasons that need no history are weighted so that either alone is
# QUESTIONABLE and the pair is SUSPECT; the history-based claim gap alone is
# SUSPECT because it is the one that directly contradicts the displayed figure.
REASON_WEIGHTS: dict[str, int] = {
    PRICE_ABOVE_LIST: 40,
    DISCOUNT_OVER_CAP: 30,
    LIST_PRICE_RAISED: 30,
    CLAIM_EXCEEDS_HISTORY: 50,
    NOT_BELOW_REFERENCE: 20,
}
SCORE_CAP = 100

STATUS_NO_DISCOUNT = "NO_DISCOUNT"  # nothing is displayed as reduced, and nothing contradicts that
STATUS_INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"  # a reduction is displayed, no reason fires, too little history to vouch
STATUS_CREDIBLE = "CREDIBLE"
STATUS_QUESTIONABLE = "QUESTIONABLE"
STATUS_SUSPECT = "SUSPECT"
STATUSES = (STATUS_NO_DISCOUNT, STATUS_INSUFFICIENT_HISTORY, STATUS_CREDIBLE, STATUS_QUESTIONABLE, STATUS_SUSPECT)

SUSPECT_FROM = 50
QUESTIONABLE_FROM = 20

ML_ISOLATION_FOREST = "ISOLATION_FOREST"
ML_NONE = "NONE"


@dataclass(frozen=True)
class DiscountRuleConfig:
    rule_version: str = DISCOUNT_RULE_VERSION
    # Calendar days before the judged day that the reference price is taken over.
    reference_window_days: int = 30
    # Observed days inside the window needed before the history-based reasons may fire.
    min_history_days: int = 3
    # Displayed reductions above this many percent are DISCOUNT_OVER_CAP.
    legal_cap_percent: Decimal = Decimal("50")
    # A displayed reduction more than this many points above the real one is CLAIM_EXCEEDS_HISTORY.
    claim_gap_percent: Decimal = Decimal("20")
    # Isolation Forest ranking: off, or the most rows it is fitted on (a deterministic sample).
    ml_enabled: bool = True
    ml_sample_rows: int = 200_000
    ml_seed: int = 20261113

    def __post_init__(self) -> None:
        if not self.rule_version.strip():
            raise ValueError("rule_version must be non-empty")
        if self.reference_window_days <= 0 or self.min_history_days <= 0:
            raise ValueError("reference_window_days and min_history_days must be positive")
        if self.min_history_days > self.reference_window_days:
            # No row could ever have enough history: the whole mart would read
            # INSUFFICIENT_HISTORY while nothing looked broken.
            raise ValueError("min_history_days cannot exceed reference_window_days")
        if not Decimal("0") < self.legal_cap_percent <= Decimal("100"):
            raise ValueError("legal_cap_percent must be within (0, 100]")
        if self.claim_gap_percent <= 0:
            raise ValueError("claim_gap_percent must be positive")
        if self.ml_sample_rows <= 0:
            raise ValueError("ml_sample_rows must be positive")


def rule_score(reasons: tuple[str, ...] | list[str]) -> int:
    """The capped sum of the reasons' weights; an unknown reason is a programming error."""
    unknown = [r for r in reasons if r not in REASON_WEIGHTS]
    if unknown:
        raise ValueError(f"unknown discount reasons: {unknown}")
    return min(SCORE_CAP, sum(REASON_WEIGHTS[r] for r in set(reasons)))


def credibility_status(*, displayed: bool, score: int, enough_history: bool) -> str:
    """The verdict from what the page shows, what the reasons add up to, and whether history can vouch.

    ``displayed`` is "a reduction is shown or a price sits above its list
    price": a row with nothing displayed and nothing contradicting it is
    NO_DISCOUNT whatever its history. A displayed reduction with no reason
    firing is CREDIBLE only once the history is long enough to have looked.
    """
    if score < 0 or score > SCORE_CAP:
        raise ValueError("score must be within 0..100")
    if not displayed and score == 0:
        return STATUS_NO_DISCOUNT
    if score >= SUSPECT_FROM:
        return STATUS_SUSPECT
    if score >= QUESTIONABLE_FROM:
        return STATUS_QUESTIONABLE
    return STATUS_CREDIBLE if enough_history else STATUS_INSUFFICIENT_HISTORY
