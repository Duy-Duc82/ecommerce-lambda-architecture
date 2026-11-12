"""Describe the marketplace promotion dashboard for Superset provisioning (ADR-0009).

The dashboard answers "can the discounts shown today be taken at face value?"
from two marts: the per-offer verdicts and their reasons, and the category
view of how many offers show a reduction, how deep, and how many the rules
graded QUESTIONABLE or SUSPECT.
"""

from __future__ import annotations

import json

DATASETS = ["marketplace_discount_credibility_daily", "marketplace_discount_daily", "marketplace_offer_current"]

CHART_TITLES = [
    "Offers showing a reduction per source and day, and their share",
    "Displayed reduction distribution (p25, median, p75) per category and day",
    "Credibility verdicts per day: CREDIBLE, QUESTIONABLE, SUSPECT, INSUFFICIENT_HISTORY",
    "Reasons fired per day (PRICE_ABOVE_LIST, DISCOUNT_OVER_CAP, LIST_PRICE_RAISED, CLAIM_EXCEEDS_HISTORY, NOT_BELOW_REFERENCE)",
    "Displayed versus effective reduction, offer-days with enough history",
    "Suspect reductions of the day, ranked by rule score then Isolation Forest rank, with title and reasons",
    "Categories by share of over-cap reductions",
    "Share of discounted offer-days with a verdict (history coverage)",
]

# Wording is contract. A verdict is graded against the offer's own prior
# prices and names its reasons; it is never a claim about the seller.
NOTES = [
    "The reference price is the lowest price this offer showed in the window before the day (Omnibus rule).",
    "A displayed reduction is what the page claims; the effective reduction is against the reference price.",
    "SUSPECT and QUESTIONABLE are graded statements with their reasons, not findings of dishonesty.",
    "INSUFFICIENT_HISTORY means no verdict: the offer has not been watched long enough.",
    "The Isolation Forest rank orders unusual reductions for review; it decides nothing.",
    "No panel compares one offer, seller or marketplace against another.",
]


def dashboard_spec() -> dict[str, object]:
    return {
        "title": "Marketplace promotions — discount credibility",
        "datasets": list(DATASETS),
        "charts": list(CHART_TITLES),
        "notes": list(NOTES),
    }


def main() -> None:
    print(json.dumps(dashboard_spec(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
