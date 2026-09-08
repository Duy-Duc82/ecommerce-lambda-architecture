"""Pure temporal rules for the marketplace batch warehouse.

No Spark, no database, no clock.  Every rule is a function of its arguments, so
the semantics that decide what the thesis reports — what counts as fresh, what
counts as a usable counter delta, what the representative daily price is — can
be verified without a cluster.

The Spark job composes these rules.  Where a rule has to run as a column
expression for performance, the expression and the function here are covered by
one shared table of cases, so the two cannot drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Sequence

DECIMAL_SCALE = 6
_QUANTUM = Decimal(1).scaleb(-DECIMAL_SCALE)
_PERCENT_QUANTUM = Decimal("0.0001")


class FreshnessStatus(str, Enum):
    FRESH = "FRESH"
    AGING = "AGING"
    STALE = "STALE"


class CounterValidity(str, Enum):
    VALID = "VALID"
    NO_PREVIOUS = "NO_PREVIOUS"
    NEGATIVE_DELTA = "NEGATIVE_DELTA"
    GAP_TOO_LONG = "GAP_TOO_LONG"
    NON_POSITIVE_ELAPSED = "NON_POSITIVE_ELAPSED"


@dataclass(frozen=True)
class FreshnessThresholds:
    fresh_after: timedelta
    stale_after: timedelta

    def __post_init__(self) -> None:
        for name in ("fresh_after", "stale_after"):
            value = getattr(self, name)
            if not isinstance(value, timedelta) or value <= timedelta(0):
                raise ValueError(f"{name} must be a positive timedelta")
        if self.stale_after <= self.fresh_after:
            raise ValueError("stale_after must be later than fresh_after")


@dataclass(frozen=True)
class CounterDeltaResult:
    observed_delta: int | None
    elapsed_minutes: int | None
    velocity_per_hour: Decimal | None
    validity: CounterValidity

    @property
    def counter_reset_or_invalid(self) -> bool:
        return self.validity is not CounterValidity.VALID

    def __post_init__(self) -> None:
        # A velocity on an invalid delta would be a number nobody should use,
        # sitting in a column that looks usable.
        if self.validity is not CounterValidity.VALID and self.velocity_per_hour is not None:
            raise ValueError("an invalid counter delta must not carry a velocity")


@dataclass(frozen=True)
class PriceMovement:
    delta_absolute: Decimal | None
    delta_percent: Decimal | None
    is_price_change: bool
    is_large_drop: bool

    def __post_init__(self) -> None:
        if self.is_large_drop and not self.is_price_change:
            raise ValueError("a large drop is always also a price change")


def quantize(value: Decimal | None) -> Decimal | None:
    """Round to the stored scale, half-up, so a value is stable across runs."""
    if value is None:
        return None
    if not isinstance(value, Decimal):
        raise TypeError(f"expected Decimal, got {type(value).__name__}")
    return value.quantize(_QUANTUM, rounding=ROUND_HALF_UP)


def classify_freshness(
    *,
    last_observation_at: datetime,
    as_of: datetime,
    thresholds: FreshnessThresholds,
) -> tuple[FreshnessStatus, int]:
    """Classify one offer's freshness against a supplied instant.

    ``as_of`` is an argument, never a clock read: two runs over the same
    observations and the same ``as_of`` must produce the same marts.
    """
    if last_observation_at.tzinfo is None or as_of.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    if not isinstance(thresholds, FreshnessThresholds):
        raise TypeError("thresholds must be a FreshnessThresholds")
    age = as_of - last_observation_at
    if age < timedelta(0):
        # An observation stamped after as_of is a clock or ordering problem
        # upstream; reporting a negative age would leak it into every average.
        return FreshnessStatus.FRESH, 0
    age_minutes = int(age.total_seconds() // 60)
    if age <= thresholds.fresh_after:
        return FreshnessStatus.FRESH, age_minutes
    if age <= thresholds.stale_after:
        return FreshnessStatus.AGING, age_minutes
    return FreshnessStatus.STALE, age_minutes


def counter_delta(
    *,
    previous_counter: int | None,
    current_counter: int | None,
    previous_observed_at: datetime | None,
    current_observed_at: datetime,
    max_gap: timedelta,
) -> CounterDeltaResult:
    """Compute a public-counter delta and say whether it can be trusted.

    The true delta is always returned, including when it is negative.  Clamping
    a negative delta to zero would turn a counter reset into fabricated
    movement, and nothing downstream could tell the difference afterwards.
    """
    if current_observed_at.tzinfo is None:
        raise ValueError("current_observed_at must be timezone-aware")
    if previous_observed_at is not None and previous_observed_at.tzinfo is None:
        raise ValueError("previous_observed_at must be timezone-aware")
    if not isinstance(max_gap, timedelta) or max_gap <= timedelta(0):
        raise ValueError("max_gap must be a positive timedelta")
    for name, value in (("previous_counter", previous_counter), ("current_counter", current_counter)):
        if isinstance(value, bool):
            raise TypeError(f"{name} must be an int, not bool")
        if value is not None and not isinstance(value, int):
            raise TypeError(f"{name} must be an int or None")

    if previous_counter is None or current_counter is None or previous_observed_at is None:
        return CounterDeltaResult(
            observed_delta=None
            if previous_counter is None or current_counter is None
            else current_counter - previous_counter,
            elapsed_minutes=None,
            velocity_per_hour=None,
            validity=CounterValidity.NO_PREVIOUS,
        )

    observed_delta = current_counter - previous_counter
    elapsed = current_observed_at - previous_observed_at
    elapsed_minutes = int(elapsed.total_seconds() // 60)

    if elapsed <= timedelta(0):
        validity = CounterValidity.NON_POSITIVE_ELAPSED
    elif elapsed > max_gap:
        validity = CounterValidity.GAP_TOO_LONG
    elif observed_delta < 0:
        validity = CounterValidity.NEGATIVE_DELTA
    else:
        validity = CounterValidity.VALID

    velocity = None
    if validity is CounterValidity.VALID:
        hours = Decimal(elapsed.total_seconds()) / Decimal(3600)
        velocity = quantize(Decimal(observed_delta) / hours)

    return CounterDeltaResult(
        observed_delta=observed_delta,
        elapsed_minutes=elapsed_minutes,
        velocity_per_hour=velocity,
        validity=validity,
    )


def price_movement(
    *,
    previous_price: Decimal | None,
    current_price: Decimal,
    large_drop_absolute: Decimal,
    large_drop_percent: Decimal,
) -> PriceMovement:
    """Classify one price transition with the same semantics the speed layer uses.

    Keeping the thresholds and the comparison identical is what makes the batch
    count and the speed-layer count comparable; a difference between them is
    then a real operational signal rather than a rule mismatch.
    """
    for name, value in (
        ("current_price", current_price),
        ("large_drop_absolute", large_drop_absolute),
        ("large_drop_percent", large_drop_percent),
    ):
        if not isinstance(value, Decimal):
            raise TypeError(f"{name} must be a Decimal, not {type(value).__name__}")
    if previous_price is not None and not isinstance(previous_price, Decimal):
        raise TypeError("previous_price must be a Decimal or None")

    if previous_price is None:
        return PriceMovement(None, None, False, False)

    delta = current_price - previous_price
    is_change = delta != 0
    percent = None
    if previous_price != 0:
        percent = (delta / previous_price * Decimal(100)).quantize(
            _PERCENT_QUANTUM, rounding=ROUND_HALF_UP
        )

    drop = -delta
    is_large_drop = False
    if is_change and drop > 0:
        if drop >= large_drop_absolute:
            is_large_drop = True
        elif previous_price != 0:
            is_large_drop = (drop / previous_price) * Decimal(100) >= large_drop_percent

    return PriceMovement(
        delta_absolute=quantize(delta),
        delta_percent=percent,
        is_price_change=is_change,
        is_large_drop=is_large_drop,
    )


def daily_representative(
    observations: Sequence[tuple[datetime, str, Decimal]],
) -> tuple[Decimal, Decimal, Decimal, Decimal, int]:
    """Return (open, close, min, max, count) for one offer-day.

    The representative price is the close — the day's last observation — not a
    mean.  A mean over unevenly spaced crawl observations measures crawl
    scheduling, not market behaviour, so no mean price is produced.
    """
    if not observations:
        raise ValueError("daily_representative() requires at least one observation")
    for observed_at, observation_id, price in observations:
        if observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        if not isinstance(price, Decimal):
            raise TypeError("price must be a Decimal")
        if not isinstance(observation_id, str) or not observation_id:
            raise ValueError("observation_id is required")
    # ID breaks ties on identical instants so the result is stable run to run.
    ordered = sorted(observations, key=lambda row: (row[0], row[1]))
    prices = [row[2] for row in ordered]
    return (
        quantize(prices[0]),
        quantize(prices[-1]),
        quantize(min(prices)),
        quantize(max(prices)),
        len(ordered),
    )
