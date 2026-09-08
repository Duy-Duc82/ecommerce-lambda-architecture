"""Phase 6 GOLD-02: pure temporal rules."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from batch_layer.marketplace_rules import (
    CounterValidity,
    FreshnessStatus,
    FreshnessThresholds,
    classify_freshness,
    counter_delta,
    daily_representative,
    price_movement,
    quantize,
)

AS_OF = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)
THRESHOLDS = FreshnessThresholds(
    fresh_after=timedelta(minutes=360),
    stale_after=timedelta(minutes=1440),
)
MAX_GAP = timedelta(minutes=2880)


def _fresh(minutes_ago: int):
    return classify_freshness(
        last_observation_at=AS_OF - timedelta(minutes=minutes_ago),
        as_of=AS_OF,
        thresholds=THRESHOLDS,
    )


# --- 1-2: freshness ---------------------------------------------------------


@pytest.mark.parametrize(
    "minutes_ago,expected",
    [
        (0, FreshnessStatus.FRESH),
        (359, FreshnessStatus.FRESH),
        (360, FreshnessStatus.FRESH),       # on the boundary, inclusive
        (361, FreshnessStatus.AGING),
        (1439, FreshnessStatus.AGING),
        (1440, FreshnessStatus.AGING),      # on the boundary, inclusive
        (1441, FreshnessStatus.STALE),
        (100000, FreshnessStatus.STALE),
    ],
)
def test_freshness_boundaries(minutes_ago, expected):
    status, age = _fresh(minutes_ago)
    assert status is expected
    assert age == minutes_ago


def test_future_observation_is_fresh_with_zero_age_never_negative():
    status, age = classify_freshness(
        last_observation_at=AS_OF + timedelta(hours=3),
        as_of=AS_OF,
        thresholds=THRESHOLDS,
    )
    assert status is FreshnessStatus.FRESH
    assert age == 0


def test_freshness_requires_aware_timestamps():
    with pytest.raises(ValueError, match="timezone-aware"):
        classify_freshness(
            last_observation_at=datetime(2026, 9, 8, 10, 0),
            as_of=AS_OF,
            thresholds=THRESHOLDS,
        )


def test_thresholds_must_be_ordered_and_positive():
    with pytest.raises(ValueError):
        FreshnessThresholds(fresh_after=timedelta(minutes=10), stale_after=timedelta(minutes=5))
    with pytest.raises(ValueError):
        FreshnessThresholds(fresh_after=timedelta(0), stale_after=timedelta(minutes=5))


# --- 3-7: counter delta -----------------------------------------------------


def _delta(previous, current, *, minutes=60, previous_at=True):
    current_at = AS_OF
    return counter_delta(
        previous_counter=previous,
        current_counter=current,
        previous_observed_at=(current_at - timedelta(minutes=minutes)) if previous_at else None,
        current_observed_at=current_at,
        max_gap=MAX_GAP,
    )


def test_valid_counter_delta_reports_velocity_per_hour():
    result = _delta(100, 160, minutes=120)
    assert result.validity is CounterValidity.VALID
    assert result.observed_delta == 60
    assert result.elapsed_minutes == 120
    assert result.velocity_per_hour == Decimal("30.000000")
    assert result.counter_reset_or_invalid is False


def test_no_previous_counter_is_invalid():
    assert _delta(None, 100).validity is CounterValidity.NO_PREVIOUS
    assert _delta(100, None).validity is CounterValidity.NO_PREVIOUS
    assert _delta(100, 120, previous_at=False).validity is CounterValidity.NO_PREVIOUS


def test_negative_delta_keeps_its_true_value_and_is_invalid():
    result = _delta(500, 120)
    assert result.validity is CounterValidity.NEGATIVE_DELTA
    assert result.observed_delta == -380
    assert result.velocity_per_hour is None
    assert result.counter_reset_or_invalid is True


def test_gap_beyond_the_maximum_invalidates_even_a_positive_delta():
    result = _delta(100, 200, minutes=2881)
    assert result.validity is CounterValidity.GAP_TOO_LONG
    assert result.observed_delta == 100
    assert result.velocity_per_hour is None


def test_non_positive_elapsed_time_invalidates():
    result = counter_delta(
        previous_counter=100,
        current_counter=120,
        previous_observed_at=AS_OF,
        current_observed_at=AS_OF,
        max_gap=MAX_GAP,
    )
    assert result.validity is CounterValidity.NON_POSITIVE_ELAPSED
    assert result.velocity_per_hour is None


@pytest.mark.parametrize("validity", list(CounterValidity))
def test_every_validity_member_is_reachable(validity):
    cases = {
        CounterValidity.VALID: _delta(100, 160),
        CounterValidity.NO_PREVIOUS: _delta(None, 100),
        CounterValidity.NEGATIVE_DELTA: _delta(500, 120),
        CounterValidity.GAP_TOO_LONG: _delta(100, 200, minutes=5000),
        CounterValidity.NON_POSITIVE_ELAPSED: counter_delta(
            previous_counter=1,
            current_counter=2,
            previous_observed_at=AS_OF + timedelta(hours=1),
            current_observed_at=AS_OF,
            max_gap=MAX_GAP,
        ),
    }
    assert cases[validity].validity is validity


def test_invalid_result_can_never_carry_a_velocity():
    from batch_layer.marketplace_rules import CounterDeltaResult

    with pytest.raises(ValueError, match="must not carry a velocity"):
        CounterDeltaResult(
            observed_delta=-5,
            elapsed_minutes=60,
            velocity_per_hour=Decimal("1"),
            validity=CounterValidity.NEGATIVE_DELTA,
        )


def test_bool_counters_are_rejected():
    with pytest.raises(TypeError):
        _delta(True, 5)


# --- 8-10: price movement ---------------------------------------------------


def _movement(previous, current, absolute="500000", percent="15"):
    return price_movement(
        previous_price=None if previous is None else Decimal(previous),
        current_price=Decimal(current),
        large_drop_absolute=Decimal(absolute),
        large_drop_percent=Decimal(percent),
    )


def test_no_previous_price_is_not_a_change():
    movement = _movement(None, "1000")
    assert movement.is_price_change is False and movement.delta_absolute is None


def test_equal_decimal_with_different_scale_is_not_a_change():
    assert _movement("100.0", "100.00").is_price_change is False


def test_price_change_reports_signed_delta_and_percent():
    movement = _movement("2000000", "1000000")
    assert movement.delta_absolute == Decimal("-1000000.000000")
    assert movement.delta_percent == Decimal("-50.0000")
    assert movement.is_price_change is True
    assert movement.is_large_drop is True


def test_absolute_threshold_alone_fires_the_drop():
    assert _movement("10000", "9800", absolute="100", percent="99").is_large_drop is True


def test_percent_threshold_alone_fires_the_drop():
    assert _movement("10000", "8000", absolute="1000000000", percent="10").is_large_drop is True


def test_a_rise_is_a_change_but_never_a_drop():
    movement = _movement("1000", "5000")
    assert movement.is_price_change is True and movement.is_large_drop is False


def test_zero_previous_price_does_not_divide():
    movement = _movement("0", "500", absolute="1000000000", percent="1")
    assert movement.delta_percent is None
    assert movement.is_large_drop is False


def test_large_drop_cannot_exist_without_a_price_change():
    from batch_layer.marketplace_rules import PriceMovement

    with pytest.raises(ValueError, match="always also a price change"):
        PriceMovement(
            delta_absolute=Decimal("-1"),
            delta_percent=None,
            is_price_change=False,
            is_large_drop=True,
        )


def test_float_prices_are_rejected():
    with pytest.raises(TypeError):
        price_movement(
            previous_price=Decimal("10"),
            current_price=10.5,
            large_drop_absolute=Decimal("1"),
            large_drop_percent=Decimal("1"),
        )


def test_price_movement_matches_the_speed_layer_on_a_shared_case_table():
    """The batch and speed rules must agree, or their counts are not comparable."""
    from speed_layer.change_rules import ChangeThresholds, _is_large_drop

    thresholds = ChangeThresholds(
        large_drop_absolute=Decimal("500000"),
        large_drop_percent=Decimal("15"),
        stale_after=timedelta(minutes=1),
    )
    cases = [
        ("2000000", "1000000"),
        ("1000000", "999999"),
        ("1000000", "850000"),
        ("1000000", "850001"),
        ("1000000", "2000000"),
        ("0", "0"),
        ("0", "100"),
        ("100", "0"),
        ("100.0", "100.00"),
    ]
    for previous, current in cases:
        batch = _movement(previous, current)
        speed = _is_large_drop(Decimal(previous), Decimal(current), thresholds)
        assert batch.is_large_drop == speed, f"disagreement on {previous}->{current}"


# --- 11-12: daily representative and quantization --------------------------


def test_daily_representative_returns_open_close_min_max_count():
    day = datetime(2026, 9, 8, tzinfo=timezone.utc)
    observations = [
        (day + timedelta(hours=9), "obs_b", Decimal("1200")),
        (day + timedelta(hours=6), "obs_a", Decimal("1000")),
        (day + timedelta(hours=18), "obs_c", Decimal("900")),
    ]
    assert daily_representative(observations) == (
        Decimal("1000.000000"),
        Decimal("900.000000"),
        Decimal("900.000000"),
        Decimal("1200.000000"),
        3,
    )


def test_daily_representative_breaks_instant_ties_by_observation_id():
    day = datetime(2026, 9, 8, 6, 0, tzinfo=timezone.utc)
    first = daily_representative([(day, "obs_a", Decimal("10")), (day, "obs_b", Decimal("20"))])
    second = daily_representative([(day, "obs_b", Decimal("20")), (day, "obs_a", Decimal("10"))])
    assert first == second
    assert first[0] == Decimal("10.000000") and first[1] == Decimal("20.000000")


def test_daily_representative_raises_on_empty_input():
    with pytest.raises(ValueError, match="at least one observation"):
        daily_representative([])


def test_daily_representative_rejects_naive_timestamps_and_floats():
    with pytest.raises(ValueError):
        daily_representative([(datetime(2026, 9, 8, 6), "obs_a", Decimal("1"))])
    with pytest.raises(TypeError):
        daily_representative([(AS_OF, "obs_a", 1.5)])


def test_quantization_is_half_up_and_stable():
    assert quantize(Decimal("1.0000005")) == Decimal("1.000001")
    assert quantize(Decimal("1.0000004")) == Decimal("1.000000")
    assert quantize(quantize(Decimal("1.23456789"))) == quantize(Decimal("1.23456789"))
    assert quantize(None) is None


def test_quantize_rejects_floats():
    with pytest.raises(TypeError):
        quantize(1.5)


# --- 13: purity -------------------------------------------------------------


def test_rules_module_imports_no_engine_or_client_library():
    source = Path("batch_layer/marketplace_rules.py").read_text(encoding="utf-8")
    forbidden = ("pyspark", "psycopg2", "redis", "elasticsearch", "kafka", "boto3")
    offenders = [
        line.strip()
        for line in source.splitlines()
        if re.match(r"^\s*(import|from)\s", line)
        and any(name in line.lower() for name in forbidden)
    ]
    assert offenders == []


def test_rules_module_reads_no_clock():
    source = Path("batch_layer/marketplace_rules.py").read_text(encoding="utf-8")
    assert "datetime.now(" not in source and "utcnow(" not in source
