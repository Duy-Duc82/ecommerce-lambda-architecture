"""Pure change-rule tests, items 1-17 of the Phase 5 plan section 13."""
from dataclasses import replace
from datetime import timedelta, timezone
from decimal import Decimal

import pytest

from common.identity import make_change_id
from config.marketplace_schema import (
    Availability,
    MarketplaceChangeType,
    create_observation_event,
)
from speed_layer.marketplace_change_rules import (
    ChangeRuleConfig,
    ObservationDisposition,
    detect_observation_changes,
    detect_stale_change,
    offer_state_from_json,
    offer_state_to_json,
    state_from_observation,
)
from tests.test_marketplace_schema import (
    OBSERVED_AT,
    make_observation,
    make_offer,
)


CONFIG = ChangeRuleConfig("speed-rules.v1", Decimal("20"), Decimal("0.2"), 10)


def _event(**overrides):
    offer = make_offer()
    observation = make_observation(offer, **overrides)
    return create_observation_event(
        marketplace_code="TIKI",
        offer=offer,
        observation=observation,
        platform_listing_id="p1",
        produced_at=observation.observed_at,
    )


def _later(seconds=1, **overrides):
    return _event(observed_at=OBSERVED_AT + timedelta(seconds=seconds), **overrides)


def _applied(previous, event, config=CONFIG):
    result = detect_observation_changes(previous, event, config)
    assert result.disposition is ObservationDisposition.APPLIED
    return result


def _types(result):
    return [change.change_type.value for change in result.changes]


# 1
def test_state_json_round_trip_preserves_decimal_null_and_utc():
    tehran = timezone(timedelta(hours=3, minutes=30))
    event = _event(
        observed_at=OBSERVED_AT.astimezone(tehran),
        current_price=Decimal("100.50"),
        list_price=None,
        rating_value=None,
        rating_count=None,
    )
    state = state_from_observation(event)

    restored = offer_state_from_json(offer_state_to_json(state))

    assert restored == state
    assert restored.current_price == Decimal("100.50")
    assert isinstance(restored.current_price, Decimal)
    assert restored.list_price is None
    assert restored.rating_value is None
    assert restored.seller_id is None
    assert restored.observed_at.tzinfo == timezone.utc
    assert restored.observed_at == OBSERVED_AT


# 2
def test_first_observation_emits_exactly_one_deterministic_new_offer():
    event = _event()

    result = _applied(None, event)

    assert len(result.changes) == 1
    change = result.changes[0]
    assert change.change_type is MarketplaceChangeType.NEW_OFFER
    assert change.previous_observation_id is None
    assert change.current_observation_id == event.payload.observation.observation_id
    assert change.event_id == make_change_id(
        event.payload.offer.offer_id,
        event.payload.observation.observation_id,
        MarketplaceChangeType.NEW_OFFER,
        CONFIG.rule_version,
    )
    assert detect_observation_changes(None, _event(), CONFIG).changes == result.changes


# 3
def test_duplicate_observation_emits_nothing_and_does_not_replace_state():
    first = _applied(None, _event())

    duplicate = detect_observation_changes(first.next_state, _event(), CONFIG)

    assert duplicate.disposition is ObservationDisposition.DUPLICATE
    assert duplicate.changes == ()
    assert duplicate.next_state is first.next_state


# 4
def test_older_tuple_is_late_and_cannot_move_state_backward():
    first = _applied(None, _later(5))
    older = _event(observed_at=OBSERVED_AT, current_price=Decimal("1"))

    late = detect_observation_changes(first.next_state, older, CONFIG)

    assert late.disposition is ObservationDisposition.LATE
    assert late.changes == ()
    assert late.next_state == first.next_state
    assert late.next_state.current_price == Decimal("100.00")


# 5
def test_same_time_observation_id_tie_break_is_deterministic():
    left = _event(raw_sha256="b" * 64)
    right = _event(raw_sha256="c" * 64)
    assert left.payload.observation.observed_at == right.payload.observation.observed_at
    lower, higher = sorted(
        (left, right), key=lambda event: event.payload.observation.observation_id
    )

    forward = detect_observation_changes(
        _applied(None, lower).next_state, higher, CONFIG
    )
    backward = detect_observation_changes(
        _applied(None, higher).next_state, lower, CONFIG
    )

    assert forward.disposition is ObservationDisposition.APPLIED
    assert backward.disposition is ObservationDisposition.LATE


# 6
def test_price_increase_emits_only_price_changed():
    first = _applied(None, _event())

    second = _applied(first.next_state, _later(current_price=Decimal("150.00")))

    assert _types(second) == ["PRICE_CHANGED"]
    assert second.changes[0].previous_value == "100.00"
    assert second.changes[0].current_value == "150.00"


# 7
def test_qualifying_decrease_emits_price_and_large_drop_events():
    first = _applied(None, _event())

    second = _applied(first.next_state, _later(current_price=Decimal("70.00")))

    assert _types(second) == ["PRICE_CHANGED", "LARGE_PRICE_DROP"]
    drop = second.changes[1]
    assert drop.current_value["drop_amount"] == "30.00"
    assert drop.current_value["previous_price"] == "100.00"
    assert drop.previous_observation_id == first.next_state.observation_id


# 8
def test_zero_previous_price_does_not_divide_by_zero():
    first = _applied(None, _event())
    zeroed = replace(first.next_state, current_price=Decimal("0"))

    second = _applied(zeroed, _later(current_price=Decimal("50.00")))

    assert _types(second) == ["PRICE_CHANGED"]


# 9
@pytest.mark.parametrize(
    "absolute, relative, price, expected",
    [
        (Decimal("30"), Decimal("1"), Decimal("70.00"), True),
        (Decimal("30"), Decimal("1"), Decimal("70.01"), False),
        (Decimal("1000"), Decimal("0.3"), Decimal("70.00"), True),
        (Decimal("1000"), Decimal("0.3"), Decimal("70.01"), False),
    ],
)
def test_absolute_and_relative_thresholds_include_the_exact_boundary(
    absolute, relative, price, expected
):
    config = ChangeRuleConfig("speed-rules.v1", absolute, relative, 10)
    first = _applied(None, _event(), config)

    second = _applied(first.next_state, _later(current_price=price), config)

    assert ("LARGE_PRICE_DROP" in _types(second)) is expected


# 10
def test_rating_value_and_count_changes_collapse_into_one_event():
    first = _applied(None, _event(rating_value=Decimal("4.0"), rating_scale=Decimal("5"), rating_count=10))

    second = _applied(
        first.next_state,
        _later(rating_value=Decimal("4.5"), rating_scale=Decimal("5"), rating_count=11),
    )

    assert _types(second) == ["RATING_CHANGED"]
    change = second.changes[0]
    assert change.previous_value == {"value": "4.0", "count": 10}
    assert change.current_value == {"value": "4.5", "count": 11}


# 11
def test_review_and_sold_changes_collapse_into_one_counter_event():
    first = _applied(None, _event(review_count=1, sold_count=2))

    second = _applied(first.next_state, _later(review_count=5, sold_count=9))

    assert _types(second) == ["COUNTER_CHANGED"]
    change = second.changes[0]
    assert change.previous_value == {"review_count": 1, "sold_count": 2}
    assert change.current_value == {"review_count": 5, "sold_count": 9}


# 12
def test_null_transitions_are_preserved():
    first = _applied(None, _event())
    assert first.next_state.rating_value is None

    appeared = _applied(
        first.next_state,
        _later(rating_value=Decimal("4.5"), rating_scale=Decimal("5"), rating_count=3),
    )
    vanished = _applied(appeared.next_state, _later(2))

    assert appeared.changes[0].previous_value == {"value": None, "count": None}
    assert vanished.changes[0].current_value == {"value": None, "count": None}


# 13
@pytest.mark.parametrize(
    "previous, current",
    [
        (Availability.UNKNOWN, Availability.IN_STOCK),
        (Availability.IN_STOCK, Availability.UNKNOWN),
    ],
)
def test_unknown_availability_suppresses_ambiguous_changes(previous, current):
    first = _applied(None, _event(availability=previous))

    second = _applied(first.next_state, _later(availability=current))

    assert "AVAILABILITY_CHANGED" not in _types(second)


# 14
def test_known_availability_transition_emits_one_event():
    first = _applied(None, _event(availability=Availability.IN_STOCK))

    second = _applied(first.next_state, _later(availability=Availability.OUT_OF_STOCK))

    assert _types(second) == ["AVAILABILITY_CHANGED"]
    assert second.changes[0].previous_value == "IN_STOCK"
    assert second.changes[0].current_value == "OUT_OF_STOCK"


# 15
def test_stale_emits_once_with_the_last_id_in_both_observation_fields():
    state = _applied(None, _event()).next_state

    stale_state, change = detect_stale_change(state, CONFIG)

    assert change.change_type is MarketplaceChangeType.OFFER_STALE
    assert change.previous_observation_id == state.observation_id
    assert change.current_observation_id == state.observation_id
    assert change.detected_at == state.observed_at + timedelta(
        seconds=CONFIG.stale_after_seconds
    )
    assert stale_state.stale_emitted is True
    assert detect_stale_change(stale_state, CONFIG)[1] is None


# 16
def test_newer_observation_clears_stale_state_and_schedules_a_new_boundary():
    first = _applied(None, _event())
    stale_state, _ = detect_stale_change(first.next_state, CONFIG)

    revived = _applied(stale_state, _later(30, current_price=Decimal("120.00")))
    _, next_stale = detect_stale_change(revived.next_state, CONFIG)

    assert revived.next_state.stale_emitted is False
    assert next_stale is not None
    assert next_stale.detected_at == revived.next_state.observed_at + timedelta(
        seconds=CONFIG.stale_after_seconds
    )


# 17
def test_all_ids_use_make_change_id_and_payloads_are_replay_identical():
    event = _later(
        current_price=Decimal("70.00"),
        rating_value=Decimal("4.5"),
        rating_scale=Decimal("5"),
        rating_count=3,
        review_count=7,
        availability=Availability.OUT_OF_STOCK,
    )
    previous = _applied(None, _event(availability=Availability.IN_STOCK)).next_state

    result = _applied(previous, event)
    replayed = _applied(previous, event)

    assert _types(result) == [
        "PRICE_CHANGED",
        "LARGE_PRICE_DROP",
        "RATING_CHANGED",
        "COUNTER_CHANGED",
        "AVAILABILITY_CHANGED",
    ]
    for change in result.changes:
        assert change.event_id == make_change_id(
            event.payload.offer.offer_id,
            event.payload.observation.observation_id,
            change.change_type,
            CONFIG.rule_version,
        )
    assert len({change.event_id for change in result.changes}) == len(result.changes)
    assert result.changes == replayed.changes
