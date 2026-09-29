from datetime import timedelta
from decimal import Decimal

from tests.test_marketplace_schema import make_event, make_observation, make_offer
from config.marketplace_schema import Availability, create_observation_event
from speed_layer.marketplace_change_rules import *


def _event(**kwargs):
    observation = make_observation(**kwargs)
    return create_observation_event(marketplace_code="tiki", offer=make_offer(), observation=observation, platform_listing_id="p1", produced_at=observation.observed_at)


def test_first_duplicate_late_and_deterministic_state():
    cfg = ChangeRuleConfig("speed-rules.v1", Decimal("20"), Decimal(".2"), 10)
    first = detect_observation_changes(None, make_event(), cfg)
    assert [x.change_type.value for x in first.changes] == ["NEW_OFFER"]
    assert detect_observation_changes(first.next_state, make_event(), cfg).disposition is ObservationDisposition.DUPLICATE
    older = _event(observed_at=make_event().occurred_at - timedelta(seconds=1), raw_sha256="b" * 64)
    assert detect_observation_changes(first.next_state, older, cfg).disposition is ObservationDisposition.LATE


def test_price_drop_grouping_and_unknown_availability():
    cfg = ChangeRuleConfig("speed-rules.v1", Decimal("20"), Decimal(".2"), 10)
    first = detect_observation_changes(None, make_event(), cfg)
    second = detect_observation_changes(first.next_state, _event(observed_at=make_event().occurred_at + timedelta(seconds=1), raw_sha256="b" * 64, current_price=Decimal("70"), availability=Availability.IN_STOCK, review_count=2, sold_count=3), cfg)
    assert [x.change_type.value for x in second.changes] == ["PRICE_CHANGED", "LARGE_PRICE_DROP", "COUNTER_CHANGED"]
    stale_state, stale = detect_stale_change(second.next_state, cfg)
    assert stale.previous_observation_id == stale.current_observation_id == stale_state.observation_id
    assert detect_stale_change(stale_state, cfg)[1] is None


def test_state_json_round_trip_preserves_decimal_null_and_utc():
    state = state_from_observation(make_event())
    assert offer_state_from_json(offer_state_to_json(state)) == state
