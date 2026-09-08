"""Phase 5 SPD-01/SPD-02: change factory and pure change rules."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from config.marketplace_schema import (
    Availability,
    CHANGE_SCHEMA_VERSION,
    MarketplaceChangeType,
    create_change_event,
    create_marketplace_offer,
    create_observation_event,
    create_offer_observation,
)
from speed_layer.change_rules import (
    ChangeThresholds,
    ObservationOutcome,
    detect_changes,
    detect_stale_offers,
    state_from_observation,
)

MARKETPLACE = "tiki"
LISTING = "279212151"
RAW_URI = "s3a://ecommerce-bronze/marketplace/raw/body.bin"
RAW_SHA = "a" * 64
RUN = "run-1"
RULE = "marketplace-change-rules.v1"
T0 = datetime(2026, 9, 1, 3, 0, tzinfo=timezone.utc)
DETECTED_AT = datetime(2026, 9, 1, 4, 0, tzinfo=timezone.utc)

THRESHOLDS = ChangeThresholds(
    large_drop_absolute=Decimal("500000"),
    large_drop_percent=Decimal("15"),
    stale_after=timedelta(minutes=1440),
)


def _event(
    *,
    observed_at: datetime = T0,
    price: str = "1000000",
    raw_sha: str = RAW_SHA,
    availability: Availability = Availability.IN_STOCK,
    rating_value: str | None = "4.5",
    review_count: int | None = 10,
    sold_count: int | None = 100,
    list_price: str | None = "1200000",
):
    offer = create_marketplace_offer(
        marketplace_code=MARKETPLACE,
        marketplace_id="marketplace-tiki",
        platform_listing_id=LISTING,
        seller_id=None,
        product_title="MacBook Neo",
        brand="Apple",
        category_path="1/2/1846",
        source_url="https://tiki.vn/macbook-neo-p279212151.html",
        currency="VND",
        first_seen_at=observed_at,
        last_seen_at=observed_at,
    )
    observation = create_offer_observation(
        marketplace_code=MARKETPLACE,
        platform_listing_id=LISTING,
        offer_id=offer.offer_id,
        observed_at=observed_at,
        fetched_at=observed_at,
        current_price=Decimal(price),
        raw_uri=RAW_URI,
        raw_sha256=raw_sha,
        adapter_version="tiki-listing-v1",
        crawl_run_id=RUN,
        list_price=None if list_price is None else Decimal(list_price),
        rating_value=None if rating_value is None else Decimal(rating_value),
        rating_scale=Decimal("5") if rating_value is not None else None,
        review_count=review_count,
        sold_count=sold_count,
        availability=availability,
    )
    return create_observation_event(
        marketplace_code=MARKETPLACE,
        offer=offer,
        observation=observation,
        platform_listing_id=LISTING,
        produced_at=observed_at,
    )


def _detect(previous_event, current_event, *, thresholds=THRESHOLDS, rule=RULE):
    previous = None if previous_event is None else state_from_observation(previous_event)
    return detect_changes(
        previous=previous,
        event=current_event,
        detected_at=DETECTED_AT,
        thresholds=thresholds,
        rule_version=rule,
    )


def _types(result):
    return [change.change_type for change in result.changes]


# --- 1-3, 4-6: factory ------------------------------------------------------


@pytest.mark.parametrize("change_type", list(MarketplaceChangeType))
def test_every_change_type_constructs(change_type):
    needs_previous = change_type is not MarketplaceChangeType.NEW_OFFER
    needs_field = change_type not in {
        MarketplaceChangeType.NEW_OFFER,
        MarketplaceChangeType.OFFER_STALE,
    }
    change = create_change_event(
        marketplace_code=MARKETPLACE,
        offer_id="offer_1",
        change_type=change_type,
        current_observation_id="obs_2",
        detected_at=DETECTED_AT,
        rule_version=RULE,
        previous_observation_id="obs_1" if needs_previous else None,
        field_name="current_price" if needs_field else None,
        previous_value=Decimal("10") if needs_field else None,
        current_value=Decimal("9") if needs_field else None,
    )
    assert change.schema_version == CHANGE_SCHEMA_VERSION
    assert change.change_type is change_type
    assert change.event_id.startswith("change_")


@pytest.mark.parametrize(
    "bad_value",
    [1.5, True, datetime(2026, 1, 1), object()],
    ids=["float", "bool", "naive-datetime", "unsupported"],
)
def test_factory_rejects_out_of_domain_values(bad_value):
    with pytest.raises((ValueError, TypeError)):
        create_change_event(
            marketplace_code=MARKETPLACE,
            offer_id="offer_1",
            change_type=MarketplaceChangeType.PRICE_CHANGED,
            current_observation_id="obs_2",
            detected_at=DETECTED_AT,
            rule_version=RULE,
            previous_observation_id="obs_1",
            field_name="current_price",
            previous_value=bad_value,
            current_value=Decimal("9"),
        )


def test_new_offer_needs_no_previous_id_but_others_do():
    create_change_event(
        marketplace_code=MARKETPLACE,
        offer_id="offer_1",
        change_type=MarketplaceChangeType.NEW_OFFER,
        current_observation_id="obs_1",
        detected_at=DETECTED_AT,
        rule_version=RULE,
    )
    with pytest.raises(ValueError, match="previous_observation_id"):
        create_change_event(
            marketplace_code=MARKETPLACE,
            offer_id="offer_1",
            change_type=MarketplaceChangeType.OFFER_STALE,
            current_observation_id="obs_1",
            detected_at=DETECTED_AT,
            rule_version=RULE,
        )


def test_identical_inputs_produce_identical_identity():
    first = _detect(None, _event())
    second = _detect(None, _event())
    assert [c.event_id for c in first.changes] == [c.event_id for c in second.changes]


def test_different_change_types_get_different_identities():
    previous = _event(price="2000000", sold_count=100)
    current = _event(observed_at=T0 + timedelta(hours=1), price="1000000", raw_sha="b" * 64, sold_count=140)
    result = _detect(previous, current)
    ids = [c.event_id for c in result.changes]
    assert len(ids) == len(set(ids))


def test_rule_version_bump_changes_every_identity():
    previous = _event(price="2000000")
    current = _event(observed_at=T0 + timedelta(hours=1), price="1900000", raw_sha="b" * 64)
    base = _detect(previous, current)
    bumped = _detect(previous, current, rule="marketplace-change-rules.v2")
    assert base.changes and bumped.changes
    assert {c.event_id for c in base.changes}.isdisjoint({c.event_id for c in bumped.changes})


# --- 7-12: price rules ------------------------------------------------------


def test_equal_decimal_with_different_scale_is_not_a_price_change():
    previous = _event(price="100.0")
    current = _event(observed_at=T0 + timedelta(hours=1), price="100.00", raw_sha="b" * 64)
    result = _detect(previous, current)
    assert MarketplaceChangeType.PRICE_CHANGED not in _types(result)


def test_large_drop_accompanies_price_change_never_replaces_it():
    previous = _event(price="2000000")
    current = _event(observed_at=T0 + timedelta(hours=1), price="1000000", raw_sha="b" * 64)
    types = _types(_detect(previous, current))
    assert types.index(MarketplaceChangeType.PRICE_CHANGED) < types.index(
        MarketplaceChangeType.LARGE_PRICE_DROP
    )


def test_absolute_threshold_alone_fires_the_drop():
    thresholds = ChangeThresholds(
        large_drop_absolute=Decimal("100"),
        large_drop_percent=Decimal("99"),
        stale_after=timedelta(minutes=10),
    )
    previous = _event(price="10000")
    current = _event(observed_at=T0 + timedelta(hours=1), price="9800", raw_sha="b" * 64)
    assert MarketplaceChangeType.LARGE_PRICE_DROP in _types(
        _detect(previous, current, thresholds=thresholds)
    )


def test_percent_threshold_alone_fires_the_drop():
    thresholds = ChangeThresholds(
        large_drop_absolute=Decimal("1000000000"),
        large_drop_percent=Decimal("10"),
        stale_after=timedelta(minutes=10),
    )
    previous = _event(price="10000")
    current = _event(observed_at=T0 + timedelta(hours=1), price="8000", raw_sha="b" * 64)
    assert MarketplaceChangeType.LARGE_PRICE_DROP in _types(
        _detect(previous, current, thresholds=thresholds)
    )


def test_price_rise_never_fires_a_drop():
    previous = _event(price="1000000")
    current = _event(observed_at=T0 + timedelta(hours=1), price="9000000", raw_sha="b" * 64)
    types = _types(_detect(previous, current))
    assert MarketplaceChangeType.PRICE_CHANGED in types
    assert MarketplaceChangeType.LARGE_PRICE_DROP not in types


def test_zero_previous_price_does_not_divide_by_zero():
    thresholds = ChangeThresholds(
        large_drop_absolute=Decimal("1000000000"),
        large_drop_percent=Decimal("1"),
        stale_after=timedelta(minutes=10),
    )
    previous = _event(price="0")
    current = _event(observed_at=T0 + timedelta(hours=1), price="0", raw_sha="b" * 64)
    result = _detect(previous, current, thresholds=thresholds)
    assert MarketplaceChangeType.LARGE_PRICE_DROP not in _types(result)


# --- 13-17: availability, counter, rating, ordering -------------------------


def test_unknown_to_known_availability_emits_no_change():
    previous = _event(availability=Availability.UNKNOWN)
    current = _event(
        observed_at=T0 + timedelta(hours=1),
        availability=Availability.IN_STOCK,
        raw_sha="b" * 64,
    )
    assert MarketplaceChangeType.AVAILABILITY_CHANGED not in _types(_detect(previous, current))


def test_in_stock_to_out_of_stock_emits_one_change():
    previous = _event(availability=Availability.IN_STOCK)
    current = _event(
        observed_at=T0 + timedelta(hours=1),
        availability=Availability.OUT_OF_STOCK,
        raw_sha="b" * 64,
    )
    result = _detect(previous, current)
    assert _types(result).count(MarketplaceChangeType.AVAILABILITY_CHANGED) == 1
    change = [c for c in result.changes if c.change_type is MarketplaceChangeType.AVAILABILITY_CHANGED][0]
    assert change.previous_value == "IN_STOCK"
    assert change.current_value == "OUT_OF_STOCK"


def test_negative_counter_delta_keeps_true_values_and_flags():
    previous = _event(sold_count=500)
    current = _event(observed_at=T0 + timedelta(hours=1), sold_count=120, raw_sha="b" * 64)
    result = _detect(previous, current)
    change = [c for c in result.changes if c.change_type is MarketplaceChangeType.COUNTER_CHANGED][0]
    assert (change.previous_value, change.current_value) == (500, 120)
    assert result.counter_reset_or_invalid is True


def test_both_rating_fields_moving_emits_exactly_one_rating_change():
    previous = _event(rating_value="4.0", review_count=10)
    current = _event(
        observed_at=T0 + timedelta(hours=1),
        rating_value="4.6",
        review_count=25,
        raw_sha="b" * 64,
    )
    result = _detect(previous, current)
    ratings = [c for c in result.changes if c.change_type is MarketplaceChangeType.RATING_CHANGED]
    assert len(ratings) == 1
    assert ratings[0].field_name == "rating_value"
    assert result.secondary_rating_field == "review_count"


def test_only_review_count_moving_reports_that_field():
    previous = _event(rating_value="4.0", review_count=10)
    current = _event(
        observed_at=T0 + timedelta(hours=1),
        rating_value="4.0",
        review_count=25,
        raw_sha="b" * 64,
    )
    result = _detect(previous, current)
    ratings = [c for c in result.changes if c.change_type is MarketplaceChangeType.RATING_CHANGED]
    assert [r.field_name for r in ratings] == ["review_count"]
    assert result.secondary_rating_field is None


def test_change_order_is_the_fixed_order():
    previous = _event(price="2000000", rating_value="4.0", sold_count=100, availability=Availability.IN_STOCK)
    current = _event(
        observed_at=T0 + timedelta(hours=1),
        price="1000000",
        rating_value="4.9",
        sold_count=180,
        availability=Availability.OUT_OF_STOCK,
        raw_sha="b" * 64,
    )
    assert _types(_detect(previous, current)) == [
        MarketplaceChangeType.PRICE_CHANGED,
        MarketplaceChangeType.LARGE_PRICE_DROP,
        MarketplaceChangeType.RATING_CHANGED,
        MarketplaceChangeType.COUNTER_CHANGED,
        MarketplaceChangeType.AVAILABILITY_CHANGED,
    ]


def test_new_offer_is_the_only_change_for_unseen_offer():
    result = _detect(None, _event())
    assert _types(result) == [MarketplaceChangeType.NEW_OFFER]
    assert result.changes[0].previous_observation_id is None
    assert result.next_state is not None


# --- 18-20: guards ----------------------------------------------------------


def test_duplicate_observation_yields_duplicate_and_no_state():
    event = _event()
    result = _detect(event, event)
    assert result.outcome is ObservationOutcome.DUPLICATE
    assert result.changes == () and result.next_state is None


def test_older_observation_yields_out_of_order():
    previous = _event(observed_at=T0 + timedelta(hours=5))
    current = _event(observed_at=T0, raw_sha="b" * 64)
    result = _detect(previous, current)
    assert result.outcome is ObservationOutcome.OUT_OF_ORDER
    assert result.changes == () and result.next_state is None


def test_same_instant_different_body_yields_conflict():
    previous = _event(observed_at=T0, raw_sha="a" * 64)
    current = _event(observed_at=T0, raw_sha="b" * 64, price="99")
    result = _detect(previous, current)
    assert result.outcome is ObservationOutcome.CONFLICT
    assert result.changes == () and result.next_state is None


def test_previous_state_of_another_offer_is_rejected():
    previous = state_from_observation(_event())
    other = _event(raw_sha="b" * 64)
    wrong = type(previous)(**{**previous.__dict__, "offer_id": "offer_other"})
    with pytest.raises(ValueError, match="different offer"):
        detect_changes(
            previous=wrong,
            event=other,
            detected_at=DETECTED_AT,
            thresholds=THRESHOLDS,
            rule_version=RULE,
        )


# --- stale sweep ------------------------------------------------------------


def test_stale_sweep_emits_one_change_per_stale_offer_and_is_idempotent():
    state = state_from_observation(_event(observed_at=T0))
    now = T0 + timedelta(days=3)
    first = detect_stale_offers(
        states=[state], now=now, thresholds=THRESHOLDS, rule_version=RULE, limit=10
    )
    second = detect_stale_offers(
        states=[state], now=now, thresholds=THRESHOLDS, rule_version=RULE, limit=10
    )
    assert len(first) == 1
    assert first[0].change_type is MarketplaceChangeType.OFFER_STALE
    assert first[0].previous_observation_id == state.observation_id
    assert [c.event_id for c in first] == [c.event_id for c in second]


def test_stale_sweep_identity_is_stable_across_sweep_times():
    state = state_from_observation(_event(observed_at=T0))
    early = detect_stale_offers(
        states=[state], now=T0 + timedelta(days=2), thresholds=THRESHOLDS, rule_version=RULE, limit=10
    )
    late = detect_stale_offers(
        states=[state], now=T0 + timedelta(days=9), thresholds=THRESHOLDS, rule_version=RULE, limit=10
    )
    assert early[0].event_id == late[0].event_id


def test_fresh_offer_is_not_stale():
    state = state_from_observation(_event(observed_at=T0))
    assert detect_stale_offers(
        states=[state], now=T0 + timedelta(minutes=10), thresholds=THRESHOLDS, rule_version=RULE, limit=10
    ) == ()


def test_stale_sweep_is_bounded_and_deterministically_ordered():
    states = [
        state_from_observation(_event(observed_at=T0 + timedelta(minutes=offset), raw_sha=chr(97 + i) * 64))
        for i, offset in enumerate((30, 10, 20))
    ]
    now = T0 + timedelta(days=3)
    changes = detect_stale_offers(
        states=states, now=now, thresholds=THRESHOLDS, rule_version=RULE, limit=2
    )
    assert len(changes) == 2
    again = detect_stale_offers(
        states=states, now=now, thresholds=THRESHOLDS, rule_version=RULE, limit=2
    )
    assert [c.event_id for c in changes] == [c.event_id for c in again]


def test_thresholds_reject_float_and_non_positive_values():
    with pytest.raises(TypeError):
        ChangeThresholds(
            large_drop_absolute=500000.0,
            large_drop_percent=Decimal("15"),
            stale_after=timedelta(minutes=1),
        )
    with pytest.raises(ValueError):
        ChangeThresholds(
            large_drop_absolute=Decimal("0"),
            large_drop_percent=Decimal("15"),
            stale_after=timedelta(minutes=1),
        )
    with pytest.raises(ValueError):
        ChangeThresholds(
            large_drop_absolute=Decimal("1"),
            large_drop_percent=Decimal("101"),
            stale_after=timedelta(minutes=1),
        )


# --- 21: purity -------------------------------------------------------------


def test_change_rules_imports_no_engine_or_client_library():
    source = Path("speed_layer/change_rules.py").read_text(encoding="utf-8")
    forbidden = ("pyspark", "kafka", "redis", "elasticsearch", "psycopg2", "boto3")
    offenders = [
        line.strip()
        for line in source.splitlines()
        if re.match(r"^\s*(import|from)\s", line)
        and any(name in line.lower() for name in forbidden)
    ]
    assert offenders == []


def test_change_rules_reads_no_clock():
    source = Path("speed_layer/change_rules.py").read_text(encoding="utf-8")
    assert "datetime.now(" not in source
    assert "utcnow(" not in source
