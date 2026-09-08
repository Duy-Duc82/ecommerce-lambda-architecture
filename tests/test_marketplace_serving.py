"""Phase 5 SPD-05: Elasticsearch serving documents and mappings."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from config.marketplace_schema import (
    Availability,
    MarketplaceChangeType,
    create_change_event,
    create_marketplace_offer,
    create_observation_event,
    create_offer_observation,
)
from serving_layer.marketplace_es import (
    FORBIDDEN_FIELD_NAMES,
    MARKETPLACE_CHANGE_MAPPING,
    MARKETPLACE_OBSERVATION_MAPPING,
    MONEY_SCALING_FACTOR,
    change_document,
    ensure_indices,
    index_documents,
    observation_document,
)
from speed_layer.change_rules import state_from_observation

T0 = datetime(2026, 9, 1, 3, 0, tzinfo=timezone.utc)
DETECTED = datetime(2026, 9, 1, 4, 0, tzinfo=timezone.utc)
RULE = "marketplace-change-rules.v1"


def _event(*, price="16990000.500000", raw_sha="a" * 64, observed_at=T0, sold_count=100):
    offer = create_marketplace_offer(
        marketplace_code="tiki",
        marketplace_id="marketplace-tiki",
        platform_listing_id="279212151",
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
        marketplace_code="tiki",
        platform_listing_id="279212151",
        offer_id=offer.offer_id,
        observed_at=observed_at,
        fetched_at=observed_at,
        current_price=Decimal(price),
        raw_uri="s3a://ecommerce-bronze/marketplace/raw/body.bin",
        raw_sha256=raw_sha,
        adapter_version="tiki-listing-v1",
        crawl_run_id="run-1",
        list_price=Decimal("18000000"),
        rating_value=Decimal("4.5"),
        rating_scale=Decimal("5"),
        review_count=10,
        sold_count=sold_count,
        availability=Availability.IN_STOCK,
        ranking_position=3,
    )
    return create_observation_event(
        marketplace_code="tiki",
        offer=offer,
        observation=observation,
        platform_listing_id="279212151",
        produced_at=observed_at,
    )


def _change(change_type=MarketplaceChangeType.PRICE_CHANGED, *, previous="2000000", current="1000000", offer_id=None):
    event = _event()
    return create_change_event(
        marketplace_code="tiki",
        offer_id=offer_id or event.payload.offer.offer_id,
        change_type=change_type,
        current_observation_id=event.payload.observation.observation_id,
        detected_at=DETECTED,
        rule_version=RULE,
        previous_observation_id="obs_prev",
        field_name="current_price",
        previous_value=None if previous is None else Decimal(previous),
        current_value=None if current is None else Decimal(current),
    )


class FakeIndices:
    def __init__(self, existing=()):
        self.existing = set(existing)
        self.created: list[tuple[str, dict]] = []

    def exists(self, index):
        return index in self.existing

    def create(self, index, body):
        self.created.append((index, body))
        self.existing.add(index)


class FakeEsClient:
    def __init__(self, existing=()):
        self.indices = FakeIndices(existing)


# --- 32: deterministic IDs --------------------------------------------------


def test_observation_document_id_is_the_event_id():
    event = _event()
    document = observation_document(event)
    assert document["event_id"] == event.event_id == event.payload.observation.observation_id


def test_change_document_id_is_the_change_event_id():
    change = _change()
    document = change_document(change, state=state_from_observation(_event()))
    assert document["event_id"] == change.event_id


def test_documents_are_stable_across_two_builds():
    event = _event()
    assert observation_document(event) == observation_document(event)


# --- 33: exact money beside every numeric ----------------------------------


def test_every_money_field_has_an_exact_string_companion():
    document = observation_document(_event(price="16990000.500000"))
    assert document["current_price_exact"] == "16990000.500000"
    assert document["current_price"] == pytest.approx(16990000.5)
    assert document["list_price_exact"] == "18000000"
    assert document["rating_value_exact"] == "4.5"


def test_change_document_carries_exact_delta_strings():
    document = change_document(_change(), state=state_from_observation(_event()))
    assert document["delta_absolute_exact"] == "-1000000"
    assert document["delta_percent_exact"] == "-50.0000"


def test_mapping_declares_scaled_float_with_an_exact_keyword():
    properties = MARKETPLACE_OBSERVATION_MAPPING["mappings"]["properties"]
    for money in ("current_price", "list_price", "rating_value"):
        assert properties[money]["type"] == "scaled_float"
        assert properties[money]["scaling_factor"] == MONEY_SCALING_FACTOR
        assert properties[f"{money}_exact"]["type"] == "keyword"


def test_mappings_are_strict_so_a_stray_field_fails_loudly():
    for mapping in (MARKETPLACE_OBSERVATION_MAPPING, MARKETPLACE_CHANGE_MAPPING):
        assert mapping["mappings"]["dynamic"] == "strict"


# --- 34: forbidden field names ---------------------------------------------


def test_no_document_or_mapping_field_is_named_after_sales():
    fields = set(observation_document(_event()))
    fields |= set(change_document(_change(), state=state_from_observation(_event())))
    for mapping in (MARKETPLACE_OBSERVATION_MAPPING, MARKETPLACE_CHANGE_MAPPING):
        fields |= set(mapping["mappings"]["properties"])
    offenders = sorted(field for field in fields if field in FORBIDDEN_FIELD_NAMES)
    assert offenders == []


def test_the_counter_is_exposed_as_sold_count():
    assert observation_document(_event())["sold_count"] == 100


# --- 35: delta edge cases ---------------------------------------------------


def test_delta_percent_is_none_when_previous_is_zero():
    document = change_document(
        _change(previous="0", current="500"),
        state=state_from_observation(_event()),
    )
    assert document["delta_absolute_exact"] == "500"
    assert document["delta_percent"] is None


def test_delta_is_none_for_a_change_without_numeric_values():
    change = create_change_event(
        marketplace_code="tiki",
        offer_id=_event().payload.offer.offer_id,
        change_type=MarketplaceChangeType.NEW_OFFER,
        current_observation_id="obs_1",
        detected_at=DETECTED,
        rule_version=RULE,
    )
    document = change_document(change, state=state_from_observation(_event()))
    assert document["delta_absolute"] is None and document["delta_percent"] is None


def test_availability_change_values_are_kept_as_strings():
    event = _event()
    change = create_change_event(
        marketplace_code="tiki",
        offer_id=event.payload.offer.offer_id,
        change_type=MarketplaceChangeType.AVAILABILITY_CHANGED,
        current_observation_id="obs_1",
        detected_at=DETECTED,
        rule_version=RULE,
        previous_observation_id="obs_0",
        field_name="availability",
        previous_value=Availability.IN_STOCK,
        current_value=Availability.OUT_OF_STOCK,
    )
    document = change_document(change, state=state_from_observation(event))
    assert document["previous_value"] == "IN_STOCK"
    assert document["current_value"] == "OUT_OF_STOCK"
    assert document["previous_value_numeric"] is None


def test_stale_change_carries_timestamps_as_iso_strings():
    event = _event()
    change = create_change_event(
        marketplace_code="tiki",
        offer_id=event.payload.offer.offer_id,
        change_type=MarketplaceChangeType.OFFER_STALE,
        current_observation_id=event.payload.observation.observation_id,
        detected_at=DETECTED,
        rule_version=RULE,
        previous_observation_id=event.payload.observation.observation_id,
        previous_value=T0,
        current_value=T0 + timedelta(days=3),
    )
    document = change_document(change, state=state_from_observation(event))
    assert document["previous_value"] == T0.isoformat()
    assert document["current_value"] == (T0 + timedelta(days=3)).isoformat()


def test_counter_flag_and_secondary_rating_field_are_carried():
    document = change_document(
        _change(change_type=MarketplaceChangeType.COUNTER_CHANGED, previous="500", current="120"),
        state=state_from_observation(_event()),
        counter_reset_or_invalid=True,
        secondary_rating_field="review_count",
    )
    assert document["counter_reset_or_invalid"] is True
    assert document["secondary_rating_field"] == "review_count"


def test_state_of_another_offer_is_rejected():
    with pytest.raises(ValueError, match="different offer"):
        change_document(_change(offer_id="offer_other"), state=state_from_observation(_event()))


def test_wrong_argument_types_are_rejected():
    with pytest.raises(TypeError):
        observation_document({"event_id": "x"})
    with pytest.raises(TypeError):
        change_document({"event_id": "x"}, state=state_from_observation(_event()))


# --- index management -------------------------------------------------------


def test_ensure_indices_creates_only_missing_indices():
    client = FakeEsClient(existing=["marketplace-observations-v1"])
    ensure_indices(
        client,
        observations_index="marketplace-observations-v1",
        changes_index="marketplace-changes-v1",
    )
    assert [index for index, _ in client.indices.created] == ["marketplace-changes-v1"]


def test_ensure_indices_is_idempotent():
    client = FakeEsClient()
    for _ in range(2):
        ensure_indices(
            client,
            observations_index="marketplace-observations-v1",
            changes_index="marketplace-changes-v1",
        )
    assert len(client.indices.created) == 2


def test_index_documents_of_nothing_needs_no_client():
    index_documents(None, "marketplace-changes-v1", [])


def test_index_documents_requires_an_index_name():
    with pytest.raises(ValueError):
        index_documents(None, "  ", [("id", {"a": 1})])
