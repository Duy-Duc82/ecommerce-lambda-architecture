import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from common.identity import (
    make_observation_id,
    make_offer_id,
    make_raw_artifact_id,
    make_seller_id,
)
from common.serialization import serialize_for_wire
from config.marketplace_schema import (
    CHANGE_SCHEMA_VERSION,
    OBSERVATION_EVENT_TYPE,
    OBSERVATION_SCHEMA_VERSION,
    Availability,
    CrawlRun,
    CrawlRunStatus,
    Marketplace,
    MarketplaceChangeType,
    MarketplaceChangeV1,
    MarketplaceObservationV1,
    ObservationPayload,
    OfferActiveStatus,
    OfferObservation,
    OfficialStatus,
    RawArtifact,
    ResourceType,
    Seller,
    create_marketplace_offer,
    create_observation_event,
    create_offer_observation,
    create_raw_artifact,
)


FETCHED_AT = datetime(2026, 9, 4, 8, 30, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 9, 4, 8, 30, 1, tzinfo=timezone.utc)
RAW_SHA256 = "a" * 64


def make_offer():
    return create_marketplace_offer(
        marketplace_code="TIKI",
        marketplace_id="marketplace-tiki",
        platform_listing_id="p1",
        seller_id=None,
        product_title="Fixture product",
        brand=None,
        category_path="1846",
        source_url="https://tiki.vn/p1.html",
        currency="vnd",
        first_seen_at=FETCHED_AT,
        last_seen_at=FETCHED_AT,
    )


def make_observation(offer=None, **overrides):
    offer = offer or make_offer()
    values = {
        "marketplace_code": "tiki",
        "platform_listing_id": "p1",
        "offer_id": offer.offer_id,
        "observed_at": OBSERVED_AT,
        "fetched_at": FETCHED_AT,
        "current_price": Decimal("100.00"),
        "raw_uri": "file:///tmp/raw.json",
        "raw_sha256": RAW_SHA256,
        "adapter_version": "test-v1",
        "crawl_run_id": "run-1",
    }
    values.update(overrides)
    return create_offer_observation(
        **values,
    )


def make_event():
    offer = make_offer()
    observation = make_observation(offer)
    return create_observation_event(
        marketplace_code="TIKI",
        offer=offer,
        observation=observation,
        platform_listing_id="p1",
        produced_at=FETCHED_AT,
    )


def make_raw_artifact(**overrides):
    values = {
        "raw_artifact_id": "raw-manually-supplied",
        "crawl_run_id": "run-1",
        "marketplace_id": "marketplace-tiki",
        "request_url": "https://tiki.vn/p1.html",
        "resource_type": ResourceType.PRODUCT_DETAIL,
        "fetched_at": FETCHED_AT,
        "http_status": 200,
        "content_type": "application/json",
        "body_sha256": RAW_SHA256,
        "raw_uri": "s3a://bronze/tiki/raw.json",
        "adapter_version": "test-v1",
        "raw_bytes": 42,
    }
    values.update(overrides)
    return RawArtifact(**values)


def test_marketplace_normalizes_code_and_currency():
    marketplace = Marketplace(
        marketplace_id="marketplace-tiki",
        code=" TIKI ",
        name="Tiki",
        base_url="https://tiki.vn/",
        default_currency="vnd",
        locale="vi-VN",
        semantics_version="v1",
    )
    assert marketplace.code == "tiki"
    assert marketplace.default_currency == "VND"


def test_marketplace_rejects_empty_code_and_invalid_url():
    with pytest.raises(ValueError, match="code"):
        Marketplace("marketplace-tiki", " ", "Tiki", "https://tiki.vn", "VND", "vi-VN", "v1")
    with pytest.raises(ValueError, match="base_url"):
        Marketplace("marketplace-tiki", "tiki", "Tiki", "ftp://tiki.vn", "VND", "vi-VN", "v1")


def test_crawl_run_enforces_status_and_completion_rules():
    running = CrawlRun("run-1", "marketplace-tiki", FETCHED_AT, None, CrawlRunStatus.RUNNING, adapter_version="v1")
    assert running.completed_at is None
    with pytest.raises(ValueError, match="RUNNING"):
        CrawlRun("run-1", "marketplace-tiki", FETCHED_AT, FETCHED_AT, CrawlRunStatus.RUNNING, adapter_version="v1")
    with pytest.raises(ValueError, match="terminal"):
        CrawlRun("run-1", "marketplace-tiki", FETCHED_AT, None, CrawlRunStatus.COMPLETED, adapter_version="v1")
    completed = CrawlRun(
        "run-1",
        "marketplace-tiki",
        FETCHED_AT,
        FETCHED_AT + timedelta(seconds=1),
        CrawlRunStatus.COMPLETED,
        adapter_version="v1",
    )
    assert completed.status is CrawlRunStatus.COMPLETED


def test_crawl_run_rejects_inconsistent_counters():
    with pytest.raises(ValueError, match=r"succeeded \+ failed"):
        CrawlRun(
            "run-1",
            "marketplace-tiki",
            FETCHED_AT,
            FETCHED_AT,
            CrawlRunStatus.COMPLETED,
            requested=1,
            succeeded=1,
            failed=1,
            adapter_version="v1",
        )


def test_raw_artifact_factory_creates_expected_deterministic_id():
    artifact = create_raw_artifact(
        marketplace_code="TIKI",
        crawl_run_id="run-1",
        marketplace_id="marketplace-tiki",
        request_url="https://tiki.vn/p1.html",
        resource_type=ResourceType.PRODUCT_DETAIL,
        fetched_at=FETCHED_AT,
        http_status=200,
        content_type="application/json",
        body_sha256=RAW_SHA256,
        raw_uri="s3a://bronze/tiki/raw.json",
        adapter_version="test-v1",
        raw_bytes=42,
    )
    assert artifact.raw_artifact_id == make_raw_artifact_id(
        "TIKI", "https://tiki.vn/p1.html", FETCHED_AT, RAW_SHA256
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"http_status": 99},
        {"http_status": 600},
        {"body_sha256": "invalid"},
        {"raw_uri": "https://bronze.example/raw.json"},
    ],
)
def test_raw_artifact_rejects_invalid_status_checksum_and_raw_uri(overrides):
    with pytest.raises(ValueError):
        make_raw_artifact(**overrides)


def test_seller_ids_are_deterministic():
    assert make_seller_id("TIKI", "seller-1") == make_seller_id("TIKI", "seller-1")


def test_seller_normalizes_timestamps_to_utc_and_validates_ordering():
    seller = Seller(
        seller_id="seller-id",
        marketplace_id="marketplace-tiki",
        platform_seller_id="seller-1",
        first_seen_at=datetime(2026, 9, 4, 10, 0, tzinfo=timezone(timedelta(hours=2))),
        last_seen_at=FETCHED_AT,
        official_status=OfficialStatus.UNKNOWN,
    )
    assert seller.first_seen_at == datetime(2026, 9, 4, 8, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="last_seen_at"):
        Seller(
            seller_id="seller-id",
            marketplace_id="marketplace-tiki",
            platform_seller_id="seller-1",
            first_seen_at=FETCHED_AT,
            last_seen_at=FETCHED_AT - timedelta(seconds=1),
        )


def test_offer_factory_creates_expected_deterministic_id():
    offer = make_offer()
    assert offer.offer_id == make_offer_id("TIKI", "p1")


def test_offer_keeps_missing_optional_values_as_none():
    offer = make_offer()
    assert offer.seller_id is None
    assert offer.brand is None


def test_offer_observation_factory_creates_expected_deterministic_id():
    observation = make_observation()
    assert observation.observation_id == make_observation_id("tiki", "p1", OBSERVED_AT, RAW_SHA256)


def test_offer_observation_defaults_availability_to_unknown():
    assert make_observation().availability is Availability.UNKNOWN


@pytest.mark.parametrize(
    "field_name",
    ["current_price", "list_price", "shipping_price", "discount_amount"],
)
def test_offer_observation_rejects_negative_prices(field_name):
    with pytest.raises(ValueError, match=field_name):
        make_observation(**{field_name: Decimal("-1")})


def test_offer_observation_rejects_negative_counts():
    with pytest.raises(ValueError, match="review_count"):
        make_observation(review_count=-1)


def test_offer_observation_rejects_bool_values_for_counts():
    with pytest.raises(ValueError, match="review_count"):
        make_observation(review_count=True)


def test_offer_observation_validates_rating_value_against_rating_scale():
    with pytest.raises(ValueError, match="rating_scale"):
        make_observation(rating_value=Decimal("4.5"))
    with pytest.raises(ValueError, match="cannot exceed"):
        make_observation(rating_value=Decimal("6"), rating_scale=Decimal("5"))


def test_offer_observation_factory_rejects_unknown_optional_fields():
    with pytest.raises(TypeError, match="unknown"):
        make_observation(not_a_field="value")


def test_observation_payload_rejects_mismatched_offer_ids():
    offer = make_offer()
    observation = make_observation()
    mismatched = replace(observation, offer_id="another-offer")
    with pytest.raises(ValueError, match="offer_id"):
        ObservationPayload(offer=offer, observation=mismatched)


def test_observation_event_enforces_version_type_and_lineage_fields():
    event = make_event()
    assert event.schema_version == OBSERVATION_SCHEMA_VERSION
    assert event.event_type == OBSERVATION_EVENT_TYPE
    assert event.event_id == event.payload.observation.observation_id
    assert event.crawl_run_id == event.payload.observation.crawl_run_id
    assert event.raw_uri == event.payload.observation.raw_uri
    with pytest.raises(ValueError, match="schema_version"):
        replace(event, schema_version="marketplace-observation.v2")
    with pytest.raises(ValueError, match="event_type"):
        replace(event, event_type="OTHER")
    with pytest.raises(ValueError, match="raw_uri"):
        replace(event, raw_uri="file:///tmp/other.json")


def test_observation_event_serializes_with_standard_json():
    payload = serialize_for_wire(make_event())
    encoded = json.dumps(payload, ensure_ascii=False)
    assert '"event_type": "OFFER_OBSERVED"' in encoded
    assert '"current_price": "100.00"' in encoded


def test_change_contract_accepts_new_offer_without_previous_observation():
    change = MarketplaceChangeV1(
        event_id="change-1",
        schema_version=CHANGE_SCHEMA_VERSION,
        change_type=MarketplaceChangeType.NEW_OFFER,
        detected_at=FETCHED_AT,
        marketplace="tiki",
        offer_id="offer-1",
        previous_observation_id=None,
        current_observation_id="obs-1",
        field_name=None,
        previous_value=None,
        current_value={"price": "100.00"},
        rule_version="rule-v1",
    )
    assert change.previous_observation_id is None


def test_change_contract_rejects_price_change_without_previous_observation():
    with pytest.raises(ValueError, match="previous_observation_id"):
        MarketplaceChangeV1(
            event_id="change-1",
            schema_version=CHANGE_SCHEMA_VERSION,
            change_type=MarketplaceChangeType.PRICE_CHANGED,
            detected_at=FETCHED_AT,
            marketplace="tiki",
            offer_id="offer-1",
            previous_observation_id=None,
            current_observation_id="obs-1",
            field_name="current_price",
            previous_value=None,
            current_value=Decimal("100.00"),
            rule_version="rule-v1",
        )


@pytest.mark.parametrize("factory", [
    lambda: CrawlRun("run-1", "marketplace-tiki", datetime(2026, 9, 4, 8, 30), None, CrawlRunStatus.RUNNING, adapter_version="v1"),
    lambda: make_raw_artifact(fetched_at=datetime(2026, 9, 4, 8, 30)),
    lambda: Seller("seller-id", "marketplace-tiki", "seller-1", datetime(2026, 9, 4, 8, 30), FETCHED_AT),
    lambda: make_offer_with_times(datetime(2026, 9, 4, 8, 30), FETCHED_AT),
    lambda: make_observation_with_times(datetime(2026, 9, 4, 8, 30), FETCHED_AT),
    lambda: replace(make_event(), occurred_at=datetime(2026, 9, 4, 8, 30)),
    lambda: replace(make_change(), detected_at=datetime(2026, 9, 4, 8, 30)),
])
def test_every_timestamped_marketplace_dataclass_rejects_naive_timestamps(factory):
    with pytest.raises(ValueError, match="timezone-aware"):
        factory()


def make_offer_with_times(first_seen_at, last_seen_at):
    return create_marketplace_offer(
        marketplace_code="tiki",
        marketplace_id="marketplace-tiki",
        platform_listing_id="p1",
        seller_id=None,
        product_title="Fixture product",
        brand=None,
        category_path=None,
        source_url="https://tiki.vn/p1.html",
        currency="VND",
        first_seen_at=first_seen_at,
        last_seen_at=last_seen_at,
    )


def make_observation_with_times(observed_at, fetched_at):
    return create_offer_observation(
        marketplace_code="tiki",
        platform_listing_id="p1",
        offer_id=make_offer_id("tiki", "p1"),
        observed_at=observed_at,
        fetched_at=fetched_at,
        current_price=Decimal("100.00"),
        raw_uri="file:///tmp/raw.json",
        raw_sha256=RAW_SHA256,
        adapter_version="test-v1",
        crawl_run_id="run-1",
    )


def make_change():
    return MarketplaceChangeV1(
        event_id="change-1",
        schema_version=CHANGE_SCHEMA_VERSION,
        change_type=MarketplaceChangeType.NEW_OFFER,
        detected_at=FETCHED_AT,
        marketplace="tiki",
        offer_id="offer-1",
        previous_observation_id=None,
        current_observation_id="obs-1",
        field_name=None,
        previous_value=None,
        current_value=Decimal("100.00"),
        rule_version="rule-v1",
    )
