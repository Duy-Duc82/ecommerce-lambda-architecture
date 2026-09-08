"""Deterministic Elasticsearch documents for marketplace observations/changes.

Separate from ``data_ingestion/es_indexer.py``, which indexes the legacy
behavioral events and is untouched here.

Two rules shape this module:

1. The document ``_id`` is the canonical event ID, and indexing uses overwrite
   semantics, so replaying a record updates one document instead of appending a
   near-duplicate.
2. Money reaches Elasticsearch twice — as ``scaled_float`` for aggregation and
   as a ``keyword`` holding the exact Decimal string.  A price must never exist
   only as a float, because the float is what the dashboards would quote back.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping, Sequence

from config.marketplace_schema import (
    MarketplaceChangeType,
    MarketplaceChangeV1,
    MarketplaceObservationV1,
)
from speed_layer.change_rules import OfferStateSnapshot

MONEY_SCALING_FACTOR = 100
_PERCENT_QUANTUM = Decimal("0.0001")

# Any of these as a field name would misrepresent a public counter as a
# transaction the marketplace never published.
FORBIDDEN_FIELD_NAMES = (
    "sale",
    "sales",
    "sold_units",
    "orders",
    "order_count",
    "demand",
    "revenue",
    "units_sold",
)

_MONEY_PROPERTIES = {
    "type": "scaled_float",
    "scaling_factor": MONEY_SCALING_FACTOR,
}

MARKETPLACE_OBSERVATION_MAPPING: dict[str, Any] = {
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "event_id": {"type": "keyword"},
            "schema_version": {"type": "keyword"},
            "marketplace": {"type": "keyword"},
            "offer_id": {"type": "keyword"},
            "platform_listing_id": {"type": "keyword"},
            "seller_id": {"type": "keyword"},
            "product_title": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
            "brand": {"type": "keyword"},
            "category_path": {"type": "keyword"},
            "source_url": {"type": "keyword", "index": False},
            "currency": {"type": "keyword"},
            "observed_at": {"type": "date"},
            "fetched_at": {"type": "date"},
            "produced_at": {"type": "date"},
            "current_price": dict(_MONEY_PROPERTIES),
            "current_price_exact": {"type": "keyword"},
            "list_price": dict(_MONEY_PROPERTIES),
            "list_price_exact": {"type": "keyword"},
            "rating_value": dict(_MONEY_PROPERTIES),
            "rating_value_exact": {"type": "keyword"},
            "review_count": {"type": "long"},
            "sold_count": {"type": "long"},
            "availability": {"type": "keyword"},
            "ranking_position": {"type": "long"},
            "crawl_run_id": {"type": "keyword"},
            "raw_uri": {"type": "keyword", "index": False},
            "raw_sha256": {"type": "keyword"},
            "adapter_version": {"type": "keyword"},
        },
    }
}

MARKETPLACE_CHANGE_MAPPING: dict[str, Any] = {
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "event_id": {"type": "keyword"},
            "schema_version": {"type": "keyword"},
            "change_type": {"type": "keyword"},
            "detected_at": {"type": "date"},
            "marketplace": {"type": "keyword"},
            "offer_id": {"type": "keyword"},
            "platform_listing_id": {"type": "keyword"},
            "previous_observation_id": {"type": "keyword"},
            "current_observation_id": {"type": "keyword"},
            "field_name": {"type": "keyword"},
            "previous_value": {"type": "keyword"},
            "current_value": {"type": "keyword"},
            "previous_value_numeric": dict(_MONEY_PROPERTIES),
            "current_value_numeric": dict(_MONEY_PROPERTIES),
            "delta_absolute": dict(_MONEY_PROPERTIES),
            "delta_absolute_exact": {"type": "keyword"},
            "delta_percent": dict(_MONEY_PROPERTIES),
            "delta_percent_exact": {"type": "keyword"},
            "observed_at": {"type": "date"},
            "previous_observed_at": {"type": "date"},
            "counter_reset_or_invalid": {"type": "boolean"},
            "secondary_rating_field": {"type": "keyword"},
            "rule_version": {"type": "keyword"},
            "source_url": {"type": "keyword", "index": False},
            "category_path": {"type": "keyword"},
        },
    }
}


def _scaled(value: Decimal | None) -> float | None:
    """The aggregation-friendly copy. Only ever used beside an exact string."""
    return None if value is None else float(value)


def _exact(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _iso(moment: datetime | None) -> str | None:
    if moment is None:
        return None
    if moment.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return moment.isoformat()


def _as_decimal(value: Any) -> Decimal | None:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return Decimal(value)
    return None


def observation_document(event: MarketplaceObservationV1) -> dict[str, Any]:
    """Build the searchable observation document."""
    if not isinstance(event, MarketplaceObservationV1):
        raise TypeError("event must be a MarketplaceObservationV1")
    offer = event.payload.offer
    observation = event.payload.observation
    return {
        "event_id": event.event_id,
        "schema_version": event.schema_version,
        "marketplace": event.marketplace,
        "offer_id": offer.offer_id,
        "platform_listing_id": offer.platform_listing_id,
        "seller_id": offer.seller_id,
        "product_title": offer.product_title,
        "brand": offer.brand,
        "category_path": offer.category_path,
        "source_url": offer.source_url,
        "currency": offer.currency,
        "observed_at": _iso(observation.observed_at),
        "fetched_at": _iso(observation.fetched_at),
        "produced_at": _iso(event.produced_at),
        "current_price": _scaled(observation.current_price),
        "current_price_exact": _exact(observation.current_price),
        "list_price": _scaled(observation.list_price),
        "list_price_exact": _exact(observation.list_price),
        "rating_value": _scaled(observation.rating_value),
        "rating_value_exact": _exact(observation.rating_value),
        "review_count": observation.review_count,
        "sold_count": observation.sold_count,
        "availability": observation.availability.value,
        "ranking_position": observation.ranking_position,
        "crawl_run_id": event.crawl_run_id,
        "raw_uri": event.raw_uri,
        "raw_sha256": observation.raw_sha256,
        "adapter_version": observation.adapter_version,
    }


def change_document(
    change: MarketplaceChangeV1,
    *,
    state: OfferStateSnapshot,
    previous_observed_at: datetime | None = None,
    counter_reset_or_invalid: bool = False,
    secondary_rating_field: str | None = None,
) -> dict[str, Any]:
    """Build the searchable change document.

    Enrichment is limited to values already present in the event or the state
    it was derived from; nothing here may contradict the Kafka event.
    """
    if not isinstance(change, MarketplaceChangeV1):
        raise TypeError("change must be a MarketplaceChangeV1")
    if not isinstance(state, OfferStateSnapshot):
        raise TypeError("state must be an OfferStateSnapshot")
    if state.offer_id != change.offer_id:
        raise ValueError("state belongs to a different offer")

    previous_numeric = _as_decimal(change.previous_value)
    current_numeric = _as_decimal(change.current_value)
    delta_absolute: Decimal | None = None
    delta_percent: Decimal | None = None
    if previous_numeric is not None and current_numeric is not None:
        delta_absolute = current_numeric - previous_numeric
        if previous_numeric != 0:
            delta_percent = (
                delta_absolute / previous_numeric * Decimal(100)
            ).quantize(_PERCENT_QUANTUM, rounding=ROUND_HALF_UP)

    def stringify(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return _iso(value)
        return str(value)

    return {
        "event_id": change.event_id,
        "schema_version": change.schema_version,
        "change_type": change.change_type.value,
        "detected_at": _iso(change.detected_at),
        "marketplace": change.marketplace,
        "offer_id": change.offer_id,
        "platform_listing_id": state.platform_listing_id,
        "previous_observation_id": change.previous_observation_id,
        "current_observation_id": change.current_observation_id,
        "field_name": change.field_name,
        "previous_value": stringify(change.previous_value),
        "current_value": stringify(change.current_value),
        "previous_value_numeric": _scaled(previous_numeric),
        "current_value_numeric": _scaled(current_numeric),
        "delta_absolute": _scaled(delta_absolute),
        "delta_absolute_exact": _exact(delta_absolute),
        "delta_percent": _scaled(delta_percent),
        "delta_percent_exact": _exact(delta_percent),
        "observed_at": _iso(state.observed_at),
        "previous_observed_at": _iso(previous_observed_at),
        "counter_reset_or_invalid": bool(counter_reset_or_invalid),
        "secondary_rating_field": secondary_rating_field,
        "source_url": None,
        "category_path": None,
    }


def index_documents(
    client,
    index: str,
    documents: Sequence[tuple[str, Mapping[str, Any]]],
) -> None:
    """Bulk-index with overwrite semantics and deterministic IDs.

    ``index`` rather than ``create``: a replay must update the document, not
    fail on a conflict or add a second one.
    """
    if not documents:
        return
    if not isinstance(index, str) or not index.strip():
        raise ValueError("index is required")
    from elasticsearch.helpers import bulk  # type: ignore

    actions = [
        {
            "_op_type": "index",
            "_index": index,
            "_id": document_id,
            "_source": {key: value for key, value in document.items() if value is not None},
        }
        for document_id, document in documents
    ]
    bulk(client, actions, refresh=False, raise_on_error=True)


def ensure_indices(client, *, observations_index: str, changes_index: str) -> None:
    """Create both indices with the exact mappings if they do not exist."""
    for index, mapping in (
        (observations_index, MARKETPLACE_OBSERVATION_MAPPING),
        (changes_index, MARKETPLACE_CHANGE_MAPPING),
    ):
        if not client.indices.exists(index=index):
            client.indices.create(index=index, body=mapping)


def stale_change_document_fields() -> tuple[str, ...]:
    """Fields a stale change legitimately leaves empty, for dashboard notes."""
    return (
        "field_name",
        "previous_value_numeric",
        "current_value_numeric",
        "delta_absolute",
        "delta_percent",
    )


CHANGE_TYPES_WITHOUT_FIELD = (
    MarketplaceChangeType.NEW_OFFER,
    MarketplaceChangeType.OFFER_STALE,
)
