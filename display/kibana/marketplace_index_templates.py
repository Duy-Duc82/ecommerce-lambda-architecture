"""Composable index templates for every ``marketplace-*`` index (plan section 11.1).

A template applies only to an index created *after* it was installed, so this
module is run by the Kibana setup container before anything writes, and the
runbook carries the procedure for recreating the two indices that were mapped
dynamically before Phase 8.

Three rules decide every mapping here:

* **money is ``scaled_float`` with scaling factor 1000000** — the same six
  decimal places as the PostgreSQL ``decimal(38,6)`` columns, so a price that
  survives the warehouse survives Elasticsearch unchanged. ``float`` would
  round 199000.99 away;
* **identifiers are ``keyword``** — they are matched and aggregated whole,
  never analysed into words;
* **the four projector indices are ``dynamic: strict``** — the projector
  builds their documents field by field, so a field it did not mean to write
  is a bug, and strict mapping turns it into a loud failure instead of a
  silent new column.

``marketplace-changes-*`` and ``marketplace-offers-current-*`` are deliberately
*not* strict. They are written by the speed layer's micro-batch, whose payload
is the frozen wire contract (``config.marketplace_wire`` validates it field for
field, so it cannot drift without a schema version bump). A strict refusal
there would fail the whole micro-batch — the speed layer, not one projection
pass — which is a worse trade than the drift it would catch.
"""
from __future__ import annotations

from typing import Any

# Six decimal places, matching cache.marketplace_* decimal(38,6).
PRICE_SCALING_FACTOR = 1_000_000
TEMPLATE_PRIORITY = 200

# Enough for the longest message the audit tables allow through; a longer one
# stays in _source and on the table panel, it is simply not aggregatable.
_MESSAGE_IGNORE_ABOVE = 1024

_KEYWORD: dict[str, Any] = {"type": "keyword"}
_DATE: dict[str, Any] = {"type": "date"}
_LONG: dict[str, Any] = {"type": "long"}
_BOOLEAN: dict[str, Any] = {"type": "boolean"}
_MESSAGE: dict[str, Any] = {"type": "keyword", "ignore_above": _MESSAGE_IGNORE_ABOVE}


def _price() -> dict[str, Any]:
    return {"type": "scaled_float", "scaling_factor": PRICE_SCALING_FACTOR}


def _change_value() -> dict[str, Any]:
    """``previous_value``/``current_value``: a scalar for most change types, the
    whole offer for NEW_OFFER.

    No Elasticsearch field is both an object and a scalar, so the sink stores
    an object value as its canonical JSON string (``_change_document``). The
    field is therefore a keyword, with a numeric sub-field for the price
    charts; ``ignore_malformed`` lets the JSON-string case skip that sub-field
    instead of rejecting the document.
    """
    return {
        "type": "keyword",
        "ignore_above": 8192,
        "fields": {"numeric": {"type": "scaled_float", "scaling_factor": PRICE_SCALING_FACTOR,
                               "ignore_malformed": True}},
    }


# A single-node cluster: one shard, no replica, or every index sits yellow.
_SETTINGS: dict[str, Any] = {"number_of_shards": 1, "number_of_replicas": 0}

# ---------------------------------------------------------------------------
# The six templates. Each maps one index family.
# ---------------------------------------------------------------------------

_CHANGE_PROPERTIES: dict[str, Any] = {
    "event_id": _KEYWORD,
    "schema_version": _KEYWORD,
    "change_type": _KEYWORD,
    "detected_at": _DATE,
    "marketplace": _KEYWORD,
    "offer_id": _KEYWORD,
    "previous_observation_id": _KEYWORD,
    "current_observation_id": _KEYWORD,
    "field_name": _KEYWORD,
    "previous_value": _change_value(),
    "current_value": _change_value(),
    "rule_version": _KEYWORD,
}

_OFFER_PROPERTIES: dict[str, Any] = {
    "marketplace": _KEYWORD,
    "marketplace_id": _KEYWORD,
    "offer_id": _KEYWORD,
    "platform_listing_id": _KEYWORD,
    "seller_id": _KEYWORD,
    # The one human-readable field: searchable as words, aggregatable whole.
    "product_title": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 512}}},
    "brand": _KEYWORD,
    "category_path": _KEYWORD,
    "source_url": _KEYWORD,
    "currency": _KEYWORD,
    "active_status": _KEYWORD,
    "observation_id": _KEYWORD,
    "observed_at": _DATE,
    "produced_at": _DATE,
    "current_price": _price(),
    "list_price": _price(),
    "rating_value": _price(),
    "rating_count": _LONG,
    "review_count": _LONG,
    "sold_count": _LONG,
    "availability": _KEYWORD,
    "stale_emitted": _BOOLEAN,
}

_SOURCE_HEALTH_PROPERTIES: dict[str, Any] = {
    "marketplace_code": _KEYWORD,
    "consecutive_failures": _LONG,
    # audit.crawl_source_state has no boolean: the circuit is open only while
    # opened_until > now. The projector resolves that against its own clock.
    "circuit_open": _BOOLEAN,
    "opened_until": _DATE,
    "last_failure_at": _DATE,
    "last_success_at": _DATE,
    "updated_at": _DATE,
    "last_observation_at": _DATE,
    "freshness_seconds": _LONG,
    "projected_at": _DATE,
}

_CRAWL_ATTEMPT_PROPERTIES: dict[str, Any] = {
    "attempt_id": _KEYWORD,
    "crawl_run_id": _KEYWORD,
    "task_id": _KEYWORD,
    "marketplace_code": _KEYWORD,
    "resource_type": _KEYWORD,
    "attempt_number": _LONG,
    "started_at": _DATE,
    "completed_at": _DATE,
    "status": _KEYWORD,
    "http_status": _LONG,
    "latency_ms": _LONG,
    "raw_artifact_id": _KEYWORD,
    "raw_uri": _KEYWORD,
    "raw_bytes": _LONG,
    "parsed_count": _LONG,
    "rejected_count": _LONG,
    "error_kind": _KEYWORD,
    "error_message": _MESSAGE,
}

_SPEED_BATCH_PROPERTIES: dict[str, Any] = {
    "query_name": _KEYWORD,
    "query_id": _KEYWORD,
    "batch_id": _LONG,
    "status": _KEYWORD,
    "started_at": _DATE,
    "completed_at": _DATE,
    # completed_at - started_at. Micro-batch duration, NOT observation-to-change
    # latency: the change contract carries no processed time, and Phase 8 does
    # not add one. Every panel built on this says so.
    "duration_ms": _LONG,
    "input_rows": _LONG,
    "invalid_rows": _LONG,
    "applied_rows": _LONG,
    "duplicate_rows": _LONG,
    "late_rows": _LONG,
    "change_rows": _LONG,
    "kafka_rows": _LONG,
    "es_rows": _LONG,
    "redis_rows": _LONG,
    "error_message": _MESSAGE,
}

_DLQ_PROPERTIES: dict[str, Any] = {
    "dlq_id": _KEYWORD,
    "schema_version": _KEYWORD,
    "failed_at": _DATE,
    "stage": _KEYWORD,
    "source_topic": _KEYWORD,
    "source_partition": _LONG,
    "source_offset": _LONG,
    "source_key": _KEYWORD,
    "marketplace": _KEYWORD,
    "crawl_run_id": _KEYWORD,
    "raw_artifact_id": _KEYWORD,
    "raw_uri": _KEYWORD,
    "error_type": _KEYWORD,
    "error_message": _MESSAGE,
}


def _template(pattern: str, properties: dict[str, Any], *, strict: bool) -> dict[str, Any]:
    return {
        "index_patterns": [pattern],
        "priority": TEMPLATE_PRIORITY,
        "template": {
            "settings": dict(_SETTINGS),
            "mappings": {"dynamic": "strict" if strict else True, "properties": properties},
        },
    }


# Template name -> body. The name is what `PUT _index_template/<name>` uses.
INDEX_TEMPLATES: dict[str, dict[str, Any]] = {
    "marketplace-changes": _template("marketplace-changes-*", _CHANGE_PROPERTIES, strict=False),
    "marketplace-offers-current": _template("marketplace-offers-current-*", _OFFER_PROPERTIES, strict=False),
    "marketplace-source-health": _template("marketplace-source-health-*", _SOURCE_HEALTH_PROPERTIES, strict=True),
    "marketplace-crawl-attempts": _template("marketplace-crawl-attempts-*", _CRAWL_ATTEMPT_PROPERTIES, strict=True),
    "marketplace-speed-batches": _template("marketplace-speed-batches-*", _SPEED_BATCH_PROPERTIES, strict=True),
    "marketplace-dlq": _template("marketplace-dlq-*", _DLQ_PROPERTIES, strict=True),
}

# The indices the projector writes, and the only ones that are strict.
PROJECTOR_TEMPLATES = ("marketplace-source-health", "marketplace-crawl-attempts",
                       "marketplace-speed-batches", "marketplace-dlq")


def index_patterns() -> tuple[str, ...]:
    """Every pattern a template claims, for the saved-object check."""
    return tuple(sorted(pattern for body in INDEX_TEMPLATES.values() for pattern in body["index_patterns"]))


def projector_indices() -> tuple[str, ...]:
    """The four indices ops/es_projector.py writes, in template order."""
    from config.settings import (
        ES_INDEX_MARKETPLACE_CRAWL_ATTEMPTS, ES_INDEX_MARKETPLACE_DLQ,
        ES_INDEX_MARKETPLACE_SOURCE_HEALTH, ES_INDEX_MARKETPLACE_SPEED_BATCHES,
    )

    return (ES_INDEX_MARKETPLACE_SOURCE_HEALTH, ES_INDEX_MARKETPLACE_CRAWL_ATTEMPTS,
            ES_INDEX_MARKETPLACE_SPEED_BATCHES, ES_INDEX_MARKETPLACE_DLQ)


def ensure_projector_indices(es: Any) -> list[str]:
    """Create each projector index if it is absent.

    An index only springs into existence with its first document, and the DLQ
    index may legitimately never get one — a stack where nothing was ever
    quarantined is a healthy stack. A Lens panel over an index that does not
    exist is an error, not an empty chart, so the empty index is created up
    front and the template gives it its mapping.
    """
    created = []
    for index in projector_indices():
        if not es.indices.exists(index=index):
            es.indices.create(index=index)
            created.append(index)
    return created


def install_index_templates(es: Any) -> list[str]:
    """PUT every template. Idempotent: a PUT replaces the template by name."""
    installed = []
    for name, body in INDEX_TEMPLATES.items():
        es.indices.put_index_template(name=name, **body)
        installed.append(name)
    return installed


def main() -> int:
    import json

    from elasticsearch import Elasticsearch

    from config.settings import ES_HOST

    es = Elasticsearch(ES_HOST)
    try:
        installed = install_index_templates(es)
        created = ensure_projector_indices(es)
    finally:
        es.close()
    print(json.dumps({"event": "index_templates_installed", "templates": installed,
                      "indices_created": created}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
