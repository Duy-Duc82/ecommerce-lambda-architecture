"""
ES Indexer — Real-time Kafka → Elasticsearch Pipeline.

Consumes events from all 3 Kafka topics and bulk-indexes them into
Elasticsearch, enriching with geo/device metadata and computed fields.

Run:
    python -m data_ingestion.es_indexer
    python -m data_ingestion.es_indexer --topics events orders prices
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── Try to import Kafka + ES ──────────────────────────────────────────
try:
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
except ImportError:
    sys.exit("kafka-python not installed. Run: pip install kafka-python")

try:
    from elasticsearch import Elasticsearch, helpers
    from elasticsearch.exceptions import ConnectionError as ESConnectionError
except ImportError:
    sys.exit("elasticsearch not installed. Run: pip install elasticsearch>=8.0.0")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ── Config ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ES_HOST         = os.getenv("ES_HOST", "http://localhost:9200")

TOPIC_EVENTS  = os.getenv("KAFKA_TOPIC_EVENTS",  "ecommerce_events")
TOPIC_ORDERS  = os.getenv("KAFKA_TOPIC_ORDERS",  "ecommerce_orders")
TOPIC_PRICES  = os.getenv("KAFKA_TOPIC_PRICES",  "ecommerce_prices")

INDEX_EVENTS        = "ecommerce-events"
INDEX_ORDERS        = "ecommerce-orders"
INDEX_PRICES        = "ecommerce-prices"
INDEX_REALTIME      = "ecommerce-realtime-metrics"

BATCH_SIZE          = int(os.getenv("ES_BATCH_SIZE",     "200"))
BATCH_TIMEOUT_MS    = int(os.getenv("ES_BATCH_TIMEOUT",  "3000"))   # ms
CONSUMER_GROUP      = "es-indexer-group"

log = logging.getLogger("es_indexer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%H:%M:%S",
)

# ── Topic → Index mapping ─────────────────────────────────────────────
TOPIC_INDEX_MAP: dict[str, str] = {
    TOPIC_EVENTS: INDEX_EVENTS,
    TOPIC_ORDERS: INDEX_ORDERS,
    TOPIC_PRICES: INDEX_PRICES,
}


# ─────────────────────────────────────────────────────────────────────
# Enrichment helpers
# ─────────────────────────────────────────────────────────────────────

_DEVICE_MAP = {
    "mobile": ["android", "iphone", "ios", "mobile"],
    "tablet": ["ipad", "tablet"],
    "desktop": ["windows", "mac", "linux", "x11"],
}

def _infer_device(user_agent: str) -> str:
    ua = (user_agent or "").lower()
    for device, keywords in _DEVICE_MAP.items():
        if any(k in ua for k in keywords):
            return device
    return "unknown"


def _enrich_event(raw: dict[str, Any]) -> dict[str, Any]:
    """Add @timestamp, ingest_timestamp, device_type inference."""
    now_iso = datetime.now(timezone.utc).isoformat()

    # Normalise timestamp
    ts = raw.get("timestamp")
    if isinstance(ts, (int, float)):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        raw["@timestamp"] = dt.isoformat()
    elif isinstance(ts, str):
        raw["@timestamp"] = ts
    else:
        raw["@timestamp"] = now_iso

    raw.setdefault("ingest_timestamp", now_iso)
    raw.setdefault("event_id", str(uuid.uuid4()))

    # Device type from user_agent
    if "user_agent" in raw and "device_type" not in raw:
        raw["device_type"] = _infer_device(raw["user_agent"])

    # Geo point from lat/lon if present
    lat = raw.pop("lat", None)
    lon = raw.pop("lon", None)
    if lat is not None and lon is not None:
        raw["geo_location"] = {"lat": lat, "lon": lon}

    return raw


def _enrich_order(raw: dict[str, Any]) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).isoformat()
    ts = raw.get("timestamp")
    if isinstance(ts, (int, float)):
        raw["@timestamp"] = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    else:
        raw["@timestamp"] = now_iso
    raw.setdefault("ingest_timestamp", now_iso)
    raw.setdefault("order_id", str(uuid.uuid4()))

    # Compute net_revenue
    total   = float(raw.get("total_amount", 0))
    discount= float(raw.get("discount_amount", 0))
    raw.setdefault("net_revenue", round(total - discount, 2))

    return raw


def _enrich_price(raw: dict[str, Any]) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).isoformat()
    raw["@timestamp"] = now_iso
    raw.setdefault("ingest_timestamp", now_iso)

    old = float(raw.get("old_price", 0) or 0)
    new = float(raw.get("new_price", 0) or 0)
    if old > 0:
        pct = round((new - old) / old * 100, 4)
        raw.setdefault("price_change_pct", pct)
        raw.setdefault("price_change_abs", round(new - old, 4))
        raw.setdefault("price_direction", "up" if pct > 0 else "down" if pct < 0 else "stable")

    return raw


_ENRICH_FN = {
    TOPIC_EVENTS: _enrich_event,
    TOPIC_ORDERS: _enrich_order,
    TOPIC_PRICES: _enrich_price,
}


# ─────────────────────────────────────────────────────────────────────
# Bulk indexer
# ─────────────────────────────────────────────────────────────────────

def _make_action(index: str, doc: dict[str, Any]) -> dict[str, Any]:
    return {
        "_index":  index,
        "_id":     doc.get("event_id") or doc.get("order_id") or doc.get("alert_id") or None,
        "_source": doc,
    }


def _flush_bulk(es: Elasticsearch, actions: list[dict], label: str) -> None:
    if not actions:
        return
    ok, errors = helpers.bulk(es, actions, raise_on_error=False, stats_only=False)
    if errors:
        log.warning("%s: %d bulk errors (sample: %s)", label, len(errors), errors[:1])
    log.info("%s: indexed %d docs", label, ok)
    actions.clear()


# ─────────────────────────────────────────────────────────────────────
# Main consumer loop
# ─────────────────────────────────────────────────────────────────────

def run(topics: list[str]) -> None:
    log.info("Connecting to Elasticsearch: %s", ES_HOST)
    es = Elasticsearch(ES_HOST)
    for attempt in range(10):
        try:
            info = es.info()
            log.info("ES connected — cluster: %s v%s", info["cluster_name"], info["version"]["number"])
            break
        except ESConnectionError:
            log.warning("ES not ready (attempt %d/10), retrying in 5s…", attempt + 1)
            time.sleep(5)
    else:
        sys.exit("Cannot connect to Elasticsearch after 10 retries.")

    log.info("Connecting to Kafka: %s | topics: %s", KAFKA_BOOTSTRAP, topics)
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=CONSUMER_GROUP,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=-1,
                max_poll_records=500,
            )
            log.info("Kafka consumer ready.")
            break
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %d/10), retrying in 5s…", attempt + 1)
            time.sleep(5)
    else:
        sys.exit("Cannot connect to Kafka after 10 retries.")

    actions: list[dict] = []
    last_flush = time.time()
    total_indexed = 0

    log.info("=== ES Indexer running — Ctrl+C to stop ===")
    try:
        for msg in consumer:
            topic = msg.topic
            raw   = msg.value

            enrich_fn = _ENRICH_FN.get(topic, _enrich_event)
            doc = enrich_fn(raw)

            index = TOPIC_INDEX_MAP.get(topic, INDEX_EVENTS)
            actions.append(_make_action(index, doc))

            elapsed_ms = (time.time() - last_flush) * 1000
            if len(actions) >= BATCH_SIZE or elapsed_ms >= BATCH_TIMEOUT_MS:
                _flush_bulk(es, actions, f"batch@{topic}")
                total_indexed += len(actions)
                actions.clear()
                last_flush = time.time()
                log.info("Total indexed so far: %d", total_indexed)

    except KeyboardInterrupt:
        log.info("Shutting down…")
        _flush_bulk(es, actions, "final-flush")
        consumer.close()
        log.info("ES Indexer stopped. Total indexed: %d", total_indexed)


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Real-time Kafka → Elasticsearch indexer")
    parser.add_argument(
        "--topics", nargs="+",
        default=[TOPIC_EVENTS, TOPIC_ORDERS, TOPIC_PRICES],
        help="Kafka topics to consume (default: all 3 topics)",
    )
    args = parser.parse_args()
    run(args.topics)


if __name__ == "__main__":
    main()
