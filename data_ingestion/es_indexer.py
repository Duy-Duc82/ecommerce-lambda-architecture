"""Real-time raw-event indexer: Kafka -> Elasticsearch.

Consumes the canonical behavioral-event topic and bulk-indexes each raw event
into the `ecommerce-events` index so Kibana can do per-event drill-down. The
speed layer (speed_layer/speed_layer.py) owns the aggregated `ecommerce-metrics`
index; this process owns raw events. Both read the same canonical contract.

Run:
    python -m data_ingestion.es_indexer
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import ES_HOST, ES_INDEX_EVENTS, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_EVENTS

try:
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
except ImportError:  # pragma: no cover
    sys.exit("kafka-python-ng not installed. Run: pip install kafka-python-ng")

try:
    from elasticsearch import Elasticsearch, helpers
    from elasticsearch.exceptions import ConnectionError as ESConnectionError
except ImportError:  # pragma: no cover
    sys.exit("elasticsearch not installed. Run: pip install elasticsearch==8.18.0")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("es_indexer")

BATCH_SIZE = 200
CONSUMER_GROUP = "es-indexer-group"

_EVENT_FIELDS = {
    "@timestamp", "event_id", "event_time", "event_type", "user_id", "user_session",
    "product_id", "category_id", "category_code", "brand", "price", "ingest_timestamp",
}


def _enrich(raw: dict[str, Any]) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).isoformat()
    raw["@timestamp"] = raw.get("event_time") or now_iso
    raw.setdefault("ingest_timestamp", now_iso)
    raw.setdefault("event_id", str(uuid.uuid4()))
    return {k: v for k, v in raw.items() if k in _EVENT_FIELDS and v is not None}


def _connect_es() -> Elasticsearch:
    es = Elasticsearch(ES_HOST)
    for attempt in range(1, 11):
        try:
            info = es.info()
            log.info("ES connected — %s v%s", info["cluster_name"], info["version"]["number"])
            return es
        except ESConnectionError:
            log.warning("ES not ready (%d/10), retrying in 5s...", attempt)
            time.sleep(5)
    sys.exit("Cannot connect to Elasticsearch.")


def _connect_kafka(topic: str) -> KafkaConsumer:
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=CONSUMER_GROUP,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                max_poll_records=500,
            )
            log.info("Kafka consumer ready on topic=%s", topic)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (%d/10), retrying in 5s...", attempt)
            time.sleep(5)
    sys.exit("Cannot connect to Kafka.")


def run(topic: str) -> None:
    es = _connect_es()
    consumer = _connect_kafka(topic)
    actions: list[dict] = []
    total = 0
    log.info("=== ES indexer running (topic=%s -> index=%s). Ctrl+C to stop ===", topic, ES_INDEX_EVENTS)
    try:
        for msg in consumer:
            doc = _enrich(msg.value)
            actions.append({"_index": ES_INDEX_EVENTS, "_id": doc["event_id"], "_source": doc})
            if len(actions) >= BATCH_SIZE:
                ok, _ = helpers.bulk(es, actions, raise_on_error=False, stats_only=True)
                total += ok
                actions.clear()
                log.info("Indexed %d events", total)
    except KeyboardInterrupt:
        if actions:
            helpers.bulk(es, actions, raise_on_error=False)
        consumer.close()
        log.info("Stopped. Total indexed: %d", total)


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka -> Elasticsearch raw-event indexer")
    parser.add_argument("--topic", default=KAFKA_TOPIC_EVENTS)
    args = parser.parse_args()
    run(args.topic)


if __name__ == "__main__":
    main()
