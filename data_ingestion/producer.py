"""Kafka producer for canonical behavioral events (Kaggle store dataset).

Reads the Kaggle Multi-Category Store CSV, normalizes each row to the single
canonical event contract (`config.schema`) and publishes it to one Kafka topic.
Both the speed layer and the ES indexer consume that same contract, so the
realtime and batch paths never disagree on the event shape.

Run:
    python -m data_ingestion.producer --source data/data_kaggle/2019-Oct.csv --eps 200
    python -m data_ingestion.producer --test-mode -n 10
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.schema import normalize_event, to_wire
from config.settings import BASE_DIR, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_EVENTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_SOURCE = BASE_DIR / "data" / "data_kaggle" / "2019-Oct.csv"


def iter_source_events(source: Path, loop: bool = False) -> Iterator[dict[str, Any]]:
    """Yield canonical events from a Kaggle CSV, skipping invalid rows."""
    if not source.exists():
        raise FileNotFoundError(f"Source CSV not found: {source}")
    while True:
        with open(source, encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                try:
                    yield normalize_event(row)
                except ValueError:
                    continue  # rows the contract rejects are dropped at the edge
        if not loop:
            return
        logger.info("Reached end of %s, looping from start", source.name)


def create_producer(bootstrap_servers: str) -> Any:
    """Create a Kafka producer with bounded connection retries."""
    try:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable
    except ImportError as exc:  # pragma: no cover - import guard
        raise ImportError("kafka-python-ng is required: pip install kafka-python-ng") from exc

    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=10,
                batch_size=32768,
            )
            logger.info("Connected to Kafka at %s", bootstrap_servers)
            return producer
        except NoBrokersAvailable:
            logger.warning("No broker (attempt %d/5), retrying in 3s...", attempt)
            time.sleep(3)
    raise ConnectionError(f"Could not connect to Kafka after 5 attempts: {bootstrap_servers}")


def run_producer(
    source: Path,
    events_per_second: int = 200,
    max_events: int | None = None,
    loop: bool = False,
    test_mode: bool = False,
) -> int:
    """Stream canonical events to Kafka. Returns the number of events emitted."""
    events = iter_source_events(source, loop=loop)

    if test_mode:
        limit = max_events or 10
        logger.info("TEST MODE: printing %d events (no Kafka)", limit)
        for i, event in enumerate(events, start=1):
            logger.info("[%d] %s", i, json.dumps(to_wire(event), ensure_ascii=False))
            if i >= limit:
                break
        return min(limit, i)

    producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)
    interval = 1.0 / events_per_second if events_per_second > 0 else 0.0
    sent = 0
    logger.info("Producing to topic=%s (%d ev/s) from %s", KAFKA_TOPIC_EVENTS, events_per_second, source.name)
    try:
        for event in events:
            key = event.get("user_session") or event.get("user_id") or None
            producer.send(KAFKA_TOPIC_EVENTS, value=to_wire(event), key=key)
            sent += 1
            if sent % 1000 == 0:
                producer.flush()
                logger.info("Sent %d events", sent)
            if max_events and sent >= max_events:
                break
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Interrupted after %d events", sent)
    finally:
        producer.flush()
        producer.close()
    logger.info("Done. Total sent: %d", sent)
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description="Canonical behavioral-event Kafka producer")
    parser.add_argument("--source", "--csv", dest="source", default=str(DEFAULT_SOURCE),
                        help="Kaggle CSV path (default: %(default)s)")
    parser.add_argument("--eps", type=int, default=200, help="Events per second")
    parser.add_argument("-n", "--count", type=int, default=None, help="Max events to send")
    parser.add_argument("--loop", action="store_true", help="Replay the file continuously")
    parser.add_argument("--test-mode", action="store_true", help="Print events instead of sending")
    args = parser.parse_args()
    run_producer(
        source=Path(args.source),
        events_per_second=args.eps,
        max_events=args.count,
        loop=args.loop,
        test_mode=args.test_mode,
    )


if __name__ == "__main__":
    main()
