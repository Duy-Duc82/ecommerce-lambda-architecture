"""
Kafka Producer - Su dung du lieu thuc tu Kaggle (E-commerce).

Chay:
  python -m data_ingestion.producer                  # Gui du lieu thuc vao Kafka
  python -m data_ingestion.producer --test-mode -n 10 # Test 10 events
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Them project root vao sys.path ───────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_EVENTS,
    KAFKA_TOPIC_ORDERS,
    KAFKA_TOPIC_PRICES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Tai du lieu thuc tu file CSV ─────────────────────────────
_DATA_FILES = [
    Path("c:/code/data/data_kaggle/2019-Oct.csv"),
    Path("c:/code/data/data_kaggle/2019-Nov.csv"),
]

_csv_reader = None
_current_file_index = 0
_current_file = None


def _get_csv_reader():
    """Tao reader cho file CSV hien tai."""
    global _csv_reader, _current_file_index, _current_file
    
    while _current_file_index < len(_DATA_FILES):
        csv_file = _DATA_FILES[_current_file_index]
        if not csv_file.exists():
            logger.warning("File CSV khong tim thay: %s", csv_file)
            _current_file_index += 1
            continue
        
        if _current_file != csv_file:
            logger.info("Dang mo file CSV: %s", csv_file)
            _current_file = csv_file
            f = open(csv_file, encoding="utf-8")
            _csv_reader = csv.DictReader(f)
        
        return _csv_reader
    
    return None


# ============================================================
# CHUYEN DOI DU LIEU THUC THANH EVENTS
# ============================================================

def _csv_row_to_event(row: dict) -> dict:
    """Chuyen dong CSV thanh event format."""
    event_type = row.get("event_type", "view").lower()
    timestamp = int(datetime.fromisoformat(row.get("event_time", "").replace(" UTC", "")).timestamp())
    
    # Chuan hoa event_type (view -> page_view, purchase giữ nguyên)
    if event_type == "view":
        event_type = "page_view"
    elif event_type == "cart":
        event_type = "add_to_cart"
    
    base_event = {
        "event_type": event_type,
        "user_id": row.get("user_id", ""),
        "product_id": row.get("product_id", ""),
        "category_code": row.get("category_code", ""),
        "brand": row.get("brand", ""),
        "price": float(row.get("price", 0)),
        "timestamp": timestamp,
    }
    
    return base_event


_data_iterator = None
_data_count = 0


def _get_next_real_event() -> dict | None:
    """Lay event tiep theo tu du lieu thuc (streaming)."""
    global _data_iterator, _current_file_index, _data_count
    
    if _data_iterator is None:
        _data_iterator = _get_csv_reader()
    
    if _data_iterator is None:
        raise FileNotFoundError("Khong tim thay du lieu CSV trong data_kaggle/")
    
    try:
        row = next(_data_iterator)
        _data_count += 1
        if _data_count % 100000 == 0:
            logger.info("Da doc %d events tu CSV", _data_count)
        return _csv_row_to_event(row)
    except StopIteration:
        # Chuyen sang file tiep theo
        _current_file_index += 1
        _data_iterator = _get_csv_reader()
        if _data_iterator is None:
            # Het file, quay lai file dau tien
            _current_file_index = 0
            logger.info("Het du lieu, quay lai tu dau")
            _data_iterator = _get_csv_reader()
        
        try:
            row = next(_data_iterator)
            _data_count += 1
            return _csv_row_to_event(row)
        except StopIteration:
            return None


# ============================================================
# KAFKA PRODUCER
# ============================================================

def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Tao Kafka producer voi retry logic."""
    max_retries = 5
    for attempt in range(1, max_retries + 1):
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
            logger.info("Ket noi Kafka thanh cong tai %s", bootstrap_servers)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Khong tim thay broker (lan %d/%d), thu lai sau 3s...",
                attempt, max_retries,
            )
            time.sleep(3)
    raise ConnectionError(f"Khong the ket noi Kafka sau {max_retries} lan thu!")


def _get_topic_for_event(event: dict) -> str:
    """Xac dinh Kafka topic phu hop cho event."""
    event_type = event.get("event_type", "")
    if event_type == "purchase":
        return KAFKA_TOPIC_ORDERS
    if event_type == "price_change":
        return KAFKA_TOPIC_PRICES
    return KAFKA_TOPIC_EVENTS


def _get_key_for_event(event: dict) -> str | None:
    """Tao partition key de dam bao thu tu event cua cung user/product."""
    return event.get("user_id") or event.get("product_id")


def run_producer(
    events_per_second: int = 10,
    max_events: int | None = None,
    test_mode: bool = False,
) -> None:
    """Chay producer voi du lieu thuc, phat event vao Kafka."""
    if test_mode:
        max_events = max_events or 10
        logger.info("TEST MODE: chi phat %d events (in ra console)", max_events)
        for i in range(max_events):
            event = _get_next_real_event()
            if event:
                topic = _get_topic_for_event(event)
                logger.info("[%d] topic=%s | %s", i + 1, topic, json.dumps(event, ensure_ascii=False))
        return

    producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)
    interval = 1.0 / events_per_second
    total_sent = 0

    logger.info("Bat dau phat events (%d/s) tro Kafka (du lieu thuc)...", events_per_second)
    try:
        while True:
            event = _get_next_real_event()
            if not event:
                break
            
            topic = _get_topic_for_event(event)
            key = _get_key_for_event(event)

            producer.send(topic, value=event, key=key)
            total_sent += 1

            if total_sent % 1000 == 0:
                producer.flush()
                logger.info("Da gui %d events", total_sent)

            if max_events and total_sent >= max_events:
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Dung producer. Tong events da gui: %d", total_sent)
    finally:
        producer.flush()
        producer.close()


# ============================================================
# CLI
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="E-commerce Data Producer (Kaggle Real Data)")
    parser.add_argument(
        "--eps", type=int, default=10,
        help="Events per second (default: %(default)s)",
    )
    parser.add_argument("--test-mode", action="store_true", help="Print to console only")
    parser.add_argument("-n", "--count", type=int, default=None, help="Max events to send")
    args = parser.parse_args()

    run_producer(
        events_per_second=args.eps,
        max_events=args.count,
        test_mode=args.test_mode,
    )


if __name__ == "__main__":
    main()
