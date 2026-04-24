"""
Kafka Producer - Mo phong du lieu hanh vi nguoi dung TMDT.

Cac loai event:
  - page_view     (50%)  : Xem trang san pham
  - add_to_cart   (20%)  : Them vao gio hang
  - purchase      (15%)  : Mua hang
  - review        (10%)  : Danh gia san pham
  - price_change  (3%)   : Thay doi gia
  - search_query  (2%)   : Tim kiem

Chay:
  python -m data_ingestion.producer                  # Normal mode
  python -m data_ingestion.producer --burst           # Black Friday simulation
  python -m data_ingestion.producer --test-mode -n 10 # Test 10 events
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import time
import uuid
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
    SAMPLE_DATA_DIR,
    SIMULATOR_EVENTS_PER_SECOND,
    SIMULATOR_PRODUCTS_COUNT,
    SIMULATOR_USERS_COUNT,
)
from data_ingestion.schemas import (
    AddToCartEvent,
    PageViewEvent,
    PriceChangeEvent,
    PurchaseEvent,
    ReviewEvent,
    SearchQueryEvent,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Tai du lieu san pham mau ─────────────────────────────────
_PRODUCTS: list[dict] = []


def _load_products() -> list[dict]:
    """Doc danh sach san pham tu file JSON."""
    global _PRODUCTS
    if _PRODUCTS:
        return _PRODUCTS
    products_file = SAMPLE_DATA_DIR / "products.json"
    with open(products_file, encoding="utf-8") as f:
        _PRODUCTS = json.load(f)
    logger.info("Da tai %d san pham tu %s", len(_PRODUCTS), products_file)
    return _PRODUCTS


# ── Du lieu gia lap bo sung ──────────────────────────────────
PAYMENT_METHODS = ["credit_card", "debit_card", "momo", "zalopay", "vnpay", "cod"]
SEARCH_QUERIES = [
    "iphone 16", "laptop gaming", "tai nghe bluetooth", "may hut bui",
    "nuoc hoa nam", "giay the thao", "ao khoac mua dong", "sua bot tre em",
    "camera hanh trinh", "dong ho thong minh", "ban phim co", "man hinh 4k",
    "tu lanh inverter", "may giat cua truoc", "robot hut bui", "loa bluetooth",
    "sach hay", "do choi lego", "kem chong nang", "vot cau long",
]
REVIEW_COMMENTS = [
    "San pham rat tot, giao hang nhanh!",
    "Chat luong tuyet voi, dung nhu mo ta.",
    "Gia hop ly, se mua lai lan sau.",
    "Dong goi can than, san pham chinh hang.",
    "Tam duoc, khong co gi dac biet.",
    "Hoi that vong vi chat luong khong nhu ky vong.",
    "Giao hang hoi cham nhung san pham ok.",
    "Rat hai long, 5 sao!",
    "San pham loi, da doi tra thanh cong.",
    "Mau sac dep, chat lieu tot.",
]


def _random_user_id() -> str:
    """Tao user_id ngau nhien trong pham vi cau hinh."""
    return f"U{random.randint(1, SIMULATOR_USERS_COUNT):04d}"


def _now_ts() -> int:
    """Timestamp hien tai (epoch seconds)."""
    return int(time.time())


# ============================================================
# TAO TUNG LOAI EVENT
# ============================================================

def generate_page_view() -> dict:
    product = random.choice(_load_products())
    return PageViewEvent(
        user_id=_random_user_id(),
        product_id=product["id"],
        product_name=product["name"],
        category=product["category"],
        timestamp=_now_ts(),
    ).to_dict()


def generate_add_to_cart() -> dict:
    product = random.choice(_load_products())
    return AddToCartEvent(
        user_id=_random_user_id(),
        product_id=product["id"],
        product_name=product["name"],
        category=product["category"],
        quantity=random.randint(1, 5),
        price=product["price"],
        timestamp=_now_ts(),
    ).to_dict()


def generate_purchase() -> dict:
    n_items = random.randint(1, 5)
    products = random.sample(_load_products(), min(n_items, len(_load_products())))
    quantities = [random.randint(1, 3) for _ in products]
    total = sum(p["price"] * q for p, q in zip(products, quantities))
    return PurchaseEvent(
        user_id=_random_user_id(),
        order_id=f"ORD-{uuid.uuid4().hex[:12].upper()}",
        product_ids=[p["id"] for p in products],
        product_names=[p["name"] for p in products],
        quantities=quantities,
        total_amount=total,
        payment_method=random.choice(PAYMENT_METHODS),
        timestamp=_now_ts(),
    ).to_dict()


def generate_review() -> dict:
    product = random.choice(_load_products())
    return ReviewEvent(
        user_id=_random_user_id(),
        product_id=product["id"],
        product_name=product["name"],
        rating=random.choices([1, 2, 3, 4, 5], weights=[5, 5, 15, 35, 40])[0],
        comment=random.choice(REVIEW_COMMENTS),
        timestamp=_now_ts(),
    ).to_dict()


def generate_price_change() -> dict:
    product = random.choice(_load_products())
    old_price = product["price"]
    change_pct = random.uniform(-0.20, 0.30)  # -20% to +30%
    new_price = round(old_price * (1 + change_pct), -3)  # Lam tron den 1000 VND
    return PriceChangeEvent(
        product_id=product["id"],
        product_name=product["name"],
        category=product["category"],
        old_price=old_price,
        new_price=max(new_price, 10000),  # Gia toi thieu
        timestamp=_now_ts(),
    ).to_dict()


def generate_search_query() -> dict:
    return SearchQueryEvent(
        user_id=_random_user_id(),
        query_text=random.choice(SEARCH_QUERIES),
        results_count=random.randint(0, 500),
        timestamp=_now_ts(),
    ).to_dict()


# ============================================================
# WEIGHTED RANDOM EVENT GENERATOR
# ============================================================

_EVENT_GENERATORS = [
    (generate_page_view, 0.50),
    (generate_add_to_cart, 0.20),
    (generate_purchase, 0.15),
    (generate_review, 0.10),
    (generate_price_change, 0.03),
    (generate_search_query, 0.02),
]

_GENERATORS = [g for g, _ in _EVENT_GENERATORS]
_WEIGHTS = [w for _, w in _EVENT_GENERATORS]


def generate_random_event() -> dict:
    """Tao mot event ngau nhien theo ty le phan bo da cau hinh."""
    generator = random.choices(_GENERATORS, weights=_WEIGHTS, k=1)[0]
    return generator()


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
    events_per_second: int = SIMULATOR_EVENTS_PER_SECOND,
    burst_mode: bool = False,
    max_events: int | None = None,
    test_mode: bool = False,
) -> None:
    """Chay producer lien tuc, phat event vao Kafka."""
    if burst_mode:
        events_per_second *= 5
        logger.info("BURST MODE (Black Friday): %d events/s", events_per_second)

    if test_mode:
        max_events = max_events or 10
        logger.info("TEST MODE: chi phat %d events (in ra console)", max_events)
        for i in range(max_events):
            event = generate_random_event()
            topic = _get_topic_for_event(event)
            logger.info("[%d] topic=%s | %s", i + 1, topic, json.dumps(event, ensure_ascii=False))
        return

    producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)
    interval = 1.0 / events_per_second
    total_sent = 0

    logger.info("Bat dau phat events (%d/s) vao Kafka...", events_per_second)
    try:
        while True:
            event = generate_random_event()
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
    parser = argparse.ArgumentParser(description="E-commerce Data Simulator (Kafka Producer)")
    parser.add_argument(
        "--eps", type=int, default=SIMULATOR_EVENTS_PER_SECOND,
        help="Events per second (default: %(default)s)",
    )
    parser.add_argument("--burst", action="store_true", help="Black Friday burst mode (5x)")
    parser.add_argument("--test-mode", action="store_true", help="Print to console only")
    parser.add_argument("-n", "--count", type=int, default=None, help="Max events to send")
    args = parser.parse_args()

    run_producer(
        events_per_second=args.eps,
        burst_mode=args.burst,
        max_events=args.count,
        test_mode=args.test_mode,
    )


if __name__ == "__main__":
    main()
