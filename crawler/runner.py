"""Runs one or more site crawlers: writes raw snapshots to MinIO bronze and
publishes normalized price snapshots to Kafka, with a DLQ for rejects.

A failure in one site never stops the others — each site's run is wrapped
independently so a broken Shopee parser can't block a healthy Tiki run.

Run:
    python -m crawler.runner --site tiki
    python -m crawler.runner --site tiki --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.dlq import publish_to_dlq
from common.object_store import put_bytes
from config.schema import to_wire
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_PRICE_SNAPSHOTS, TIKI_CATEGORIES
from crawler.sites.tiki import TikiCrawler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Registry of available site adapters, keyed by --site value. Shopee/Lazada
# (Playwright-based, JS-rendered) join this dict once their adapters land.
SITE_CRAWLERS = {
    "tiki": lambda: TikiCrawler(categories=TIKI_CATEGORIES),
}


def create_producer(bootstrap_servers: str) -> Any:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=10,
            )
            logger.info("Connected to Kafka at %s", bootstrap_servers)
            return producer
        except NoBrokersAvailable:
            logger.warning("No broker (attempt %d/5), retrying in 3s...", attempt)
            time.sleep(3)
    raise ConnectionError(f"Could not connect to Kafka after 5 attempts: {bootstrap_servers}")


def run_site(site: str, producer: Any | None, dry_run: bool = False) -> dict[str, int]:
    """Crawl one registered site. Returns {"ok": n, "failed": n}."""
    crawler = SITE_CRAWLERS[site]()
    counts = {"ok": 0, "failed": 0}
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for snapshot, raw, error in crawler.crawl():
        if error is not None or snapshot is None:
            counts["failed"] += 1
            logger.warning("%s: rejected record (%s): %r", site, error, raw)
            if not dry_run:
                publish_to_dlq(
                    producer, KAFKA_TOPIC_PRICE_SNAPSHOTS, raw,
                    error or ValueError("no snapshot produced"), source=f"crawler.{site}",
                )
            continue

        counts["ok"] += 1
        if dry_run:
            continue

        ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        raw_key = f"{site}/{run_date}/{snapshot['product_id']}_{ts_ms}.json"
        put_bytes("bronze", f"crawl_raw/{raw_key}", json.dumps(raw, ensure_ascii=False).encode("utf-8"))
        producer.send(KAFKA_TOPIC_PRICE_SNAPSHOTS, value=to_wire(snapshot), key=snapshot["product_id"])

    logger.info("%s: done. ok=%d failed=%d", site, counts["ok"], counts["failed"])
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-site product/price crawler")
    parser.add_argument(
        "--site", action="append", dest="sites", choices=sorted(SITE_CRAWLERS),
        help="Site to crawl (repeatable). Default: all registered sites.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Crawl and log only; no Kafka/MinIO writes")
    args = parser.parse_args()
    sites = args.sites or sorted(SITE_CRAWLERS)

    producer = None if args.dry_run else create_producer(KAFKA_BOOTSTRAP_SERVERS)
    try:
        for site in sites:
            try:
                run_site(site, producer, dry_run=args.dry_run)
            except Exception:
                logger.exception("%s: crawl run failed entirely; continuing with remaining sites", site)
    finally:
        if producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
