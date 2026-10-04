"""Deterministic observation load for the benchmark — Phase 9 plan section 7.

Produces ``MarketplaceObservationV1`` events straight into
``marketplace.observations.v1``, so the ingest and speed scenarios can push
more than a polite crawler ever would. Everything it sends is a **replayed
fixture** (plan decision D4): the frozen Tiki fixture's titles, reshaped into
many offers. Nothing it produces may be presented as a marketplace
observation, and it only ever runs in an isolated project, never in
``mp-live``.

Determinism. Event ``i`` of a run is a function of ``(seed, start, i)``:

- its offer is ``i % offers``, and its round ``i // offers``;
- ``observed_at`` is ``start + round * cadence``, plus the offer index in
  milliseconds, so one round's observations do not share an instant;
- its price is the offer's base price scaled by the stub's own table,
  ``PRICE_STEPS[round % 4]``, so the speed layer has the same
  ``PRICE_CHANGED`` and ``LARGE_PRICE_DROP`` to emit as in the smoke;
- ``observation_id`` follows the frozen derivation (marketplace, listing,
  observed instant, raw hash), so Silver dedups it as it would a crawl.

Only ``produced_at`` is the wall clock, taken at send: it is what the speed
layer's latency is measured from. Two runs with one seed and one ``start``
therefore send the same observations, byte for byte apart from that field.

``--duplicate-ratio`` re-sends a deterministic share of events immediately,
which exercises the dedup path. ``--source silver-export`` replays exported
Silver events instead, re-stamping ``produced_at``; those are labelled
``replayed_live``.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

from config.marketplace_schema import (
    MarketplaceObservationV1, create_marketplace_offer, create_observation_event, create_offer_observation,
)
from ops.stub_source import FIXTURE, PRICE_STEPS

MARKETPLACE = "tiki"
MARKETPLACE_ID = "marketplace-tiki"
ADAPTER_VERSION = "bench-load.v1"
DATASETS = ("replayed_fixture", "replayed_live")
LIVE_PROJECT = "mp-live"


@dataclass(frozen=True)
class LoadSpec:
    seed: int
    start: datetime
    count: int
    offers: int = 1000
    cadence_seconds: int = 3600
    duplicate_ratio: float = 0.0

    def __post_init__(self) -> None:
        if self.start.tzinfo is None:
            raise ValueError("start must be timezone-aware")
        if self.count < 0 or self.offers < 1 or self.cadence_seconds < 1:
            raise ValueError("count must be >= 0, offers and cadence_seconds >= 1")
        if not 0.0 <= self.duplicate_ratio < 1.0:
            raise ValueError("duplicate_ratio must be in [0, 1)")


def _titles(path: Path = FIXTURE) -> list[tuple[str, int]]:
    rows = json.loads(path.read_text(encoding="utf-8"))["data"]
    usable = [(str(r["name"]), int(r["price"])) for r in rows
              if isinstance(r.get("id"), int) and isinstance(r.get("price"), int) and r["price"] > 0 and r.get("name")]
    if not usable:
        raise ValueError(f"{path} holds no usable fixture row")
    return usable


def _digest(*parts: Any) -> str:
    return hashlib.sha256(":".join(str(p) for p in parts).encode("utf-8")).hexdigest()


def is_duplicate(spec: LoadSpec, index: int) -> bool:
    """Whether event ``index`` is sent a second time, by a fixed hash."""
    return int(_digest("dup", spec.seed, index)[:8], 16) % 10_000 < round(spec.duplicate_ratio * 10_000)


def build_event(spec: LoadSpec, index: int, produced_at: datetime,
                titles: list[tuple[str, int]] | None = None) -> MarketplaceObservationV1:
    titles = titles or _titles()
    offer_index, round_ = index % spec.offers, index // spec.offers
    title, base_price = titles[offer_index % len(titles)]
    listing = f"bench-{spec.seed}-{offer_index}"
    observed_at = spec.start + timedelta(seconds=round_ * spec.cadence_seconds, milliseconds=offer_index)
    price = Decimal(base_price * PRICE_STEPS[round_ % len(PRICE_STEPS)] // 100)
    raw_sha256 = _digest("raw", spec.seed, index)
    crawl_run_id = f"replayed_fixture-{spec.seed}-r{round_}"
    offer = create_marketplace_offer(
        marketplace_code=MARKETPLACE, marketplace_id=MARKETPLACE_ID, platform_listing_id=listing, seller_id=None,
        product_title=f"{title} #{offer_index}", brand=None, category_path=f"bench/{offer_index % 10}",
        source_url=f"https://bench.invalid/{listing}", currency="VND",
        first_seen_at=spec.start, last_seen_at=observed_at,
    )
    observation = create_offer_observation(
        marketplace_code=MARKETPLACE, platform_listing_id=listing, offer_id=offer.offer_id,
        observed_at=observed_at, fetched_at=observed_at, current_price=price,
        raw_uri=f"s3a://ecommerce-bronze/bench/replayed_fixture/{spec.seed}/{index}.json",
        raw_sha256=raw_sha256, adapter_version=ADAPTER_VERSION, crawl_run_id=crawl_run_id,
    )
    return create_observation_event(marketplace_code=MARKETPLACE, offer=offer, observation=observation,
                                    platform_listing_id=listing, produced_at=produced_at)


def fixture_events(spec: LoadSpec, clock: Callable[[], datetime]) -> Iterator[MarketplaceObservationV1]:
    """The run's events in send order, duplicates directly after their original."""
    titles = _titles()
    for index in range(spec.count):
        event = build_event(spec, index, clock(), titles)
        yield event
        if is_duplicate(spec, index):
            yield event


def silver_events(path: Path, clock: Callable[[], datetime]) -> Iterator[MarketplaceObservationV1]:
    """Exported Silver events, one JSON object per line, re-stamped at send."""
    from config.marketplace_wire import marketplace_observation_from_wire

    with path.open(encoding="utf-8") as lines:
        for line in lines:
            if line.strip():
                yield replace(marketplace_observation_from_wire(json.loads(line)), produced_at=clock())


class Pacer:
    """Holds the send rate at ``rate`` events per second; 0 means unpaced."""

    def __init__(self, rate: float, monotonic: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.rate, self.monotonic, self.sleep = rate, monotonic, sleep
        self.started: float | None = None
        self.sent = 0

    def wait(self) -> None:
        if self.started is None:
            self.started = self.monotonic()
        if self.rate > 0:
            due = self.started + self.sent / self.rate
            delay = due - self.monotonic()
            if delay > 0:
                self.sleep(delay)
        self.sent += 1


def send(events: Iterable[MarketplaceObservationV1], producer: Any, pacer: Pacer, *, flush_every: int = 1000) -> int:
    """Send every event, keyed as the crawler keys them, and wait for every ack.

    Asynchronous, unlike the crawler's one-ack-per-record path: the point is
    load the pipeline must absorb, not the crawler's own publish rate, which
    the crawl scenario measures separately.
    """
    from config.marketplace_wire import canonical_json
    from config.topics import MARKETPLACE_OBSERVATIONS

    futures, sent = [], 0
    for event in events:
        pacer.wait()
        wire = json.loads(canonical_json(event))
        futures.append(producer.send(MARKETPLACE_OBSERVATIONS.name, key=event.partition_key, value=wire))
        sent += 1
        if len(futures) >= flush_every:
            producer.flush()
            for future in futures:
                future.get(timeout=30)
            futures.clear()
    producer.flush()
    for future in futures:
        future.get(timeout=30)
    return sent


def refuse_live() -> None:
    if os.environ.get("COMPOSE_PROJECT_NAME", "") == LIVE_PROJECT:
        raise SystemExit(f"bench_load refuses {LIVE_PROJECT}: replayed load would contaminate live collection")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Deterministic replayed-fixture observation load (benchmarks only)")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--offers", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=0.0, help="events per second; 0 sends as fast as Kafka acks")
    parser.add_argument("--start", default=None, help="ISO instant of round 0; default: now, to the minute")
    parser.add_argument("--cadence-seconds", type=int, default=3600)
    parser.add_argument("--duplicate-ratio", type=float, default=0.0)
    parser.add_argument("--source", choices=("fixture", "silver-export"), default="fixture")
    parser.add_argument("--path", default=None, help="for --source silver-export: a JSON-lines file of Silver events")
    args = parser.parse_args(argv)
    refuse_live()

    clock = lambda: datetime.now(timezone.utc)  # noqa: E731
    if args.source == "silver-export":
        if not args.path:
            parser.error("--source silver-export needs --path")
        events, dataset = silver_events(Path(args.path), clock), "replayed_live"
    else:
        start = (datetime.fromisoformat(args.start.replace("Z", "+00:00")) if args.start
                 else clock().replace(second=0, microsecond=0))
        spec = LoadSpec(seed=args.seed, start=start, count=args.count, offers=args.offers,
                        cadence_seconds=args.cadence_seconds, duplicate_ratio=args.duplicate_ratio)
        events, dataset = fixture_events(spec, clock), "replayed_fixture"

    from data_ingestion.marketplace_producer import create_marketplace_producer

    producer = create_marketplace_producer()
    began = time.monotonic()
    try:
        sent = send(events, producer, Pacer(args.rate))
    finally:
        producer.close()
    elapsed = time.monotonic() - began
    print(json.dumps({"event": "bench_load_sent", "dataset": dataset, "sent": sent, "seconds": round(elapsed, 3),
                      "events_per_second": round(sent / elapsed, 1) if elapsed > 0 else None,
                      "seed": args.seed, "source": args.source}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
