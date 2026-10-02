"""Seed the crawl frontier with listing-page targets, idempotently.

Phase 8 plan section 6.1. Each frontier row is one scheduled occurrence, and
``mark_succeeded`` inserts the next occurrence under a new ``task_id``. Seeding
with ``scheduled_for = now`` would therefore start a second chain for a target
already being crawled. The first occurrence of every seeded target is
anchored at :data:`SEED_SCHEDULED_FOR` instead, so its ``task_id`` depends only
on the target and a re-seed is a no-op through ``ON CONFLICT DO NOTHING``.

The anchored row stays whatever its status, so a re-seed can neither revive
nor duplicate a chain. Reviving a target that ended ``FAILED`` is a deliberate
operator action: pass ``--scheduled-for``.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any, Iterable

from config.marketplace_schema import ResourceType
from crawler.contracts import encode_listing_page_task_target
from crawler.scheduling import CrawlTask, CrawlTaskStatus, CrawlTier, make_crawl_task_id

# Due at once, and fixed, so the seeded task ID is a function of the target.
SEED_SCHEDULED_FOR = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Marketplace code -> ID, kept here so seeding needs no HTTP adapter.
MARKETPLACE_IDS = {"tiki": "marketplace-tiki"}


def seed(
    frontier: Any,
    *,
    marketplace_code: str,
    marketplace_id: str,
    categories: Iterable[str],
    pages: int,
    tier: CrawlTier,
    priority: int,
    max_attempts: int,
    scheduled_for: datetime = SEED_SCHEDULED_FOR,
) -> int:
    """Enqueue one READY task per category page; return how many were new."""
    targets = [str(category).strip() for category in categories]
    if not targets or any(not target for target in targets):
        raise ValueError("categories must list at least one non-blank category")
    if isinstance(pages, bool) or not isinstance(pages, int) or pages < 1:
        raise ValueError("pages must be a positive integer")
    created = 0
    for category in targets:
        for page in range(1, pages + 1):
            target = encode_listing_page_task_target(category, page)
            task = CrawlTask(
                task_id=make_crawl_task_id(marketplace_code, ResourceType.LISTING_PAGE, target, scheduled_for),
                marketplace_code=marketplace_code,
                marketplace_id=marketplace_id,
                target=target,
                resource_type=ResourceType.LISTING_PAGE,
                tier=tier,
                priority=priority,
                scheduled_for=scheduled_for,
                status=CrawlTaskStatus.READY,
                attempts=0,
                max_attempts=max_attempts,
            )
            if frontier.enqueue(task):
                created += 1
    return created


def main() -> None:
    from config.settings import CRAWL_MAX_ATTEMPTS, TIKI_CATEGORIES

    parser = argparse.ArgumentParser(description="Seed the crawl frontier with listing-page targets")
    parser.add_argument("--marketplace", default="tiki", choices=sorted(MARKETPLACE_IDS))
    parser.add_argument("--category", action="append", help="repeatable; defaults to TIKI_CATEGORIES")
    parser.add_argument("--pages", type=int, default=1, help="listing pages per category")
    parser.add_argument("--tier", default=CrawlTier.NORMAL.value, choices=[tier.value for tier in CrawlTier])
    parser.add_argument("--priority", type=int, default=0)
    parser.add_argument("--scheduled-for", default=None,
                        help="ISO instant for a new chain; omit to stay idempotent")
    args = parser.parse_args()

    from crawler.frontier import PostgresCrawlFrontier
    from crawler.service import postgres_connection_factory

    scheduled_for = SEED_SCHEDULED_FOR
    if args.scheduled_for:
        scheduled_for = datetime.fromisoformat(args.scheduled_for.replace("Z", "+00:00"))
    created = seed(
        PostgresCrawlFrontier(postgres_connection_factory()),
        marketplace_code=args.marketplace,
        marketplace_id=MARKETPLACE_IDS[args.marketplace],
        categories=args.category or TIKI_CATEGORIES,
        pages=args.pages,
        tier=CrawlTier(args.tier),
        priority=args.priority,
        max_attempts=CRAWL_MAX_ATTEMPTS,
        scheduled_for=scheduled_for,
    )
    print(f"seeded {created} new task(s)")


if __name__ == "__main__":
    main()
