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

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from ecomdata.domain.acquisition import encode_listing_page_task_target
from ecomdata.domain.observations import ResourceType
from ecomdata.domain.sources import SOURCES
from ecomdata.ingestion.scheduling import CrawlTask, CrawlTaskStatus, CrawlTier, make_crawl_task_id

# Due at once, and fixed, so the seeded task ID is a function of the target.
SEED_SCHEDULED_FOR = datetime(1970, 1, 1, tzinfo=UTC)

# Marketplace code -> ID, kept here so seeding needs no HTTP adapter.
MARKETPLACE_IDS = {spec.code: spec.marketplace_id for spec in SOURCES}


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


def main(argv: list[str] | None = None) -> None:
    from ecomdata.ingestion.cli import seed_main

    seed_main(argv)


if __name__ == "__main__":
    main()
