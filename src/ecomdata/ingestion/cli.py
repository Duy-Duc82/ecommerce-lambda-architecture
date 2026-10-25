"""One-shot raw-first collection, and the frontier seeding command.

``python -m ecomdata.ingestion.cli`` crawls the given categories once, writing
raw responses and records to Bronze (nothing is published to Kafka). The
long-running path is ``ecomdata.ingestion.service``.
"""
from __future__ import annotations

import argparse
import json
import logging
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

from ecomdata.domain.acquisition import AcquisitionReport, ListingPageRequest
from ecomdata.domain.records import record_json
from ecomdata.domain.sources import SOURCES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_MAX_PAGES = 50


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _write_records(reports: list[AcquisitionReport], output: TextIO | None) -> None:
    if output is None:
        return
    for report in reports:
        for record in report.records:
            output.write(record_json(record) + "\n")
    output.flush()


def run_source(
    runtime: Any,
    code: str,
    *,
    categories: list[str],
    max_pages: int = DEFAULT_MAX_PAGES,
    crawl_run_id: str | None = None,
    writer: Callable[[str, str, bytes], str],
    output: TextIO | None = None,
) -> dict[str, int]:
    """Run bounded raw-first collection for one source."""
    if max_pages < 1:
        raise ValueError("max_pages must be at least one")
    spec = runtime.spec(code)
    if not categories:
        raise ValueError(f"no categories configured for source: {code}")
    run_id = crawl_run_id or str(uuid.uuid4())
    reports: list[AcquisitionReport] = []
    counts = {"pages": 0, "succeeded_pages": 0, "failed_pages": 0, "rows": 0, "raw_bytes": 0}
    pages_per_category = max_pages if spec.paginated else 1

    for category in categories:
        for page in range(1, pages_per_category + 1):
            request = ListingPageRequest(spec.code, spec.marketplace_id, category, page)
            report = runtime.acquire(spec.code, request, crawl_run_id=run_id, writer=writer)
            reports.append(report)
            counts["pages"] += 1
            counts["rows"] += report.record_count
            counts["raw_bytes"] += report.raw_bytes
            if not report.is_task_success:
                counts["failed_pages"] += 1
                logger.warning(
                    "%s target=%s page=%d failed at %s: %s",
                    code,
                    category,
                    page,
                    report.failure.stage.value if report.failure else "UNKNOWN",
                    report.failure.error_message if report.failure else "unknown failure",
                )
                break
            counts["succeeded_pages"] += 1
            if report.page_exhausted:
                break

    _write_records(reports, output)
    logger.info(
        "%s: pages=%d succeeded=%d failed=%d rows=%d raw_bytes=%d",
        code,
        counts["pages"],
        counts["succeeded_pages"],
        counts["failed_pages"],
        counts["rows"],
        counts["raw_bytes"],
    )
    return counts


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Raw-first marketplace crawler (one shot)")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        choices=SOURCES.codes(),
        help="Source to crawl (repeatable). Default: all registered sources.",
    )
    parser.add_argument("--out-jsonl", metavar="PATH", help="Also write the records of every page to this file.")
    parser.add_argument("--category", action="append", dest="categories", help="Override categories (repeatable).")
    parser.add_argument("--max-pages", type=int, default=None, metavar="N", help="Pages per category (default: CRAWL_MAX_PAGES).")
    args = parser.parse_args(argv)
    from ecomdata.infra.object_store import object_store_for
    from ecomdata.ingestion.bootstrap import build_runtime
    from ecomdata.settings import load_settings

    settings = load_settings(dotenv=True)
    store = object_store_for(settings.storage)
    codes = tuple(args.sources or SOURCES.codes())
    runtime = build_runtime(settings, codes)

    output_file = None
    if args.out_jsonl:
        output_path = Path(args.out_jsonl)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_file = output_path.open("w", encoding="utf-8", newline="")
    try:
        for code in codes:
            try:
                run_source(
                    runtime,
                    code,
                    categories=list(args.categories or settings.crawl.default_categories.get(code, ())),
                    max_pages=args.max_pages or settings.crawl.max_pages,
                    writer=store.put,
                    output=output_file,
                )
            except Exception:
                logger.exception("%s: crawl run failed; continuing with remaining sources", code)
    finally:
        runtime.close()
        if output_file is not None:
            output_file.close()
            logger.info("JSONL written: %s", args.out_jsonl)


def discover_targets(config: Any, code: str, *, limit: int | None = None) -> list[str]:
    """The categories a source's discovery finds, fetched politely (robots.txt, throttle)."""
    from ecomdata.domain.acquisition import FetchTransportError
    from ecomdata.ingestion.bootstrap import http_stack

    spec = SOURCES.get(code)
    client, robots, throttle = http_stack(config)

    def fetch_text(url: str) -> str:
        if not robots.allowed(url):
            raise FetchTransportError(f"robots.txt disallows {url}")
        throttle.wait(url, min_interval_seconds=spec.min_delay_seconds)
        response = client.get(url)
        if response.status_code != 200:
            raise FetchTransportError(f"{url} answered HTTP {response.status_code}")
        return response.text

    discovery = SOURCES.discovery(code)
    discovery = discovery(fetch_text) if isinstance(discovery, type) else discovery
    try:
        return discovery.discover(limit=limit)
    finally:
        client.close()


def seed_main(argv: list[str] | None = None) -> None:
    """``seed``: enqueue listing-page targets, idempotently (see ``ecomdata.ingestion.seed``)."""
    from ecomdata.infra.postgres import connection_factory
    from ecomdata.ingestion.frontier import PostgresCrawlFrontier
    from ecomdata.ingestion.scheduling import CrawlTier
    from ecomdata.ingestion.seed import SEED_SCHEDULED_FOR, seed
    from ecomdata.settings import load_settings

    parser = argparse.ArgumentParser(description="Seed the crawl frontier with listing-page targets")
    parser.add_argument("--source", "--marketplace", dest="source", required=True, choices=SOURCES.codes())
    parser.add_argument("--category", action="append", help="repeatable; defaults to <CODE>_CATEGORIES")
    parser.add_argument("--from-discovery", action="store_true", help="seed the categories the source's discovery finds")
    parser.add_argument("--limit", type=int, default=None, help="with --from-discovery: at most N categories")
    parser.add_argument("--pages", type=int, default=1, help="listing pages per category")
    parser.add_argument("--tier", default=CrawlTier.NORMAL.value, choices=[tier.value for tier in CrawlTier])
    parser.add_argument("--priority", type=int, default=0)
    parser.add_argument("--scheduled-for", default=None, help="ISO instant for a new chain; omit to stay idempotent")
    args = parser.parse_args(argv)
    if args.from_discovery and args.category:
        parser.error("--from-discovery and --category are exclusive")

    settings = load_settings(dotenv=True)
    spec = SOURCES.get(args.source)
    scheduled_for = SEED_SCHEDULED_FOR
    if args.scheduled_for:
        scheduled_for = datetime.fromisoformat(args.scheduled_for.replace("Z", "+00:00"))
    if args.from_discovery:
        categories = discover_targets(settings, spec.code, limit=args.limit)
        logger.info("%s: discovery found %d categories", spec.code, len(categories))
    else:
        categories = args.category or settings.crawl.default_categories.get(spec.code, ())
    created = seed(
        PostgresCrawlFrontier(connection_factory(settings.postgres)),
        marketplace_code=spec.code,
        marketplace_id=spec.marketplace_id,
        categories=categories,
        pages=args.pages,
        tier=CrawlTier(args.tier),
        priority=args.priority,
        max_attempts=settings.crawl.max_attempts,
        scheduled_for=scheduled_for,
    )
    print(json.dumps({"source": spec.code, "categories": len(list(categories)), "new_tasks": created}))


if __name__ == "__main__":
    main()
