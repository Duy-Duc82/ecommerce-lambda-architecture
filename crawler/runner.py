"""One-shot raw-first marketplace collection runner.

This module deliberately stops at Bronze plus canonical in-memory observation
events. Kafka publication belongs to Phase 4 and scheduling/audit belong to
Phase 3.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, TextIO

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.object_store import put_bytes
from common.serialization import serialize_for_wire
from config.settings import CRAWL_MAX_PAGES, TIKI_CATEGORIES
from crawler.acquisition import acquire_listing_page
from crawler.base import SiteCrawler
from crawler.contracts import (
    AcquisitionStatus,
    ListingPageRequest,
    Phase2AcquisitionReport,
    decode_listing_page_task_target,
)
from crawler.sites.tiki import TikiCrawler


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


SITE_CRAWLERS = {
    "tiki": TikiCrawler,
}
SITE_CATEGORIES = {
    "tiki": TIKI_CATEGORIES,
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _adapter_for(
    site: str,
    *,
    categories: list[str],
    max_pages: int,
    clock: Callable[[], datetime] | None = None,
):
    try:
        adapter_class = SITE_CRAWLERS[site]
    except KeyError as exc:
        raise ValueError(f"unknown crawler site: {site}") from exc
    kwargs = {"categories": categories, "max_pages": max_pages}
    if clock is not None and isinstance(adapter_class, type) and issubclass(adapter_class, SiteCrawler):
        kwargs["clock"] = clock
    return adapter_class(**kwargs)


def execute_listing_page(
    *,
    site: str,
    task_target: str,
    crawl_run_id: str,
    writer: Callable[[str, str, bytes], str] = put_bytes,
    clock: Callable[[], datetime],
) -> Phase2AcquisitionReport:
    """Execute exactly one encoded target/page for the Phase 3 worker."""
    target, page = decode_listing_page_task_target(task_target)
    adapter = _adapter_for(site, categories=[target], max_pages=1, clock=clock)
    request = ListingPageRequest(
        marketplace_code=adapter.site_name,
        marketplace_id=adapter.marketplace_id,
        target=target,
        page=page,
    )
    return acquire_listing_page(
        adapter,
        request,
        crawl_run_id=crawl_run_id,
        writer=writer,
        clock=clock,
    )


def listing_page_executor(
    *,
    site: str,
    writer: Callable[[str, str, bytes], str] = put_bytes,
    clock: Callable[[], datetime],
):
    """Bind Phase 2 acquisition into the executor the Phase 3 worker calls.

    This is the whole adapter: the worker holds no site name, no writer and
    no parsing, and Phase 2 keeps its raw-first ordering untouched.
    """

    def execute(*, task, crawl_run_id: str) -> Phase2AcquisitionReport:
        return execute_listing_page(
            site=site,
            task_target=task.target,
            crawl_run_id=crawl_run_id,
            writer=writer,
            clock=clock,
        )

    return execute


def _write_events(reports: list[Phase2AcquisitionReport], output: TextIO | None) -> None:
    if output is None:
        return
    for report in reports:
        for event in report.observations:
            output.write(
                json.dumps(
                    serialize_for_wire(event),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            )
    output.flush()


def run_site(
    site: str,
    *,
    categories: list[str] | None = None,
    max_pages: int = CRAWL_MAX_PAGES,
    crawl_run_id: str | None = None,
    writer: Callable[[str, str, bytes], str] = put_bytes,
    clock: Callable[[], datetime] = _utc_now,
    output: TextIO | None = None,
) -> dict[str, int]:
    """Run bounded raw-first collection for one registered site."""
    if max_pages < 1:
        raise ValueError("max_pages must be at least one")
    try:
        configured_categories = SITE_CATEGORIES[site]
    except KeyError as exc:
        raise ValueError(f"unknown crawler site: {site}") from exc
    selected_categories = list(categories or configured_categories)
    if not selected_categories:
        raise ValueError(f"no categories configured for site: {site}")
    run_id = crawl_run_id or str(uuid.uuid4())
    adapter = _adapter_for(
        site,
        categories=selected_categories,
        max_pages=max_pages,
        clock=clock,
    )
    reports: list[Phase2AcquisitionReport] = []
    counts = {
        "pages": 0,
        "succeeded_pages": 0,
        "partial_pages": 0,
        "failed_pages": 0,
        "observations": 0,
        "rejected": 0,
        "duplicates": 0,
        "raw_bytes": 0,
    }

    for category in selected_categories:
        seen_ids: set[str] = set()
        for page in range(1, max_pages + 1):
            request = ListingPageRequest(
                marketplace_code=adapter.site_name,
                marketplace_id=adapter.marketplace_id,
                target=category,
                page=page,
            )
            report = acquire_listing_page(
                adapter,
                request,
                crawl_run_id=run_id,
                writer=writer,
                clock=clock,
            )
            reports.append(report)
            counts["pages"] += 1
            counts["observations"] += report.canonical_observation_count
            counts["rejected"] += report.rejected_count
            counts["duplicates"] += report.duplicate_count
            counts["raw_bytes"] += report.raw_bytes
            if report.status is AcquisitionStatus.SUCCEEDED:
                counts["succeeded_pages"] += 1
            elif report.status is AcquisitionStatus.PARTIAL:
                counts["partial_pages"] += 1
            else:
                counts["failed_pages"] += 1

            if not report.is_task_success:
                logger.warning(
                    "%s target=%s page=%d failed at %s: %s",
                    site,
                    category,
                    page,
                    report.failure.stage.value if report.failure else "UNKNOWN",
                    report.failure.error_message if report.failure else "unknown failure",
                )
                break

            current_ids = {
                event.payload.offer.platform_listing_id for event in report.observations
            }
            if report.page_exhausted:
                break
            if current_ids and current_ids <= seen_ids:
                logger.warning(
                    "%s target=%s page=%d repeated previous listing IDs; stopping",
                    site,
                    category,
                    page,
                )
                break
            seen_ids.update(current_ids)

    _write_events(reports, output)
    logger.info(
        "%s: pages=%d succeeded=%d partial=%d failed=%d observations=%d rejected=%d raw_bytes=%d",
        site,
        counts["pages"],
        counts["succeeded_pages"],
        counts["partial_pages"],
        counts["failed_pages"],
        counts["observations"],
        counts["rejected"],
        counts["raw_bytes"],
    )
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Raw-first marketplace crawler")
    parser.add_argument(
        "--site",
        action="append",
        dest="sites",
        choices=sorted(SITE_CRAWLERS),
        help="Site to crawl (repeatable). Default: all registered sites.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Deprecated compatibility flag; raw Bronze is still written.",
    )
    parser.add_argument(
        "--out-jsonl",
        metavar="PATH",
        help="Write accepted canonical observation envelopes after raw persistence.",
    )
    parser.add_argument(
        "--category",
        action="append",
        dest="categories",
        help="Override configured categories (repeatable).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=CRAWL_MAX_PAGES,
        metavar="N",
        help=f"Maximum pages per category (default: {CRAWL_MAX_PAGES}).",
    )
    args = parser.parse_args()
    if args.dry_run:
        logger.warning("--dry-run no longer bypasses Bronze; raw responses will still be persisted")

    output_file = None
    output = None
    if args.out_jsonl:
        output_path = Path(args.out_jsonl)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_file = output_path.open("w", encoding="utf-8", newline="")
        output = output_file

    try:
        for site in args.sites or sorted(SITE_CRAWLERS):
            try:
                run_site(
                    site,
                    categories=args.categories,
                    max_pages=args.max_pages,
                    output=output,
                )
            except Exception:
                logger.exception("%s: crawl run failed; continuing with remaining sites", site)
    finally:
        if output_file is not None:
            output_file.close()
            logger.info("JSONL written: %s", args.out_jsonl)


if __name__ == "__main__":
    main()
