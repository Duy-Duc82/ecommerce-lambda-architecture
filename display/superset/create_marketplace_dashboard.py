"""Provision the marketplace batch dashboard in Superset.

Reads the ``marketplace_gold`` cache, never the run-scoped object storage: the
cache is refreshed in a single transaction, so a dashboard cannot catch one mart
from this run beside another from the previous one.

Chart titles and subtitles are part of the contract, not decoration:

* the counter chart says "public counter", because the marketplace does not
  document what ``sold_count`` counts or when it resets;
* every median chart states its method and accuracy, since an approximate
  quantile presented as exact is not evidence;
* coverage charts show missing and stale counts rather than hiding the gaps,
  because a price series with unexplained holes cannot support a trend claim.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import POSTGRES_MARKETPLACE_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.getenv("SUPERSET_USER", "admin")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "admin")
DATABASE_NAME = os.getenv("SUPERSET_DATABASE", "PostgreSQL DW")

SCHEMA = POSTGRES_MARKETPLACE_SCHEMA

# (slug, title, sql, description)
MARKETPLACE_VIRTUAL_DATASETS = (
    (
        "marketplace_price_trend",
        "Offer price trend (daily close)",
        f"""
        SELECT observed_date, marketplace, offer_id, price_close, price_min, price_max,
               observation_count
        FROM {SCHEMA}.offer_price_history_daily
        """,
        "Daily representative price is the day's closing observation, not a mean: "
        "a mean over unevenly spaced crawl observations measures crawl scheduling.",
    ),
    (
        "marketplace_change_frequency",
        "Price change frequency and magnitude",
        f"""
        SELECT observed_date, marketplace, offer_id, price_changes, large_price_drops,
               availability_changes, availability_transitions_excluded,
               max_abs_price_delta, avg_abs_price_delta, max_drop_percent
        FROM {SCHEMA}.offer_change_daily
        """,
        "Recomputed in batch from consecutive observations. A difference against the "
        "speed layer's live count is an operational signal, not an error to reconcile away.",
    ),
    (
        "marketplace_category_distribution",
        "Category price distribution",
        f"""
        SELECT observed_date, marketplace, category_path, offer_count, observation_count,
               price_min, price_p25, price_median, price_p75, price_max,
               quantile_method, quantile_accuracy
        FROM {SCHEMA}.category_price_daily
        """,
        "Quantiles are approximate; every row carries the method and accuracy used. "
        "Min, max and counts are exact.",
    ),
    (
        "marketplace_coverage",
        "Source coverage and freshness",
        f"""
        SELECT observed_date, marketplace, offers_observed, observations, offers_missing,
               offers_stale, offers_without_seller, rows_rejected
        FROM {SCHEMA}.source_coverage_daily
        """,
        "Missing offers are counted, never imputed. rows_rejected comes from the "
        "quarantine objects, so rejects are counted rather than estimated.",
    ),
    (
        "marketplace_reliability",
        "Crawl reliability",
        f"""
        SELECT observed_date, marketplace, requests, succeeded, failed, success_rate,
               p50_latency_ms, p95_latency_ms, errors_json, skipped_reason
        FROM {SCHEMA}.crawl_reliability_daily
        """,
        "Sourced from the crawl audit trail. A populated skipped_reason means the audit "
        "source was unavailable and the row is empty by design, not zero by measurement.",
    ),
    (
        "marketplace_public_counter",
        "Public counter movement (not sales)",
        f"""
        SELECT observed_date, marketplace, offer_id, total_valid_delta, valid_rows,
               invalid_rows, no_previous_rows, negative_delta_rows, gap_too_long_rows,
               non_positive_elapsed_rows, max_velocity_per_hour
        FROM {SCHEMA}.counter_delta_daily
        """,
        "sold_count is a public counter with undocumented semantics. Only VALID rows "
        "enter total_valid_delta; negative deltas and long gaps are reported separately "
        "and never clamped to zero.",
    ),
    (
        "marketplace_current_state",
        "Current offer state with freshness",
        f"""
        SELECT offer_id, marketplace, platform_listing_id, product_title, brand,
               category_path, currency, observed_at, current_price, list_price,
               rating_value, review_count, sold_count, availability,
               freshness_status, age_minutes
        FROM {SCHEMA}.offer_current
        """,
        "Stale rows are kept and flagged rather than dropped or forward-filled.",
    ),
    (
        "marketplace_gold_runs",
        "Gold run history and assertions",
        f"""
        SELECT r.gold_run_id, r.as_of, r.window_days, r.rule_version, r.status,
               r.failure_stage, r.observations_read, r.quarantined_rows, r.skipped_marts,
               a.assertion_name, a.status AS assertion_status, a.observed_value, a.expectation
        FROM {SCHEMA}.gold_run r
        LEFT JOIN {SCHEMA}.gold_assertion a ON a.gold_run_id = r.gold_run_id
        """,
        "Every published number traces to a run, an as_of instant and a rule version.",
    ),
)


def _session() -> tuple[requests.Session, str]:
    session = requests.Session()
    response = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={
            "username": SUPERSET_USER,
            "password": SUPERSET_PASSWORD,
            "provider": "db",
            "refresh": True,
        },
        timeout=30,
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session, token


def _database_id(session: requests.Session) -> int:
    response = session.get(f"{SUPERSET_URL}/api/v1/database/", timeout=30)
    response.raise_for_status()
    for item in response.json()["result"]:
        if item["database_name"] == DATABASE_NAME:
            return int(item["id"])
    raise RuntimeError(f"Superset database {DATABASE_NAME!r} not found")


def create_virtual_datasets(session: requests.Session, database_id: int) -> list[int]:
    """Create or refresh one virtual dataset per marketplace view."""
    created: list[int] = []
    for slug, title, sql, description in MARKETPLACE_VIRTUAL_DATASETS:
        payload = {
            "database": database_id,
            "schema": SCHEMA,
            "table_name": slug,
            "sql": " ".join(sql.split()),
            "description": f"{title} — {description}",
        }
        response = session.post(f"{SUPERSET_URL}/api/v1/dataset/", json=payload, timeout=60)
        if response.status_code < 300:
            created.append(int(response.json()["id"]))
            logger.info("dataset ready: %s", title)
        elif response.status_code == 422:
            # Already present: Superset rejects a duplicate table_name, which
            # is the behaviour that makes re-running this script safe.
            logger.info("dataset already exists, left as is: %s", title)
        else:
            logger.warning(
                "dataset %s failed (%s): %s", title, response.status_code, response.text[:300]
            )
    return created


def main() -> None:
    session, _ = _session()
    database_id = _database_id(session)
    created = create_virtual_datasets(session, database_id)
    logger.info(
        "marketplace Superset draft provisioned: %d datasets created, %d declared",
        len(created),
        len(MARKETPLACE_VIRTUAL_DATASETS),
    )
    logger.info(
        "charts are left to be assembled in the UI over these datasets; every median "
        "panel must surface quantile_method and quantile_accuracy."
    )


if __name__ == "__main__":
    main()
