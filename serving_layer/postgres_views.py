"""Serving layer — convenience SQL views over the PostgreSQL BI cache.

The cache tables (populated by the Spark warehouse job and the ML job) are
already BI-ready. These lightweight views join/reshape them for common Superset
charts. They read only from the `cache` schema; nothing here is a source of
truth.

Run:
    python -m serving_layer.postgres_views create
    python -m serving_layer.postgres_views drop
"""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER

VIEWS = {
    # Daily revenue with its forecast(s) side by side for a "actual vs forecast" chart.
    "cache.v_revenue_forecast": """
        CREATE OR REPLACE VIEW cache.v_revenue_forecast AS
        SELECT d.event_date AS date, d.revenue AS actual_revenue, NULL::text AS model,
               NULL::double precision AS predicted_revenue
        FROM cache.daily_revenue d
        UNION ALL
        SELECT p.forecast_date AS date, NULL::double precision AS actual_revenue,
               p.model, p.predicted_revenue
        FROM cache.predictions p;
    """,
    # Anomalous days enriched with their revenue context.
    "cache.v_anomaly_days": """
        CREATE OR REPLACE VIEW cache.v_anomaly_days AS
        SELECT a.event_date, a.anomaly_score, a.revenue, a.purchase_events, a.avg_purchase_value
        FROM cache.anomalies a
        WHERE a.anomaly_label = 1
        ORDER BY a.event_date;
    """,
    # Top products by revenue across the whole window.
    "cache.v_top_products": """
        CREATE OR REPLACE VIEW cache.v_top_products AS
        SELECT product_id, category_code, brand,
               SUM(views) AS views, SUM(cart_adds) AS cart_adds,
               SUM(purchase_events) AS purchase_events, SUM(revenue) AS revenue
        FROM cache.product_daily
        GROUP BY product_id, category_code, brand
        ORDER BY revenue DESC;
    """,
}


def _connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD, dbname=POSTGRES_DB,
    )


def create_views() -> None:
    with _connect() as conn, conn.cursor() as cur:
        for name, sql in VIEWS.items():
            cur.execute(sql)
            print(f"  [OK] created {name}")


def drop_views() -> None:
    with _connect() as conn, conn.cursor() as cur:
        for name in VIEWS:
            cur.execute(f"DROP VIEW IF EXISTS {name} CASCADE;")
            print(f"  [OK] dropped {name}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Manage cache convenience views")
    parser.add_argument("action", choices=["create", "drop"])
    args = parser.parse_args()
    if args.action == "create":
        create_views()
    else:
        drop_views()


if __name__ == "__main__":
    main()
