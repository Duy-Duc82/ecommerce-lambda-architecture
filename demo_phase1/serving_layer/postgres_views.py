"""
Serving Layer — Postgres Views.

Tao va cap nhat cac materialized views trong Data Warehouse
phuc vu truy van nhanh cho dashboard va API.
"""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER


def get_connection():
    """Tao ket noi toi Postgres Data Warehouse."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )


# ============================================================
# MATERIALIZED VIEWS
# ============================================================

MATERIALIZED_VIEWS = {
    "mv_daily_sales": """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_sales AS
        SELECT
            event_date,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(total_amount) AS total_revenue,
            AVG(total_amount) AS avg_order_value,
            COUNT(DISTINCT user_id) AS unique_buyers
        FROM fact_events
        WHERE event_type = 'purchase'
        GROUP BY event_date
        ORDER BY event_date DESC;
    """,

    "mv_product_ranking": """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_ranking AS
        SELECT
            product_id,
            product_name,
            category,
            SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS views,
            SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS cart_adds,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
            AVG(CASE WHEN event_type = 'review' THEN rating END) AS avg_rating,
            COUNT(CASE WHEN event_type = 'review' THEN 1 END) AS num_reviews,
            CASE WHEN SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) > 0
                THEN SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)::FLOAT
                     / SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)
                ELSE 0
            END AS conversion_rate
        FROM fact_events
        WHERE product_id IS NOT NULL
        GROUP BY product_id, product_name, category
        ORDER BY purchases DESC;
    """,

    "mv_category_performance": """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_category_performance AS
        SELECT
            category,
            SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS total_views,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
            SUM(CASE WHEN event_type = 'purchase' THEN total_amount ELSE 0 END) AS total_revenue,
            COUNT(DISTINCT product_id) AS num_products,
            COUNT(DISTINCT user_id) AS unique_users
        FROM fact_events
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY total_revenue DESC;
    """,

    "mv_hourly_traffic": """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_traffic AS
        SELECT
            event_date,
            event_hour,
            event_type,
            COUNT(*) AS event_count,
            COUNT(DISTINCT user_id) AS unique_users
        FROM fact_events
        GROUP BY event_date, event_hour, event_type
        ORDER BY event_date DESC, event_hour;
    """,

    "mv_user_segments": """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_segments AS
        WITH rfm AS (
            SELECT
                user_id,
                MAX(event_date) AS last_purchase_date,
                COUNT(DISTINCT order_id) AS frequency,
                COALESCE(SUM(total_amount), 0) AS monetary
            FROM fact_events
            WHERE event_type = 'purchase'
            GROUP BY user_id
        )
        SELECT
            user_id,
            last_purchase_date,
            frequency,
            monetary,
            CASE
                WHEN frequency >= 5 AND monetary > 100000000 THEN 'VIP'
                WHEN frequency >= 3 THEN 'Loyal'
                WHEN frequency >= 1 THEN 'Active'
                ELSE 'New'
            END AS segment
        FROM rfm;
    """,
}


def create_materialized_views() -> None:
    """Tao tat ca materialized views."""
    conn = get_connection()
    cur = conn.cursor()

    for name, sql in MATERIALIZED_VIEWS.items():
        try:
            cur.execute(sql)
            conn.commit()
            print(f"  ✓ Created: {name}")
        except Exception as e:
            conn.rollback()
            print(f"  ✗ Failed {name}: {e}")

    cur.close()
    conn.close()


def refresh_materialized_views() -> None:
    """Lam moi (refresh) tat ca materialized views."""
    conn = get_connection()
    cur = conn.cursor()

    for name in MATERIALIZED_VIEWS:
        try:
            cur.execute(f"REFRESH MATERIALIZED VIEW {name};")
            conn.commit()
            print(f"  ✓ Refreshed: {name}")
        except Exception as e:
            conn.rollback()
            print(f"  ✗ Failed refresh {name}: {e}")

    cur.close()
    conn.close()


def drop_materialized_views() -> None:
    """Xoa tat ca materialized views."""
    conn = get_connection()
    cur = conn.cursor()

    for name in MATERIALIZED_VIEWS:
        try:
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {name} CASCADE;")
            conn.commit()
            print(f"  ✓ Dropped: {name}")
        except Exception as e:
            conn.rollback()
            print(f"  ✗ Failed drop {name}: {e}")

    cur.close()
    conn.close()


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Postgres Materialized Views Manager")
    parser.add_argument("action", choices=["create", "refresh", "drop"], help="Action to perform")
    args = parser.parse_args()

    print(f"{'=' * 50}")
    print(f"POSTGRES VIEWS - {args.action.upper()}")
    print(f"{'=' * 50}")

    if args.action == "create":
        create_materialized_views()
    elif args.action == "refresh":
        refresh_materialized_views()
    elif args.action == "drop":
        drop_materialized_views()
