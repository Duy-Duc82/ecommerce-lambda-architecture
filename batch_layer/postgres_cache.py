"""PostgreSQL BI-cache access for the ML job.

The Spark warehouse job publishes the descriptive marts. This module handles
the ML side: it reads the `cache.daily_revenue` feature table and writes the
`cache.predictions` / `cache.anomalies` outputs. Writes truncate-and-insert so
Superset always sees a complete, consistent set.
"""

from __future__ import annotations

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from config.settings import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)


class PostgresCacheSync:
    def __init__(self) -> None:
        self._conn_kwargs = dict(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB,
        )

    def _connect(self):
        return psycopg2.connect(**self._conn_kwargs)

    def read_daily_revenue(self) -> pd.DataFrame:
        """Load the daily revenue feature series for model training."""
        with self._connect() as conn:
            return pd.read_sql_query(
                "SELECT event_date AS date, revenue, purchase_events, avg_purchase_value "
                "FROM cache.daily_revenue ORDER BY event_date",
                conn,
            )

    def _replace(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"TRUNCATE {table}")
            if rows:
                execute_values(
                    cur,
                    f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s",
                    rows,
                )

    def sync_predictions(self, df: pd.DataFrame) -> None:
        columns = ["forecast_date", "model", "predicted_revenue"]
        rows = [tuple(r) for r in df[columns].itertuples(index=False, name=None)]
        self._replace("cache.predictions", columns, rows)

    def sync_anomalies(self, df: pd.DataFrame) -> None:
        columns = ["event_date", "anomaly_label", "anomaly_score", "revenue",
                   "purchase_events", "avg_purchase_value"]
        rows = [tuple(r) for r in df[columns].itertuples(index=False, name=None)]
        self._replace("cache.anomalies", columns, rows)
