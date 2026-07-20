"""Internal ML evaluation over the PostgreSQL BI cache.

Reads the ML output tables and prints a compact report (plus a JSON dump under
data/logs). This is an internal consistency check, not a production-grade
holdout evaluation — the dataset spans a limited number of days.

Run:
    python -m batch_layer.evaluate_ml
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    BASE_DIR,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)


def _connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD, dbname=POSTGRES_DB,
    )


def _df(conn, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn)


def evaluate(conn) -> dict:
    daily = _df(conn, "SELECT event_date, revenue FROM cache.daily_revenue ORDER BY event_date")
    preds = _df(conn, "SELECT forecast_date, model, predicted_revenue FROM cache.predictions")
    anomalies = _df(conn, "SELECT event_date, anomaly_score FROM cache.anomalies")

    report = {
        "data_window": {
            "days": int(len(daily)),
            "min_day": str(daily["event_date"].min()) if not daily.empty else None,
            "max_day": str(daily["event_date"].max()) if not daily.empty else None,
            "mean_revenue": float(daily["revenue"].mean()) if not daily.empty else None,
        },
        "forecast": {
            "models": sorted(preds["model"].unique().tolist()) if not preds.empty else [],
            "horizon_days": int(preds["forecast_date"].nunique()) if not preds.empty else 0,
            "mean_predicted_by_model": (
                preds.groupby("model")["predicted_revenue"].mean().round(2).to_dict()
                if not preds.empty else {}
            ),
        },
        "anomalies": {
            "flagged_days": int(len(anomalies)),
            "rate": float(len(anomalies) / len(daily)) if len(daily) else 0.0,
        },
    }
    if len(daily) < 14:
        report["warning"] = "low time coverage: not enough days for a robust temporal backtest"
    return report


def main() -> None:
    report = {"generated_at_utc": datetime.now(timezone.utc).isoformat()}
    with _connect() as conn:
        report["evaluation"] = evaluate(conn)

    logs_dir = BASE_DIR / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_path = logs_dir / f"ml_eval_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=" * 60)
    print("INTERNAL ML EVALUATION")
    print("=" * 60)
    print(json.dumps(report["evaluation"], ensure_ascii=False, indent=2))
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
