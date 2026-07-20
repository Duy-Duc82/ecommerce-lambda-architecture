"""Batch ML job: revenue forecasting + anomaly detection.

Runs after the Spark warehouse job has published `cache.daily_revenue`. Reads
that daily series, trains Darts N-BEATS + LSTM to forecast the next 7 days, and
runs the PyOD AutoEncoder anomaly detector, writing both back into the
PostgreSQL BI cache (`cache.predictions`, `cache.anomalies`) for Superset.

Run:
    python -m batch_layer.ml_job
    python -m batch_layer.ml_job --horizon 14
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_layer.analytics.anomaly_detector import AnomalyDetectorAutoEncoder
from batch_layer.analytics.trend_predictor import TrendPredictor
from batch_layer.postgres_cache import PostgresCacheSync

MIN_DAYS = 5


def run_ml(horizon: int = 7) -> dict[str, int]:
    cache = PostgresCacheSync()
    daily = cache.read_daily_revenue()
    if len(daily) < MIN_DAYS:
        print(f"[ml_job] only {len(daily)} days in cache.daily_revenue "
              f"(need >= {MIN_DAYS}); skipping ML.")
        return {"days": len(daily), "predictions": 0, "anomalies": 0}

    # --- Trend forecasting (Darts N-BEATS + LSTM, Strategy pattern) ---
    predictor = TrendPredictor()
    metrics = predictor.backtest(daily, holdout=min(7, max(2, len(daily) // 5)))
    if not metrics.empty:
        print("[ml_job] backtest metrics:\n", metrics.to_string(index=False))
    predictor.fit(daily)
    predictions = predictor.forecast(horizon=horizon)
    cache.sync_predictions(predictions)

    # --- Anomaly detection (PyOD AutoEncoder) ---
    detector = AnomalyDetectorAutoEncoder()
    scored = detector.fit(daily).detect(daily)
    anomalies = scored[scored["anomaly_label"] == 1].rename(columns={"date": "event_date"})
    cache.sync_anomalies(anomalies)

    print(f"[ml_job] backend={detector.backend} | days={len(daily)} | "
          f"predictions={len(predictions)} | anomalies={len(anomalies)}")
    return {"days": len(daily), "predictions": len(predictions), "anomalies": len(anomalies)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch ML: revenue forecast + anomaly detection")
    parser.add_argument("--horizon", type=int, default=7, help="Forecast horizon in days")
    args = parser.parse_args()
    run_ml(horizon=args.horizon)


if __name__ == "__main__":
    main()
