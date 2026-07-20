"""Test ML job standalone — run without writing to cache tables.

This script executes the full ML pipeline but outputs results to stdout + file
instead of writing to PostgreSQL cache. Useful for debugging parameters and
inspecting model outputs without affecting the BI dashboard.

Run:
    python -m batch_layer.test_ml_standalone
    python -m batch_layer.test_ml_standalone --horizon 14 --verbose
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_layer.analytics.anomaly_detector import AnomalyDetectorAutoEncoder
from batch_layer.analytics.trend_predictor import TrendPredictor
from batch_layer.postgres_cache import PostgresCacheSync


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Test ML job standalone (no database writes)"
    )
    parser.add_argument("--horizon", type=int, default=7, help="Forecast horizon (days)")
    parser.add_argument("--verbose", action="store_true", help="Detailed output")
    parser.add_argument(
        "--output",
        type=str,
        default="data/logs/ml_test_output.json",
        help="Output JSON file path"
    )
    args = parser.parse_args()

    print("\n" + "="*70)
    print("  ML JOB STANDALONE TEST (NO BI IMPACT)")
    print("="*70)
    print(f"Horizon: {args.horizon} days")
    print(f"Output file: {args.output}")
    print("="*70 + "\n")

    # --- Read data ---
    print("[1/5] Reading daily_revenue from PostgreSQL cache...")
    cache = PostgresCacheSync()
    daily = cache.read_daily_revenue()
    print(f"  ✓ Loaded {len(daily)} days")
    if len(daily) > 0:
        print(f"    Date range: {daily['date'].min()} → {daily['date'].max()}")
        print(f"    Revenue: min={daily['revenue'].min():.2f}, max={daily['revenue'].max():.2f}, mean={daily['revenue'].mean():.2f}")

    # Check minimum data
    MIN_DAYS = 5
    if len(daily) < MIN_DAYS:
        print(f"\n[ERROR] Only {len(daily)} days available (need >= {MIN_DAYS}). Skipping ML.")
        return

    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": {
            "horizon": args.horizon,
            "min_days": MIN_DAYS,
            "data_points": len(daily),
        },
        "backtest": {},
        "forecast": {},
        "anomalies": {},
    }

    # --- Backtest ---
    print("\n[2/5] Running backtest (evaluate on holdout)...")
    predictor = TrendPredictor()
    holdout = min(7, max(2, len(daily) // 5))
    metrics = predictor.backtest(daily, holdout=holdout)
    print(f"  Holdout size: {holdout} days")
    if not metrics.empty:
        print("  Backtest metrics:")
        print(metrics.to_string(index=False))
        results["backtest"] = {
            "holdout_days": holdout,
            "metrics": metrics.to_dict("records"),
        }
    else:
        print("  (Not enough data for backtest)")

    # --- Real train & forecast ---
    print(f"\n[3/5] Training models on ALL {len(daily)} days...")
    predictor.fit(daily)
    print(f"  Models: {predictor.model_names}")
    if args.verbose:
        for model in predictor.forecasters:
            backend = model._model.__class__.__name__ if model._model else "none"
            print(f"    - {model.name}: {backend}")

    print(f"\n[4/5] Forecasting {args.horizon} days ahead...")
    predictions = predictor.forecast(horizon=args.horizon)
    print(f"  Generated {len(predictions)} prediction rows")
    print("\n  Predictions:")
    print(predictions.to_string(index=False))
    results["forecast"] = {
        "horizon_days": args.horizon,
        "predictions": predictions.to_dict("records"),
    }

    # --- Anomaly detection ---
    print(f"\n[5/5] Detecting anomalies (all-data in-sample)...")
    detector = AnomalyDetectorAutoEncoder()
    scored = detector.fit(daily).detect(daily)
    anomalies = scored[scored["anomaly_label"] == 1].rename(columns={"date": "event_date"})
    print(f"  Backend: {detector.backend}")
    print(f"  Contamination: {detector.contamination * 100:.1f}%")
    print(f"  Flagged days: {len(anomalies)} / {len(daily)}")
    if len(anomalies) > 0:
        print("\n  Top anomalies (by score):")
        top_anom = anomalies.nlargest(5, "anomaly_score")[
            ["event_date", "revenue", "anomaly_score"]
        ]
        print(top_anom.to_string(index=False))
    results["anomalies"] = {
        "backend": detector.backend,
        "contamination": detector.contamination,
        "flagged_days": len(anomalies),
        "anomalies": anomalies[["event_date", "anomaly_label", "anomaly_score", "revenue"]].to_dict("records")[:10],  # Top 10
    }

    # --- Summary ---
    print("\n" + "="*70)
    print("  SUMMARY")
    print("="*70)
    print(f"Data points: {len(daily)}")
    print(f"Backtest holdout: {holdout} days")
    print(f"Forecast horizon: {args.horizon} days")
    print(f"Anomalies detected: {len(anomalies)}")
    print(f"Anomaly rate: {len(anomalies) / len(daily) * 100:.2f}%")
    print("="*70)

    # --- Save to file ---
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n✓ Full results saved: {output_path}")

    if args.verbose:
        print("\n[DEBUG] Results JSON:")
        print(json.dumps(results, ensure_ascii=False, indent=2))

    print("\n⚠️  NOTE: Results NOT written to cache tables (Superset unaffected)\n")


if __name__ == "__main__":
    main()
