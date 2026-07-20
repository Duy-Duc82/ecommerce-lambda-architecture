"""Test ML job with sample data (no database required).

Run ML pipeline with synthetic daily_revenue data to inspect parameters,
backtest metrics, forecast, and anomaly detection without database dependency.

Run:
    python -m batch_layer.test_ml_offline
    python -m batch_layer.test_ml_offline --horizon 14 --verbose
    python -m batch_layer.test_ml_offline --days 30
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_layer.analytics.anomaly_detector import AnomalyDetectorAutoEncoder
from batch_layer.analytics.trend_predictor import TrendPredictor


def generate_sample_daily_revenue(n_days: int = 30) -> pd.DataFrame:
    """Generate synthetic daily revenue timeseries with trend, seasonality, and noise."""
    dates = pd.date_range(end=datetime.now(timezone.utc), periods=n_days, freq="D")

    # Base trend (linear increase)
    trend = np.linspace(10000, 15000, n_days)

    # Weekly seasonality (lower on Mon, higher on Fri)
    seasonality = 1500 * np.sin(2 * np.pi * np.arange(n_days) / 7)

    # Random noise
    noise = np.random.normal(0, 500, n_days)

    # One anomaly spike (day 20)
    anomaly_spike = np.zeros(n_days)
    anomaly_spike[20] = 5000

    revenue = trend + seasonality + noise + anomaly_spike
    revenue = np.maximum(revenue, 0)  # No negative revenue

    # Derived metrics
    purchase_events = np.maximum(100 + revenue / 100 + np.random.normal(0, 10, n_days), 10).astype(int)
    avg_purchase_value = revenue / np.maximum(purchase_events, 1)

    return pd.DataFrame({
        "date": dates,
        "revenue": revenue,
        "purchase_events": purchase_events,
        "avg_purchase_value": avg_purchase_value,
    })


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Test ML job offline (with sample data, no database)"
    )
    parser.add_argument("--days", type=int, default=30, help="Number of sample days to generate")
    parser.add_argument("--horizon", type=int, default=7, help="Forecast horizon (days)")
    parser.add_argument("--verbose", action="store_true", help="Detailed output")
    parser.add_argument(
        "--output",
        type=str,
        default="data/logs/ml_test_offline.json",
        help="Output JSON file path"
    )
    args = parser.parse_args()

    print("\n" + "="*80)
    print("  ML JOB OFFLINE TEST (Sample Data — No Database)")
    print("="*80)
    print(f"Sample days: {args.days}")
    print(f"Forecast horizon: {args.horizon} days")
    print(f"Output file: {args.output}")
    print("="*80 + "\n")

    # --- Generate sample data ---
    print(f"[1/5] Generating {args.days} days of sample daily_revenue...")
    daily = generate_sample_daily_revenue(n_days=args.days)
    print(f"  [OK] Generated {len(daily)} days")
    print(f"    Date range: {daily['date'].min().date()} -> {daily['date'].max().date()}")
    print(f"    Revenue: min={daily['revenue'].min():.2f}, max={daily['revenue'].max():.2f}, mean={daily['revenue'].mean():.2f}")
    print(f"    Purchase events: min={daily['purchase_events'].min()}, max={daily['purchase_events'].max()}, mean={daily['purchase_events'].mean():.1f}")

    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": {
            "source": "synthetic_sample_data",
            "horizon": args.horizon,
            "data_points": len(daily),
            "sample_days_generated": args.days,
        },
        "data_summary": {
            "date_range": f"{daily['date'].min().date()} to {daily['date'].max().date()}",
            "revenue_stats": {
                "min": float(daily['revenue'].min()),
                "max": float(daily['revenue'].max()),
                "mean": float(daily['revenue'].mean()),
                "std": float(daily['revenue'].std()),
            },
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
    print(f"  Holdout size: {holdout} days (test set)")
    print(f"  Train size: {len(daily) - holdout} days")

    if not metrics.empty:
        print("\n  [BACKTEST METRICS]")
        print("  " + "-" * 76)
        print(metrics.to_string(index=False))
        print("  " + "-" * 76)
        results["backtest"] = {
            "holdout_days": holdout,
            "train_days": len(daily) - holdout,
            "metrics": metrics.to_dict("records"),
        }

        if args.verbose:
            print("\n  [DEBUG] Detailed metrics:")
            for _, row in metrics.iterrows():
                print(f"    {row['model'].upper()}:")
                print(f"      MAE:  {row['mae']:.2f}")
                print(f"      RMSE: {row['rmse']:.2f}")
                print(f"      MAPE: {row['mape']:.2f}%")
    else:
        print("  [WARNING] Not enough data for backtest")

    # --- Real train & forecast ---
    print(f"\n[3/5] Training models on ALL {len(daily)} days (production training)...")
    predictor.fit(daily)
    print(f"  Models: {', '.join(predictor.model_names)}")

    if args.verbose:
        print("\n  [DEBUG] Model details:")
        for model in predictor.forecasters:
            backend = model._model.__class__.__name__ if model._model else "none"
            print(f"    {model.name}: {backend}")
            print(f"      n_epochs: {model.n_epochs}")
            print(f"      history size: {len(model._history) if model._history is not None else 0} days")

    print(f"\n[4/5] Forecasting {args.horizon} days ahead...")
    predictions = predictor.forecast(horizon=args.horizon)
    print(f"  Generated {len(predictions)} prediction rows ({len(set(predictions['model']))} models × {args.horizon} days)")

    print("\n  [FORECAST RESULTS]")
    print("  " + "-" * 76)
    print(predictions.to_string(index=False))
    print("  " + "-" * 76)

    results["forecast"] = {
        "horizon_days": args.horizon,
        "num_rows": len(predictions),
        "models": list(set(predictions['model'])),
        "predictions": predictions.to_dict("records"),
    }

    # --- Anomaly detection ---
    print(f"\n[5/5] Detecting anomalies...")
    detector = AnomalyDetectorAutoEncoder()
    scored = detector.fit(daily).detect(daily)
    anomalies = scored[scored["anomaly_label"] == 1]

    print(f"  Backend: {detector.backend}")
    print(f"  Contamination: {detector.contamination * 100:.1f}%")
    print(f"  Flagged days: {len(anomalies)} / {len(daily)} ({len(anomalies) / len(daily) * 100:.1f}%)")

    if len(anomalies) > 0:
        print("\n  [ANOMALIES DETECTED] (sorted by score):")
        print("  " + "-" * 76)
        top_anom = anomalies.nlargest(10, "anomaly_score")[
            ["date", "revenue", "purchase_events", "avg_purchase_value", "anomaly_score"]
        ]
        print(top_anom.to_string(index=False))
        print("  " + "-" * 76)
    else:
        print("  [OK] No anomalies detected")

    results["anomalies"] = {
        "backend": detector.backend,
        "contamination": detector.contamination,
        "flagged_days": len(anomalies),
        "flagged_rate_percent": len(anomalies) / len(daily) * 100,
        "anomalies": anomalies[[
            "date", "anomaly_label", "anomaly_score", "revenue", "purchase_events", "avg_purchase_value"
        ]].to_dict("records"),
    }

    # --- Summary ---
    print("\n" + "="*80)
    print("  SUMMARY")
    print("="*80)
    print(f"Data window:         {len(daily)} days ({daily['date'].min().date()} to {daily['date'].max().date()})")
    print(f"Backtest split:      {len(daily) - holdout} train / {holdout} test")
    print(f"Forecast horizon:    {args.horizon} days")
    print(f"Anomalies detected:  {len(anomalies)} / {len(daily)} ({len(anomalies) / len(daily) * 100:.1f}%)")
    print(f"Models trained:      {', '.join(predictor.model_names)}")
    print(f"Anomaly backend:     {detector.backend}")
    print("="*80)

    # --- Save to file ---
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n[OK] Full results saved: {output_path}")

    if args.verbose:
        print("\n[DEBUG] Complete JSON output:")
        print(json.dumps(results, ensure_ascii=False, indent=2, default=str))

    print("\n[SUCCESS] Test complete - NO database writes, Superset unaffected\n")


if __name__ == "__main__":
    main()
