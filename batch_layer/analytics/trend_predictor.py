"""Revenue trend forecasting orchestrator.

`TrendPredictor` runs a set of `Forecaster` strategies (default: Darts N-BEATS
and LSTM) over the daily revenue series and returns their forecasts in a tidy
long format. It is open for extension: pass any list of `Forecaster` subclasses
without changing this class or its callers.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .base import Forecaster, prepare_history
from .forecasters import DEFAULT_FORECASTERS


class TrendPredictor:
    def __init__(self, forecasters: list[type[Forecaster]] | None = None, n_epochs: int = 50) -> None:
        specs = forecasters or DEFAULT_FORECASTERS
        self.forecasters: list[Forecaster] = [spec(n_epochs=n_epochs) for spec in specs]
        self.n_epochs = n_epochs
        self._history: pd.DataFrame | None = None
        # Per-fold detail from the most recent backtest() call: one row per
        # (fold, model) with the fold's test-window dates alongside its
        # metrics -- `backtest()`'s return value is just the fold-averaged
        # summary, this is for callers that want the "time x metric" detail.
        self.last_backtest_detail: pd.DataFrame | None = None

    @property
    def model_names(self) -> list[str]:
        return [f.name for f in self.forecasters]

    def fit(self, history: pd.DataFrame) -> "TrendPredictor":
        self._history = prepare_history(history)
        for forecaster in self.forecasters:
            forecaster.fit(self._history)
        return self

    def forecast(self, horizon: int = 7) -> pd.DataFrame:
        """Return one row per (forecast_date, model) with predicted_revenue."""
        if self._history is None:
            raise ValueError("fit() must be called before forecast()")
        dates = pd.date_range(self._history["date"].max() + pd.Timedelta(days=1), periods=horizon)
        rows = []
        for forecaster in self.forecasters:
            preds = forecaster.predict(horizon)
            for forecast_date, value in zip(dates, preds):
                rows.append({
                    "forecast_date": forecast_date.date(),
                    "model": forecaster.name,
                    "predicted_revenue": float(value),
                })
        return pd.DataFrame(rows)

    def backtest(self, history: pd.DataFrame, holdout: int = 7, n_splits: int = 3) -> pd.DataFrame:
        """Rolling-origin (walk-forward) backtest.

        Instead of a single train/test cut, run `n_splits` folds: fold k
        trains on an expanding window and scores on the next `holdout` days,
        moving the origin forward each time. Metrics are averaged across all
        folds that actually ran, per model. On a short history (this project's
        30-ish day window) this squeezes several independent evaluations out
        of the same data instead of trusting one lucky/unlucky split.

        Falls back to a single split (n_splits=1) if there isn't enough
        history to fit more folds.
        """
        prepared = prepare_history(history)
        min_train = max(5, holdout)
        max_splits = max(0, (len(prepared) - min_train) // holdout) if holdout > 0 else 0
        n_splits = max(1, min(n_splits, max_splits))
        if max_splits < 1:
            self.last_backtest_detail = pd.DataFrame(
                columns=["fold", "test_start", "test_end", "model", "mae", "mse", "rmse", "mape"]
            )
            return pd.DataFrame(columns=["model", "mae", "mse", "rmse", "mape", "folds"])

        total = len(prepared)
        fold_rows = []
        for fold in range(n_splits):
            test_start = total - (n_splits - fold) * holdout
            test_end = test_start + holdout
            train_slice = prepared.iloc[:test_start]
            test_slice = prepared.iloc[test_start:test_end]
            actual = test_slice["revenue"].to_numpy()

            self.fit(train_slice)
            preds = self.forecast(holdout)
            for name in self.model_names:
                pred = preds[preds["model"] == name]["predicted_revenue"].to_numpy()
                mae = float(np.mean(np.abs(actual - pred)))
                mse = float(np.mean((actual - pred) ** 2))
                rmse = float(np.sqrt(mse))
                mape = float(np.mean(np.abs((actual - pred) / np.maximum(actual, 1))) * 100)
                fold_rows.append({
                    "fold": fold,
                    "test_start": test_slice["date"].iloc[0].date(),
                    "test_end": test_slice["date"].iloc[-1].date(),
                    "model": name,
                    "mae": mae, "mse": mse, "rmse": rmse, "mape": mape,
                })

        fold_df = pd.DataFrame(fold_rows)
        self.last_backtest_detail = fold_df[["fold", "test_start", "test_end", "model", "mae", "mse", "rmse", "mape"]]
        summary = fold_df.groupby("model", as_index=False)[["mae", "mse", "rmse", "mape"]].mean()
        folds_run = fold_df.groupby("model")["fold"].nunique().rename("folds").reset_index()
        return summary.merge(folds_run, on="model")
