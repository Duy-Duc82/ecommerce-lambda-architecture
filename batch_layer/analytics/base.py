"""Forecasting strategy contract (Strategy + Template Method patterns).

`Forecaster` is the abstract strategy. Concrete strategies (N-BEATS, LSTM, ...)
implement only `_train`; the base class owns the shared workflow — history
preparation, building a Darts series, and a naive fallback whenever the model
library is missing or a fit/predict call fails. New models can be added without
touching the orchestrator (`TrendPredictor`) or the callers.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd


def prepare_history(df: pd.DataFrame) -> pd.DataFrame:
    """Return a clean, day-sorted (date, revenue) frame."""
    data = df[["date", "revenue"]].copy()
    data["date"] = pd.to_datetime(data["date"])
    data["revenue"] = pd.to_numeric(data["revenue"], errors="coerce").fillna(0.0)
    return data.sort_values("date").reset_index(drop=True)


# How far past the end of history to build calendar covariates. Must cover
# any backtest holdout or forecast horizon actually used (both are small,
# single-digit-to-low-double-digit days here) with headroom to spare.
COVARIATE_BUFFER_DAYS = 60


class Forecaster(ABC):
    """Abstract forecasting strategy over a daily revenue series."""

    name: str = "base"
    # Which covariate argument this model's Darts class accepts -- set by
    # subclasses. NBEATSModel only supports past_covariates; RNNModel only
    # supports future_covariates (verified via `model.supports_*_covariates`).
    # None disables covariates for that strategy.
    covariate_kind: str | None = None

    def __init__(self, n_epochs: int = 50) -> None:
        self.n_epochs = n_epochs
        self._history: pd.DataFrame | None = None
        self._model: object | None = None
        self._scaler: object | None = None
        self._train_series: object | None = None
        self._covariates: object | None = None

    # --- Template method ---------------------------------------------------
    def fit(self, history: pd.DataFrame) -> "Forecaster":
        self._history = prepare_history(history)
        try:
            from darts.dataprocessing.transformers import Scaler

            # Revenue is raw-scale (tens of thousands). Feeding that directly
            # to a neural model (esp. RNNModel) makes training numerically
            # unstable on a handful of samples and it collapses toward ~0.
            # Fit-transform here, train on the scaled series, then
            # inverse_transform predictions back to revenue units in predict().
            self._scaler = Scaler()
            self._train_series = self._scaler.fit_transform(self._series())
            self._covariates = self._build_covariates() if self.covariate_kind else None
            self._model = self._train()
        except Exception:
            self._model = None  # -> naive fallback at predict time
            self._scaler = None
            self._covariates = None
        return self

    def predict(self, horizon: int) -> np.ndarray:
        if self._history is None:
            raise ValueError("fit() must be called before predict()")
        if self._model is None or self._scaler is None:
            return self._naive(horizon)
        try:
            scaled_pred = self._model.predict(horizon, **self._covariate_kwargs())
            values = np.asarray(self._scaler.inverse_transform(scaled_pred).values()).ravel()
            return np.clip(values, 0.0, None)
        except Exception:
            return self._naive(horizon)

    # --- Hooks / helpers ---------------------------------------------------
    @abstractmethod
    def _train(self) -> object:
        """Fit and return the underlying model from `self._train_series` (scaled)."""

    def _series(self):
        from darts import TimeSeries

        return TimeSeries.from_dataframe(self._history, "date", "revenue", freq="D")

    def _covariate_kwargs(self) -> dict:
        """kwarg to pass to both model.fit() and model.predict() -- centralized
        so the two call sites can't disagree on which covariate argument name
        this strategy uses."""
        if not self.covariate_kind or self._covariates is None:
            return {}
        key = "past_covariates" if self.covariate_kind == "past" else "future_covariates"
        return {key: self._covariates}

    def _build_covariates(self):
        """Day-of-week (cyclical) + weekend flag, the same 31-ish days of
        history already carry -- squeezes an extra signal out of existing
        data instead of requiring a longer history."""
        from darts import TimeSeries

        start = self._history["date"].min()
        dates = pd.date_range(start, periods=len(self._history) + COVARIATE_BUFFER_DAYS, freq="D")
        dow = dates.dayofweek.to_numpy()
        cov = pd.DataFrame({
            "date": dates,
            "dow_sin": np.sin(2 * np.pi * dow / 7),
            "dow_cos": np.cos(2 * np.pi * dow / 7),
            "is_weekend": (dow >= 5).astype(float),
        })
        return TimeSeries.from_dataframe(cov, "date", ["dow_sin", "dow_cos", "is_weekend"], freq="D")

    def _chunk_length(self) -> int:
        return max(1, min(14, len(self._history) - 1))

    def _naive(self, horizon: int) -> np.ndarray:
        window = self._history["revenue"].tail(min(7, len(self._history)))
        return np.repeat(float(window.mean()), horizon)
