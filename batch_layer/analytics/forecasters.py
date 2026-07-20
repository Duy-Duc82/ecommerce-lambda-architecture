"""Concrete forecasting strategies backed by Darts."""

from __future__ import annotations

from .base import Forecaster


class NBeatsForecaster(Forecaster):
    """Darts N-BEATS deep forecasting model."""

    name = "nbeats"
    covariate_kind = "past"  # NBEATSModel.supports_past_covariates == True

    def _train(self) -> object:
        from darts.models import NBEATSModel

        model = NBEATSModel(
            input_chunk_length=self._chunk_length(),
            output_chunk_length=1,
            n_epochs=self.n_epochs,
            # RevIN (Reversible Instance Normalization): re-normalizes each
            # input window internally and denormalizes the output, which
            # helps when the series' level/variance shifts over time (the
            # kind of small-sample distribution shift a 25-31 day window is
            # prone to). Complements, not replaces, the Scaler in base.py.
            use_reversible_instance_norm=True,
        )
        model.fit(self._train_series, past_covariates=self._covariates, verbose=False)
        return model


class LstmForecaster(Forecaster):
    """Darts LSTM (RNN) forecasting model."""

    name = "lstm"
    covariate_kind = "future"  # RNNModel.supports_future_covariates == True

    def _train(self) -> object:
        from darts.models import RNNModel

        # Note: RNNModel hardcodes use_reversible_instance_norm=False
        # internally (it warns and ignores the kwarg if passed) -- Darts
        # doesn't support RevIN for this architecture. The Scaler in
        # base.py.fit() is what fixes LSTM's previous collapse-to-~0 behavior.
        chunk = self._chunk_length()
        model = RNNModel(
            model="LSTM",
            input_chunk_length=chunk,
            training_length=max(chunk + 1, min(20, len(self._history) - 1)),
            n_epochs=self.n_epochs,
        )
        model.fit(self._train_series, future_covariates=self._covariates, verbose=False)
        return model


# Registry of the default strategy set, keyed by model name.
DEFAULT_FORECASTERS = (NBeatsForecaster, LstmForecaster)
