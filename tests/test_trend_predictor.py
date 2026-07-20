import pandas as pd

from batch_layer.analytics import TrendPredictor


def test_trend_predictor_forecasts_nbeats_and_lstm():
    df = pd.DataFrame({"date": pd.date_range("2019-10-01", periods=20), "revenue": range(10, 30)})
    predictor = TrendPredictor(n_epochs=1).fit(df)
    preds = predictor.forecast(horizon=7)

    assert set(preds.columns) == {"forecast_date", "model", "predicted_revenue"}
    assert set(preds["model"].unique()) == set(predictor.model_names)
    assert predictor.model_names == ["nbeats", "lstm"]
    assert len(preds) == 7 * len(predictor.model_names)
    assert (preds["predicted_revenue"] >= 0).all()


def test_backtest_returns_metrics_per_model():
    df = pd.DataFrame({"date": pd.date_range("2019-10-01", periods=20), "revenue": range(10, 30)})
    predictor = TrendPredictor(n_epochs=1)
    metrics = predictor.backtest(df, holdout=5)
    if not metrics.empty:
        assert set(metrics["model"]) == set(predictor.model_names)
        assert {"mae", "rmse", "mape"}.issubset(metrics.columns)


def test_custom_forecaster_set_is_pluggable():
    from batch_layer.analytics import LstmForecaster

    df = pd.DataFrame({"date": pd.date_range("2019-10-01", periods=15), "revenue": range(15)})
    predictor = TrendPredictor(forecasters=[LstmForecaster], n_epochs=1).fit(df)
    assert predictor.model_names == ["lstm"]
    assert set(predictor.forecast(3)["model"]) == {"lstm"}
