"""Batch ML analytics: revenue forecasting and anomaly detection.

Public API (facade):
    TrendPredictor              — orchestrates forecasting strategies
    Forecaster                  — abstract strategy base
    NBeatsForecaster, LstmForecaster — concrete Darts strategies
    AnomalyDetectorAutoEncoder  — PyOD AutoEncoder anomaly detector
"""

from .anomaly_detector import AnomalyDetectorAutoEncoder
from .base import Forecaster
from .forecasters import DEFAULT_FORECASTERS, LstmForecaster, NBeatsForecaster
from .trend_predictor import TrendPredictor

__all__ = [
    "TrendPredictor",
    "Forecaster",
    "NBeatsForecaster",
    "LstmForecaster",
    "DEFAULT_FORECASTERS",
    "AnomalyDetectorAutoEncoder",
]
