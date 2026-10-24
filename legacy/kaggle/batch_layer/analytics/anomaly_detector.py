"""AutoEncoder anomaly detection (PyOD) over the daily revenue series.

Flags days whose (revenue, purchase_events, avg_purchase_value) profile deviates
from the norm. Uses PyOD's AutoEncoder when available; falls back to
IsolationForest so the pipeline still produces labels in a light environment.
"""

from __future__ import annotations

import pandas as pd
from sklearn.preprocessing import StandardScaler

FEATURES = ["revenue", "purchase_events", "avg_purchase_value"]


class AnomalyDetectorAutoEncoder:
    def __init__(self, contamination: float = 0.05, random_state: int = 42) -> None:
        self.contamination = contamination
        self.random_state = random_state
        self.scaler = StandardScaler()
        self.model = None
        self.backend = "none"

    def fit(self, df: pd.DataFrame) -> "AnomalyDetectorAutoEncoder":
        x = self.scaler.fit_transform(df[FEATURES].fillna(0.0))
        try:
            from pyod.models.auto_encoder import AutoEncoder

            # PyOD's DataLoader drops the last partial batch, so a batch_size
            # larger than the sample count yields zero batches and crashes
            # training; cap it well under len(x) for short daily series.
            batch_size = max(2, min(32, len(x) // 2))
            self.model = AutoEncoder(
                contamination=self.contamination,
                batch_size=batch_size,
                random_state=self.random_state,
            )
            self.model.fit(x)
            self.backend = "pyod_autoencoder"
        except Exception:
            from sklearn.ensemble import IsolationForest

            self.model = IsolationForest(contamination=self.contamination, random_state=self.random_state)
            self.model.fit(x)
            self.backend = "isolation_forest"
        return self

    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.model is None:
            raise ValueError("fit must be called before detect")
        x = self.scaler.transform(df[FEATURES].fillna(0.0))
        result = df.copy()
        labels = self.model.predict(x)
        # Normalize to 1 = anomaly, 0 = normal.
        # PyOD: 1 = outlier, 0 = inlier. IsolationForest: -1 = outlier, 1 = inlier.
        if self.backend == "isolation_forest":
            result["anomaly_label"] = [1 if label == -1 else 0 for label in labels]
        else:
            result["anomaly_label"] = [int(label) for label in labels]
        result["anomaly_score"] = self.model.decision_function(x)
        return result
