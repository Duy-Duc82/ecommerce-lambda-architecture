import pandas as pd

from batch_layer.analytics.anomaly_detector import AnomalyDetectorAutoEncoder


def test_anomaly_detector_adds_labels_and_scores():
    df = pd.DataFrame({
        "revenue": [10, 11, 12, 1000, 13, 14, 15, 16, 17, 18],
        "purchase_events": [1, 1, 1, 10, 1, 1, 1, 1, 1, 1],
        "avg_purchase_value": [10, 11, 12, 100, 13, 14, 15, 16, 17, 18],
    })
    detector = AnomalyDetectorAutoEncoder(contamination=0.1).fit(df)
    result = detector.detect(df)
    assert {"anomaly_label", "anomaly_score"}.issubset(result.columns)
    # Labels are normalized to 1 = anomaly / 0 = normal regardless of backend.
    assert set(result["anomaly_label"].unique()).issubset({0, 1})
    assert result["anomaly_label"].sum() >= 1
    assert detector.backend in ("pyod_autoencoder", "isolation_forest")
