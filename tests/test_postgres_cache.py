import pandas as pd

import batch_layer.postgres_cache as pc
from batch_layer.postgres_cache import PostgresCacheSync


class _FakeCursor:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.log.append(("execute", " ".join(sql.split())))


class _FakeConn:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return _FakeCursor(self.log)


def test_sync_predictions_truncates_then_inserts(monkeypatch):
    log = []
    monkeypatch.setattr(pc, "psycopg2", type("m", (), {"connect": lambda **kw: _FakeConn(log)}))
    captured = {}
    monkeypatch.setattr(pc, "execute_values",
                        lambda cur, sql, rows: captured.update(sql=" ".join(sql.split()), rows=list(rows)))

    df = pd.DataFrame([
        {"forecast_date": "2019-10-08", "model": "nbeats", "predicted_revenue": 100.0},
        {"forecast_date": "2019-10-08", "model": "lstm", "predicted_revenue": 110.0},
    ])
    PostgresCacheSync().sync_predictions(df)

    assert ("execute", "TRUNCATE cache.predictions") in log
    assert "INSERT INTO cache.predictions" in captured["sql"]
    assert len(captured["rows"]) == 2
