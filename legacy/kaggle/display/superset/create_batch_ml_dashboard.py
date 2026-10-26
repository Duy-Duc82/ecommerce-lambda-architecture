"""Create/refresh Superset BI for batch ML outputs.

The dashboard uses cache tables populated from the MinIO gold layer:
cache.predictions and cache.anomalies.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import requests


SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"
DASHBOARD_TITLE = "Batch ML - Forecast Comparison & Anomalies"
DASHBOARD_SLUG = "batch-ml-forecast-anomalies"


@dataclass
class ChartSpec:
    name: str
    schema: str
    table: str
    viz_type: str
    params: dict


CHART_SPECS = [
    ChartSpec(
        name="Forecast Revenue by Model",
        schema="cache",
        table="predictions",
        viz_type="line",
        params={
            "granularity_sqla": "forecast_date",
            "time_grain_sqla": "P1D",
            "metrics": [{
                "expressionType": "SIMPLE",
                "column": {"column_name": "predicted_revenue"},
                "aggregate": "SUM",
                "label": "SUM(predicted_revenue)",
            }],
            "groupby": ["model"],
            "adhoc_filters": [],
            "row_limit": 1000,
            "x_axis": "forecast_date",
        },
    ),
    ChartSpec(
        name="Forecast Model Comparison Table",
        schema="cache",
        table="predictions",
        viz_type="table",
        params={
            "query_mode": "raw",
            "all_columns": ["forecast_date", "model", "predicted_revenue"],
            "orderby": [["forecast_date", False], ["model", True]],
            "row_limit": 500,
        },
    ),
    ChartSpec(
        name="Detected Revenue Anomalies",
        schema="cache",
        table="anomalies",
        viz_type="table",
        params={
            "query_mode": "raw",
            "all_columns": ["event_date", "revenue", "purchase_events", "avg_purchase_value", "anomaly_label", "anomaly_score"],
            "orderby": [["anomaly_score", False]],
            "row_limit": 200,
        },
    ),
    ChartSpec(
        name="Anomaly Score Timeline",
        schema="cache",
        table="anomalies",
        viz_type="line",
        params={
            "granularity_sqla": "event_date",
            "time_grain_sqla": "P1D",
            "metrics": [{
                "expressionType": "SIMPLE",
                "column": {"column_name": "anomaly_score"},
                "aggregate": "MAX",
                "label": "MAX(anomaly_score)",
            }],
            "adhoc_filters": [],
            "row_limit": 1000,
            "x_axis": "event_date",
        },
    ),
]


class SupersetClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.access_token = ""
        self.csrf_token = ""

    def login(self) -> None:
        login = self.session.post(
            f"{SUPERSET_URL}/api/v1/security/login",
            json={"username": USERNAME, "password": PASSWORD, "provider": "db", "refresh": True},
            timeout=30,
        )
        login.raise_for_status()
        self.access_token = login.json()["access_token"]
        csrf = self.session.get(
            f"{SUPERSET_URL}/api/v1/security/csrf_token/",
            headers=self.headers(),
            timeout=30,
        )
        csrf.raise_for_status()
        self.csrf_token = csrf.json()["result"]

    def headers(self, write: bool = False) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        if write:
            headers.update({"X-CSRFToken": self.csrf_token, "Referer": SUPERSET_URL, "Content-Type": "application/json"})
        return headers

    def dataset_id(self, schema: str, table: str) -> int:
        resp = self.session.get(
            f"{SUPERSET_URL}/api/v1/dataset/?q=(page_size:500)",
            headers=self.headers(),
            timeout=30,
        )
        resp.raise_for_status()
        for dataset in resp.json().get("result", []):
            if dataset.get("schema") == schema and dataset.get("table_name") == table:
                return int(dataset["id"])
        raise RuntimeError(f"Superset dataset not found: {schema}.{table}. Import display/superset/datasources.yaml first.")

    def create_chart(self, spec: ChartSpec) -> int:
        dataset_id = self.dataset_id(spec.schema, spec.table)
        params = dict(spec.params)
        params["datasource"] = f"{dataset_id}__table"
        payload = {
            "slice_name": spec.name,
            "viz_type": spec.viz_type,
            "datasource_type": "table",
            "datasource_id": dataset_id,
            "params": json.dumps(params),
        }
        resp = self.session.post(
            f"{SUPERSET_URL}/api/v1/chart/",
            headers=self.headers(write=True),
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        return int(resp.json()["id"])

    def delete_existing_charts(self) -> None:
        resp = self.session.get(f"{SUPERSET_URL}/api/v1/chart/?q=(page_size:500)", headers=self.headers(), timeout=30)
        resp.raise_for_status()
        target_names = {spec.name for spec in CHART_SPECS}
        for chart in resp.json().get("result", []):
            if chart.get("slice_name") in target_names:
                self.session.delete(f"{SUPERSET_URL}/api/v1/chart/{chart['id']}", headers=self.headers(write=True), timeout=30)

    def dashboard_id(self) -> int:
        resp = self.session.get(f"{SUPERSET_URL}/api/v1/dashboard/?q=(page_size:200)", headers=self.headers(), timeout=30)
        resp.raise_for_status()
        for dashboard in resp.json().get("result", []):
            if dashboard.get("slug") == DASHBOARD_SLUG or dashboard.get("dashboard_title") == DASHBOARD_TITLE:
                return int(dashboard["id"])
        create = self.session.post(
            f"{SUPERSET_URL}/api/v1/dashboard/",
            headers=self.headers(write=True),
            json={"dashboard_title": DASHBOARD_TITLE, "slug": DASHBOARD_SLUG, "published": True},
            timeout=30,
        )
        create.raise_for_status()
        return int(create.json()["id"])

    def link_charts_to_dashboard(self, dashboard_id: int, chart_ids: list[int]) -> None:
        """Set the dashboard<->chart association Superset checks at render time.

        position_json alone places charts in the layout grid, but the
        frontend also needs each chart's own `dashboards` relation to include
        this dashboard — otherwise it renders "There is no chart definition
        associated with this component" even though the layout looks correct.
        """
        for chart_id in chart_ids:
            resp = self.session.put(
                f"{SUPERSET_URL}/api/v1/chart/{chart_id}",
                headers=self.headers(write=True),
                json={"dashboards": [dashboard_id]},
                timeout=30,
            )
            resp.raise_for_status()

    def update_dashboard(self, dashboard_id: int, chart_ids: list[int]) -> None:
        position = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"], "meta": {}},
            "GRID_ID": {"id": "GRID_ID", "type": "GRID", "children": ["ROW-1", "ROW-2"], "parents": ["ROOT_ID"], "meta": {}},
            "ROW-1": {"id": "ROW-1", "type": "ROW", "children": [f"CHART-{chart_ids[0]}", f"CHART-{chart_ids[1]}"], "parents": ["GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}},
            "ROW-2": {"id": "ROW-2", "type": "ROW", "children": [f"CHART-{chart_ids[2]}", f"CHART-{chart_ids[3]}"], "parents": ["GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}},
        }
        for idx, cid in enumerate(chart_ids):
            row = "ROW-1" if idx < 2 else "ROW-2"
            position[f"CHART-{cid}"] = {"id": f"CHART-{cid}", "type": "CHART", "children": [], "parents": [row], "meta": {"chartId": cid, "height": 50, "width": 6}}
        resp = self.session.put(
            f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}",
            headers=self.headers(write=True),
            json={"slug": DASHBOARD_SLUG, "published": True, "position_json": json.dumps(position)},
            timeout=30,
        )
        resp.raise_for_status()


def main() -> None:
    client = SupersetClient()
    client.login()
    client.delete_existing_charts()
    chart_ids = [client.create_chart(spec) for spec in CHART_SPECS]
    dashboard_id = client.dashboard_id()
    client.link_charts_to_dashboard(dashboard_id, chart_ids)
    client.update_dashboard(dashboard_id, chart_ids)
    print(f"[OK] Open: {SUPERSET_URL}/superset/dashboard/{DASHBOARD_SLUG}/")


if __name__ == "__main__":
    main()
