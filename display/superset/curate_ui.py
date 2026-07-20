"""
Curate Superset UI for report/demo:
- Keep only one dashboard.
- Rename it to the official report title.
- Remove extra dashboards/charts to avoid UI noise.
"""

from __future__ import annotations

import json

from superset.app import create_app
from superset import db

TARGET_DASHBOARD_TITLE = "Serving Layer: Superset BI Dashboard"
TARGET_DASHBOARD_SLUG = "serving-layer-superset-bi-dashboard"
TARGET_CHART_SPECS = [
    ("1. Conversion Funnel", "funnel_daily"),
    ("2. Product Performance", "product_daily"),
    ("3. Category Performance", "category_daily"),
    ("4. Session Behavior", "session_daily"),
]

# Row-level detail tables here have a long tail: e.g. product_daily has one
# row per (date, product) and most products are view-only (cart_adds=0,
# revenue=0). Sorting by the alphabetically-first column (the old default)
# surfaces an arbitrary alphabetical slice, which is overwhelmingly those
# zero-activity rows -- the table then reads as "everything is 0". Sort by
# a meaningful activity metric instead so the top rows are the ones that
# actually converted/generated revenue.
PREFERRED_ORDER_COLUMN = {
    "funnel_daily": ("event_date", False),      # chronological, oldest first
    "product_daily": ("revenue", True),         # top revenue-generating products first
    "category_daily": ("revenue", True),        # top revenue-generating categories first
    "session_daily": ("purchase_count", True),  # converting sessions first
}


def _build_chart_payload(dataset_id: int, columns: list[str], table_name: str) -> tuple[dict, dict]:
    safe_columns = columns[:8] if columns else []
    preferred_col, order_desc = PREFERRED_ORDER_COLUMN.get(table_name, (None, True))
    if preferred_col and preferred_col in columns:
        order_col = preferred_col
        if preferred_col not in safe_columns:
            safe_columns = [preferred_col] + safe_columns[:7]
    else:
        order_col = safe_columns[0] if safe_columns else None

    # Superset's orderby tuple is [column, ascending] (True=ascending,
    # False=descending) -- the opposite sense of our order_desc flag.
    ascending = not order_desc

    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": safe_columns,
        "row_limit": 200,
        "adhoc_filters": [],
        "order_desc": order_desc,
        "show_cell_bars": False,
        "table_timestamp_format": "smart_date",
    }
    if order_col:
        params["orderby"] = [[order_col, ascending]]

    query_object = {
        "columns": safe_columns,
        "metrics": [],
        "filters": [],
        "is_timeseries": False,
        "row_limit": 200,
    }
    if order_col:
        query_object["orderby"] = [[order_col, ascending]]

    query_context = {
        "datasource": {"id": dataset_id, "type": "table"},
        "queries": [query_object],
        "result_format": "json",
        "result_type": "full",
    }
    return params, query_context


def main() -> None:
    app = create_app()
    with app.app_context():
        from flask_appbuilder.security.sqla.models import User
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        dashboards = db.session.query(Dashboard).order_by(Dashboard.id.asc()).all()
        existing_charts = db.session.query(Slice).order_by(Slice.id.asc()).all()

        admin_user = db.session.query(User).filter(User.username == "admin").first()

        # Pick first dashboard as primary. If none exists, create one.
        if dashboards:
            primary = dashboards[0]
        else:
            primary = Dashboard(dashboard_title=TARGET_DASHBOARD_TITLE, slug=TARGET_DASHBOARD_SLUG, published=True)
            db.session.add(primary)
            db.session.flush()

        primary.dashboard_title = TARGET_DASHBOARD_TITLE
        primary.slug = TARGET_DASHBOARD_SLUG
        primary.published = True

        created_or_kept = []
        for chart_title, table_name in TARGET_CHART_SPECS:
            ds = (
                db.session.query(SqlaTable)
                .filter(SqlaTable.table_name == table_name, SqlaTable.schema == "cache")
                .first()
            )
            if not ds:
                print(f"[Superset Curate] Dataset not found: {table_name} (skip chart '{chart_title}')")
                continue
            ds.fetch_metadata()
            db.session.flush()
            params, query_context = _build_chart_payload(ds.id, list(ds.column_names), table_name)

            chart = db.session.query(Slice).filter(Slice.slice_name == chart_title).first()
            if chart is None:
                chart = Slice(
                    slice_name=chart_title,
                    datasource_id=ds.id,
                    datasource_type="table",
                    datasource_name=f"{ds.table_name} ({ds.id})",
                    viz_type="table",
                    params=json.dumps(params),
                    query_context=json.dumps(query_context),
                    created_by_fk=admin_user.id if admin_user else None,
                    changed_by_fk=admin_user.id if admin_user else None,
                )
                db.session.add(chart)
                db.session.flush()
            else:
                chart.datasource_id = ds.id
                chart.datasource_type = "table"
                chart.datasource_name = f"{ds.table_name} ({ds.id})"
                chart.viz_type = "table"
                chart.params = json.dumps(params)
                chart.query_context = json.dumps(query_context)

            created_or_kept.append(chart)

        primary.slices = created_or_kept
        keep_dashboard_id = primary.id
        keep_chart_ids = {s.id for s in created_or_kept}

        # Build deterministic layout: each chart takes one full-width row
        # so long numeric values are easier to read.
        grid_children = []
        position = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"], "meta": {}},
            "GRID_ID": {
                "id": "GRID_ID",
                "type": "GRID",
                "children": grid_children,
                "parents": ["ROOT_ID"],
                "meta": {},
            },
        }
        for idx, chart in enumerate(created_or_kept, start=1):
            row_id = f"ROW-{idx}"
            chart_node = f"CHART-{chart.id}"
            grid_children.append(row_id)
            position[row_id] = {
                "id": row_id,
                "type": "ROW",
                "children": [chart_node],
                "parents": ["GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            position[chart_node] = {
                "id": chart_node,
                "type": "CHART",
                "children": [],
                "parents": [row_id],
                "meta": {"chartId": chart.id, "height": 44, "width": 12},
            }
        primary.position_json = json.dumps(position)

        removed_dashboards = 0
        for dash in dashboards:
            if dash.id == keep_dashboard_id:
                continue
            db.session.delete(dash)
            removed_dashboards += 1

        removed_charts = 0
        all_charts = db.session.query(Slice).order_by(Slice.id.asc()).all()
        for chart in all_charts:
            if chart.id in keep_chart_ids:
                continue
            db.session.delete(chart)
            removed_charts += 1

        db.session.commit()
        print(
            f"[Superset Curate] Kept dashboard id={keep_dashboard_id}, "
            f"kept charts={len(keep_chart_ids)}, removed dashboards={removed_dashboards}, removed charts={removed_charts}."
        )


if __name__ == "__main__":
    main()
