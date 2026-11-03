"""
Patch Superset runtime code for dashboard compatibility.

Why:
- Some dashboard flows call:
  POST /api/v1/chart/data?form_data={"slice_id":...}
- Superset 4.1.1 chart-data endpoint expects JSON body query_context.
- Without this patch, panels fail with 400 "Request is not JSON".

What this patch does:
- Adds fallback to parse form_data from query string.
- If only slice_id is provided, loads saved chart.query_context automatically.
"""

from __future__ import annotations

from pathlib import Path

TARGET = Path("/app/superset/charts/data/api.py")
MARKER = "codex compatibility patch: read form_data from query args + slice_id fallback"

OLD = """        json_body = None
        if request.is_json:
            json_body = request.json
        elif request.form.get("form_data"):
            # CSV export submits regular form data
            with contextlib.suppress(TypeError, json.JSONDecodeError):
                json_body = json.loads(request.form["form_data"])
        if json_body is None:
            return self.response_400(message=_(\"Request is not JSON\"))
"""

NEW = """        json_body = None
        if request.is_json:
            json_body = request.json
        elif request.form.get(\"form_data\"):
            # CSV export submits regular form data
            with contextlib.suppress(TypeError, json.JSONDecodeError):
                json_body = json.loads(request.form[\"form_data\"])
        elif request.args.get(\"form_data\"):
            # codex compatibility patch: read form_data from query args + slice_id fallback
            with contextlib.suppress(TypeError, json.JSONDecodeError):
                json_body = json.loads(request.args[\"form_data\"])

        if (
            isinstance(json_body, dict)
            and json_body.get(\"slice_id\")
            and \"queries\" not in json_body
        ):
            chart = self.datamodel.get(json_body[\"slice_id\"], self._base_filters)
            if chart and chart.query_context:
                with contextlib.suppress(TypeError, json.JSONDecodeError):
                    json_body = json.loads(chart.query_context)

        if json_body is None:
            return self.response_400(message=_(\"Request is not JSON\"))
"""


def main() -> None:
    if not TARGET.exists():
        print(f"[Superset Patch] Target not found: {TARGET}")
        return

    content = TARGET.read_text(encoding="utf-8")
    if MARKER in content:
        print("[Superset Patch] Already applied.")
        return

    if OLD not in content:
        print("[Superset Patch] Expected block not found; skip patch.")
        return

    TARGET.write_text(content.replace(OLD, NEW), encoding="utf-8")
    print("[Superset Patch] Applied chart-data compatibility patch.")


if __name__ == "__main__":
    main()
