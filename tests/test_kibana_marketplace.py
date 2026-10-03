"""Phase 8 plan section 11: index templates and the committed saved objects (tests 29-30).

Offline: the templates and the dashboards are data, and everything asserted
here is asserted against that data, not against a running Kibana.
"""
from __future__ import annotations

import json

import pytest

from display.kibana import marketplace_dashboards as d
from display.kibana import marketplace_index_templates as t


def _committed() -> list[dict]:
    """The committed export, without Kibana's trailing summary line."""
    lines = [json.loads(line) for line in d.NDJSON_PATH.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert "exportedCount" in lines[-1], "the export must end with Kibana's summary line"
    assert lines[-1]["exportedCount"] == len(lines) - 1
    return lines[:-1]


def _properties_by_pattern() -> dict[str, dict]:
    return {body["index_patterns"][0]: body["template"]["mappings"]["properties"]
            for body in t.INDEX_TEMPLATES.values()}


def _data_view_patterns(objects: list[dict]) -> dict[str, str]:
    return {obj["id"]: obj["attributes"]["title"] for obj in objects if obj["type"] == "index-pattern"}


# --- test 30: the index templates -------------------------------------------

# Every field that carries money or a rating, across all six templates. Each
# must survive six decimal places, the same as the decimal(38,6) cache columns.
PRICE_FIELDS = (("marketplace-offers-current", "current_price"),
                ("marketplace-offers-current", "list_price"),
                ("marketplace-offers-current", "rating_value"))

IDENTIFIER_FIELDS = (("marketplace-changes", "event_id"), ("marketplace-changes", "offer_id"),
                     ("marketplace-changes", "current_observation_id"),
                     ("marketplace-offers-current", "offer_id"),
                     ("marketplace-offers-current", "platform_listing_id"),
                     ("marketplace-source-health", "marketplace_code"),
                     ("marketplace-crawl-attempts", "attempt_id"),
                     ("marketplace-crawl-attempts", "crawl_run_id"),
                     ("marketplace-crawl-attempts", "task_id"),
                     ("marketplace-speed-batches", "query_id"),
                     ("marketplace-dlq", "dlq_id"))


@pytest.mark.parametrize("template,field", PRICE_FIELDS)
def test_a_price_field_is_a_scaled_float_with_six_decimal_places(template, field):
    mapping = t.INDEX_TEMPLATES[template]["template"]["mappings"]["properties"][field]

    assert mapping["type"] == "scaled_float"
    # 1000000: the same six places as cache.marketplace_* decimal(38,6). A
    # float would round 199000.99 away.
    assert mapping["scaling_factor"] == 1_000_000 == t.PRICE_SCALING_FACTOR


@pytest.mark.parametrize("template,field", IDENTIFIER_FIELDS)
def test_an_identifier_is_a_keyword(template, field):
    assert t.INDEX_TEMPLATES[template]["template"]["mappings"]["properties"][field]["type"] == "keyword"


def test_a_change_value_is_a_keyword_with_a_numeric_subfield_that_tolerates_json():
    for template in ("marketplace-changes",):
        for field in ("previous_value", "current_value"):
            mapping = t.INDEX_TEMPLATES[template]["template"]["mappings"]["properties"][field]
            # NEW_OFFER carries the whole offer, stored as canonical JSON text;
            # a price change carries a number. One field cannot be both, so the
            # field is text and the number lives in a sub-field that skips what
            # it cannot parse.
            assert mapping["type"] == "keyword"
            numeric = mapping["fields"]["numeric"]
            assert numeric["type"] == "scaled_float" and numeric["ignore_malformed"] is True
            assert numeric["scaling_factor"] == t.PRICE_SCALING_FACTOR


@pytest.mark.parametrize("name", t.PROJECTOR_TEMPLATES)
def test_a_projector_index_refuses_a_field_it_does_not_declare(name):
    # The projector builds these documents field by field; a field it did not
    # mean to write is a bug, and strict turns it into a loud failure.
    assert t.INDEX_TEMPLATES[name]["template"]["mappings"]["dynamic"] == "strict"


def test_the_speed_layers_own_indices_are_not_strict():
    # Their payload is the frozen wire contract, which cannot drift without a
    # schema version bump; a strict refusal there would fail a whole
    # micro-batch rather than one projection pass.
    for name in ("marketplace-changes", "marketplace-offers-current"):
        assert t.INDEX_TEMPLATES[name]["template"]["mappings"]["dynamic"] is True


def test_every_template_claims_exactly_its_own_index_family():
    patterns = t.index_patterns()

    assert len(patterns) == len(set(patterns)) == len(t.INDEX_TEMPLATES)
    for name, body in t.INDEX_TEMPLATES.items():
        assert body["index_patterns"] == [f"{name}-*"]
        assert body["priority"] == t.TEMPLATE_PRIORITY


def test_every_mapped_field_has_a_type():
    for name, body in t.INDEX_TEMPLATES.items():
        for field, mapping in body["template"]["mappings"]["properties"].items():
            assert "type" in mapping, f"{name}.{field} has no type"


def test_a_single_node_cluster_gets_no_replica_it_cannot_place():
    for body in t.INDEX_TEMPLATES.values():
        assert body["template"]["settings"]["number_of_replicas"] == 0


def test_installing_the_templates_puts_each_one_by_name():
    class FakeIndices:
        def __init__(self):
            self.put = {}

        def put_index_template(self, *, name, **body):
            self.put[name] = body

    class FakeEs:
        def __init__(self):
            self.indices = FakeIndices()

    es = FakeEs()
    installed = t.install_index_templates(es)

    assert sorted(installed) == sorted(t.INDEX_TEMPLATES)
    assert es.indices.put == t.INDEX_TEMPLATES


# --- test 29: the committed saved objects ------------------------------------

def test_the_committed_export_is_what_the_builder_produces():
    # The .ndjson is generated; a hand edit to either side would otherwise
    # drift silently.
    assert d.NDJSON_PATH.read_text(encoding="utf-8") == d.ndjson()


def test_every_committed_object_parses_and_has_an_id_and_a_type():
    objects = _committed()

    assert objects, "the export is empty"
    for obj in objects:
        assert obj["id"] and obj["type"] and obj["attributes"]
    ids = [obj["id"] for obj in objects]
    assert len(ids) == len(set(ids))


def test_every_data_view_matches_a_pattern_an_index_template_installs():
    patterns = set(t.index_patterns())

    for obj in _committed():
        if obj["type"] == "index-pattern":
            assert obj["attributes"]["title"] in patterns, obj["attributes"]["title"]


def test_every_reference_resolves_inside_the_export():
    objects = _committed()
    known = {(obj["type"], obj["id"]) for obj in objects}

    for obj in objects:
        for reference in obj["references"]:
            assert (reference["type"], reference["id"]) in known, f"{obj['id']} -> {reference}"


def test_each_dashboard_has_panels_and_a_reference_for_every_one_of_them():
    dashboards = [obj for obj in _committed() if obj["type"] == "dashboard"]

    assert len(dashboards) == 2
    for dashboard in dashboards:
        panels = json.loads(dashboard["attributes"]["panelsJSON"])
        assert panels, f"{dashboard['id']} has no panel"
        reference_names = {reference["name"] for reference in dashboard["references"]}
        for panel in panels:
            index = panel["panelIndex"]
            assert panel["gridData"]["i"] == index
            owned = {name for name in reference_names if name.startswith(f"{index}:")
                     or name == f"panel_{index}"}
            assert owned, f"panel {index} of {dashboard['id']} references nothing"


def test_every_lens_panel_reads_a_field_its_index_template_declares():
    objects = _committed()
    patterns = _data_view_patterns(objects)
    properties = _properties_by_pattern()

    for dashboard in (obj for obj in objects if obj["type"] == "dashboard"):
        references = {reference["name"]: reference["id"] for reference in dashboard["references"]}
        for panel in json.loads(dashboard["attributes"]["panelsJSON"]):
            if panel["type"] != "lens":
                continue
            index = panel["panelIndex"]
            data_view = next(value for name, value in references.items() if name.startswith(f"{index}:"))
            declared = properties[patterns[data_view]]
            layers = panel["embeddableConfig"]["attributes"]["state"]["datasourceStates"]["formBased"]["layers"]
            for layer in layers.values():
                assert sorted(layer["columnOrder"]) == sorted(layer["columns"])
                # Lens reads columnOrder, and it wants every bucket before
                # every metric; a metric listed first renders an empty panel.
                buckets = [layer["columns"][name]["isBucketed"] for name in layer["columnOrder"]]
                assert buckets == sorted(buckets, reverse=True), f"{dashboard['id']}/{index} orders a metric before a bucket"
                for column in layer["columns"].values():
                    field = column.get("sourceField")
                    # ___records___ is Lens's name for "count the documents".
                    if field and field != "___records___":
                        assert field in declared, f"{dashboard['id']}/{index} reads undeclared {field}"


def test_every_saved_search_column_is_a_field_its_index_template_declares():
    objects = _committed()
    patterns = _data_view_patterns(objects)
    properties = _properties_by_pattern()

    searches = [obj for obj in objects if obj["type"] == "search"]
    assert searches
    for search in searches:
        data_view = next(reference["id"] for reference in search["references"]
                         if reference["type"] == "index-pattern")
        declared = properties[patterns[data_view]]
        for column in search["attributes"]["columns"]:
            assert column in declared, f"{search['id']} shows undeclared {column}"
        assert search["attributes"]["sort"][0][0] in declared


def test_every_data_views_time_field_is_a_date_in_its_template():
    patterns = _properties_by_pattern()

    for view in d.DATA_VIEWS:
        assert patterns[view["title"]][view["timeFieldName"]]["type"] == "date"


def test_a_panel_that_says_latency_says_which_latency_it_means():
    """A mandatory rejection condition: a panel labelled "latency" must show
    the quantity the runbook names, and say so."""
    for dashboard in (obj for obj in _committed() if obj["type"] == "dashboard"):
        for panel in json.loads(dashboard["attributes"]["panelsJSON"]):
            if panel["type"] != "lens":
                continue
            attributes = panel["embeddableConfig"]["attributes"]
            text = f"{attributes['title']} {attributes['description']}".lower()
            if "latency" not in text:
                continue
            fields = {column.get("sourceField") for layer in
                      attributes["state"]["datasourceStates"]["formBased"]["layers"].values()
                      for column in layer["columns"].values()}
            if "duration_ms" in fields:
                # Micro-batch duration. The change contract carries no
                # processed time, so end-to-end latency cannot be shown here.
                assert "completed_at minus started_at" in text
                assert "not" in text and "end-to-end" in text
            else:
                assert "latency_ms" in fields
                assert "the fetch alone" in text


def test_the_micro_batch_panel_is_never_titled_plain_latency():
    for dashboard in (obj for obj in _committed() if obj["type"] == "dashboard"):
        for panel in json.loads(dashboard["attributes"]["panelsJSON"]):
            if panel["type"] != "lens":
                continue
            attributes = panel["embeddableConfig"]["attributes"]
            fields = {column.get("sourceField") for layer in
                      attributes["state"]["datasourceStates"]["formBased"]["layers"].values()
                      for column in layer["columns"].values()}
            if "duration_ms" in fields:
                assert "duration" in attributes["title"].lower()


def test_the_dashboards_cover_every_panel_the_plan_lists():
    text = d.NDJSON_PATH.read_text(encoding="utf-8")

    for expected in ("Recent changes", "Large price drops", "New and stale offers",
                     "Changes by change_type", "Offers by availability",
                     "Source last success, circuit state and consecutive failures",
                     "Freshness", "Crawl attempts by error_kind", "HTTP status distribution",
                     "Fetch latency", "Parsed and rejected", "Observation rate",
                     "DLQ records by stage", "Speed micro-batch duration", "Rows per speed micro-batch"):
        assert expected in text, f"no panel for {expected}"


def test_a_public_counter_change_is_never_called_a_sale():
    text = d.NDJSON_PATH.read_text(encoding="utf-8").lower()

    assert "not a sale" in text
    for forbidden in ("units sold", "sales volume", "demand signal\"", "revenue"):
        assert forbidden not in text


def test_the_phase_five_shell_is_superseded_by_id_not_left_behind():
    objects = {(obj["type"], obj["id"]) for obj in _committed()}

    for superseded in d.SUPERSEDED_OBJECTS:
        # Deleted by the importer, so it must not also be re-imported here.
        assert superseded not in objects


def test_the_builder_refuses_a_data_view_no_template_installs(monkeypatch):
    monkeypatch.setattr(d, "DATA_VIEWS", (*d.DATA_VIEWS,
                                          {"id": "dv-x", "title": "marketplace-nothing-*",
                                           "name": "x", "timeFieldName": "t"}))

    with pytest.raises(SystemExit, match="marketplace-nothing"):
        d.main()


def test_every_projector_index_name_matches_a_template_pattern():
    patterns = t.index_patterns()

    for index in t.projector_indices():
        assert any(index.startswith(pattern[:-1]) for pattern in patterns), index
    assert len(t.projector_indices()) == len(t.PROJECTOR_TEMPLATES)


def test_an_absent_projector_index_is_created_and_a_present_one_is_left_alone():
    class FakeIndices:
        def __init__(self, existing):
            self.existing = set(existing)
            self.created: list[str] = []

        def exists(self, *, index):
            return index in self.existing

        def create(self, *, index):
            self.created.append(index)
            self.existing.add(index)

    class FakeEs:
        def __init__(self, existing):
            self.indices = FakeIndices(existing)

    indices = t.projector_indices()
    es = FakeEs(indices[:2])

    created = t.ensure_projector_indices(es)

    # The DLQ index may never get a document on a healthy stack, and a Lens
    # panel over a missing index is an error rather than an empty chart.
    assert created == list(indices[2:])
    assert t.ensure_projector_indices(es) == []


def test_every_object_carries_the_migration_stamps_kibana_expects():
    for obj in _committed():
        # Unstamped, Kibana drags a by-value Lens panel through the pre-8.2
        # migrations and the import fails with a 500.
        assert obj["coreMigrationVersion"] == d.CORE_MIGRATION_VERSION
        assert obj["typeMigrationVersion"] == d.TYPE_MIGRATION_VERSIONS[obj["type"]]
