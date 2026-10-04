"""The evidence bundle index — Phase 9 plan section 12.1 (tests 16-17)."""
from __future__ import annotations

from ops import evidence
from ops.evidence import Item

BRIEF_25 = (
    "Source feasibility matrix", "Monitored-universe definition and volume projection", "Crawl-run audit samples",
    "Raw artifact and raw-to-Silver lineage example", "Adapter version / schema-drift example",
    "Data quality and quarantine samples", "Offer/observation counts by source and day", "Fresh/stale coverage",
    "Request success/error/latency distributions", "Observation and Kafka throughput",
    "Spark streaming p50/p95 latency", "Batch runtime and rows/second", "Raw/Parquet storage growth",
    "Replay/idempotency/restart test results", "Price history/change/anomaly examples",
    "Superset/Kibana screenshots", "Hardware/software/test configuration", "Limitations and failed experiments",
)


def test_the_index_lists_every_item_of_brief_section_25():
    titles = [item.title for item in evidence.ITEMS]

    assert all(title in titles for title in BRIEF_25)


def test_every_item_carries_a_dataset_label():
    assert all(item.dataset for item in evidence.ITEMS)


def test_replayed_numbers_are_labelled_replayed():
    for item in evidence.ITEMS:
        if any(path.startswith("bench") for path in item.paths):
            assert item.dataset == "replayed_fixture"


def test_an_absent_file_reads_missing_and_a_manual_item_never_does(tmp_path):
    (tmp_path / "environment.json").write_text("{}")
    items = (Item("present", ("environment.json",), "this machine"), Item("absent", ("nowhere.json",), "live"),
             Item("screenshots", (), "manual", manual=True))

    text, missing = evidence.render_index(tmp_path, items, header={"commit": "abc"})

    assert missing == ["absent"]
    assert "| 1 | present | PRESENT | this machine | [environment.json](environment.json) |" in text
    assert "| 2 | absent | MISSING | live |" in text
    assert "| 3 | screenshots | MANUAL | manual |" in text
    assert "- **commit:** abc" in text


def test_any_one_of_an_item_s_paths_satisfies_it(tmp_path):
    (tmp_path / "integration" / "drills").mkdir(parents=True)
    (tmp_path / "integration" / "drills" / "d5.json").write_text("{}")

    item = Item("restart", ("tests/junit.xml", "integration/drills/d5.json"), "stub stack")

    assert evidence.item_status(item, tmp_path) == "PRESENT"


def test_samples_are_written_as_json_and_an_empty_query_writes_nothing(tmp_path):
    from datetime import date
    from decimal import Decimal

    def query(sql):
        if "FROM audit.crawl_run " in sql:
            return [{"crawl_run_id": "r1", "started_at": date(2026, 10, 4), "requested": Decimal("15")}]
        return []

    evidence.collect_samples(tmp_path, query)

    written = sorted(p.name for p in (tmp_path / "samples").iterdir())
    assert written == ["crawl_runs.json"]
    assert '"requested": "15"' in (tmp_path / "samples" / "crawl_runs.json").read_text()
