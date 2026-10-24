"""The dataset registry against every place that spells a marketplace dataset.

``config/marketplace_datasets.py`` is the one list. These tests are what keeps
the DDL, the Superset provisioning, the publisher and the quality registry from
drifting away from it: a column added to one and not the other fails here,
before a publish fails at night.
"""
import re
from pathlib import Path

import pytest

from config.marketplace_datasets import (
    BRIDGE,
    CACHE_SCHEMA,
    DATASETS,
    DIM,
    DW_SCHEMA,
    FACT,
    MART,
    MART_NAMES,
    MARTS,
    STAR,
    MarketplaceDataset,
    dataset,
    datasets_of_kind,
)

ROOT = Path(__file__).resolve().parents[1]
INIT_SQL = ROOT / "scripts" / "init_postgres.sql"
STAR_SQL = ROOT / "docs" / "model" / "star-schema.sql"
DATASOURCES = ROOT / "display" / "superset" / "datasources.yaml"

_TABLE = re.compile(r"CREATE TABLE(?: IF NOT EXISTS)?\s+(\w+)\.(\w+)\s*\((.*?)\n\);", re.S)
_TABLE_CONSTRAINT = ("PRIMARY", "UNIQUE", "CHECK", "CONSTRAINT", "FOREIGN")


def _split_top_level(body: str) -> list[str]:
    parts, depth, current = [], 0, []
    for char in body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return [part.strip() for part in parts if part.strip()]


def _strip_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def ddl_tables(path: Path) -> dict[str, tuple[list[str], tuple[str, ...]]]:
    """``schema.table`` -> (columns in order, primary key) parsed from a DDL file."""
    tables = {}
    for schema, table, body in _TABLE.findall(_strip_comments(path.read_text(encoding="utf-8"))):
        columns, key = [], ()
        for item in _split_top_level(body):
            first = item.split()[0].upper()
            if first.startswith("PRIMARY"):
                key = tuple(c.strip() for c in re.search(r"\((.*)\)", item).group(1).split(","))
            elif first in _TABLE_CONSTRAINT:
                continue
            else:
                name = item.split()[0]
                columns.append(name)
                if "PRIMARY KEY" in item.upper():
                    key = (name,)
        tables[f"{schema}.{table}"] = (columns, key)
    return tables


def test_registry_holds_ten_marts_and_the_twelve_star_tables():
    assert len(MARTS) == 10
    assert len(STAR) == 12
    assert {d.kind for d in STAR} == {DIM, BRIDGE, FACT}
    assert [d.name for d in datasets_of_kind(FACT)] == [
        "fact_offer_observation", "fact_price_snapshot_daily", "fact_price_change",
        "fact_counter_daily", "fact_crawl_attempt",
    ]
    assert len(datasets_of_kind(DIM)) == 6


def test_kind_and_name_agree_and_schemas_follow_the_kind():
    for d in STAR:
        assert d.name.startswith({DIM: "dim_", BRIDGE: "bridge_", FACT: "fact_"}[d.kind]), d.name
        assert d.postgres_schema == DW_SCHEMA
        assert d.qualified_table == f"dw.{d.name}"
    for d in MARTS:
        assert d.postgres_schema == CACHE_SCHEMA
        assert d.qualified_table == f"cache.marketplace_{d.name}"


@pytest.mark.parametrize("mart", MARTS, ids=lambda d: d.name)
def test_mart_columns_and_key_match_init_postgres(mart):
    tables = ddl_tables(INIT_SQL)
    assert mart.qualified_table in tables, mart.qualified_table
    columns, key = tables[mart.qualified_table]
    assert list(mart.columns) == columns
    assert mart.key == key


@pytest.mark.parametrize("table", STAR, ids=lambda d: d.name)
def test_star_columns_and_key_match_the_model_ddl(table):
    tables = ddl_tables(STAR_SQL)
    assert table.qualified_table in tables, table.qualified_table
    columns, key = tables[table.qualified_table]
    assert list(table.columns) == columns
    assert table.key == key


def test_the_model_ddl_declares_nothing_the_registry_lacks():
    assert set(ddl_tables(STAR_SQL)) == {d.qualified_table for d in STAR}


def test_init_postgres_declares_no_unregistered_marketplace_mart():
    declared = {name for name in ddl_tables(INIT_SQL) if name.startswith("cache.marketplace_")}
    assert declared == {d.qualified_table for d in MARTS}


def test_superset_datasources_register_every_mart_in_cache():
    text = DATASOURCES.read_text(encoding="utf-8")
    entries = set(re.findall(r"- table_name: (\w+)\s*\n\s*schema: (\w+)", text))
    for mart in MARTS:
        assert (mart.postgres_table, CACHE_SCHEMA) in entries, mart.postgres_table
    marketplace_cache = {table for table, schema in entries if schema == CACHE_SCHEMA and table.startswith("marketplace_")}
    assert marketplace_cache == {d.postgres_table for d in MARTS}


def test_every_cache_marketplace_table_named_in_code_is_registered():
    known = {d.qualified_table for d in MARTS}
    pattern = re.compile(r"cache\.marketplace_\w+")
    offenders = {}
    for folder in ("batch_layer", "ops", "display", "observability", "speed_layer"):
        for path in (ROOT / folder).rglob("*.py"):
            for name in set(pattern.findall(path.read_text(encoding="utf-8"))) - known:
                offenders.setdefault(name, []).append(str(path.relative_to(ROOT)))
    assert not offenders, offenders


def test_superset_dashboards_name_registered_marts_only():
    from display.superset.create_marketplace_batch_dashboard import DATASETS as batch
    from display.superset.create_marketplace_quality_dashboard import CACHE_DATASETS as quality

    tables = {d.postgres_table for d in MARTS}
    assert set(batch) <= tables
    assert set(quality) <= tables


def test_quality_registry_and_publisher_read_the_registry():
    from batch_layer.marketplace_manifest import _DAILY_PARTITIONS
    from batch_layer.marketplace_postgres import DATASET_COLUMNS, DATASETS
    from config.quality_rules import GOLD_DATASETS

    assert tuple(GOLD_DATASETS) == MART_NAMES
    assert tuple(DATASETS) == MART_NAMES
    assert DATASET_COLUMNS == {d.name: list(d.columns) for d in MARTS}
    assert _DAILY_PARTITIONS == {d.name: d.partition_columns for d in MARTS if d.partition_columns}


def test_daily_marts_partition_by_marketplace_and_their_day():
    assert {d.name: d.partition_columns for d in MARTS if d.partition_columns} == {
        "offer_price_history_daily": ("marketplace", "observed_date"),
        "offer_change_daily": ("marketplace", "observed_date"),
        "category_price_daily": ("marketplace", "observed_date"),
        "source_coverage_daily": ("marketplace", "observed_date"),
        "crawl_reliability_daily": ("marketplace", "request_date"),
        "counter_delta_daily": ("marketplace", "observed_date"),
        "price_anomaly_daily": ("marketplace", "observed_date"),
    }


def test_lookup_refuses_an_unknown_name_and_kind():
    assert dataset("dim_seller").key == ("seller_key",)
    with pytest.raises(ValueError, match="unknown marketplace dataset"):
        dataset("dim_customer")
    with pytest.raises(ValueError, match="unknown kind"):
        datasets_of_kind("cube")


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"kind": "cube"}, "kind must be one of"),
        ({"grain": " "}, "grain is required"),
        ({"columns": ("a", "a")}, "duplicate column"),
        ({"key": ("missing",)}, "not in columns"),
        ({"partition_columns": ("missing",)}, "not in columns"),
    ],
)
def test_a_malformed_entry_is_refused(kwargs, message):
    base = {"name": "x", "kind": MART, "grain": "one x", "key": ("a",), "columns": ("a", "b")}
    with pytest.raises(ValueError, match=message):
        MarketplaceDataset(**{**base, **kwargs})


def test_every_dataset_has_a_grain_sentence():
    assert all(d.grain.strip() for d in DATASETS)
