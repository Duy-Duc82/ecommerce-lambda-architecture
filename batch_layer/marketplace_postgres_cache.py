"""Transactional PostgreSQL BI cache for the marketplace Gold marts.

Superset reads these tables rather than the run-scoped object storage, which is
the whole reason the refresh has to be atomic: a dashboard must never show
offer_current from one run beside offer_price_history_daily from another.

So every mart is staged first, and then all targets are swapped inside one
transaction with exactly one commit.  On any error the transaction rolls back
and the exception propagates; a half-published cache is never left behind and
never logged-and-ignored.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Mapping, Sequence

from batch_layer.marketplace_gold_contracts import (
    MARKETPLACE_MART_SPECS,
    MONEY,
    MartSpec,
)
from config.settings import (
    MARKETPLACE_DECIMAL_PRECISION,
    MARKETPLACE_DECIMAL_SCALE,
    POSTGRES_MARKETPLACE_SCHEMA,
    POSTGRES_MARKETPLACE_STAGING_SCHEMA,
)

logger = logging.getLogger(__name__)

_SQL_TYPES = {
    "string": "TEXT",
    "long": "BIGINT",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMPTZ",
    "date": "DATE",
    MONEY: f"NUMERIC({MARKETPLACE_DECIMAL_PRECISION}, {MARKETPLACE_DECIMAL_SCALE})",
}


def column_ddl(spec: MartSpec) -> str:
    lines = []
    for column in spec.columns:
        sql_type = _SQL_TYPES[column.type]
        null = "" if column.nullable else " NOT NULL"
        lines.append(f"    {column.name:<36} {sql_type}{null}")
    return ",\n".join(lines)


def mart_table_ddl(spec: MartSpec, *, schema: str, with_primary_key: bool) -> str:
    body = column_ddl(spec)
    if with_primary_key:
        key = ", ".join(("gold_run_id",) + spec.grain)
        body = f"{body},\n    PRIMARY KEY ({key})"
    return f"CREATE TABLE IF NOT EXISTS {schema}.{spec.name} (\n{body}\n);"


def generate_ddl(
    *,
    schema: str = POSTGRES_MARKETPLACE_SCHEMA,
    staging_schema: str = POSTGRES_MARKETPLACE_STAGING_SCHEMA,
) -> str:
    """Full idempotent DDL for the cache and its staging mirror."""
    parts = [
        "-- ============================================================",
        "-- PHASE 6 - marketplace Gold BI cache",
        "-- Superset reads <schema>; Spark lands in <staging_schema> and the",
        "-- publish step swaps every mart in one transaction.",
        "-- Money is NUMERIC, never DOUBLE PRECISION: a float here would make",
        "-- the published number disagree with the Decimal it came from.",
        "-- ============================================================",
        f"CREATE SCHEMA IF NOT EXISTS {schema};",
        f"CREATE SCHEMA IF NOT EXISTS {staging_schema};",
        "",
        f"""CREATE TABLE IF NOT EXISTS {schema}.gold_run (
    gold_run_id       TEXT         PRIMARY KEY,
    as_of             TIMESTAMPTZ  NOT NULL,
    window_days       INTEGER      NOT NULL,
    rule_version      TEXT         NOT NULL,
    started_at        TIMESTAMPTZ  NOT NULL,
    completed_at      TIMESTAMPTZ,
    observations_read BIGINT       NOT NULL DEFAULT 0,
    quarantined_rows  BIGINT       NOT NULL DEFAULT 0,
    status            TEXT         NOT NULL,
    failure_stage     TEXT,
    skipped_marts     JSONB        NOT NULL DEFAULT '{{}}'::jsonb
);""",
        "",
        f"""CREATE TABLE IF NOT EXISTS {schema}.gold_assertion (
    gold_run_id     TEXT        NOT NULL REFERENCES {schema}.gold_run(gold_run_id),
    assertion_name  TEXT        NOT NULL,
    observed_value  TEXT        NOT NULL,
    expectation     TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    rule_version    TEXT        NOT NULL,
    PRIMARY KEY (gold_run_id, assertion_name)
);""",
        "",
    ]
    for spec in MARKETPLACE_MART_SPECS:
        parts.append(mart_table_ddl(spec, schema=schema, with_primary_key=True))
        parts.append("")
    parts.append("-- staging mirror: same columns, no keys, truncated per run")
    for spec in MARKETPLACE_MART_SPECS:
        parts.append(mart_table_ddl(spec, schema=staging_schema, with_primary_key=False))
        parts.append("")
    return "\n".join(parts)


def stage_mart(
    dataframe,
    spec: MartSpec,
    *,
    jdbc_url: str,
    jdbc_properties: Mapping[str, str],
    staging_schema: str = POSTGRES_MARKETPLACE_STAGING_SCHEMA,
) -> None:
    """Overwrite one staging table from Spark. pyspark stays out of module scope."""
    (
        dataframe.select(*spec.column_names)
        .write.mode("overwrite")
        .jdbc(
            url=jdbc_url,
            table=f"{staging_schema}.{spec.name}",
            properties=dict(jdbc_properties),
        )
    )


def publish_marts(
    *,
    gold_run_id: str,
    connect: Callable[[], Any],
    mart_names: Sequence[str] | None = None,
    schema: str = POSTGRES_MARKETPLACE_SCHEMA,
    staging_schema: str = POSTGRES_MARKETPLACE_STAGING_SCHEMA,
    run_row: Mapping[str, Any] | None = None,
    assertions: Sequence[Mapping[str, Any]] = (),
) -> list[str]:
    """Swap every staged mart into the served schema in one transaction.

    Delete-then-insert scoped to ``gold_run_id`` rather than TRUNCATE, so
    re-publishing one logical window replaces its own rows and does not
    duplicate them, while other runs' history stays intact.
    """
    if not isinstance(gold_run_id, str) or not gold_run_id.strip():
        raise ValueError("gold_run_id is required")
    names = list(mart_names) if mart_names is not None else [s.name for s in MARKETPLACE_MART_SPECS]
    unknown = [name for name in names if name not in {s.name for s in MARKETPLACE_MART_SPECS}]
    if unknown:
        raise ValueError(f"unknown marts: {unknown}")

    executed: list[str] = []
    connection = connect()
    try:
        cursor = connection.cursor()
        try:
            if run_row is not None:
                statement = (
                    f"INSERT INTO {schema}.gold_run (gold_run_id, as_of, window_days, "
                    "rule_version, started_at, completed_at, observations_read, "
                    "quarantined_rows, status, failure_stage, skipped_marts) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (gold_run_id) DO UPDATE SET "
                    "completed_at = EXCLUDED.completed_at, "
                    "observations_read = EXCLUDED.observations_read, "
                    "quarantined_rows = EXCLUDED.quarantined_rows, "
                    "status = EXCLUDED.status, "
                    "failure_stage = EXCLUDED.failure_stage, "
                    "skipped_marts = EXCLUDED.skipped_marts"
                )
                cursor.execute(
                    statement,
                    (
                        gold_run_id,
                        run_row.get("as_of"),
                        run_row.get("window_days"),
                        run_row.get("rule_version"),
                        run_row.get("started_at"),
                        run_row.get("completed_at"),
                        run_row.get("observations_read", 0),
                        run_row.get("quarantined_rows", 0),
                        run_row.get("status"),
                        run_row.get("failure_stage"),
                        json.dumps(dict(run_row.get("skipped_marts") or {}), sort_keys=True),
                    ),
                )
                executed.append("gold_run")

            for name in names:
                cursor.execute(f"DELETE FROM {schema}.{name} WHERE gold_run_id = %s", (gold_run_id,))
                cursor.execute(
                    f"INSERT INTO {schema}.{name} SELECT * FROM {staging_schema}.{name}"
                )
                executed.append(name)

            if assertions:
                cursor.execute(
                    f"DELETE FROM {schema}.gold_assertion WHERE gold_run_id = %s",
                    (gold_run_id,),
                )
                for assertion in assertions:
                    cursor.execute(
                        f"INSERT INTO {schema}.gold_assertion (gold_run_id, assertion_name, "
                        "observed_value, expectation, status, rule_version) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (
                            gold_run_id,
                            assertion["assertion_name"],
                            assertion["observed_value"],
                            assertion["expectation"],
                            assertion["status"],
                            assertion["rule_version"],
                        ),
                    )
                executed.append("gold_assertion")
        finally:
            cursor.close()
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    logger.info("published %d marketplace marts for run %s", len(names), gold_run_id)
    return executed


def apply_marketplace_ddl(connect: Callable[[], Any]) -> None:
    """Create the cache schemas and tables if absent."""
    connection = connect()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(generate_ddl())
        finally:
            cursor.close()
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
