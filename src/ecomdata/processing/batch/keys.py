"""Surrogate keys and the unknown member for the marketplace star.

``docs/DATA_MODEL.md`` section 4.5. A surrogate key is ``xxhash64`` of the
natural key, the function the Kaggle star already uses, so the same data gives
the same keys in every run, in any order, and a fact can compute its foreign
keys without reading the dimension's assignments back.

Every dimension carries one row keyed ``-1`` so that no fact foreign key is
ever NULL: an inner join keeps the row, a dashboard labels it Unknown, and the
share of such rows can be counted.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

UNKNOWN_KEY = -1
# The natural-key spelling of the unknown member. The same string the marts
# already use for an offer without a category path.
UNKNOWN_NATURAL_KEY = "__UNKNOWN__"
UNKNOWN_LABEL = "Unknown"
# valid_to of an open SCD2 version; exclusive like every valid_to.
OPEN_VALID_TO = datetime(9999, 12, 31, tzinfo=UTC)
UNKNOWN_VALID_FROM = datetime(1970, 1, 1, tzinfo=UTC)


def _column(value: str | Column) -> Column:
    return F.col(value) if isinstance(value, str) else value


def surrogate_key(*natural_key: str | Column) -> Column:
    """Deterministic bigint key of a natural key.

    Callers pass non-null parts only: ``xxhash64`` skips NULL inputs, so a
    NULL part would silently hash like a shorter key.
    """
    if not natural_key:
        raise ValueError("a surrogate key needs at least one natural-key column")
    return F.xxhash64(*[_column(part) for part in natural_key])


def key_or_unknown(*natural_key: str | Column) -> Column:
    """The surrogate key, or ``-1`` when any part of the natural key is NULL."""
    parts = [_column(part) for part in natural_key]
    missing = parts[0].isNull()
    for part in parts[1:]:
        missing = missing | part.isNull()
    return F.when(missing, F.lit(UNKNOWN_KEY).cast("bigint")).otherwise(surrogate_key(*parts))


def date_key(value: str | Column) -> Column:
    """``yyyymmdd`` as an int, or ``-1`` for a NULL date."""
    column = _column(value)
    return F.coalesce(F.date_format(column, "yyyyMMdd").cast("int"), F.lit(UNKNOWN_KEY))


def with_unknown_member(frame: DataFrame, values: Mapping[str, Any]) -> DataFrame:
    """Append the ``-1`` row: ``values`` for the named columns, NULL elsewhere.

    The row is built from the frame's own schema, so it cannot add or drop a
    column, and a value of the wrong type fails here rather than in a union.
    """
    unknown = set(values) - set(frame.columns)
    if unknown:
        raise ValueError(f"unknown-member values for columns the frame lacks: {sorted(unknown)}")
    row = tuple(values.get(field.name) for field in frame.schema.fields)
    # Nullable copy: an aggregate column is non-nullable in the frame, and the
    # unknown row legitimately has no value for it.
    schema = StructType([StructField(f.name, f.dataType, True) for f in frame.schema.fields])
    member = frame.sparkSession.createDataFrame([row], schema=schema)
    return frame.unionByName(member)
