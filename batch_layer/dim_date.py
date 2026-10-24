"""The conformed date dimension, shared by the Kaggle and the marketplace stars.

``docs/DATA_MODEL.md`` sections 4.3–4.4. One builder, one set of rows and one
meaning for every column: that is what lets the two stars share ``dim_date``
even though they share nothing else.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from batch_layer.marketplace_keys import UNKNOWN_KEY, with_unknown_member

# Covers the Kaggle file (October 2019 to April 2020) and the marketplace
# collection with room to spare: about 4 400 rows.
DIM_DATE_START = date(2019, 1, 1)
DIM_DATE_END = date(2030, 12, 31)

# Statutory holidays whose date the Labour Code fixes (2019 code, article 112):
# New Year, Reunification Day, Labour Day and National Day.
_SOLAR_HOLIDAYS = ((1, 1), (4, 30), (5, 1), (9, 2))

# Lunar holidays converted to solar dates: Tet eve and the first three days of
# Tet, and the Hung Kings' day (10th of the third lunar month). Computed with
# lunardate 0.2.2 and fixed here so the build needs no calendar library. The
# extra days off the government places around them each year are not included.
_LUNAR_HOLIDAYS = {
    2019: ("2019-02-04", "2019-02-05", "2019-02-06", "2019-02-07", "2019-04-14"),
    2020: ("2020-01-24", "2020-01-25", "2020-01-26", "2020-01-27", "2020-04-02"),
    2021: ("2021-02-11", "2021-02-12", "2021-02-13", "2021-02-14", "2021-04-21"),
    2022: ("2022-01-31", "2022-02-01", "2022-02-02", "2022-02-03", "2022-04-10"),
    2023: ("2023-01-21", "2023-01-22", "2023-01-23", "2023-01-24", "2023-04-29"),
    2024: ("2024-02-09", "2024-02-10", "2024-02-11", "2024-02-12", "2024-04-18"),
    2025: ("2025-01-28", "2025-01-29", "2025-01-30", "2025-01-31", "2025-04-07"),
    2026: ("2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-04-26"),
    2027: ("2027-02-05", "2027-02-06", "2027-02-07", "2027-02-08", "2027-04-16"),
    2028: ("2028-01-25", "2028-01-26", "2028-01-27", "2028-01-28", "2028-04-04"),
    2029: ("2029-02-12", "2029-02-13", "2029-02-14", "2029-02-15", "2029-04-23"),
    2030: ("2030-02-02", "2030-02-03", "2030-02-04", "2030-02-05", "2030-04-12"),
}


def vn_holidays(start: date = DIM_DATE_START, end: date = DIM_DATE_END) -> frozenset[date]:
    missing = [year for year in range(start.year, end.year + 1) if year not in _LUNAR_HOLIDAYS]
    if missing:
        raise ValueError(f"no lunar holiday dates recorded for {missing}; extend _LUNAR_HOLIDAYS")
    days = {date(year, month, day) for year in range(start.year, end.year + 1) for month, day in _SOLAR_HOLIDAYS}
    days |= {date.fromisoformat(text) for year in range(start.year, end.year + 1) for text in _LUNAR_HOLIDAYS[year]}
    return frozenset(day for day in days if start <= day <= end)


def build_dim_date(spark: SparkSession, start: date = DIM_DATE_START, end: date = DIM_DATE_END) -> DataFrame:
    if end < start:
        raise ValueError("end must not be before start")
    days = spark.createDataFrame([(start, end)], "start date, end date").select(
        F.explode(F.sequence("start", "end")).alias("full_date"))
    smallint = lambda column: column.cast("smallint")  # noqa: E731
    built = days.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        # Spark's numbering, 1 = Sunday, as the Kaggle star always had it.
        smallint(F.dayofweek("full_date")).alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        smallint(F.weekofyear("full_date")).alias("week_of_year"),
        smallint(F.month("full_date")).alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        smallint(F.quarter("full_date")).alias("quarter"),
        smallint(F.year("full_date")).alias("year"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
        (F.dayofmonth("full_date") == F.month("full_date")).alias("is_double_day_sale"),
        F.col("full_date").isin(sorted(vn_holidays(start, end))).alias("is_vn_holiday"),
    )
    return with_unknown_member(built, {"date_key": UNKNOWN_KEY, "day_name": "Unknown", "month_name": "Unknown"})
