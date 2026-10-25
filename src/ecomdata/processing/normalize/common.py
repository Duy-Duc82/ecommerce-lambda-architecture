"""What every source's normaliser shares: reading records, canonical columns, the rules.

A source's module turns its records into the **canonical offer-observation
columns** (the ones the marts, the quality gates and the dimensions read) from
its own payload paths. Everything that is not a property of one source lives
here: how Bronze/records is read, how ids and timestamps are derived (identical
to ``ecomdata.domain.identity``), how money is typed, which rows are quarantined
and why, and how drift in a payload is counted.

Canonical columns are derived with ``get_json_object`` on the untouched
``payload_json``, one path per field, so a field of an unexpected type nulls
only itself (and, if it is required, sends the row to quarantine with a reason)
instead of nulling every column of the row, which is what a typed ``from_json``
does on a mismatch. The typed wide table is built separately, per source.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ecomdata.domain.records import RECORD_SCHEMA_VERSION

# The scalar fields of the envelope; the payload is read as text, see read_records.
ENVELOPE_SCHEMA = StructType(
    [
        StructField("schema_version", StringType()),
        StructField("record_id", StringType()),
        StructField("source_code", StringType()),
        StructField("marketplace_id", StringType()),
        StructField("record_kind", StringType()),
        StructField("record_index", IntegerType()),
        StructField("raw_artifact_id", StringType()),
        StructField("raw_uri", StringType()),
        StructField("body_sha256", StringType()),
        StructField("crawl_run_id", StringType()),
        StructField("adapter_version", StringType()),
        StructField("extractor_version", StringType()),
        StructField("request_target", StringType()),
        StructField("request_page", IntegerType()),
        StructField("fetched_at", StringType()),
        StructField("http_status", IntegerType()),
    ]
)

KIND_ROW = "listing_row"
KIND_CONTEXT = "page_context"
# Display rows per listing page, used to number the rows of page N after those of page N-1.
PAGE_SIZE = 40
MONEY = "decimal(38,6)"

# The canonical Silver columns, in order, with their Spark types: the columns the
# marts and gates read, plus the columns every source can fill.
CANONICAL_COLUMNS: tuple[tuple[str, str], ...] = (
    ("event_id", "string"),
    ("schema_version", "string"),
    ("event_type", "string"),
    ("occurred_at", "timestamp"),
    ("produced_at", "timestamp"),
    ("marketplace", "string"),
    ("partition_key", "string"),
    ("crawl_run_id", "string"),
    ("raw_uri", "string"),
    ("offer_id", "string"),
    ("marketplace_id", "string"),
    ("platform_listing_id", "string"),
    ("seller_id", "string"),
    ("product_title", "string"),
    ("brand", "string"),
    ("category_path", "string"),
    ("source_url", "string"),
    ("currency", "string"),
    ("first_seen_at", "timestamp"),
    ("last_seen_at", "timestamp"),
    ("active_status", "string"),
    ("observation_id", "string"),
    ("observed_at", "timestamp"),
    ("fetched_at", "timestamp"),
    ("current_price", MONEY),
    ("list_price", MONEY),
    ("shipping_price", MONEY),
    ("discount_amount", MONEY),
    ("discount_percent", MONEY),
    ("rating_value", MONEY),
    ("rating_scale", MONEY),
    ("rating_count", "bigint"),
    ("review_count", "bigint"),
    ("sold_count", "bigint"),
    ("availability", "string"),
    ("promotion_json", "string"),
    ("ranking_position", "bigint"),
    ("raw_sha256", "string"),
    ("adapter_version", "string"),
    ("observed_date", "date"),
    # Shared columns every source can fill; the first block above is what the batch reads today.
    ("source_code", "string"),
    ("seller_name", "string"),
    ("brand_id", "string"),
    ("category_ids", "array<string>"),
    ("sku", "string"),
    ("search_rank", "bigint"),
    ("is_advertisement", "boolean"),
    ("quantity_sold", "bigint"),
    ("location", "string"),
    ("record_id", "string"),
    ("raw_artifact_id", "string"),
    ("request_target", "string"),
    ("request_page", "int"),
)
CANONICAL_NAMES = tuple(name for name, _ in CANONICAL_COLUMNS)
OBSERVATION_SCHEMA_VERSION = "marketplace-observation.v1"
OBSERVATION_EVENT_TYPE = "OFFER_OBSERVED"


def decode_records(lines: DataFrame, *, source_code: str | None = None) -> DataFrame:
    """Envelope columns plus ``payload_json``, the payload object as sent, from JSON lines (a ``value`` column).

    The payload is taken as text so it survives untouched; the envelope is
    decoded with an explicit schema.
    """
    decoded = lines.select(
        F.from_json("value", ENVELOPE_SCHEMA).alias("e"),
        F.get_json_object("value", "$.payload").alias("payload_json"),
        F.get_json_object("value", "$.dropped_fields").alias("dropped_fields_json"),
    )
    frame = decoded.select("e.*", "payload_json", "dropped_fields_json").withColumn(
        "fetched_at", F.try_to_timestamp("fetched_at")
    )
    if source_code is not None:
        frame = frame.filter(F.col("source_code") == source_code)
    return frame


def read_records(spark: SparkSession, uri: str, *, source_code: str | None = None) -> DataFrame:
    """Bronze/records under ``uri`` (gzip JSON lines, any depth) as :func:`decode_records` returns them."""
    return decode_records(spark.read.option("recursiveFileLookup", "true").text(uri), source_code=source_code)


def unsupported_envelopes(records: DataFrame) -> DataFrame:
    """Rows whose envelope is not the one this code reads; they never reach a normaliser."""
    return records.filter(
        (F.col("schema_version") != RECORD_SCHEMA_VERSION) | F.col("schema_version").isNull() | F.col("record_id").isNull()
    )


def listing_rows(records: DataFrame) -> DataFrame:
    return records.filter((F.col("record_kind") == KIND_ROW) & (F.col("schema_version") == RECORD_SCHEMA_VERSION))


def page_contexts(records: DataFrame) -> DataFrame:
    return records.filter((F.col("record_kind") == KIND_CONTEXT) & (F.col("schema_version") == RECORD_SCHEMA_VERSION))


# ------------------------------------------------------------------ expressions
UNIT_SEPARATOR = "\x1f"


def json_path(path: str, column: str = "payload_json") -> Column:
    """One value of the payload, as text (``null`` when absent or JSON null)."""
    return F.get_json_object(F.col(column), path)


def clean_text(expr: Column) -> Column:
    """Trimmed text; blank becomes null (the same rule as the domain's ``optional_text``)."""
    trimmed = F.trim(expr)
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def money(expr: Column) -> Column:
    """Decimal(38,6); text that is not a number is null (ANSI ``cast`` would fail the whole job)."""
    return expr.try_cast(MONEY)


def whole(expr: Column) -> Column:
    """A non-negative integer; anything else is null."""
    value = expr.try_cast("bigint")
    return F.when(value >= 0, value)


def iso_microseconds(ts: Column) -> Column:
    """``datetime.isoformat(timespec="microseconds")`` of a UTC timestamp (the session zone is UTC)."""
    return F.concat(F.date_format(ts, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"), F.lit("+00:00"))


def deterministic_id(prefix: str, *parts: Column) -> Column:
    """``domain.identity.deterministic_id`` as a Spark expression: prefix_sha256(parts joined by 0x1f)."""
    payload = F.concat_ws(UNIT_SEPARATOR, F.lit(prefix), *[F.trim(part.cast("string")) for part in parts])
    return F.concat(F.lit(prefix + "_"), F.sha2(payload, 256))


def availability_expr(availability: Column, shippable: Column) -> Column:
    """The rule of the pre-ELT adapter: not shippable or not available is out of stock."""
    return (
        F.when(shippable.isin("false", "0"), "OUT_OF_STOCK")
        .when(availability.isin("false", "0"), "OUT_OF_STOCK")
        .when(availability.isin("true", "1"), "IN_STOCK")
        .otherwise("UNKNOWN")
    )


def _page_window(partition: Iterable[str]):
    """The rows of one page (or file) in display order."""
    return Window.partitionBy(*partition).orderBy("record_index")


@dataclass(frozen=True)
class SourceFields:
    """What a source reads from one row, as Spark expressions over ``payload_json``.

    ``listing_id``, ``title`` and ``price`` are required. ``is_advertisement`` is
    a boolean expression. Every other field may be left ``None``.
    """

    listing_id: Column
    title: Column
    price: Column
    url: Column | None = None
    list_price: Column | None = None
    seller_platform_id: Column | None = None
    seller_name: Column | None = None
    brand: Column | None = None
    brand_id: Column | None = None
    category_path: Column | None = None
    category_ids: Column | None = None
    discount_amount: Column | None = None
    discount_percent: Column | None = None
    rating_value: Column | None = None
    rating_scale: Column | None = None
    rating_count: Column | None = None
    review_count: Column | None = None
    sold_count: Column | None = None
    availability: Column | None = None
    sku: Column | None = None
    location: Column | None = None
    is_advertisement: Column | None = None


def _null(dtype: str) -> Column:
    return F.lit(None).cast(dtype)


def canonical(records: DataFrame, spec: Any, fields: SourceFields) -> DataFrame:
    """The canonical columns of ``records`` (listing rows of one source), plus ``reason`` for a row to quarantine.

    ``spec`` is the source's :class:`~ecomdata.domain.sources.SourceSpec`: its
    code, marketplace id and currency are the only facts taken from it.
    """
    code, marketplace_id, currency = spec.code, spec.marketplace_id, spec.currency
    listing = clean_text(fields.listing_id)
    seller_platform = clean_text(fields.seller_platform_id) if fields.seller_platform_id is not None else _null("string")
    ad = fields.is_advertisement if fields.is_advertisement is not None else F.lit(False)
    rating_value = money(fields.rating_value) if fields.rating_value is not None else _null(MONEY)
    category_path = clean_text(fields.category_path) if fields.category_path is not None else _null("string")
    list_price = money(fields.list_price) if fields.list_price is not None else _null(MONEY)

    page = _page_window(["raw_artifact_id"])
    rank = F.row_number().over(page)
    organic_rank = F.sum(F.when(ad, 0).otherwise(1)).over(page.rowsBetween(Window.unboundedPreceding, Window.currentRow))
    base = records.select(
        "*",
        listing.alias("_listing"),
        clean_text(fields.title).alias("_title"),
        money(fields.price).alias("_price"),
        ad.cast("boolean").alias("_ad"),
        rank.alias("_display_rank"),
        organic_rank.alias("_organic_rank"),
    )
    observed_at = F.col("fetched_at")
    selected = base.select(
        "*",
        deterministic_id("obs", F.lit(code), F.col("_listing"), iso_microseconds(observed_at), F.col("body_sha256")).alias(
            "_observation_id"
        ),
    )
    result = selected.select(
        F.col("_observation_id").alias("event_id"),
        F.lit(OBSERVATION_SCHEMA_VERSION).alias("schema_version"),
        F.lit(OBSERVATION_EVENT_TYPE).alias("event_type"),
        observed_at.alias("occurred_at"),
        observed_at.alias("produced_at"),
        F.lit(code).alias("marketplace"),
        F.concat(F.lit(f"{code}:"), F.col("_listing")).alias("partition_key"),
        F.col("crawl_run_id"),
        F.col("raw_uri"),
        deterministic_id("offer", F.lit(code), F.col("_listing")).alias("offer_id"),
        F.lit(marketplace_id).alias("marketplace_id"),
        F.col("_listing").alias("platform_listing_id"),
        F.when(seller_platform.isNotNull(), deterministic_id("seller", F.lit(code), seller_platform)).alias("seller_id"),
        F.col("_title").alias("product_title"),
        (clean_text(fields.brand) if fields.brand is not None else _null("string")).alias("brand"),
        F.coalesce(category_path, F.col("request_target")).alias("category_path"),
        (clean_text(fields.url) if fields.url is not None else _null("string")).alias("source_url"),
        F.lit(currency).alias("currency"),
        observed_at.alias("first_seen_at"),
        observed_at.alias("last_seen_at"),
        F.lit("UNKNOWN").alias("active_status"),
        F.col("_observation_id").alias("observation_id"),
        observed_at.alias("observed_at"),
        observed_at.alias("fetched_at"),
        F.when(F.col("_price") >= 0, F.col("_price")).alias("current_price"),
        F.when(list_price > 0, list_price).alias("list_price"),
        _null(MONEY).alias("shipping_price"),
        (money(fields.discount_amount) if fields.discount_amount is not None else _null(MONEY)).alias("discount_amount"),
        (money(fields.discount_percent) if fields.discount_percent is not None else _null(MONEY)).alias("discount_percent"),
        rating_value.alias("rating_value"),
        F.when(rating_value.isNotNull(), money(fields.rating_scale) if fields.rating_scale is not None else _null(MONEY)).alias(
            "rating_scale"
        ),
        (whole(fields.rating_count) if fields.rating_count is not None else _null("bigint")).alias("rating_count"),
        (whole(fields.review_count) if fields.review_count is not None else _null("bigint")).alias("review_count"),
        (whole(fields.sold_count) if fields.sold_count is not None else _null("bigint")).alias("sold_count"),
        (fields.availability if fields.availability is not None else F.lit("UNKNOWN")).alias("availability"),
        _null("string").alias("promotion_json"),
        F.when(~F.col("_ad"), (F.col("request_page") - 1) * PAGE_SIZE + F.col("_organic_rank")).cast("bigint").alias(
            "ranking_position"
        ),
        F.col("body_sha256").alias("raw_sha256"),
        F.col("adapter_version"),
        F.to_date(observed_at).alias("observed_date"),
        F.lit(code).alias("source_code"),
        (clean_text(fields.seller_name) if fields.seller_name is not None else _null("string")).alias("seller_name"),
        (clean_text(fields.brand_id) if fields.brand_id is not None else _null("string")).alias("brand_id"),
        (fields.category_ids if fields.category_ids is not None else _null("array<string>")).alias("category_ids"),
        (clean_text(fields.sku) if fields.sku is not None else _null("string")).alias("sku"),
        ((F.col("request_page") - 1) * PAGE_SIZE + F.col("_display_rank")).cast("bigint").alias("search_rank"),
        F.col("_ad").alias("is_advertisement"),
        (whole(fields.sold_count) if fields.sold_count is not None else _null("bigint")).alias("quantity_sold"),
        (clean_text(fields.location) if fields.location is not None else _null("string")).alias("location"),
        F.col("record_id"),
        F.col("raw_artifact_id"),
        F.col("request_target"),
        F.col("request_page"),
        F.col("payload_json"),
    )
    return result.withColumn("reason", quarantine_reason())


def quarantine_reason() -> Column:
    """The first reason a row cannot be a valid observation, or null."""
    reasons = [
        F.when(F.col("platform_listing_id").isNull(), "missing_listing_id"),
        F.when(F.col("product_title").isNull(), "missing_title"),
        F.when(F.col("current_price").isNull(), "missing_or_invalid_price"),
        F.when(F.col("fetched_at").isNull(), "missing_fetched_at"),
        F.when(F.col("raw_sha256").isNull() | F.col("raw_uri").isNull() | F.col("crawl_run_id").isNull(), "missing_lineage"),
    ]
    return F.coalesce(*reasons)


def split_quarantine(frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(valid observations in canonical column order, quarantined rows with their reason)."""
    valid = frame.filter(F.col("reason").isNull()).select(*[F.col(name).cast(dtype) for name, dtype in CANONICAL_COLUMNS])
    quarantined = frame.filter(F.col("reason").isNotNull()).select(
        "record_id",
        "source_code",
        "raw_artifact_id",
        "crawl_run_id",
        "request_target",
        "request_page",
        F.col("fetched_at"),
        "reason",
        "payload_json",
    )
    return valid, quarantined


# ------------------------------------------------------------------ drift
def payload_drift(rows: DataFrame, schema: StructType) -> DataFrame:
    """Top-level payload keys the source's declared schema does not know, with how often they occur.

    A key that appears here is data the typed wide table drops: the report tells
    a maintainer to add it to the source's ``PAYLOAD_SCHEMA``.
    """
    known = [field.name for field in schema.fields]
    keys = rows.select(F.explode(F.expr("json_object_keys(payload_json)")).alias("key"), "source_code")
    return (
        keys.filter(~F.col("key").isin(*known))
        .groupBy("source_code", "key")
        .agg(F.count("*").alias("occurrences"))
        .orderBy(F.col("occurrences").desc(), "key")
    )


__all__ = [
    "CANONICAL_COLUMNS",
    "CANONICAL_NAMES",
    "ENVELOPE_SCHEMA",
    "KIND_CONTEXT",
    "KIND_ROW",
    "MONEY",
    "PAGE_SIZE",
    "SourceFields",
    "availability_expr",
    "canonical",
    "clean_text",
    "deterministic_id",
    "iso_microseconds",
    "json_path",
    "decode_records",
    "listing_rows",
    "money",
    "page_contexts",
    "payload_drift",
    "read_records",
    "split_quarantine",
    "unsupported_envelopes",
    "whole",
]
