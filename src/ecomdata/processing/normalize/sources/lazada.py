"""Lazada: the payload schema and the mapping to the canonical observation columns.

PAYLOAD_SCHEMA declares every field seen in the product rows of Lazada's
?ajax=true listing response (53 leaf fields, two pages probed 2026-10-24),
as Lazada types them: prices and counts arrive as text. to_offer_observation
derives the canonical columns from the same rows. itemSoldCntShow is a
rounded display string ("10.4K Đã bán*"), not a counter, so no sold count is
taken from it.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from ecomdata.domain.sources import LAZADA
from ecomdata.processing.normalize.common import (
    CATEGORY_NAME_COLUMNS,
    CATEGORY_PATH_COLUMNS,
    LISTING_ATTRIBUTE_COLUMNS,
    SourceFields,
    availability_expr,
    canonical,
    clean_text,
    json_path,
    listing_rows,
    typed,
)

PAYLOAD_SCHEMA = StructType([
    StructField("name", StringType()),
    StructField("nid", StringType()),
    StructField("itemId", StringType()),
    StructField("icons", ArrayType(StructType([
        StructField("domClass", StringType()),
        StructField("type", StringType()),
        StructField("group", StringType()),
        StructField("showType", StringType()),
        StructField("bizType", StringType()),
        StructField("text", StringType()),
    ]))),
    StructField("image", StringType()),
    StructField("isSmartImage", BooleanType()),
    StructField("originalPriceShow", StringType()),
    StructField("priceShow", StringType()),
    StructField("ratingScore", StringType()),
    StructField("review", StringType()),
    StructField("location", StringType()),
    StructField("thumbs", ArrayType(StructType([
        StructField("image", StringType()),
        StructField("sku", StringType()),
        StructField("skuId", StringType()),
        StructField("itemUrl", StringType()),
        StructField("querystring", StringType()),
    ]))),
    StructField("sellerName", StringType()),
    StructField("sellerId", StringType()),
    StructField("brandName", StringType()),
    StructField("brandId", StringType()),
    StructField("cheapest_sku", StringType()),
    StructField("skuId", StringType()),
    StructField("sku", StringType()),
    StructField("categories", ArrayType(LongType())),
    StructField("price", StringType()),
    StructField("restrictedAge", LongType()),
    StructField("inStock", BooleanType()),
    StructField("clickTrace", StringType()),
    StructField("longImageDisplayable", BooleanType()),
    StructField("skus", ArrayType(StructType([
        StructField("id", StringType()),
    ]))),
    StructField("promotionId", StringType()),
    StructField("isSponsored", BooleanType()),
    StructField("tItemType", StringType()),
    StructField("skuType", StringType()),
    StructField("adFlag", StringType()),
    StructField("directSimilarUrl", StringType()),
    StructField("gridTitleLine", StringType()),
    StructField("isFission", StringType()),
    StructField("isBadgeAutoScroll", BooleanType()),
    StructField("showCart", BooleanType()),
    StructField("showBackIcon", BooleanType()),
    StructField("showUnitPrice", BooleanType()),
    StructField("itemUrl", StringType()),
    StructField("querystring", StringType()),
    StructField("itemSoldCntShow", StringType()),
    StructField("discount", StringType()),
    StructField("originalPrice", StringType()),
    StructField("pClusterType", StringType()),
])



RATING_SCALE = "5"


def to_wide(records: DataFrame) -> DataFrame:
    """Listing rows with every declared payload field as a typed column p (and payload_json)."""
    return listing_rows(records).withColumn("p", F.from_json("payload_json", PAYLOAD_SCHEMA))


def _is_advertisement() -> Column:
    """Sponsored rows: isSponsored true, or an adFlag other than 0."""
    flag = json_path("$.adFlag")
    return (json_path("$.isSponsored") == "true") | (flag.isNotNull() & ~flag.isin("", "0"))


def _url() -> Column:
    item = json_path("$.itemUrl")
    return F.when(item.startswith("//"), F.concat(F.lit("https:"), item)).when(
        item.startswith("/"), F.concat(F.lit(LAZADA.base_url), item)
    ).otherwise(item)


def fields() -> SourceFields:
    original = json_path("$.originalPrice").try_cast("decimal(38,6)")
    return SourceFields(
        listing_id=json_path("$.itemId"),
        title=json_path("$.name"),
        price=json_path("$.price"),
        url=_url(),
        list_price=original,
        seller_platform_id=json_path("$.sellerId"),
        seller_name=json_path("$.sellerName"),
        brand=json_path("$.brandName"),
        brand_id=json_path("$.brandId"),
        category_ids=F.from_json(json_path("$.categories"), "array<string>"),
        discount_percent=F.regexp_extract(json_path("$.discount"), r"(\d+(?:\.\d+)?)\s*%", 1),
        rating_value=json_path("$.ratingScore"),
        rating_scale=F.lit(RATING_SCALE),
        review_count=json_path("$.review"),
        availability=availability_expr(json_path("$.inStock"), F.lit(None).cast("string")),
        sku=json_path("$.sku"),
        location=json_path("$.location"),
        is_advertisement=_is_advertisement(),
    )


def to_offer_observation(records: DataFrame) -> DataFrame:
    """Canonical columns of Lazada's listing rows, and reason for a row that cannot be an observation."""
    return canonical(listing_rows(records), LAZADA, fields())


# ------------------------------------------------------------------ the star's attributes
# The part of a page context the category dimension reads.
BREADCRUMB_SCHEMA = StructType([
    StructField("mods", StructType([
        StructField("breadcrumb", ArrayType(StructType([
            StructField("url", StringType()),
            StructField("title", StringType()),
        ]))),
    ])),
])

LAZMALL = "lazMall"
FREE_SHIPPING = "Freeshipping"
SELLER_TYPE_LAZMALL = "LAZMALL"


def _has_icon(biz_type: str) -> Column:
    # Lazada lists every badge a row has, so a missing badge is a no, not unknown.
    return F.coalesce(F.exists(F.col("p")["icons"], lambda icon: icon["bizType"] == biz_type), F.lit(False))


def listing_attributes(wide: DataFrame) -> DataFrame:
    """``LISTING_ATTRIBUTE_COLUMNS`` of each wide row (docs/DATA_MODEL.md 4.4).

    Lazada's only seller badge is LazMall, its official-store programme, and the
    only delivery badge is free shipping; every other attribute stays NULL.
    """
    lazmall = _has_icon(LAZMALL)
    frame = wide.select(
        "record_id",
        F.when(lazmall, F.lit(SELLER_TYPE_LAZMALL)).alias("seller_type"),
        lazmall.alias("is_official_store"),
        _has_icon(FREE_SHIPPING).alias("has_free_shipping"),
    )
    return typed(frame, LISTING_ATTRIBUTE_COLUMNS)


def _slug(url: Column) -> Column:
    """``/dien-thoai-di-dong/`` -> ``dien-thoai-di-dong``; the home entry, a bare host, has none."""
    path = F.regexp_replace(url, r"^(https?:)?//[^/]+|[?#].*$", "")
    return F.try_element_at(F.filter(F.split(path, "/"), lambda part: part != ""), F.lit(-1))


def _breadcrumb_slugs(contexts: DataFrame) -> DataFrame:
    """Per page: its breadcrumb entries below the home page (slug, title), top first."""
    crumbs = contexts.select(
        F.from_json("payload_json", BREADCRUMB_SCHEMA)["mods"]["breadcrumb"].alias("crumbs"), "fetched_at"
    )
    entries = crumbs.select(
        F.filter(
            F.transform("crumbs", lambda c: F.struct(_slug(c["url"]).alias("slug"), c["title"].alias("title"))),
            lambda e: e["slug"].isNotNull(),
        ).alias("entries"),
        "fetched_at",
    ).filter(F.size("entries") > 0)
    return entries


def category_names(wide: DataFrame, contexts: DataFrame) -> DataFrame:
    """Each breadcrumb node's title; its slug is its id."""
    entries = _breadcrumb_slugs(contexts).select(F.explode("entries").alias("e"), "fetched_at")
    named = entries.select(
        F.lit(LAZADA.code).alias("marketplace"),
        F.col("e.slug").alias("category_id"),
        clean_text(F.col("e.title")).alias("category_name"),
        F.col("e.slug").alias("category_slug"),
        F.col("fetched_at").alias("observed_at"),
    )
    return typed(named, CATEGORY_NAME_COLUMNS)


def category_paths(contexts: DataFrame) -> DataFrame:
    """The page's breadcrumb as a path of slugs, top first."""
    paths = _breadcrumb_slugs(contexts).select(
        F.lit(LAZADA.code).alias("marketplace"),
        F.array_join(F.transform("entries", lambda e: e["slug"]), "/").alias("category_path"),
        F.col("fetched_at").alias("observed_at"),
    )
    return typed(paths, CATEGORY_PATH_COLUMNS)
