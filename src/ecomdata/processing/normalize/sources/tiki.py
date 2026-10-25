"""Tiki: the payload schema and the mapping to the canonical observation columns.

PAYLOAD_SCHEMA declares every field seen in Tiki's category-page product rows
(136 leaf fields across the 45-row page probed 2026-10-24 and the stub fixture),
typed. to_wide is the typed table of those rows; to_offer_observation
derives the canonical columns from the same rows by the rules of the pre-ELT
adapter, so offer and observation ids stay continuous with it.

Fields Tiki adds later are not lost: Bronze/records keeps them in the payload,
payload_json carries them into the wide table, and the Silver drift report
names them.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from ecomdata.domain.sources import TIKI
from ecomdata.processing.normalize.common import (
    SourceFields,
    availability_expr,
    canonical,
    json_path,
    listing_rows,
)

PAYLOAD_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("sku", StringType()),
    StructField("name", StringType()),
    StructField("url_key", StringType()),
    StructField("url_path", StringType()),
    StructField("availability", LongType()),
    StructField("seller_id", LongType()),
    StructField("seller_name", StringType()),
    StructField("brand_id", LongType()),
    StructField("brand_name", StringType()),
    StructField("price", LongType()),
    StructField("original_price", LongType()),
    StructField("badges_new", ArrayType(StructType([
        StructField("placement", StringType()),
        StructField("type", StringType()),
        StructField("code", StringType()),
        StructField("icon", StringType()),
        StructField("icon_width", LongType()),
        StructField("icon_height", LongType()),
        StructField("text", StringType()),
        StructField("text_color", StringType()),
    ]))),
    StructField("badges_v3", ArrayType(StructType([
        StructField("placement", StringType()),
        StructField("type", StringType()),
        StructField("code", StringType()),
        StructField("image", StringType()),
        StructField("text", StringType()),
        StructField("style", StructType([
            StructField("text_color", StringType()),
            StructField("background", StringType()),
            StructField("border_color", StringType()),
        ])),
    ]))),
    StructField("discount", LongType()),
    StructField("discount_rate", DoubleType()),
    StructField("rating_average", DoubleType()),
    StructField("review_count", LongType()),
    StructField("category_ids", ArrayType(LongType())),
    StructField("primary_category_path", StringType()),
    StructField("primary_category_name", StringType()),
    StructField("thumbnail_url", StringType()),
    StructField("thumbnail_width", LongType()),
    StructField("thumbnail_height", LongType()),
    StructField("productset_id", LongType()),
    StructField("seller_product_id", LongType()),
    StructField("seller_product_sku", StringType()),
    StructField("master_product_sku", StringType()),
    StructField("quantity_sold", StructType([
        StructField("text", StringType()),
        StructField("value", LongType()),
    ])),
    StructField("shippable", BooleanType()),
    StructField("isGiftAvailable", BooleanType()),
    StructField("is_tikinow_delivery", BooleanType()),
    StructField("is_nextday_delivery", BooleanType()),
    StructField("is_from_official_store", BooleanType()),
    StructField("advertisement", StructType([
        StructField("ad", ArrayType(StructType([
            StructField("clickUrl", StringType()),
            StructField("impUrl", StringType()),
            StructField("trueImpUrl", StringType()),
            StructField("properties", StructType([
                StructField("product_id", StringType()),
                StructField("url", StringType()),
                StructField("cta", StringType()),
            ])),
            StructField("type", StringType()),
        ]))),
    ])),
    StructField("is_authentic", LongType()),
    StructField("tiki_verified", LongType()),
    StructField("tiki_hero", LongType()),
    StructField("origin", StringType()),
    StructField("impression_info", ArrayType(StructType([
        StructField("impression_id", StringType()),
        StructField("metadata", StructType([
            StructField("request_id", StringType()),
            StructField("params", StructType([
                StructField("include", StringType()),
                StructField("_v", StringType()),
                StructField("limit", StringType()),
                StructField("page", StringType()),
                StructField("category", StringType()),
                StructField("aggregations", StringType()),
                StructField("version", StringType()),
                StructField("trackity_id", StringType()),
            ])),
            StructField("delivery_zone", StringType()),
            StructField("category", LongType()),
            StructField("mpid", LongType()),
            StructField("spid", LongType()),
            StructField("quantity_sold", LongType()),
            StructField("astra_reward", LongType()),
            StructField("price", LongType()),
            StructField("discount_rate", DoubleType()),
            StructField("rating", LongType()),
            StructField("is_ad", LongType()),
            StructField("is_official_store", LongType()),
            StructField("is_trusted_store", LongType()),
            StructField("is_tikinow", LongType()),
            StructField("is_tikipro", LongType()),
            StructField("is_astra_plus", LongType()),
            StructField("position", LongType()),
            StructField("tiki_verified", LongType()),
            StructField("flags", StructType([
                StructField("_v", StringType()),
            ])),
            StructField("delivery_info_badge_text", StringType()),
            StructField("delivery_date", StringType()),
        ])),
    ]))),
    StructField("visible_impression_info", StructType([
        StructField("amplitude", StructType([
            StructField("seller_type", StringType()),
            StructField("seller_id", LongType()),
            StructField("all_time_quantity_sold", LongType()),
            StructField("brand_name", StringType()),
            StructField("category_l1_name", StringType()),
            StructField("is_best_offer_available", BooleanType()),
            StructField("is_flash_deal", BooleanType()),
            StructField("is_gift_available", BooleanType()),
            StructField("master_product_sku", StringType()),
            StructField("seller_product_id", LongType()),
            StructField("seller_product_sku", StringType()),
            StructField("number_of_reviews", LongType()),
            StructField("order_route", StringType()),
            StructField("partner_rewards_amount", LongType()),
            StructField("price", LongType()),
            StructField("primary_category_name", StringType()),
            StructField("product_rating", LongType()),
            StructField("search_rank", LongType()),
            StructField("tiki_verified", LongType()),
            StructField("is_authentic", LongType()),
            StructField("standard_delivery_estimate", DoubleType()),
            StructField("earliest_delivery_estimate", LongType()),
            StructField("tikipro_delivery_estimate", LongType()),
            StructField("tikinow_delivery_estimate", DoubleType()),
            StructField("layout", StringType()),
            StructField("variant", BooleanType()),
            StructField("is_hero", BooleanType()),
            StructField("is_imported", BooleanType()),
            StructField("origin", StringType()),
            StructField("is_high_price_penalty", BooleanType()),
            StructField("deboosted_high_price_diff_position", LongType()),
            StructField("joined_strategy_rerank", BooleanType()),
            StructField("is_freeship_xtra", BooleanType()),
            StructField("is_top_brand", BooleanType()),
        ])),
    ])),
    StructField("layout_type", StringType()),
    StructField("is_high_price_penalty", BooleanType()),
    StructField("is_top_brand", BooleanType()),
    StructField("uiConfig", StructType([
        StructField("showUnderProductImage", BooleanType()),
        StructField("showUnderProductImage2", BooleanType()),
        StructField("isShowOverlayProductImg", BooleanType()),
    ])),
    StructField("is_normal_delivery", BooleanType()),
    StructField("fastest_delivery_time", StringType()),
    StructField("order_route", StringType()),
    StructField("freeship_campaign", StringType()),
    StructField("video_url", StringType()),
    StructField("imported", StringType()),
])



# Rating scale of Tiki's rating_average.
RATING_SCALE = "5"


def to_wide(records: DataFrame) -> DataFrame:
    """Listing rows with every declared payload field as a typed column p (and payload_json)."""
    rows = listing_rows(records)
    return rows.withColumn("p", F.from_json("payload_json", PAYLOAD_SCHEMA))


def _is_advertisement() -> F.Column:
    """A non-empty advertisement object marks a sponsored placement; absent or null is organic."""
    advert = json_path("$.advertisement")
    return advert.isNotNull() & (advert != "") & (advert != "null") & (advert != "{}") & (advert != "[]")


def fields() -> SourceFields:
    original = json_path("$.original_price").try_cast("decimal(38,6)")
    listed = json_path("$.list_price").try_cast("decimal(38,6)")
    return SourceFields(
        listing_id=json_path("$.id"),
        title=json_path("$.name"),
        price=json_path("$.price"),
        url=F.concat(F.lit(TIKI.base_url + "/"), F.regexp_replace(json_path("$.url_key"), "^/+", ""), F.lit(".html")),
        list_price=F.when(listed > 0, listed).otherwise(F.when(original > 0, original)),
        seller_platform_id=json_path("$.seller_id"),
        seller_name=json_path("$.seller_name"),
        brand=json_path("$.brand_name"),
        brand_id=json_path("$.brand_id"),
        category_path=json_path("$.primary_category_path"),
        category_ids=F.from_json(json_path("$.category_ids"), "array<string>"),
        discount_amount=json_path("$.discount"),
        discount_percent=json_path("$.discount_rate"),
        rating_value=json_path("$.rating_average"),
        rating_scale=F.lit(RATING_SCALE),
        review_count=json_path("$.review_count"),
        sold_count=json_path("$.quantity_sold.value"),
        availability=availability_expr(json_path("$.availability"), json_path("$.shippable")),
        sku=json_path("$.sku"),
        is_advertisement=_is_advertisement(),
    )


def to_offer_observation(records: DataFrame) -> DataFrame:
    """Canonical columns of Tiki's listing rows, and reason for a row that cannot be an observation."""
    return canonical(listing_rows(records), TIKI, fields())
