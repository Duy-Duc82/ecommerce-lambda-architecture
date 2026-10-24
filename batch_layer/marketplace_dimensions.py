"""Dimensions of the marketplace star, built from Silver observations.

``docs/DATA_MODEL.md`` sections 4.4–4.6. Every builder is a pure function of
its input frames: the same observations give the same rows and the same keys,
which is what lets a replay, a backfill and an incremental run agree.

Input is the flattened observation frame of
``marketplace_warehouse.flatten_marketplace_observations``.
"""
from __future__ import annotations

from typing import Mapping

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from batch_layer.dim_date import build_dim_date
from batch_layer.marketplace_keys import (
    OPEN_VALID_TO,
    UNKNOWN_KEY,
    UNKNOWN_LABEL,
    UNKNOWN_NATURAL_KEY,
    UNKNOWN_VALID_FROM,
    date_key,
    key_or_unknown,
    surrogate_key,
    with_unknown_member,
)

__all__ = [
    "build_dim_date", "build_dim_marketplace", "build_dim_seller", "build_dim_category",
    "build_bridge_category_ancestry", "build_dim_product_listing", "build_dim_offer_status",
    "marketplace_key", "seller_key", "category_key", "offer_key", "offer_status_key", "date_key",
]

# Attributes the observations do not carry. A marketplace missing here still
# gets a row, with these columns empty.
KNOWN_MARKETPLACES: Mapping[str, dict[str, str]] = {
    "tiki": {"marketplace_name": "Tiki", "platform_type": "marketplace", "country": "VN", "default_currency": "VND"},
}

# Attributes that open a new dim_product_listing version (section 4.6).
SCD2_ATTRIBUTES = ("product_title", "brand", "category_key", "seller_key")

AVAILABILITY_VALUES = ("IN_STOCK", "OUT_OF_STOCK", "UNKNOWN")
ACTIVE_STATUS_VALUES = ("ACTIVE", "INACTIVE", "UNKNOWN")

# A frontier target of the HTML adapter: <slug>/c<id>.
_TARGET_SLUG_ID = r"^([^/]+)/c(\d+)$"


# ---------- keys a fact computes the same way the dimension does ----------

def marketplace_key(marketplace: str | Column = "marketplace") -> Column:
    return key_or_unknown(marketplace)


def seller_key(seller_id: str | Column = "seller_id") -> Column:
    return key_or_unknown(seller_id)


def _path_ids(category_path: str | Column) -> Column:
    path = F.col(category_path) if isinstance(category_path, str) else category_path
    return F.filter(F.split(path, "/"), lambda part: part != "")


def category_key(marketplace: str | Column = "marketplace", category_path: str | Column = "category_path") -> Column:
    """Key of the leaf node of an observation's path; ``-1`` without a path."""
    ids = _path_ids(category_path)
    leaf = F.when(F.size(ids) > 0, F.element_at(ids, -1))
    return key_or_unknown(marketplace, leaf)


def offer_key(offer_id: str | Column = "offer_id") -> Column:
    return key_or_unknown(offer_id)


def has_discount(list_price: str = "list_price", current_price: str = "current_price",
                 discount_percent: str = "discount_percent") -> Column:
    return F.coalesce(F.col(list_price) > F.col(current_price), F.lit(False)) | F.coalesce(F.col(discount_percent) > 0, F.lit(False))


def has_rating(rating_count: str = "rating_count") -> Column:
    # A count of 0 means not yet rated, which is not a rating of 0.
    return F.coalesce(F.col(rating_count) > 0, F.lit(False))


def offer_status_key(availability: str | Column = "availability", active_status: str | Column = "active_status",
                     discount: Column | None = None, rating: Column | None = None) -> Column:
    """Ordinal 1-36 of the combination, as ``build_dim_offer_status`` numbers it."""
    availability = F.col(availability) if isinstance(availability, str) else availability
    active_status = F.col(active_status) if isinstance(active_status, str) else active_status
    discount = has_discount() if discount is None else discount
    rating = has_rating() if rating is None else rating
    a = F.array_position(F.array(*[F.lit(v) for v in AVAILABILITY_VALUES]), availability)
    s = F.array_position(F.array(*[F.lit(v) for v in ACTIVE_STATUS_VALUES]), active_status)
    ordinal = 1 + (a - 1) * 12 + (s - 1) * 4 + discount.cast("int") * 2 + rating.cast("int")
    known = (a > 0) & (s > 0) & discount.isNotNull() & rating.isNotNull()
    return F.when(F.coalesce(known, F.lit(False)), ordinal).otherwise(F.lit(UNKNOWN_KEY)).cast("smallint")


# ---------- dimensions ----------

def build_dim_marketplace(observations: DataFrame) -> DataFrame:
    spark = observations.sparkSession
    attributes = spark.createDataFrame(
        [(code, a["marketplace_name"], a["platform_type"], a["country"], a["default_currency"]) for code, a in KNOWN_MARKETPLACES.items()],
        "marketplace_code string, marketplace_name string, platform_type string, country string, default_currency string",
    )
    seen = (observations.groupBy(F.col("marketplace").alias("marketplace_code"))
            .agg(F.max("marketplace_id").alias("marketplace_id"), F.min("observed_at").alias("first_crawled_at")))
    built = seen.join(attributes, "marketplace_code", "left").select(
        marketplace_key("marketplace_code").alias("marketplace_key"), "marketplace_code", "marketplace_id",
        "marketplace_name", "platform_type", "country", "default_currency", "first_crawled_at",
    )
    return with_unknown_member(built, {"marketplace_key": UNKNOWN_KEY, "marketplace_code": UNKNOWN_NATURAL_KEY,
                                       "marketplace_name": UNKNOWN_LABEL})


def build_dim_seller(observations: DataFrame) -> DataFrame:
    built = (observations.filter(F.col("seller_id").isNotNull())
             .groupBy("seller_id")
             # seller_id is scoped to its marketplace (make_seller_id), so one
             # seller has one marketplace; max() only makes that explicit.
             .agg(F.max("marketplace").alias("marketplace"), F.min("observed_at").alias("first_seen_at"),
                  F.max("observed_at").alias("last_seen_at"), F.countDistinct("offer_id").cast("int").alias("observed_listing_count"))
             .select(seller_key().alias("seller_key"), "seller_id", marketplace_key().alias("marketplace_key"),
                     "first_seen_at", "last_seen_at", "observed_listing_count"))
    return with_unknown_member(built, {"seller_key": UNKNOWN_KEY, "seller_id": UNKNOWN_NATURAL_KEY,
                                       "marketplace_key": UNKNOWN_KEY})


def build_dim_category(observations: DataFrame, frontier: DataFrame | None = None) -> DataFrame:
    """One row per node on any observed category path.

    If a node shows up under two different paths, the path observed most
    recently wins (ties: the greater path string), so the tree stays a tree.
    ``frontier`` (``marketplace_code``, ``target``) supplies slugs for the
    category pages it crawls.
    """
    paths = (observations.filter(F.size(_path_ids("category_path")) > 0)
             .groupBy("marketplace", "category_path")
             .agg(F.max("observed_at").alias("_last_seen"))
             .withColumn("_ids", _path_ids("category_path")))
    nodes = paths.select("marketplace", "_last_seen", "_ids", F.posexplode("_ids").alias("_pos", "category_id"))
    nodes = nodes.select(
        "marketplace", "category_id", "_last_seen",
        F.array_join(F.slice("_ids", 1, F.col("_pos") + 1), "/").alias("full_path"),
        F.when(F.col("_pos") > 0, F.element_at("_ids", F.col("_pos"))).alias("_parent_id"),
        (F.col("_pos") + 1).cast("smallint").alias("level"),
    )
    winner = Window.partitionBy("marketplace", "category_id").orderBy(F.col("_last_seen").desc(), F.col("full_path").desc())
    chosen = nodes.withColumn("_rn", F.row_number().over(winner)).filter("_rn = 1").drop("_rn", "_last_seen")
    parents = chosen.filter(F.col("_parent_id").isNotNull()).select(
        F.col("marketplace").alias("_pm"), F.col("_parent_id").alias("_pid")).distinct()
    chosen = (chosen.join(parents, (chosen.marketplace == parents._pm) & (chosen.category_id == parents._pid), "left")
              .withColumn("is_leaf", F.col("_pid").isNull()).drop("_pm", "_pid"))
    if frontier is not None:
        slugs = (frontier.select(F.col("marketplace_code").alias("_sm"),
                                 F.regexp_extract("target", _TARGET_SLUG_ID, 1).alias("_slug"),
                                 F.regexp_extract("target", _TARGET_SLUG_ID, 2).alias("_sid"))
                 .filter(F.col("_sid") != "")
                 .groupBy("_sm", "_sid").agg(F.max("_slug").alias("category_slug")))
        chosen = (chosen.join(slugs, (chosen.marketplace == slugs._sm) & (chosen.category_id == slugs._sid), "left")
                  .drop("_sm", "_sid"))
    else:
        chosen = chosen.withColumn("category_slug", F.lit(None).cast("string"))
    built = chosen.select(
        key_or_unknown("marketplace", "category_id").alias("category_key"),
        marketplace_key().alias("marketplace_key"),
        "category_id",
        # Names are not on the path, only ids (plan risk R1).
        F.lit(None).cast("string").alias("category_name"),
        "category_slug",
        F.when(F.col("_parent_id").isNotNull(), surrogate_key("marketplace", "_parent_id")).alias("parent_key"),
        "level", "full_path", "is_leaf",
    )
    return with_unknown_member(built, {"category_key": UNKNOWN_KEY, "marketplace_key": UNKNOWN_KEY,
                                       "category_id": UNKNOWN_NATURAL_KEY, "category_name": UNKNOWN_LABEL,
                                       "level": 0, "full_path": UNKNOWN_NATURAL_KEY, "is_leaf": True})


def build_bridge_category_ancestry(dim_category: DataFrame) -> DataFrame:
    """Closure of ``parent_key``: every (ancestor, descendant) pair, each node with itself at depth 0."""
    edges = dim_category.filter(F.col("parent_key").isNotNull()).select(
        F.col("category_key").alias("_child"), F.col("parent_key").alias("_parent"))
    level = dim_category.select("category_key", F.col("category_key").alias("ancestor_key"))
    closure = dim_category.select(F.col("category_key").alias("ancestor_key"),
                                  F.col("category_key").alias("descendant_key"), F.lit(0).cast("smallint").alias("depth"))
    max_level = dim_category.agg(F.max("level")).first()[0] or 0
    # One step up per pass. The tree is at most max_level deep, so more passes
    # than that can only come from a cycle, which the loop bound cuts off.
    for depth in range(1, max_level + 1):
        level = (level.join(edges, level.ancestor_key == edges._child)
                 .select("category_key", F.col("_parent").alias("ancestor_key")))
        if level.limit(1).count() == 0:
            break
        closure = closure.unionByName(level.select("ancestor_key", F.col("category_key").alias("descendant_key"),
                                                   F.lit(depth).cast("smallint").alias("depth")))
    return closure


def build_dim_offer_status(spark: SparkSession) -> DataFrame:
    rows = []
    for a, availability in enumerate(AVAILABILITY_VALUES):
        for s, active_status in enumerate(ACTIVE_STATUS_VALUES):
            for discount in (False, True):
                for rating in (False, True):
                    rows.append((1 + a * 12 + s * 4 + int(discount) * 2 + int(rating), availability, active_status, discount, rating))
    rows.append((UNKNOWN_KEY, UNKNOWN_NATURAL_KEY, UNKNOWN_NATURAL_KEY, None, None))
    return spark.createDataFrame(rows, "offer_status_key smallint, availability string, active_status string, "
                                       "has_discount boolean, has_rating boolean")


def _same(left: str, right: str) -> Column:
    return F.col(left).eqNullSafe(F.col(right))


def build_dim_product_listing(observations: DataFrame) -> DataFrame:
    """SCD2 versions of each offer (section 4.6).

    Sightings at the same instant are first reduced to the last one by the
    marts' tie-break, so no version can open and close at the same instant and
    ``(offer_id, valid_from)`` stays unique.
    """
    order = ["observed_at", "fetched_at", "observation_id"]
    tracked = observations.select(
        "offer_id", "marketplace", "platform_listing_id", "source_url", *order, "product_title", "brand",
        category_key().alias("category_key"), seller_key().alias("seller_key"))
    per_instant = Window.partitionBy("offer_id", "observed_at").orderBy(F.col("fetched_at").desc(), F.col("observation_id").desc())
    instants = tracked.withColumn("_rn", F.row_number().over(per_instant)).filter("_rn = 1").drop("_rn")

    by_time = Window.partitionBy("offer_id").orderBy("observed_at")
    for name in SCD2_ATTRIBUTES:
        instants = instants.withColumn(f"_prev_{name}", F.lag(name).over(by_time))
    unchanged = F.lit(True)
    for name in SCD2_ATTRIBUTES:
        unchanged = unchanged & _same(name, f"_prev_{name}")
    first = F.row_number().over(by_time) == 1
    starts = instants.withColumn("_opens", first | ~unchanged).filter("_opens")

    versions = Window.partitionBy("offer_id").orderBy("observed_at")
    latest = Window.partitionBy("offer_id").orderBy(F.col("observed_at").desc())
    # SCD1 attributes: the latest sighting's value, on every version.
    scd1 = (instants.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1")
            .select("offer_id", F.col("source_url").alias("_latest_url"), F.col("platform_listing_id").alias("_latest_pid")))
    built = (starts.withColumn("valid_from", F.col("observed_at"))
             .withColumn("valid_to", F.coalesce(F.lead("observed_at").over(versions), F.lit(OPEN_VALID_TO).cast("timestamp")))
             .withColumn("version", F.row_number().over(versions))
             .withColumn("is_current", F.lead("observed_at").over(versions).isNull())
             .join(scd1, "offer_id")
             .select(
                 surrogate_key("offer_id", "valid_from").alias("listing_key"),
                 offer_key().alias("offer_key"),
                 "offer_id",
                 marketplace_key().alias("marketplace_key"),
                 F.col("_latest_pid").alias("platform_listing_id"),
                 "product_title", "brand", "category_key", "seller_key",
                 F.col("_latest_url").alias("source_url"),
                 "valid_from", "valid_to", "is_current", F.col("version").cast("int").alias("version"),
             ))
    return with_unknown_member(built, {
        "listing_key": UNKNOWN_KEY, "offer_key": UNKNOWN_KEY, "offer_id": UNKNOWN_NATURAL_KEY,
        "marketplace_key": UNKNOWN_KEY, "product_title": UNKNOWN_LABEL, "category_key": UNKNOWN_KEY,
        "seller_key": UNKNOWN_KEY, "valid_from": UNKNOWN_VALID_FROM, "valid_to": OPEN_VALID_TO,
        "is_current": True, "version": 0,
    })


def build_marketplace_dimensions(observations: DataFrame, frontier: DataFrame | None = None) -> dict[str, DataFrame]:
    spark = observations.sparkSession
    dim_category = build_dim_category(observations, frontier)
    return {
        "dim_date": build_dim_date(spark),
        "dim_marketplace": build_dim_marketplace(observations),
        "dim_seller": build_dim_seller(observations),
        "dim_category": dim_category,
        "bridge_category_ancestry": build_bridge_category_ancestry(dim_category),
        "dim_product_listing": build_dim_product_listing(observations),
        "dim_offer_status": build_dim_offer_status(spark),
    }
