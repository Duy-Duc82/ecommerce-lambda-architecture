"""Dimensions of the marketplace star, built from Silver.

``docs/DATA_MODEL.md`` sections 4.4–4.6. Every builder is a pure function of
its input frames: the same observations give the same rows and the same keys,
which is what lets a replay, a backfill and an incremental run agree.

Input is Silver's canonical observations joined to their listing attributes
(``star_inputs.star_inputs``), plus the category names and paths the pages
state. A frame without the attribute columns reads them as NULL.
"""

from __future__ import annotations

import itertools

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ecomdata.domain.sources import SOURCES, SourceRegistry
from ecomdata.processing.batch.dim_date import build_dim_date
from ecomdata.processing.batch.keys import (
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
from ecomdata.processing.batch.star_inputs import with_listing_columns

__all__ = [
    "build_dim_date",
    "build_dim_marketplace",
    "build_dim_seller",
    "build_dim_brand",
    "build_dim_category",
    "build_bridge_category_ancestry",
    "build_dim_product_listing",
    "build_dim_listing_flags",
    "marketplace_key",
    "seller_key",
    "brand_key",
    "category_key",
    "page_category_key",
    "offer_key",
    "listing_flags_key",
    "date_key",
]

# Attributes the observations do not carry. A marketplace missing here still
# gets a row, with these columns empty.
def known_marketplaces(registry: SourceRegistry = SOURCES) -> dict[str, dict[str, str]]:
    """Attributes the observations do not carry, from the source registry.

    A marketplace missing from the registry still gets a row, with these
    columns empty.
    """
    return {spec.code: {"marketplace_name": spec.name, "platform_type": spec.platform_type, "country": spec.country,
                        "default_currency": spec.currency} for spec in registry}

# Attributes that open a new dim_product_listing version (section 4.6).
SCD2_ATTRIBUTES = ("product_title", "brand_key", "category_key", "seller_key")
# Overwritten on every version with the latest value seen (SCD1).
LISTING_SCD1_ATTRIBUTES = (
    "platform_listing_id",
    "sku",
    "master_product_sku",
    "origin",
    "is_authentic",
    "is_platform_verified",
    "source_url",
)
# The order of the sightings of one offer; the marts' tie-break.
SIGHTING_ORDER = ("observed_at", "fetched_at", "observation_id")

AVAILABILITY_VALUES = ("IN_STOCK", "OUT_OF_STOCK", "UNKNOWN")
ACTIVE_STATUS_VALUES = ("ACTIVE", "INACTIVE", "UNKNOWN")
# dim_listing_flags beyond the four of the old dim_offer_status; NULL means the
# source does not publish the flag.
NULLABLE_FLAGS = (
    "has_free_shipping",
    "is_express_delivery",
    "is_next_day_delivery",
    "is_flash_deal",
    "is_gift_available",
)
BREADCRUMB_SEPARATOR = " > "
NAMED_LEVELS = 4

# A frontier target of the HTML adapter: <slug>/c<id>.
_TARGET_SLUG_ID = r"^([^/]+)/c(\d+)$"
# Any other request target that names a category page is a slug path (Lazada:
# dien-thoai-di-dong, or a/b), whose node is its last slug, as category_key reads it.
_TARGET_SLUG_PATH = r"^[a-z0-9]+(?:-[a-z0-9]+)*(?:/[a-z0-9]+(?:-[a-z0-9]+)*)*$"


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


def page_category_id(request_target: str | Column = "request_target") -> Column:
    """The category id a request target names, or NULL for any other target."""
    target = F.col(request_target) if isinstance(request_target, str) else request_target
    tiki_id = F.regexp_extract(target, _TARGET_SLUG_ID, 2)
    slug = F.try_element_at(_path_ids(target), F.lit(-1))
    return F.when(tiki_id != "", tiki_id).when(target.rlike(_TARGET_SLUG_PATH), slug)


def page_category_key(
    marketplace: str | Column = "marketplace", request_target: str | Column = "request_target"
) -> Column:
    """Key of the category page a sighting was found on; ``-1`` for any other page."""
    return key_or_unknown(marketplace, page_category_id(request_target))


def brand_key(marketplace: str | Column = "marketplace", brand_id: str | Column = "brand_id") -> Column:
    return key_or_unknown(marketplace, brand_id)


def offer_key(offer_id: str | Column = "offer_id") -> Column:
    return key_or_unknown(offer_id)


def latest(column: str, order: tuple[str, ...] = SIGHTING_ORDER) -> Column:
    """Aggregate: the value of ``column`` on the latest sighting that has one."""
    return F.max_by(column, F.when(F.col(column).isNotNull(), F.struct(*order)))


def has_discount(
    list_price: str = "list_price", current_price: str = "current_price", discount_percent: str = "discount_percent"
) -> Column:
    return F.coalesce(F.col(list_price) > F.col(current_price), F.lit(False)) | F.coalesce(
        F.col(discount_percent) > 0, F.lit(False)
    )


def has_rating(rating_count: str = "rating_count") -> Column:
    # A count of 0 means not yet rated, which is not a rating of 0.
    return F.coalesce(F.col(rating_count) > 0, F.lit(False))


def _flag_token(flag: Column) -> Column:
    # xxhash64 skips NULL inputs, so NULL is hashed as a token of its own.
    return F.when(flag, "t").when(~flag, "f").otherwise("n")


def listing_flags_key(
    availability: str | Column = "availability",
    active_status: str | Column = "active_status",
    discount: Column | None = None,
    rating: Column | None = None,
    **flags: Column,
) -> Column:
    """``xxhash64`` of the nine flags, as ``build_dim_listing_flags`` keys them; ``-1`` if a fixed flag is unknown.

    ``flags`` overrides the column of a nullable flag (``NULLABLE_FLAGS``).
    """
    availability = F.col(availability) if isinstance(availability, str) else availability
    active_status = F.col(active_status) if isinstance(active_status, str) else active_status
    discount = has_discount() if discount is None else discount
    rating = has_rating() if rating is None else rating
    unknown = set(flags) - set(NULLABLE_FLAGS)
    if unknown:
        raise ValueError(f"not a nullable listing flag: {sorted(unknown)}")
    nullable = [flags.get(name, F.col(name)) for name in NULLABLE_FLAGS]
    known = (
        availability.isin(*AVAILABILITY_VALUES)
        & active_status.isin(*ACTIVE_STATUS_VALUES)
        & discount.isNotNull()
        & rating.isNotNull()
    )
    hashed = surrogate_key(
        availability, active_status, _flag_token(discount), _flag_token(rating), *[_flag_token(f) for f in nullable]
    )
    return F.when(F.coalesce(known, F.lit(False)), hashed).otherwise(F.lit(UNKNOWN_KEY).cast("bigint"))


# ---------- dimensions ----------


def build_dim_marketplace(observations: DataFrame) -> DataFrame:
    spark = observations.sparkSession
    attributes = spark.createDataFrame(
        [
            (code, a["marketplace_name"], a["platform_type"], a["country"], a["default_currency"])
            for code, a in known_marketplaces().items()
        ],
        "marketplace_code string, marketplace_name string, platform_type string, country string, default_currency string",
    )
    seen = observations.groupBy(F.col("marketplace").alias("marketplace_code")).agg(
        F.max("marketplace_id").alias("marketplace_id"), F.min("observed_at").alias("first_crawled_at")
    )
    built = seen.join(attributes, "marketplace_code", "left").select(
        marketplace_key("marketplace_code").alias("marketplace_key"),
        "marketplace_code",
        "marketplace_id",
        "marketplace_name",
        "platform_type",
        "country",
        "default_currency",
        "first_crawled_at",
    )
    return with_unknown_member(
        built,
        {"marketplace_key": UNKNOWN_KEY, "marketplace_code": UNKNOWN_NATURAL_KEY, "marketplace_name": UNKNOWN_LABEL},
    )


def build_dim_seller(observations: DataFrame) -> DataFrame:
    observations = with_listing_columns(observations)
    built = (
        observations.filter(F.col("seller_id").isNotNull())
        .groupBy("seller_id")
        # seller_id is scoped to its marketplace (make_seller_id), so one
        # seller has one marketplace; max() only makes that explicit.
        .agg(
            F.max("marketplace").alias("marketplace"),
            latest("seller_name").alias("seller_name"),
            latest("seller_type").alias("seller_type"),
            latest("is_official_store").alias("is_official_store"),
            latest("is_trusted_store").alias("is_trusted_store"),
            F.min("observed_at").alias("first_seen_at"),
            F.max("observed_at").alias("last_seen_at"),
            F.countDistinct("offer_id").cast("int").alias("observed_listing_count"),
        )
        .select(
            seller_key().alias("seller_key"),
            "seller_id",
            marketplace_key().alias("marketplace_key"),
            "seller_name",
            "seller_type",
            "is_official_store",
            "is_trusted_store",
            "first_seen_at",
            "last_seen_at",
            "observed_listing_count",
        )
    )
    return with_unknown_member(
        built,
        {
            "seller_key": UNKNOWN_KEY,
            "seller_id": UNKNOWN_NATURAL_KEY,
            "marketplace_key": UNKNOWN_KEY,
            "seller_name": UNKNOWN_LABEL,
        },
    )


def build_dim_brand(observations: DataFrame) -> DataFrame:
    """One row per (marketplace, brand id); a name without an id makes no row (section 4.4)."""
    observations = with_listing_columns(observations)
    built = (
        observations.filter(F.col("brand_id").isNotNull())
        .groupBy("marketplace", "brand_id")
        .agg(
            latest("brand").alias("brand_name"),
            F.min("observed_at").alias("first_seen_at"),
            F.max("observed_at").alias("last_seen_at"),
            F.countDistinct("offer_id").cast("int").alias("observed_listing_count"),
        )
        .select(
            brand_key().alias("brand_key"),
            marketplace_key().alias("marketplace_key"),
            "brand_id",
            "brand_name",
            "first_seen_at",
            "last_seen_at",
            "observed_listing_count",
        )
    )
    return with_unknown_member(
        built,
        {
            "brand_key": UNKNOWN_KEY,
            "marketplace_key": UNKNOWN_KEY,
            "brand_id": UNKNOWN_NATURAL_KEY,
            "brand_name": UNKNOWN_LABEL,
        },
    )


def _latest_names(names: DataFrame) -> DataFrame:
    """(``_nm``, ``_nid``) -> the latest name and slug any page gave the node."""
    return names.groupBy(F.col("marketplace").alias("_nm"), F.col("category_id").alias("_nid")).agg(
        latest("category_name", ("observed_at",)).alias("_name"),
        latest("category_slug", ("observed_at",)).alias("_name_slug"),
    )


def build_dim_category(
    observations: DataFrame,
    frontier: DataFrame | None = None,
    names: DataFrame | None = None,
    page_paths: DataFrame | None = None,
) -> DataFrame:
    """One row per node on any observed category path (section 4.4).

    Paths come from the observations and, where a source states them, from its
    pages (``page_paths``, ``CATEGORY_PATH_COLUMNS``). If a node shows up under
    two different paths, a path a page states wins over one a listing implies,
    then the path observed most recently, then the greater path string, so the
    tree stays a tree. ``frontier`` (``marketplace_code``, ``target``) and
    ``names`` (``CATEGORY_NAME_COLUMNS``) supply names and slugs.

    A category page that was requested (a sighting's ``request_target``, or a
    frontier target such as a crawl attempt that failed) but whose node no path
    names yet is an **inferred member**: a row with its natural key, its slug,
    and NULL ``level`` and ``parent_key``, so every fact that points at the page
    resolves. Once a page states the node's path, the full row replaces it under
    the same key.
    """
    from_rows = (
        observations.filter(F.size(_path_ids("category_path")) > 0)
        .groupBy("marketplace", "category_path")
        .agg(F.max("observed_at").alias("_last_seen"))
        .withColumn("_stated", F.lit(0))
    )
    paths = from_rows
    if page_paths is not None:
        stated = (
            page_paths.filter(F.size(_path_ids("category_path")) > 0)
            .groupBy("marketplace", "category_path")
            .agg(F.max("observed_at").alias("_last_seen"))
            .withColumn("_stated", F.lit(1))
        )
        paths = paths.unionByName(stated)
    paths = paths.withColumn("_ids", _path_ids("category_path"))
    nodes = paths.select(
        "marketplace", "_last_seen", "_stated", "_ids", F.posexplode("_ids").alias("_pos", "category_id")
    )
    nodes = nodes.select(
        "marketplace",
        "category_id",
        "_last_seen",
        "_stated",
        F.array_join(F.slice("_ids", 1, F.col("_pos") + 1), "/").alias("full_path"),
        F.when(F.col("_pos") > 0, F.element_at("_ids", F.col("_pos"))).alias("_parent_id"),
        (F.col("_pos") + 1).cast("smallint").alias("level"),
    )
    winner = Window.partitionBy("marketplace", "category_id").orderBy(
        F.col("_stated").desc(), F.col("_last_seen").desc(), F.col("full_path").desc()
    )
    chosen = (
        nodes.withColumn("_rn", F.row_number().over(winner)).filter("_rn = 1").drop("_rn", "_last_seen", "_stated")
    )
    if "request_target" in observations.columns:
        requested = observations.select("marketplace", F.col("request_target").alias("_target"))
    else:
        requested = observations.select("marketplace", F.lit(None).cast("string").alias("_target"))
    if frontier is not None:
        requested = requested.unionByName(
            frontier.select(F.col("marketplace_code").alias("marketplace"), F.col("target").alias("_target"))
        )
    inferred = (
        requested.select("marketplace", page_category_id("_target").alias("category_id"))
        .filter(F.col("marketplace").isNotNull() & F.col("category_id").isNotNull())
        .distinct()
        .join(chosen.select("marketplace", "category_id"), ["marketplace", "category_id"], "left_anti")
        .select(
            "marketplace",
            "category_id",
            F.col("category_id").alias("full_path"),
            F.lit(None).cast("string").alias("_parent_id"),
            F.lit(None).cast("smallint").alias("level"),
        )
    )
    chosen = chosen.unionByName(inferred)
    parents = (
        chosen.filter(F.col("_parent_id").isNotNull())
        .select(F.col("marketplace").alias("_pm"), F.col("_parent_id").alias("_pid"))
        .distinct()
    )
    chosen = (
        chosen.join(parents, (chosen.marketplace == parents._pm) & (chosen.category_id == parents._pid), "left")
        .withColumn("is_leaf", F.col("_pid").isNull())
        .drop("_pm", "_pid")
    )
    if frontier is not None:
        slugs = (
            frontier.select(
                F.col("marketplace_code").alias("_sm"),
                F.regexp_extract("target", _TARGET_SLUG_ID, 1).alias("_slug"),
                F.regexp_extract("target", _TARGET_SLUG_ID, 2).alias("_sid"),
            )
            .filter(F.col("_sid") != "")
            .groupBy("_sm", "_sid")
            .agg(F.max("_slug").alias("_frontier_slug"))
        )
        chosen = chosen.join(
            slugs, (chosen.marketplace == slugs._sm) & (chosen.category_id == slugs._sid), "left"
        ).drop("_sm", "_sid")
    else:
        chosen = chosen.withColumn("_frontier_slug", F.lit(None).cast("string"))
    if names is not None:
        named = _latest_names(names)
        chosen = chosen.join(
            named, (chosen.marketplace == named._nm) & (chosen.category_id == named._nid), "left"
        ).drop("_nm", "_nid")
        # The names of the node's named ancestors and itself, top first.
        ancestry = chosen.select(
            "marketplace", "category_id", F.posexplode(_path_ids("full_path")).alias("_pos", "_aid")
        )
        lookup = named.filter(F.col("_name").isNotNull()).select("_nm", "_nid", "_name")
        trail = (
            ancestry.join(lookup, (ancestry.marketplace == lookup._nm) & (ancestry._aid == lookup._nid))
            .groupBy(F.col("marketplace").alias("_tm"), F.col("category_id").alias("_tid"))
            .agg(F.sort_array(F.collect_list(F.struct("_pos", "_name"))).alias("_trail"))
            .withColumn("_trail", F.transform("_trail", lambda step: step["_name"]))
        )
        chosen = chosen.join(
            trail, (chosen.marketplace == trail._tm) & (chosen.category_id == trail._tid), "left"
        ).drop("_tm", "_tid")
    else:
        chosen = chosen.select(
            "*",
            F.lit(None).cast("string").alias("_name"),
            F.lit(None).cast("string").alias("_name_slug"),
            F.lit(None).cast("array<string>").alias("_trail"),
        )
    built = chosen.select(
        key_or_unknown("marketplace", "category_id").alias("category_key"),
        marketplace_key().alias("marketplace_key"),
        "category_id",
        F.col("_name").alias("category_name"),
        F.coalesce("_frontier_slug", "_name_slug").alias("category_slug"),
        F.when(F.col("_parent_id").isNotNull(), surrogate_key("marketplace", "_parent_id")).alias("parent_key"),
        "level",
        "full_path",
        F.array_join("_trail", BREADCRUMB_SEPARATOR).alias("breadcrumb"),
        *[F.try_element_at("_trail", F.lit(n)).alias(f"category_l{n}_name") for n in range(1, NAMED_LEVELS + 1)],
        "is_leaf",
    )
    return with_unknown_member(
        built,
        {
            "category_key": UNKNOWN_KEY,
            "marketplace_key": UNKNOWN_KEY,
            "category_id": UNKNOWN_NATURAL_KEY,
            "category_name": UNKNOWN_LABEL,
            "level": 0,
            "full_path": UNKNOWN_NATURAL_KEY,
            "breadcrumb": UNKNOWN_LABEL,
            "is_leaf": True,
        },
    )


def build_bridge_category_ancestry(dim_category: DataFrame) -> DataFrame:
    """Closure of ``parent_key``: every (ancestor, descendant) pair, each node with itself at depth 0."""
    edges = dim_category.filter(F.col("parent_key").isNotNull()).select(
        F.col("category_key").alias("_child"), F.col("parent_key").alias("_parent")
    )
    level = dim_category.select("category_key", F.col("category_key").alias("ancestor_key"))
    closure = dim_category.select(
        F.col("category_key").alias("ancestor_key"),
        F.col("category_key").alias("descendant_key"),
        F.lit(0).cast("smallint").alias("depth"),
    )
    max_level = dim_category.agg(F.max("level")).first()[0] or 0
    # One step up per pass. The tree is at most max_level deep, so more passes
    # than that can only come from a cycle, which the loop bound cuts off.
    for depth in range(1, max_level + 1):
        level = level.join(edges, level.ancestor_key == edges._child).select(
            "category_key", F.col("_parent").alias("ancestor_key")
        )
        if level.limit(1).count() == 0:
            break
        closure = closure.unionByName(
            level.select(
                "ancestor_key",
                F.col("category_key").alias("descendant_key"),
                F.lit(depth).cast("smallint").alias("depth"),
            )
        )
    return closure


def build_dim_listing_flags(spark: SparkSession) -> DataFrame:
    """Every combination of the nine flags, keyed as ``listing_flags_key`` computes it (section 4.4)."""
    fixed = [
        (availability, active_status, discount, rating)
        for availability in AVAILABILITY_VALUES
        for active_status in ACTIVE_STATUS_VALUES
        for discount in (False, True)
        for rating in (False, True)
    ]
    rows = [
        (*combination, *values)
        for combination in fixed
        for values in itertools.product((False, True, None), repeat=len(NULLABLE_FLAGS))
    ]
    schema = "availability string, active_status string, has_discount boolean, has_rating boolean, " + ", ".join(
        f"{name} boolean" for name in NULLABLE_FLAGS
    )
    flags = spark.createDataFrame(rows, schema)
    key = listing_flags_key(
        discount=F.col("has_discount"),
        rating=F.col("has_rating"),
        **{name: F.col(name) for name in NULLABLE_FLAGS},
    )
    built = flags.select(key.alias("listing_flags_key"), *flags.columns)
    return with_unknown_member(
        built,
        {"listing_flags_key": UNKNOWN_KEY, "availability": UNKNOWN_NATURAL_KEY, "active_status": UNKNOWN_NATURAL_KEY},
    )


def _same(left: str, right: str) -> Column:
    return F.col(left).eqNullSafe(F.col(right))


def build_dim_product_listing(observations: DataFrame) -> DataFrame:
    """SCD2 versions of each offer (section 4.6).

    Sightings at the same instant are first reduced to the last one by the
    marts' tie-break, so no version can open and close at the same instant and
    ``(offer_id, valid_from)`` stays unique.
    """
    observations = with_listing_columns(observations)
    order = list(SIGHTING_ORDER)
    tracked = observations.select(
        "offer_id",
        "marketplace",
        *order,
        *LISTING_SCD1_ATTRIBUTES,
        "product_title",
        brand_key().alias("brand_key"),
        category_key().alias("category_key"),
        seller_key().alias("seller_key"),
    )
    per_instant = Window.partitionBy("offer_id", "observed_at").orderBy(
        F.col("fetched_at").desc(), F.col("observation_id").desc()
    )
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
    # SCD1 attributes: the latest value seen, on every version.
    scd1 = tracked.groupBy(F.col("offer_id").alias("_oid")).agg(
        *[latest(name).alias(f"_latest_{name}") for name in LISTING_SCD1_ATTRIBUTES]
    )
    built = (
        starts.withColumn("valid_from", F.col("observed_at"))
        .withColumn(
            "valid_to", F.coalesce(F.lead("observed_at").over(versions), F.lit(OPEN_VALID_TO).cast("timestamp"))
        )
        .withColumn("version", F.row_number().over(versions))
        .withColumn("is_current", F.lead("observed_at").over(versions).isNull())
        .join(scd1, F.col("offer_id") == F.col("_oid"))
        .select(
            surrogate_key("offer_id", "valid_from").alias("listing_key"),
            offer_key().alias("offer_key"),
            "offer_id",
            marketplace_key().alias("marketplace_key"),
            F.col("_latest_platform_listing_id").alias("platform_listing_id"),
            F.col("_latest_sku").alias("sku"),
            F.col("_latest_master_product_sku").alias("master_product_sku"),
            "product_title",
            "brand_key",
            "category_key",
            "seller_key",
            F.col("_latest_origin").alias("origin"),
            F.col("_latest_is_authentic").alias("is_authentic"),
            F.col("_latest_is_platform_verified").alias("is_platform_verified"),
            F.col("_latest_source_url").alias("source_url"),
            "valid_from",
            "valid_to",
            "is_current",
            F.col("version").cast("int").alias("version"),
        )
    )
    return with_unknown_member(
        built,
        {
            "listing_key": UNKNOWN_KEY,
            "offer_key": UNKNOWN_KEY,
            "offer_id": UNKNOWN_NATURAL_KEY,
            "marketplace_key": UNKNOWN_KEY,
            "product_title": UNKNOWN_LABEL,
            "brand_key": UNKNOWN_KEY,
            "category_key": UNKNOWN_KEY,
            "seller_key": UNKNOWN_KEY,
            "valid_from": UNKNOWN_VALID_FROM,
            "valid_to": OPEN_VALID_TO,
            "is_current": True,
            "version": 0,
        },
    )


def requested_category_pages(attempts: DataFrame | None) -> DataFrame | None:
    """The category pages the crawler asked for (``marketplace_code``, ``target``), from the crawl attempts.

    ``attempts`` carries the frontier's ``request_target`` and ``resource_type``
    (``marts.attach_marketplace_code``); without them there is nothing to add.
    """
    if attempts is None or not {"marketplace_code", "request_target", "resource_type"} <= set(attempts.columns):
        return None
    return (
        attempts.filter(F.col("resource_type") == "LISTING_PAGE")
        .select("marketplace_code", F.col("request_target").alias("target"))
        .distinct()
    )


def build_marketplace_dimensions(
    observations: DataFrame,
    frontier: DataFrame | None = None,
    category_names: DataFrame | None = None,
    category_paths: DataFrame | None = None,
) -> dict[str, DataFrame]:
    """Every dimension and the bridge, in registry order.

    ``observations`` is ``StarInputs.observations`` (or bare canonical
    observations, whose attributes then read as unknown); ``category_names``
    and ``category_paths`` are the matching ``StarInputs`` frames.
    """
    spark = observations.sparkSession
    observations = with_listing_columns(observations)
    dim_category = build_dim_category(observations, frontier, category_names, category_paths)
    return {
        "dim_date": build_dim_date(spark),
        "dim_marketplace": build_dim_marketplace(observations),
        "dim_seller": build_dim_seller(observations),
        "dim_brand": build_dim_brand(observations),
        "dim_category": dim_category,
        "bridge_category_ancestry": build_bridge_category_ancestry(dim_category),
        "dim_product_listing": build_dim_product_listing(observations),
        "dim_listing_flags": build_dim_listing_flags(spark),
    }
