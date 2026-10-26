"""The one list of marketplace Gold datasets: the star (``dw``) and the marts (``cache``).

Every place that needs to know which datasets exist, their grain, key,
partitioning or PostgreSQL columns reads them from here. Before Phase 10 the
ten mart names were repeated in the publisher, the warehouse writer, the
manifest, the quality registry and the Superset provisioning, and nothing but
review kept those copies in step.

Pure data, like the rest of ``config``: importing it starts no Spark session
and opens no connection. ``tests/test_marketplace_datasets.py`` checks it
against ``scripts/init_postgres.sql``, ``docs/model/star-schema.sql`` and
``docker/superset/datasources.yaml``.

The grain sentences are the ones in ``docs/DATA_MODEL.md`` section 4.
"""

from __future__ import annotations

from dataclasses import dataclass

DIM = "dim"
BRIDGE = "bridge"
FACT = "fact"
MART = "mart"
KINDS = (DIM, BRIDGE, FACT, MART)

# The star lives only in the lake (gold/<GOLD_DATASET>/dw/, Delta); dw is its
# namespace there. The marts are published to the PostgreSQL schema cache.
# The Gold dataset root: run-scoped marts, manifests and the serving pointer
# all live under gold/<GOLD_DATASET>/.
GOLD_DATASET = "marketplace"
DW_SCHEMA = "dw"
CACHE_SCHEMA = "cache"

DAILY_BY_OBSERVED = ("marketplace", "observed_date")
DAILY_BY_REQUEST = ("marketplace", "request_date")


@dataclass(frozen=True)
class MarketplaceDataset:
    name: str
    kind: str
    grain: str
    key: tuple[str, ...]
    columns: tuple[str, ...]
    partition_columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise ValueError(f"kind must be one of {KINDS}: {self.kind!r}")
        if not self.grain.strip():
            raise ValueError(f"grain is required: {self.name}")
        if len(set(self.columns)) != len(self.columns):
            raise ValueError(f"duplicate column in {self.name}")
        missing = [c for c in self.key + self.partition_columns if c not in self.columns]
        if missing:
            raise ValueError(f"{self.name}: key or partition columns not in columns: {missing}")

    @property
    def postgres_schema(self) -> str:
        return CACHE_SCHEMA if self.kind == MART else DW_SCHEMA

    @property
    def postgres_table(self) -> str:
        # Mart tables carry a prefix because cache is shared with the Kaggle
        # marts; dw belongs to the marketplace star alone.
        return f"marketplace_{self.name}" if self.kind == MART else self.name

    @property
    def qualified_table(self) -> str:
        return f"{self.postgres_schema}.{self.postgres_table}"


def _cols(text: str) -> tuple[str, ...]:
    return tuple(text.split())


MARTS: tuple[MarketplaceDataset, ...] = (
    MarketplaceDataset(
        "offer_current",
        MART,
        "one row per offer: its latest observed state",
        ("offer_id",),
        _cols("""offer_id marketplace marketplace_id platform_listing_id seller_id product_title brand category_path
                 source_url currency active_status first_seen_at last_seen_at current_observation_id observed_at
                 fetched_at current_price list_price shipping_price rating_value rating_scale rating_count review_count
                 sold_count availability ranking_position raw_uri raw_sha256 adapter_version crawl_run_id"""),
    ),
    MarketplaceDataset(
        "seller_current",
        MART,
        "one row per seller and marketplace",
        ("seller_id", "marketplace"),
        _cols("seller_id marketplace marketplace_id first_seen_at last_seen_at observed_offer_count"),
    ),
    MarketplaceDataset(
        "offer_price_history_daily",
        MART,
        "one offer, one UTC day, one currency",
        ("marketplace", "offer_id", "observed_date", "currency"),
        _cols("""marketplace offer_id observed_date currency first_observed_at last_observed_at first_price last_price
                 min_price max_price avg_price first_list_price last_list_price observation_count distinct_price_count
                 last_availability last_observation_id"""),
        DAILY_BY_OBSERVED,
    ),
    MarketplaceDataset(
        "offer_change_daily",
        MART,
        "one offer, one UTC day, one currency",
        ("marketplace", "offer_id", "observed_date", "currency"),
        _cols("""marketplace offer_id observed_date currency transition_count price_change_count price_drop_count
                 price_increase_count absolute_price_change_sum signed_price_change_sum max_price_drop max_price_increase
                 rating_change_count counter_change_count availability_change_count"""),
        DAILY_BY_OBSERVED,
    ),
    MarketplaceDataset(
        "offer_freshness",
        MART,
        "one row per offer, judged at the run's as_of",
        ("offer_id",),
        _cols("""as_of marketplace offer_id last_observation_id last_observed_at age_seconds freshness_status
                 stale_after_seconds freshness_rule_version"""),
    ),
    MarketplaceDataset(
        "category_price_daily",
        MART,
        "one category path, one UTC day, one currency",
        ("marketplace", "category_path", "observed_date", "currency"),
        _cols("""marketplace category_path observed_date currency observed_offer_count observation_count min_price
                 p25_price median_price p75_price max_price avg_price"""),
        DAILY_BY_OBSERVED,
    ),
    MarketplaceDataset(
        "source_coverage_daily",
        MART,
        "one marketplace, one UTC day",
        ("marketplace", "observed_date"),
        _cols("""marketplace observed_date eligible_offer_count observed_offer_count missing_offer_count
                 fresh_offer_count stale_offer_count observation_count parsed_count rejected_count coverage_rate
                 rejection_rate freshness_rule_version"""),
        DAILY_BY_OBSERVED,
    ),
    MarketplaceDataset(
        "crawl_reliability_daily",
        MART,
        "one marketplace, one UTC request day",
        ("marketplace", "request_date"),
        _cols("""marketplace request_date request_count succeeded_count failed_count success_rate avg_latency_ms
                 p95_latency_ms raw_bytes parsed_count rejected_count rate_limited_count transport_error_count
                 server_error_count parse_error_count validation_error_count"""),
        DAILY_BY_REQUEST,
    ),
    MarketplaceDataset(
        "counter_delta_daily",
        MART,
        "one offer, one UTC day, one public counter",
        ("marketplace", "offer_id", "observed_date", "counter_name"),
        _cols("""marketplace offer_id observed_date counter_name first_value last_value raw_delta_sum valid_delta_sum
                 valid_transition_count invalid_transition_count elapsed_seconds_valid velocity_proxy_per_hour
                 counter_reset_or_invalid invalid_reasons_json counter_rule_version"""),
        DAILY_BY_OBSERVED,
    ),
    MarketplaceDataset(
        "price_anomaly_daily",
        MART,
        "one offer, one UTC day, one currency",
        ("marketplace", "offer_id", "observed_date", "currency"),
        _cols("""marketplace offer_id observed_date currency evaluated_price baseline_sample_size baseline_median
                 baseline_mad baseline_p25 baseline_p75 baseline_iqr deviation_amount deviation_percent robust_score
                 lower_fence upper_fence anomaly_method anomaly_status anomaly_reason window_days min_samples
                 mad_threshold iqr_multiplier anomaly_rule_version"""),
        DAILY_BY_OBSERVED,
    ),
)

STAR: tuple[MarketplaceDataset, ...] = (
    MarketplaceDataset(
        "dim_date",
        DIM,
        "one calendar day",
        ("date_key",),
        _cols("""date_key full_date day_of_week day_name week_of_year month month_name quarter year is_weekend
                 is_double_day_sale is_vn_holiday"""),
    ),
    MarketplaceDataset(
        "dim_marketplace",
        DIM,
        "one marketplace",
        ("marketplace_key",),
        _cols(
            "marketplace_key marketplace_code marketplace_id marketplace_name platform_type country default_currency first_crawled_at"
        ),
    ),
    MarketplaceDataset(
        "dim_seller",
        DIM,
        "one seller",
        ("seller_key",),
        _cols("""seller_key seller_id marketplace_key seller_name seller_type is_official_store is_trusted_store
                 first_seen_at last_seen_at observed_listing_count"""),
    ),
    MarketplaceDataset(
        "dim_brand",
        DIM,
        "one brand of one marketplace",
        ("brand_key",),
        _cols("brand_key marketplace_key brand_id brand_name first_seen_at last_seen_at observed_listing_count"),
    ),
    MarketplaceDataset(
        "dim_category",
        DIM,
        "one node of a marketplace's category tree",
        ("category_key",),
        _cols("""category_key marketplace_key category_id category_name category_slug parent_key level full_path
                 breadcrumb category_l1_name category_l2_name category_l3_name category_l4_name is_leaf"""),
    ),
    MarketplaceDataset(
        "bridge_category_ancestry",
        BRIDGE,
        "one category node and one of its ancestors, itself included",
        ("ancestor_key", "descendant_key"),
        _cols("ancestor_key descendant_key depth"),
    ),
    MarketplaceDataset(
        "dim_product_listing",
        DIM,
        "one version of one offer's descriptive attributes",
        ("listing_key",),
        _cols("""listing_key offer_key offer_id marketplace_key platform_listing_id sku master_product_sku
                 product_title brand_key category_key seller_key origin is_authentic is_platform_verified source_url
                 valid_from valid_to is_current version"""),
    ),
    MarketplaceDataset(
        "dim_listing_flags",
        DIM,
        "one combination of a sighting's stock, status, discount, rating, delivery and promotion flags",
        ("listing_flags_key",),
        _cols("""listing_flags_key availability active_status has_discount has_rating has_free_shipping
                 is_express_delivery is_next_day_delivery is_flash_deal is_gift_available"""),
    ),
    MarketplaceDataset(
        "fact_offer_observation",
        FACT,
        "one sighting of one offer by the crawler at one instant",
        ("observation_id",),
        _cols("""observation_id date_key marketplace_key listing_key offer_key seller_key brand_key category_key
                 page_category_key listing_flags_key observed_at fetched_at currency current_price list_price
                 shipping_price discount_amount discount_percent rating_value rating_scale rating_count review_count
                 sold_count ranking_position search_rank request_page is_advertisement crawl_run_id raw_uri raw_sha256
                 adapter_version"""),
        partition_columns=("date_key",),
    ),
    MarketplaceDataset(
        "fact_price_snapshot_daily",
        FACT,
        "one offer, one UTC day, one currency, on which the offer was sighted at least once",
        ("date_key", "offer_key", "currency"),
        _cols("""date_key offer_key currency listing_key marketplace_key seller_key brand_key category_key
                 listing_flags_key first_observed_at last_observed_at last_observation_id open_price close_price min_price max_price
                 avg_price first_list_price last_list_price observation_count distinct_price_count in_stock_ratio"""),
        partition_columns=("date_key",),
    ),
    MarketplaceDataset(
        "fact_price_change",
        FACT,
        "one sighting whose price differs from the same offer's previous sighting",
        ("observation_id",),
        _cols("""observation_id previous_observation_id date_key marketplace_key listing_key offer_key seller_key
                 brand_key category_key changed_at previous_observed_at currency previous_currency old_price new_price price_delta
                 pct_change minutes_since_previous direction is_large_drop change_rule_version"""),
        partition_columns=("date_key",),
    ),
    MarketplaceDataset(
        "fact_counter_daily",
        FACT,
        "one offer, one UTC day, one public counter",
        ("date_key", "offer_key", "counter_name"),
        _cols("""date_key offer_key counter_name listing_key marketplace_key first_value last_value raw_delta_sum
                 valid_delta_sum valid_transition_count invalid_transition_count elapsed_seconds_valid
                 velocity_proxy_per_hour counter_reset_or_invalid invalid_reasons_json counter_rule_version"""),
        partition_columns=("date_key",),
    ),
    MarketplaceDataset(
        "fact_category_page_daily",
        FACT,
        "one crawled category page, one UTC day, one currency",
        ("date_key", "category_key", "currency"),
        _cols("""date_key category_key currency marketplace_key crawled_page_count listed_product_count
                 observation_count observed_offer_count advertisement_count seller_count brand_count min_price p25_price
                 median_price p75_price max_price avg_price discounted_offer_count discounted_offer_ratio
                 median_discount_percent in_stock_ratio"""),
        partition_columns=("date_key",),
    ),
    MarketplaceDataset(
        "fact_crawl_attempt",
        FACT,
        "one HTTP attempt to fetch one page",
        ("attempt_id",),
        _cols("""attempt_id date_key marketplace_key category_key crawl_run_id task_id resource_type attempt_number
                 started_at completed_at status http_status error_kind latency_ms raw_bytes parsed_count rejected_count
                 raw_uri"""),
        partition_columns=("date_key",),
    ),
)

DATASETS: tuple[MarketplaceDataset, ...] = STAR + MARTS
MART_NAMES: tuple[str, ...] = tuple(d.name for d in MARTS)
STAR_NAMES: tuple[str, ...] = tuple(d.name for d in STAR)
_BY_NAME = {d.name: d for d in DATASETS}
if len(_BY_NAME) != len(DATASETS):
    raise RuntimeError("marketplace dataset names must be unique")


def dataset(name: str) -> MarketplaceDataset:
    try:
        return _BY_NAME[name]
    except KeyError:
        raise ValueError(f"unknown marketplace dataset: {name!r}") from None


def datasets_of_kind(*kinds: str) -> tuple[MarketplaceDataset, ...]:
    unknown = set(kinds) - set(KINDS)
    if unknown:
        raise ValueError(f"unknown kind: {sorted(unknown)}")
    return tuple(d for d in DATASETS if d.kind in kinds)
