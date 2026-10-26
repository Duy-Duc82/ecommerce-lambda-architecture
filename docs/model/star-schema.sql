-- Phase 10 marketplace star schema (docs/DATA_MODEL.md section 4), in PostgreSQL dialect.
-- The star itself lives only in the lake as Delta tables (gold/marketplace/dw/); this file is
-- for drawing it: run in an empty database, then use DBeaver (schema dw > View Diagram)
-- or pgAdmin (ERD Tool). Surrogate keys are deterministic xxhash64 values; -1 is the unknown member.

CREATE SCHEMA IF NOT EXISTS dw;

-- Conformed with the Kaggle behavior star: one builder for both.
CREATE TABLE dw.dim_date (
    date_key            integer PRIMARY KEY,            -- yyyymmdd; -1 unknown
    full_date           date UNIQUE,                    -- NULL only on the unknown row
    day_of_week         smallint,
    day_name            varchar(16),
    week_of_year        smallint,                       -- ISO week
    month               smallint,
    month_name          varchar(16),
    quarter             smallint,
    year                smallint,
    is_weekend          boolean,
    is_double_day_sale  boolean,                        -- 1/1, 2/2 ... 12/12
    is_vn_holiday       boolean
);

CREATE TABLE dw.dim_marketplace (
    marketplace_key     bigint PRIMARY KEY,
    marketplace_code    varchar(32) NOT NULL UNIQUE,    -- tiki | lazada
    marketplace_id      varchar(128),
    marketplace_name    varchar(128),
    platform_type       varchar(32),                    -- marketplace | retailer | classifieds | price_comparison
    country             char(2),
    default_currency    char(3),
    first_crawled_at    timestamptz
);

-- SCD1. seller_id is already scoped to its marketplace (make_seller_id).
-- Descriptive columns from the latest sighting in <source>_listing_wide.
CREATE TABLE dw.dim_seller (
    seller_key              bigint PRIMARY KEY,
    seller_id               varchar(128) NOT NULL UNIQUE,
    marketplace_key         bigint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    seller_name             varchar(256),
    seller_type             varchar(64),                -- as the source names it: OFFICIAL_STORE, NONE, LAZMALL
    is_official_store       boolean,
    is_trusted_store        boolean,                    -- NULL when the source has no such badge (Lazada)
    first_seen_at           timestamptz,
    last_seen_at            timestamptz,
    observed_listing_count  integer
);

-- SCD1. Brand ids are each marketplace's own; a listing without one points to -1.
CREATE TABLE dw.dim_brand (
    brand_key               bigint PRIMARY KEY,         -- xxhash64(marketplace_code, brand_id)
    marketplace_key         bigint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    brand_id                varchar(64) NOT NULL,
    brand_name              varchar(256),
    first_seen_at           timestamptz,
    last_seen_at            timestamptz,
    observed_listing_count  integer,
    UNIQUE (marketplace_key, brand_id)
);

-- Ragged hierarchy (4-8 levels): the one deliberate snowflake.
CREATE TABLE dw.dim_category (
    category_key     bigint PRIMARY KEY,                -- xxhash64(marketplace_code, category_id)
    marketplace_key  bigint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    category_id      varchar(256) NOT NULL,             -- Tiki numeric id, Lazada URL slug
    category_name    varchar(256),                      -- NULL where no page named the node
    category_slug    varchar(256),                      -- Tiki url_key or frontier target; Lazada = category_id
    parent_key       bigint REFERENCES dw.dim_category (category_key),
    level            smallint NOT NULL,                 -- root = 1
    full_path        varchar(2048) NOT NULL,            -- 1/2/1789/1795
    breadcrumb       varchar(2048),                     -- named ancestors and the node, top first, ' > '
    category_l1_name varchar(256),
    category_l2_name varchar(256),
    category_l3_name varchar(256),
    category_l4_name varchar(256),
    is_leaf          boolean NOT NULL,
    UNIQUE (marketplace_key, category_id)
);

-- Closure table: one row per (ancestor, descendant), the node itself at depth 0.
CREATE TABLE dw.bridge_category_ancestry (
    ancestor_key    bigint NOT NULL REFERENCES dw.dim_category (category_key),
    descendant_key  bigint NOT NULL REFERENCES dw.dim_category (category_key),
    depth           smallint NOT NULL,
    PRIMARY KEY (ancestor_key, descendant_key)
);

-- SCD Type 2 on product_title, brand_key, category_key, seller_key; SCD1 on the rest.
CREATE TABLE dw.dim_product_listing (
    listing_key          bigint PRIMARY KEY,            -- xxhash64(offer_id, valid_from)
    offer_key            bigint NOT NULL,               -- durable: xxhash64(offer_id)
    offer_id             varchar(128) NOT NULL,
    marketplace_key      bigint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    platform_listing_id  varchar(128),
    sku                  varchar(128),
    master_product_sku   varchar(128),                  -- Tiki only
    product_title        text,
    brand_key            bigint NOT NULL REFERENCES dw.dim_brand (brand_key),  -- -1 for 13.9% of Tiki products
    category_key         bigint NOT NULL REFERENCES dw.dim_category (category_key),
    seller_key           bigint NOT NULL REFERENCES dw.dim_seller (seller_key),
    origin               varchar(128),                  -- Tiki only, as written
    is_authentic         boolean,                       -- Tiki only
    is_platform_verified boolean,                       -- Tiki tiki_verified
    source_url           text,
    valid_from           timestamptz NOT NULL,
    valid_to             timestamptz NOT NULL,          -- exclusive; 9999-12-31 for the current row
    is_current           boolean NOT NULL,
    version              integer NOT NULL,
    UNIQUE (offer_id, valid_from)
);
CREATE INDEX ix_dim_product_listing_offer_key ON dw.dim_product_listing (offer_key);

-- Junk dimension: 3 x 3 x 2 x 2 x 3^5 = 8748 rows generated up front, plus the unknown row.
-- NULL in a flag means the source does not publish it.
CREATE TABLE dw.dim_listing_flags (
    listing_flags_key     bigint PRIMARY KEY,           -- xxhash64 of the nine flags
    availability          varchar(32),
    active_status         varchar(32),
    has_discount          boolean,
    has_rating            boolean,
    has_free_shipping     boolean,
    is_express_delivery   boolean,
    is_next_day_delivery  boolean,
    is_flash_deal         boolean,
    is_gift_available     boolean
);

-- Transaction fact. Grain: one sighting of one offer by the crawler at one instant.
CREATE TABLE dw.fact_offer_observation (
    observation_id    varchar(128) PRIMARY KEY,         -- degenerate dimension
    date_key          integer NOT NULL REFERENCES dw.dim_date (date_key),
    marketplace_key   bigint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    listing_key       bigint NOT NULL REFERENCES dw.dim_product_listing (listing_key),
    offer_key         bigint NOT NULL,                  -- durable
    seller_key        bigint NOT NULL REFERENCES dw.dim_seller (seller_key),
    brand_key         bigint NOT NULL REFERENCES dw.dim_brand (brand_key),
    category_key      bigint NOT NULL REFERENCES dw.dim_category (category_key),  -- the listing's own
    page_category_key bigint NOT NULL REFERENCES dw.dim_category (category_key),  -- the page it was found on
    listing_flags_key bigint NOT NULL REFERENCES dw.dim_listing_flags (listing_flags_key),
    observed_at       timestamptz NOT NULL,
    fetched_at        timestamptz NOT NULL,
    currency          char(3) NOT NULL,
    current_price     numeric(38,6) NOT NULL,
    list_price        numeric(38,6),
    shipping_price    numeric(38,6),
    discount_amount   numeric(38,6),
    discount_percent  numeric(9,4),
    rating_value      numeric(38,6),
    rating_scale      numeric(38,6),
    rating_count      bigint,
    review_count      bigint,
    sold_count        bigint,
    ranking_position  bigint,                           -- organic rank; NULL for a sponsored row
    search_rank       bigint,                           -- rank as displayed, sponsored rows included
    request_page      integer,
    is_advertisement  boolean,
    crawl_run_id      varchar(128),
    raw_uri           text,
    raw_sha256        char(64),
    adapter_version   varchar(64)
);

-- Periodic snapshot (sparse). Grain: one offer x UTC day x currency with at least one sighting.
CREATE TABLE dw.fact_price_snapshot_daily (
    date_key              integer NOT NULL REFERENCES dw.dim_date (date_key),
    offer_key             bigint NOT NULL,              -- durable
    currency              char(3) NOT NULL,
    listing_key           bigint NOT NULL REFERENCES dw.dim_product_listing (listing_key),  -- at the day's last sighting
    marketplace_key       bigint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    seller_key            bigint NOT NULL REFERENCES dw.dim_seller (seller_key),
    brand_key             bigint NOT NULL REFERENCES dw.dim_brand (brand_key),
    category_key          bigint NOT NULL REFERENCES dw.dim_category (category_key),
    listing_flags_key     bigint NOT NULL REFERENCES dw.dim_listing_flags (listing_flags_key),  -- end of day
    first_observed_at     timestamptz NOT NULL,
    last_observed_at      timestamptz NOT NULL,
    last_observation_id   varchar(128) NOT NULL,
    open_price            numeric(38,6),
    close_price           numeric(38,6),
    min_price             numeric(38,6),
    max_price             numeric(38,6),
    avg_price             numeric(38,6),
    first_list_price      numeric(38,6),
    last_list_price       numeric(38,6),
    observation_count     integer NOT NULL,
    distinct_price_count  integer NOT NULL,
    in_stock_ratio        numeric(5,4),
    PRIMARY KEY (date_key, offer_key, currency)
);

-- Transaction fact (derived). Grain: one sighting whose price differs from the offer's previous sighting.
CREATE TABLE dw.fact_price_change (
    observation_id           varchar(128) PRIMARY KEY,  -- the sighting that showed the new price
    previous_observation_id  varchar(128) NOT NULL,
    date_key                 integer NOT NULL REFERENCES dw.dim_date (date_key),
    marketplace_key          bigint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    listing_key              bigint NOT NULL REFERENCES dw.dim_product_listing (listing_key),
    offer_key                bigint NOT NULL,           -- durable
    seller_key               bigint NOT NULL REFERENCES dw.dim_seller (seller_key),
    brand_key                bigint NOT NULL REFERENCES dw.dim_brand (brand_key),
    category_key             bigint NOT NULL REFERENCES dw.dim_category (category_key),
    changed_at               timestamptz NOT NULL,      -- when the crawler saw it, not when the site changed it
    previous_observed_at     timestamptz NOT NULL,
    currency                 char(3) NOT NULL,
    previous_currency        char(3) NOT NULL,
    old_price                numeric(38,6) NOT NULL,
    new_price                numeric(38,6) NOT NULL,
    price_delta              numeric(38,6) NOT NULL,
    pct_change               numeric(18,6),             -- NULL when the currency changed or old_price = 0
    minutes_since_previous   numeric(12,2),
    direction                varchar(8) NOT NULL,       -- UP | DOWN
    is_large_drop            boolean NOT NULL,
    change_rule_version      varchar(64) NOT NULL
);

-- Periodic snapshot (sparse) of public counters: a proxy, not sales.
CREATE TABLE dw.fact_counter_daily (
    date_key                  integer NOT NULL REFERENCES dw.dim_date (date_key),
    offer_key                 bigint NOT NULL,          -- durable
    counter_name              varchar(32) NOT NULL,     -- rating_count | review_count | sold_count
    listing_key               bigint NOT NULL REFERENCES dw.dim_product_listing (listing_key),  -- at the day's last sighting
    marketplace_key           bigint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    first_value               bigint,
    last_value                bigint,
    raw_delta_sum             bigint,
    valid_delta_sum           bigint NOT NULL,
    valid_transition_count    integer NOT NULL,
    invalid_transition_count  integer NOT NULL,
    elapsed_seconds_valid     bigint NOT NULL,
    velocity_proxy_per_hour   double precision,
    counter_reset_or_invalid  boolean NOT NULL,
    invalid_reasons_json      text,
    counter_rule_version      varchar(64) NOT NULL,
    PRIMARY KEY (date_key, offer_key, counter_name)
);

-- Periodic snapshot (sparse). Grain: one crawled category page x UTC day x currency.
-- Prices over offers (each offer's last price of the day on the page), not sightings.
CREATE TABLE dw.fact_category_page_daily (
    date_key                 integer NOT NULL REFERENCES dw.dim_date (date_key),
    category_key             bigint NOT NULL REFERENCES dw.dim_category (category_key),  -- the crawled page's
    currency                 char(3) NOT NULL,
    marketplace_key          bigint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    crawled_page_count       integer NOT NULL,
    listed_product_count     bigint,                    -- the site's own total, last of the day
    observation_count        integer NOT NULL,
    observed_offer_count     integer NOT NULL,
    advertisement_count      integer NOT NULL,
    seller_count             integer NOT NULL,
    brand_count              integer NOT NULL,
    min_price                numeric(38,6),
    p25_price                numeric(38,6),
    median_price             numeric(38,6),
    p75_price                numeric(38,6),
    max_price                numeric(38,6),
    avg_price                numeric(38,6),
    discounted_offer_count   integer NOT NULL,
    discounted_offer_ratio   numeric(5,4),
    median_discount_percent  numeric(9,4),
    in_stock_ratio           numeric(5,4),
    PRIMARY KEY (date_key, category_key, currency)
);

-- Transaction fact. Grain: one HTTP attempt to fetch one page.
CREATE TABLE dw.fact_crawl_attempt (
    attempt_id       bigint PRIMARY KEY,                -- audit.crawl_request_attempt sequence
    date_key         integer NOT NULL REFERENCES dw.dim_date (date_key),
    marketplace_key  bigint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    category_key     bigint NOT NULL REFERENCES dw.dim_category (category_key),  -- -1 if not a category page
    crawl_run_id     varchar(128),
    task_id          varchar(80),
    resource_type    varchar(32),
    attempt_number   integer,
    started_at       timestamptz,
    completed_at     timestamptz,
    status           varchar(16) NOT NULL,
    http_status      integer,
    error_kind       varchar(32),
    latency_ms       bigint,
    raw_bytes        bigint,
    parsed_count     bigint,
    rejected_count   bigint,
    raw_uri          text
);
