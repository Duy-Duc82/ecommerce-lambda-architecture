-- Phase 10 marketplace star schema (proposed, docs/PLAN_PHASE10_DIMENSIONAL_MODEL.md), PostgreSQL DDL.
-- To see it as a diagram: run in an empty database, then use DBeaver (schema dw > View Diagram)
-- or pgAdmin (ERD Tool). Not wired into the pipeline yet.

CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE dw.dim_date (
    date_key            integer PRIMARY KEY,            -- yyyymmdd
    full_date           date NOT NULL UNIQUE,
    day_of_week         smallint NOT NULL,
    iso_week            smallint NOT NULL,
    month               smallint NOT NULL,
    quarter             smallint NOT NULL,
    year                smallint NOT NULL,
    is_weekend          boolean NOT NULL,
    is_double_day_sale  boolean NOT NULL,                -- 1/1, 2/2 ... 12/12
    is_vn_holiday       boolean NOT NULL
);

CREATE TABLE dw.dim_marketplace (
    marketplace_key     smallint PRIMARY KEY,
    marketplace_code    varchar(32) NOT NULL UNIQUE,     -- tiki
    marketplace_id      varchar(64),
    marketplace_name    varchar(128),
    platform_type       varchar(32),                     -- marketplace | retailer | classifieds | price_comparison
    country             char(2),
    currency            char(3),
    first_crawled_at    timestamptz
);

CREATE TABLE dw.dim_seller (
    seller_key              bigint PRIMARY KEY,
    seller_id               varchar(128) NOT NULL UNIQUE,
    marketplace_key         smallint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    first_seen_at           timestamptz,
    last_seen_at            timestamptz,
    observed_listing_count  integer
);

CREATE TABLE dw.dim_category (
    category_key     integer PRIMARY KEY,
    category_id      varchar(64) NOT NULL UNIQUE,
    marketplace_key  smallint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    category_name    varchar(256),                       -- NULL when no name could be resolved
    parent_key       integer REFERENCES dw.dim_category (category_key),
    level            smallint NOT NULL,
    full_path        varchar(512) NOT NULL,              -- 1/2/1789/1795
    is_leaf          boolean NOT NULL
);

-- Closure table for the ragged category tree (4-8 levels).
CREATE TABLE dw.bridge_category_ancestry (
    ancestor_key    integer NOT NULL REFERENCES dw.dim_category (category_key),
    descendant_key  integer NOT NULL REFERENCES dw.dim_category (category_key),
    depth           smallint NOT NULL,
    PRIMARY KEY (ancestor_key, descendant_key)
);

-- SCD Type 2 on product_title, brand, category_key, seller_key.
CREATE TABLE dw.dim_product_listing (
    listing_key          bigint PRIMARY KEY,             -- xxhash64(offer_id, valid_from)
    offer_id             varchar(128) NOT NULL,
    marketplace_key      smallint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    platform_listing_id  varchar(128),
    product_title        text,
    brand                varchar(256),                   -- empty for 13.9% of products
    category_key         integer NOT NULL REFERENCES dw.dim_category (category_key),
    seller_key           bigint NOT NULL REFERENCES dw.dim_seller (seller_key),
    source_url           text,
    currency             char(3),
    valid_from           timestamptz NOT NULL,
    valid_to             timestamptz NOT NULL,           -- exclusive; 9999-12-31 for the current row
    is_current           boolean NOT NULL,
    version              integer NOT NULL,
    UNIQUE (offer_id, valid_from)
);

-- Junk dimension.
CREATE TABLE dw.dim_offer_status (
    offer_status_key  smallint PRIMARY KEY,
    availability      varchar(32),
    active_status     varchar(32),
    has_discount      boolean,
    has_rating        boolean
);

-- Transaction fact: one sighting of one listing at one instant.
CREATE TABLE dw.fact_offer_observation (
    observation_id    varchar(128) PRIMARY KEY,          -- degenerate dimension
    date_key          integer NOT NULL REFERENCES dw.dim_date (date_key),
    marketplace_key   smallint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    listing_key       bigint NOT NULL REFERENCES dw.dim_product_listing (listing_key),
    seller_key        bigint NOT NULL REFERENCES dw.dim_seller (seller_key),
    category_key      integer NOT NULL REFERENCES dw.dim_category (category_key),
    offer_status_key  smallint NOT NULL REFERENCES dw.dim_offer_status (offer_status_key),
    observed_at       timestamptz NOT NULL,
    current_price     numeric(38,6) NOT NULL,
    list_price        numeric(38,6),
    discount_percent  numeric(9,4),
    rating_value      numeric(4,2),
    rating_count      bigint,
    review_count      bigint,
    sold_count        bigint,
    ranking_position  integer,
    crawl_run_id      varchar(128),
    raw_uri           text
);

-- Periodic snapshot: one listing x day x currency with at least one sighting.
CREATE TABLE dw.fact_price_snapshot_daily (
    date_key              integer NOT NULL REFERENCES dw.dim_date (date_key),
    listing_key           bigint NOT NULL REFERENCES dw.dim_product_listing (listing_key),
    currency              char(3) NOT NULL,
    marketplace_key       smallint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    seller_key            bigint NOT NULL REFERENCES dw.dim_seller (seller_key),
    category_key          integer NOT NULL REFERENCES dw.dim_category (category_key),
    offer_status_key      smallint NOT NULL REFERENCES dw.dim_offer_status (offer_status_key),
    open_price            numeric(38,6),
    close_price           numeric(38,6),
    min_price             numeric(38,6),
    max_price             numeric(38,6),
    avg_price             numeric(38,6),
    observation_count     integer NOT NULL,
    distinct_price_count  integer NOT NULL,
    in_stock_ratio        numeric(5,4),
    PRIMARY KEY (date_key, listing_key, currency)
);

-- Transaction fact (derived): one change of a listing price versus its previous sighting.
CREATE TABLE dw.fact_price_change (
    price_change_id           varchar(128) PRIMARY KEY,
    date_key                  integer NOT NULL REFERENCES dw.dim_date (date_key),
    marketplace_key           smallint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    listing_key               bigint NOT NULL REFERENCES dw.dim_product_listing (listing_key),
    seller_key                bigint NOT NULL REFERENCES dw.dim_seller (seller_key),
    category_key              integer NOT NULL REFERENCES dw.dim_category (category_key),
    changed_at                timestamptz NOT NULL,
    old_price                 numeric(38,6) NOT NULL,
    new_price                 numeric(38,6) NOT NULL,
    delta                     numeric(38,6) NOT NULL,
    pct_change                numeric(9,4),
    minutes_since_previous    numeric(10,2),
    direction                 varchar(8) NOT NULL,       -- up | down
    is_large_drop             boolean NOT NULL,
    reverted_within_next_obs  boolean
);

-- Periodic snapshot of public counters (a proxy, not sales).
CREATE TABLE dw.fact_counter_daily (
    date_key                  integer NOT NULL REFERENCES dw.dim_date (date_key),
    listing_key               bigint NOT NULL REFERENCES dw.dim_product_listing (listing_key),
    counter_name              varchar(32) NOT NULL,
    marketplace_key           smallint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    first_value               bigint,
    last_value                bigint,
    valid_delta_sum           bigint,
    invalid_transition_count  integer,
    velocity_proxy_per_hour   double precision,
    PRIMARY KEY (date_key, listing_key, counter_name)
);

-- Transaction fact: one attempt to crawl one page.
CREATE TABLE dw.fact_crawl_attempt (
    attempt_id       varchar(128) PRIMARY KEY,
    date_key         integer NOT NULL REFERENCES dw.dim_date (date_key),
    marketplace_key  smallint NOT NULL REFERENCES dw.dim_marketplace (marketplace_key),
    category_key     integer NOT NULL REFERENCES dw.dim_category (category_key),
    started_at       timestamptz,
    completed_at     timestamptz,
    status           varchar(32) NOT NULL,
    http_status      integer,
    error_kind       varchar(32),
    latency_ms       bigint,
    raw_bytes        bigint,
    parsed_count     integer,
    rejected_count   integer
);
