-- ============================================================
-- PostgreSQL BI serving cache + pipeline audit
-- ------------------------------------------------------------
-- PostgreSQL is NOT the warehouse. The authoritative warehouse is the MinIO
-- gold zone (star schema + marts). This database only caches compact,
-- BI-ready marts and ML outputs so Superset always has data to render, even
-- when the batch pipeline is not running.
--
-- This script is idempotent and non-destructive: it is run at container init,
-- by scripts/start_all.ps1, and by the batch job before each publish.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS cache;    -- BI-ready marts + ML outputs (Superset reads here)
CREATE SCHEMA IF NOT EXISTS staging;  -- landing zone for Spark JDBC before atomic swap
CREATE SCHEMA IF NOT EXISTS audit;    -- pipeline run + data-quality history

-- ---------- BI marts (grain documented in docs/DATA_MODEL.md) ----------

CREATE TABLE IF NOT EXISTS cache.funnel_daily (
    event_date              DATE PRIMARY KEY,
    viewers                 BIGINT NOT NULL DEFAULT 0,
    cart_users              BIGINT NOT NULL DEFAULT 0,
    buyers                  BIGINT NOT NULL DEFAULT 0,
    view_to_cart_rate       DOUBLE PRECISION NOT NULL DEFAULT 0,
    cart_to_purchase_rate   DOUBLE PRECISION NOT NULL DEFAULT 0,
    conversion_rate         DOUBLE PRECISION NOT NULL DEFAULT 0,
    cart_abandonment_rate   DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS cache.product_daily (
    event_date              DATE NOT NULL,
    product_id              VARCHAR(128) NOT NULL,
    brand                   VARCHAR(255),
    category_code           VARCHAR(255),
    views                   BIGINT NOT NULL DEFAULT 0,
    cart_adds               BIGINT NOT NULL DEFAULT 0,
    purchase_events         BIGINT NOT NULL DEFAULT 0,
    revenue                 DOUBLE PRECISION NOT NULL DEFAULT 0,
    view_to_cart_rate       DOUBLE PRECISION NOT NULL DEFAULT 0,
    cart_to_purchase_rate   DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (event_date, product_id)
);

CREATE TABLE IF NOT EXISTS cache.category_daily (
    event_date              DATE NOT NULL,
    category_code           VARCHAR(255) NOT NULL,
    views                   BIGINT NOT NULL DEFAULT 0,
    cart_adds               BIGINT NOT NULL DEFAULT 0,
    purchase_events         BIGINT NOT NULL DEFAULT 0,
    revenue                 DOUBLE PRECISION NOT NULL DEFAULT 0,
    conversion_rate         DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (event_date, category_code)
);

CREATE TABLE IF NOT EXISTS cache.session_daily (
    event_date              DATE,
    session_id              VARCHAR(128) NOT NULL,
    user_id                 VARCHAR(128),
    session_start           TIMESTAMP,
    session_end             TIMESTAMP,
    duration_seconds        BIGINT,
    event_count             BIGINT,
    product_count           BIGINT,
    view_count              BIGINT,
    cart_count              BIGINT,
    purchase_count          BIGINT,
    session_outcome         VARCHAR(32),
    PRIMARY KEY (session_id)
);

-- Daily revenue series: the input feature table for the ML job.
CREATE TABLE IF NOT EXISTS cache.daily_revenue (
    event_date              DATE PRIMARY KEY,
    revenue                 DOUBLE PRECISION NOT NULL DEFAULT 0,
    purchase_events         BIGINT NOT NULL DEFAULT 0,
    buyers                  BIGINT NOT NULL DEFAULT 0,
    avg_purchase_value      DOUBLE PRECISION NOT NULL DEFAULT 0
);

-- ---------- ML outputs ----------

-- Revenue forecast from Darts N-BEATS and LSTM (one row per model per date).
CREATE TABLE IF NOT EXISTS cache.predictions (
    forecast_date           DATE NOT NULL,
    model                   VARCHAR(40) NOT NULL,
    predicted_revenue       DOUBLE PRECISION NOT NULL,
    generated_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (forecast_date, model)
);

-- Daily anomalies from the PyOD AutoEncoder detector.
CREATE TABLE IF NOT EXISTS cache.anomalies (
    event_date              DATE PRIMARY KEY,
    anomaly_label           INTEGER NOT NULL,
    anomaly_score           DOUBLE PRECISION NOT NULL,
    revenue                 DOUBLE PRECISION,
    purchase_events         BIGINT,
    avg_purchase_value      DOUBLE PRECISION,
    detected_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---------- Pipeline audit ----------

CREATE TABLE IF NOT EXISTS audit.pipeline_run (
    run_id                  VARCHAR(64) PRIMARY KEY,
    source_path             TEXT,
    started_at              TIMESTAMP,
    completed_at            TIMESTAMP,
    status                  VARCHAR(16) NOT NULL,
    bronze_rows             BIGINT,
    silver_rows             BIGINT,
    rejected_rows           BIGINT,
    gold_rows               BIGINT,
    error_message           TEXT
);

CREATE TABLE IF NOT EXISTS audit.data_quality_result (
    run_id                  VARCHAR(64) NOT NULL,
    check_name              VARCHAR(64) NOT NULL,
    status                  VARCHAR(8) NOT NULL,
    observed_value          DOUBLE PRECISION,
    expectation             TEXT,
    checked_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, check_name)
);

-- ============================================================
-- PHASE 5 — speed-layer micro-batch audit
-- ============================================================
-- Primary key is (rule_version, batch_id): a re-run under a new rule version
-- emits different change identities, so it must not collide with the history
-- written under the old rules.
CREATE TABLE IF NOT EXISTS audit.speed_micro_batch (
    batch_id                BIGINT       NOT NULL,
    rule_version            TEXT         NOT NULL,
    started_at              TIMESTAMPTZ  NOT NULL,
    completed_at            TIMESTAMPTZ  NOT NULL,
    observations_in         INTEGER      NOT NULL,
    detected                INTEGER      NOT NULL,
    duplicates              INTEGER      NOT NULL,
    out_of_order            INTEGER      NOT NULL,
    conflicts               INTEGER      NOT NULL,
    decode_failures         INTEGER      NOT NULL,
    changes_published       INTEGER      NOT NULL,
    changes_by_type         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    status                  TEXT         NOT NULL,
    failure_stage           TEXT,
    failure_message         TEXT,
    PRIMARY KEY (rule_version, batch_id),
    -- The only honest explanation of a gap between observations consumed and
    -- changes emitted is that every record is accounted for.
    CONSTRAINT speed_micro_batch_counts_reconcile CHECK (
        observations_in = detected + duplicates + out_of_order + conflicts + decode_failures
    )
);

CREATE INDEX IF NOT EXISTS idx_speed_micro_batch_started
    ON audit.speed_micro_batch (started_at DESC);

-- ============================================================
-- PHASE 6 - marketplace Gold BI cache
-- Superset reads <schema>; Spark lands in <staging_schema> and the
-- publish step swaps every mart in one transaction.
-- Money is NUMERIC, never DOUBLE PRECISION: a float here would make
-- the published number disagree with the Decimal it came from.
-- ============================================================
CREATE SCHEMA IF NOT EXISTS marketplace_gold;
CREATE SCHEMA IF NOT EXISTS marketplace_gold_staging;

CREATE TABLE IF NOT EXISTS marketplace_gold.gold_run (
    gold_run_id       TEXT         PRIMARY KEY,
    as_of             TIMESTAMPTZ  NOT NULL,
    window_days       INTEGER      NOT NULL,
    rule_version      TEXT         NOT NULL,
    started_at        TIMESTAMPTZ  NOT NULL,
    completed_at      TIMESTAMPTZ,
    observations_read BIGINT       NOT NULL DEFAULT 0,
    quarantined_rows  BIGINT       NOT NULL DEFAULT 0,
    status            TEXT         NOT NULL,
    failure_stage     TEXT,
    skipped_marts     JSONB        NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS marketplace_gold.gold_assertion (
    gold_run_id     TEXT        NOT NULL REFERENCES marketplace_gold.gold_run(gold_run_id),
    assertion_name  TEXT        NOT NULL,
    observed_value  TEXT        NOT NULL,
    expectation     TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    rule_version    TEXT        NOT NULL,
    PRIMARY KEY (gold_run_id, assertion_name)
);

CREATE TABLE IF NOT EXISTS marketplace_gold.offer_current (
    offer_id                             TEXT NOT NULL,
    marketplace                          TEXT NOT NULL,
    platform_listing_id                  TEXT NOT NULL,
    seller_id                            TEXT,
    product_title                        TEXT,
    brand                                TEXT,
    category_path                        TEXT,
    source_url                           TEXT,
    currency                             TEXT,
    observation_id                       TEXT NOT NULL,
    observed_at                          TIMESTAMPTZ NOT NULL,
    current_price                        NUMERIC(38, 6),
    list_price                           NUMERIC(38, 6),
    rating_value                         NUMERIC(38, 6),
    review_count                         BIGINT,
    sold_count                           BIGINT,
    availability                         TEXT,
    freshness_status                     TEXT NOT NULL,
    age_minutes                          BIGINT NOT NULL,
    raw_uri                              TEXT,
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL,
    PRIMARY KEY (gold_run_id, offer_id)
);

CREATE TABLE IF NOT EXISTS marketplace_gold.offer_price_history_daily (
    offer_id                             TEXT NOT NULL,
    observed_date                        DATE NOT NULL,
    marketplace                          TEXT NOT NULL,
    price_open                           NUMERIC(38, 6),
    price_close                          NUMERIC(38, 6),
    price_min                            NUMERIC(38, 6),
    price_max                            NUMERIC(38, 6),
    observation_count                    BIGINT NOT NULL,
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL,
    PRIMARY KEY (gold_run_id, offer_id, observed_date)
);

CREATE TABLE IF NOT EXISTS marketplace_gold.offer_change_daily (
    offer_id                             TEXT NOT NULL,
    observed_date                        DATE NOT NULL,
    marketplace                          TEXT NOT NULL,
    price_changes                        BIGINT NOT NULL,
    large_price_drops                    BIGINT NOT NULL,
    availability_changes                 BIGINT NOT NULL,
    availability_transitions_excluded    BIGINT NOT NULL,
    counter_changes                      BIGINT NOT NULL,
    max_abs_price_delta                  NUMERIC(38, 6),
    avg_abs_price_delta                  NUMERIC(38, 6),
    max_drop_percent                     NUMERIC(38, 6),
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL,
    PRIMARY KEY (gold_run_id, offer_id, observed_date)
);

CREATE TABLE IF NOT EXISTS marketplace_gold.offer_freshness (
    offer_id                             TEXT NOT NULL,
    marketplace                          TEXT NOT NULL,
    last_observation_at                  TIMESTAMPTZ NOT NULL,
    last_observation_id                  TEXT NOT NULL,
    age_minutes                          BIGINT NOT NULL,
    freshness_status                     TEXT NOT NULL,
    fresh_threshold_minutes              BIGINT NOT NULL,
    stale_threshold_minutes              BIGINT NOT NULL,
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL,
    PRIMARY KEY (gold_run_id, offer_id)
);

CREATE TABLE IF NOT EXISTS marketplace_gold.category_price_daily (
    marketplace                          TEXT NOT NULL,
    category_path                        TEXT NOT NULL,
    observed_date                        DATE NOT NULL,
    offer_count                          BIGINT NOT NULL,
    observation_count                    BIGINT NOT NULL,
    price_min                            NUMERIC(38, 6),
    price_max                            NUMERIC(38, 6),
    price_p25                            NUMERIC(38, 6),
    price_median                         NUMERIC(38, 6),
    price_p75                            NUMERIC(38, 6),
    quantile_method                      TEXT NOT NULL,
    quantile_accuracy                    BIGINT NOT NULL,
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL,
    PRIMARY KEY (gold_run_id, marketplace, category_path, observed_date)
);

CREATE TABLE IF NOT EXISTS marketplace_gold.source_coverage_daily (
    marketplace                          TEXT NOT NULL,
    observed_date                        DATE NOT NULL,
    offers_observed                      BIGINT NOT NULL,
    observations                         BIGINT NOT NULL,
    offers_missing                       BIGINT NOT NULL,
    offers_stale                         BIGINT NOT NULL,
    offers_without_seller                BIGINT NOT NULL,
    rows_rejected                        BIGINT NOT NULL,
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL,
    PRIMARY KEY (gold_run_id, marketplace, observed_date)
);

CREATE TABLE IF NOT EXISTS marketplace_gold.crawl_reliability_daily (
    marketplace                          TEXT NOT NULL,
    observed_date                        DATE NOT NULL,
    requests                             BIGINT NOT NULL,
    succeeded                            BIGINT NOT NULL,
    failed                               BIGINT NOT NULL,
    success_rate                         NUMERIC(38, 6),
    p50_latency_ms                       BIGINT,
    p95_latency_ms                       BIGINT,
    errors_json                          TEXT,
    skipped_reason                       TEXT,
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL,
    PRIMARY KEY (gold_run_id, marketplace, observed_date)
);

CREATE TABLE IF NOT EXISTS marketplace_gold.counter_delta_daily (
    offer_id                             TEXT NOT NULL,
    observed_date                        DATE NOT NULL,
    marketplace                          TEXT NOT NULL,
    total_valid_delta                    BIGINT NOT NULL,
    valid_rows                           BIGINT NOT NULL,
    invalid_rows                         BIGINT NOT NULL,
    no_previous_rows                     BIGINT NOT NULL,
    negative_delta_rows                  BIGINT NOT NULL,
    gap_too_long_rows                    BIGINT NOT NULL,
    non_positive_elapsed_rows            BIGINT NOT NULL,
    max_velocity_per_hour                NUMERIC(38, 6),
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL,
    PRIMARY KEY (gold_run_id, offer_id, observed_date)
);

-- staging mirror: same columns, no keys, truncated per run
CREATE TABLE IF NOT EXISTS marketplace_gold_staging.offer_current (
    offer_id                             TEXT NOT NULL,
    marketplace                          TEXT NOT NULL,
    platform_listing_id                  TEXT NOT NULL,
    seller_id                            TEXT,
    product_title                        TEXT,
    brand                                TEXT,
    category_path                        TEXT,
    source_url                           TEXT,
    currency                             TEXT,
    observation_id                       TEXT NOT NULL,
    observed_at                          TIMESTAMPTZ NOT NULL,
    current_price                        NUMERIC(38, 6),
    list_price                           NUMERIC(38, 6),
    rating_value                         NUMERIC(38, 6),
    review_count                         BIGINT,
    sold_count                           BIGINT,
    availability                         TEXT,
    freshness_status                     TEXT NOT NULL,
    age_minutes                          BIGINT NOT NULL,
    raw_uri                              TEXT,
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS marketplace_gold_staging.offer_price_history_daily (
    offer_id                             TEXT NOT NULL,
    observed_date                        DATE NOT NULL,
    marketplace                          TEXT NOT NULL,
    price_open                           NUMERIC(38, 6),
    price_close                          NUMERIC(38, 6),
    price_min                            NUMERIC(38, 6),
    price_max                            NUMERIC(38, 6),
    observation_count                    BIGINT NOT NULL,
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS marketplace_gold_staging.offer_change_daily (
    offer_id                             TEXT NOT NULL,
    observed_date                        DATE NOT NULL,
    marketplace                          TEXT NOT NULL,
    price_changes                        BIGINT NOT NULL,
    large_price_drops                    BIGINT NOT NULL,
    availability_changes                 BIGINT NOT NULL,
    availability_transitions_excluded    BIGINT NOT NULL,
    counter_changes                      BIGINT NOT NULL,
    max_abs_price_delta                  NUMERIC(38, 6),
    avg_abs_price_delta                  NUMERIC(38, 6),
    max_drop_percent                     NUMERIC(38, 6),
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS marketplace_gold_staging.offer_freshness (
    offer_id                             TEXT NOT NULL,
    marketplace                          TEXT NOT NULL,
    last_observation_at                  TIMESTAMPTZ NOT NULL,
    last_observation_id                  TEXT NOT NULL,
    age_minutes                          BIGINT NOT NULL,
    freshness_status                     TEXT NOT NULL,
    fresh_threshold_minutes              BIGINT NOT NULL,
    stale_threshold_minutes              BIGINT NOT NULL,
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS marketplace_gold_staging.category_price_daily (
    marketplace                          TEXT NOT NULL,
    category_path                        TEXT NOT NULL,
    observed_date                        DATE NOT NULL,
    offer_count                          BIGINT NOT NULL,
    observation_count                    BIGINT NOT NULL,
    price_min                            NUMERIC(38, 6),
    price_max                            NUMERIC(38, 6),
    price_p25                            NUMERIC(38, 6),
    price_median                         NUMERIC(38, 6),
    price_p75                            NUMERIC(38, 6),
    quantile_method                      TEXT NOT NULL,
    quantile_accuracy                    BIGINT NOT NULL,
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS marketplace_gold_staging.source_coverage_daily (
    marketplace                          TEXT NOT NULL,
    observed_date                        DATE NOT NULL,
    offers_observed                      BIGINT NOT NULL,
    observations                         BIGINT NOT NULL,
    offers_missing                       BIGINT NOT NULL,
    offers_stale                         BIGINT NOT NULL,
    offers_without_seller                BIGINT NOT NULL,
    rows_rejected                        BIGINT NOT NULL,
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS marketplace_gold_staging.crawl_reliability_daily (
    marketplace                          TEXT NOT NULL,
    observed_date                        DATE NOT NULL,
    requests                             BIGINT NOT NULL,
    succeeded                            BIGINT NOT NULL,
    failed                               BIGINT NOT NULL,
    success_rate                         NUMERIC(38, 6),
    p50_latency_ms                       BIGINT,
    p95_latency_ms                       BIGINT,
    errors_json                          TEXT,
    skipped_reason                       TEXT,
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS marketplace_gold_staging.counter_delta_daily (
    offer_id                             TEXT NOT NULL,
    observed_date                        DATE NOT NULL,
    marketplace                          TEXT NOT NULL,
    total_valid_delta                    BIGINT NOT NULL,
    valid_rows                           BIGINT NOT NULL,
    invalid_rows                         BIGINT NOT NULL,
    no_previous_rows                     BIGINT NOT NULL,
    negative_delta_rows                  BIGINT NOT NULL,
    gap_too_long_rows                    BIGINT NOT NULL,
    non_positive_elapsed_rows            BIGINT NOT NULL,
    max_velocity_per_hour                NUMERIC(38, 6),
    gold_run_id                          TEXT NOT NULL,
    as_of                                TIMESTAMPTZ NOT NULL,
    rule_version                         TEXT NOT NULL
);
