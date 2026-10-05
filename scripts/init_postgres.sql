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

-- ---------- Marketplace speed-layer audit (Phase 5) ----------
CREATE TABLE IF NOT EXISTS audit.marketplace_speed_batch (
    query_name VARCHAR(128) NOT NULL, query_id VARCHAR(64) NOT NULL DEFAULT '', batch_id BIGINT NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
    started_at TIMESTAMPTZ NOT NULL, completed_at TIMESTAMPTZ,
    input_rows BIGINT NOT NULL DEFAULT 0 CHECK (input_rows >= 0),
    invalid_rows BIGINT NOT NULL DEFAULT 0 CHECK (invalid_rows >= 0),
    applied_rows BIGINT NOT NULL DEFAULT 0 CHECK (applied_rows >= 0),
    duplicate_rows BIGINT NOT NULL DEFAULT 0 CHECK (duplicate_rows >= 0),
    late_rows BIGINT NOT NULL DEFAULT 0 CHECK (late_rows >= 0),
    change_rows BIGINT NOT NULL DEFAULT 0 CHECK (change_rows >= 0),
    kafka_rows BIGINT NOT NULL DEFAULT 0 CHECK (kafka_rows >= 0),
    es_rows BIGINT NOT NULL DEFAULT 0 CHECK (es_rows >= 0),
    redis_rows BIGINT NOT NULL DEFAULT 0 CHECK (redis_rows >= 0),
    error_message TEXT CHECK (length(error_message) <= 2000),
    PRIMARY KEY (query_name, query_id, batch_id),
    CHECK (completed_at IS NULL OR status IN ('SUCCEEDED','FAILED')),
    CHECK (status = 'RUNNING' OR completed_at IS NOT NULL),
    CHECK (input_rows = 0 OR input_rows = invalid_rows + applied_rows + duplicate_rows + late_rows)
);
-- Phase 8: batch IDs restart at 0 under a new checkpoint, so a batch is
-- identified by the streaming query id as well. Rows written before this
-- carry '' and stay distinct from any real query id. Safe to run twice.
ALTER TABLE audit.marketplace_speed_batch ADD COLUMN IF NOT EXISTS query_id VARCHAR(64) NOT NULL DEFAULT '';
ALTER TABLE audit.marketplace_speed_batch DROP CONSTRAINT IF EXISTS marketplace_speed_batch_pkey;
ALTER TABLE audit.marketplace_speed_batch ADD CONSTRAINT marketplace_speed_batch_pkey PRIMARY KEY (query_name, query_id, batch_id);

-- Phase 9 (plan section 6.1): processing latency per micro-batch, in ms.
-- Latency is the batch's completion -- every sink written -- minus each
-- applied observation's produced_at, the instant the crawler sent it to
-- Kafka (kafka-python stamps the record's CreateTime at that send). It
-- includes the time the record waited in Kafka for the next trigger.
-- Nullable: a batch with no applied observation has no latency, and rows
-- from before Phase 9 have none either. No sign check: a clock step between
-- containers must not fail a batch that wrote its sinks. Safe to run twice.
ALTER TABLE audit.marketplace_speed_batch ADD COLUMN IF NOT EXISTS latency_p50_ms BIGINT;
ALTER TABLE audit.marketplace_speed_batch ADD COLUMN IF NOT EXISTS latency_p95_ms BIGINT;
ALTER TABLE audit.marketplace_speed_batch ADD COLUMN IF NOT EXISTS latency_max_ms BIGINT;

-- Speed-cost benchmark: where one micro-batch's time went, in ms. clients
-- opens the three sink clients, collect is Spark computing the batch (the
-- stateful comparison included), then one column per sink. Nullable: a
-- failed batch and every batch before these columns have none. Safe to run twice.
ALTER TABLE audit.marketplace_speed_batch ADD COLUMN IF NOT EXISTS stage_clients_ms BIGINT;
ALTER TABLE audit.marketplace_speed_batch ADD COLUMN IF NOT EXISTS stage_collect_ms BIGINT;
ALTER TABLE audit.marketplace_speed_batch ADD COLUMN IF NOT EXISTS stage_kafka_ms BIGINT;
ALTER TABLE audit.marketplace_speed_batch ADD COLUMN IF NOT EXISTS stage_es_ms BIGINT;
ALTER TABLE audit.marketplace_speed_batch ADD COLUMN IF NOT EXISTS stage_redis_ms BIGINT;

-- Phase 9 (plan section 6.1): Spark's own StreamingQueryProgress, one row per
-- micro-batch. query_id is the id Spark keeps in the checkpoint, the same one
-- marketplace_speed_batch is keyed by, so the two join on
-- (query_name, query_id, batch_id). Written by the speed service, which polls
-- recentProgress; a batch it has already stored is left alone.
CREATE TABLE IF NOT EXISTS audit.marketplace_stream_progress (
    query_name VARCHAR(128) NOT NULL,
    query_id VARCHAR(64) NOT NULL,
    batch_id BIGINT NOT NULL CHECK (batch_id >= 0),
    run_id VARCHAR(64) NOT NULL,
    progress_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    num_input_rows BIGINT CHECK (num_input_rows IS NULL OR num_input_rows >= 0),
    input_rows_per_second DOUBLE PRECISION,
    processed_rows_per_second DOUBLE PRECISION,
    batch_duration_ms BIGINT,
    trigger_execution_ms BIGINT,
    add_batch_ms BIGINT,
    get_batch_ms BIGINT,
    latest_offset_ms BIGINT,
    query_planning_ms BIGINT,
    wal_commit_ms BIGINT,
    commit_offsets_ms BIGINT,
    state_rows_total BIGINT,
    state_memory_bytes BIGINT,
    PRIMARY KEY (query_name, query_id, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_stream_progress_at ON audit.marketplace_stream_progress (progress_at);

-- Phase 9 (plan section 6.3): what each store holds, once per UTC day. Read
-- by the storage-growth report (P2-04). One row per (day, component, scope);
-- a second snapshot on the same day writes nothing.
CREATE TABLE IF NOT EXISTS audit.storage_snapshot (
    snapshot_date DATE NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    component VARCHAR(16) NOT NULL CHECK (component IN ('minio','postgres','elasticsearch','kafka')),
    scope VARCHAR(256) NOT NULL,
    bytes BIGINT NOT NULL CHECK (bytes >= 0),
    objects BIGINT CHECK (objects IS NULL OR objects >= 0),
    PRIMARY KEY (snapshot_date, component, scope)
);

-- ---------- Marketplace temporal warehouse cache (Phase 6) ----------
CREATE TABLE IF NOT EXISTS cache.marketplace_offer_current (
    offer_id VARCHAR(128) PRIMARY KEY, marketplace VARCHAR(64) NOT NULL, marketplace_id VARCHAR(128) NOT NULL,
    platform_listing_id VARCHAR(128) NOT NULL, seller_id VARCHAR(128), product_title TEXT NOT NULL, brand TEXT,
    category_path TEXT, source_url TEXT NOT NULL, currency CHAR(3) NOT NULL, active_status VARCHAR(16) NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL, last_seen_at TIMESTAMPTZ NOT NULL, current_observation_id VARCHAR(128) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL, fetched_at TIMESTAMPTZ NOT NULL, current_price NUMERIC(38,6) NOT NULL,
    list_price NUMERIC(38,6), shipping_price NUMERIC(38,6), rating_value NUMERIC(38,6), rating_scale NUMERIC(38,6),
    rating_count BIGINT, review_count BIGINT, sold_count BIGINT, availability VARCHAR(16) NOT NULL, ranking_position BIGINT,
    raw_uri TEXT NOT NULL, raw_sha256 VARCHAR(64) NOT NULL, adapter_version VARCHAR(128) NOT NULL, crawl_run_id VARCHAR(128) NOT NULL
);
CREATE TABLE IF NOT EXISTS cache.marketplace_seller_current (
    seller_id VARCHAR(128) NOT NULL, marketplace VARCHAR(64) NOT NULL, marketplace_id VARCHAR(128) NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL, last_seen_at TIMESTAMPTZ NOT NULL, observed_offer_count BIGINT NOT NULL,
    PRIMARY KEY (seller_id, marketplace)
);
CREATE TABLE IF NOT EXISTS cache.marketplace_offer_price_history_daily (
    marketplace VARCHAR(64) NOT NULL, offer_id VARCHAR(128) NOT NULL, observed_date DATE NOT NULL, currency CHAR(3) NOT NULL,
    first_observed_at TIMESTAMPTZ NOT NULL, last_observed_at TIMESTAMPTZ NOT NULL, first_price NUMERIC(38,6) NOT NULL,
    last_price NUMERIC(38,6) NOT NULL, min_price NUMERIC(38,6) NOT NULL, max_price NUMERIC(38,6) NOT NULL, avg_price NUMERIC(38,6) NOT NULL,
    first_list_price NUMERIC(38,6), last_list_price NUMERIC(38,6), observation_count BIGINT NOT NULL, distinct_price_count BIGINT NOT NULL,
    last_availability VARCHAR(16) NOT NULL, last_observation_id VARCHAR(128) NOT NULL, PRIMARY KEY (marketplace,offer_id,observed_date,currency)
);
CREATE TABLE IF NOT EXISTS cache.marketplace_offer_change_daily (
    marketplace VARCHAR(64) NOT NULL, offer_id VARCHAR(128) NOT NULL, observed_date DATE NOT NULL, currency CHAR(3) NOT NULL,
    transition_count BIGINT NOT NULL, price_change_count BIGINT NOT NULL, price_drop_count BIGINT NOT NULL, price_increase_count BIGINT NOT NULL,
    absolute_price_change_sum NUMERIC(38,6) NOT NULL, signed_price_change_sum NUMERIC(38,6) NOT NULL, max_price_drop NUMERIC(38,6), max_price_increase NUMERIC(38,6),
    rating_change_count BIGINT NOT NULL, counter_change_count BIGINT NOT NULL, availability_change_count BIGINT NOT NULL, PRIMARY KEY (marketplace,offer_id,observed_date,currency)
);
CREATE TABLE IF NOT EXISTS cache.marketplace_offer_freshness (
    as_of TIMESTAMPTZ NOT NULL, marketplace VARCHAR(64) NOT NULL, offer_id VARCHAR(128) NOT NULL, last_observation_id VARCHAR(128) NOT NULL,
    last_observed_at TIMESTAMPTZ NOT NULL, age_seconds BIGINT NOT NULL, freshness_status VARCHAR(8) NOT NULL CHECK (freshness_status IN ('FRESH','STALE','FUTURE')),
    stale_after_seconds BIGINT NOT NULL, freshness_rule_version VARCHAR(128) NOT NULL, PRIMARY KEY (offer_id)
);
CREATE TABLE IF NOT EXISTS cache.marketplace_category_price_daily (
    marketplace VARCHAR(64) NOT NULL, category_path TEXT NOT NULL, observed_date DATE NOT NULL, currency CHAR(3) NOT NULL,
    observed_offer_count BIGINT NOT NULL, observation_count BIGINT NOT NULL, min_price NUMERIC(38,6) NOT NULL, p25_price NUMERIC(38,6) NOT NULL,
    median_price NUMERIC(38,6) NOT NULL, p75_price NUMERIC(38,6) NOT NULL, max_price NUMERIC(38,6) NOT NULL, avg_price NUMERIC(38,6) NOT NULL,
    PRIMARY KEY (marketplace,category_path,observed_date,currency)
);
CREATE TABLE IF NOT EXISTS cache.marketplace_source_coverage_daily (
    marketplace VARCHAR(64) NOT NULL, observed_date DATE NOT NULL, eligible_offer_count BIGINT NOT NULL, observed_offer_count BIGINT NOT NULL,
    missing_offer_count BIGINT NOT NULL, fresh_offer_count BIGINT, stale_offer_count BIGINT, observation_count BIGINT NOT NULL, parsed_count BIGINT NOT NULL,
    rejected_count BIGINT NOT NULL, coverage_rate DOUBLE PRECISION, rejection_rate DOUBLE PRECISION, freshness_rule_version VARCHAR(128) NOT NULL,
    PRIMARY KEY (marketplace,observed_date)
);
CREATE TABLE IF NOT EXISTS cache.marketplace_crawl_reliability_daily (
    marketplace VARCHAR(64) NOT NULL, request_date DATE NOT NULL, request_count BIGINT NOT NULL, succeeded_count BIGINT NOT NULL, failed_count BIGINT NOT NULL,
    success_rate DOUBLE PRECISION, avg_latency_ms DOUBLE PRECISION, p95_latency_ms DOUBLE PRECISION, raw_bytes BIGINT NOT NULL, parsed_count BIGINT NOT NULL,
    rejected_count BIGINT NOT NULL, rate_limited_count BIGINT NOT NULL, transport_error_count BIGINT NOT NULL, server_error_count BIGINT NOT NULL,
    parse_error_count BIGINT NOT NULL, validation_error_count BIGINT NOT NULL, PRIMARY KEY (marketplace,request_date)
);
CREATE TABLE IF NOT EXISTS cache.marketplace_counter_delta_daily (
    marketplace VARCHAR(64) NOT NULL, offer_id VARCHAR(128) NOT NULL, observed_date DATE NOT NULL, counter_name VARCHAR(32) NOT NULL,
    first_value BIGINT, last_value BIGINT, raw_delta_sum BIGINT, valid_delta_sum BIGINT, valid_transition_count BIGINT NOT NULL,
    invalid_transition_count BIGINT NOT NULL, elapsed_seconds_valid BIGINT NOT NULL, velocity_proxy_per_hour DOUBLE PRECISION,
    counter_reset_or_invalid BOOLEAN NOT NULL, invalid_reasons_json TEXT NOT NULL, counter_rule_version VARCHAR(128) NOT NULL,
    PRIMARY KEY (marketplace,offer_id,observed_date,counter_name)
);
-- Phase 7. A row is a statistical price outlier relative to this offer's own
-- recent observed price history. It is not a claim that a price is wrong,
-- dishonest or a bargain, and it never compares one offer against another.
-- The rule parameters travel with the row so a stored verdict stays checkable
-- after the configuration moves on.
CREATE TABLE IF NOT EXISTS cache.marketplace_price_anomaly_daily (
    marketplace VARCHAR(64) NOT NULL, offer_id VARCHAR(128) NOT NULL, observed_date DATE NOT NULL, currency VARCHAR(8) NOT NULL,
    evaluated_price NUMERIC(38,6) NOT NULL, baseline_sample_size BIGINT NOT NULL CHECK (baseline_sample_size >= 0),
    baseline_median NUMERIC(38,6), baseline_mad NUMERIC(38,6), baseline_p25 NUMERIC(38,6), baseline_p75 NUMERIC(38,6), baseline_iqr NUMERIC(38,6),
    deviation_amount NUMERIC(38,6), deviation_percent DOUBLE PRECISION, robust_score DOUBLE PRECISION,
    lower_fence NUMERIC(38,6), upper_fence NUMERIC(38,6),
    anomaly_method VARCHAR(16) NOT NULL CHECK (anomaly_method IN ('ROLLING_MAD','IQR_FALLBACK','NONE')),
    anomaly_status VARCHAR(24) NOT NULL CHECK (anomaly_status IN ('NORMAL','ANOMALOUS_HIGH','ANOMALOUS_LOW','INSUFFICIENT_HISTORY','INSUFFICIENT_DISPERSION')),
    anomaly_reason VARCHAR(32) NOT NULL, window_days BIGINT NOT NULL, min_samples BIGINT NOT NULL,
    mad_threshold NUMERIC(38,6) NOT NULL, iqr_multiplier NUMERIC(38,6) NOT NULL, anomaly_rule_version VARCHAR(64) NOT NULL,
    PRIMARY KEY (marketplace,offer_id,observed_date,currency)
);
-- Re-key a table created before currency joined the key. The grain is the price
-- history's, and a same-day currency switch yields one row per currency.
ALTER TABLE cache.marketplace_price_anomaly_daily DROP CONSTRAINT IF EXISTS marketplace_price_anomaly_daily_pkey;
ALTER TABLE cache.marketplace_price_anomaly_daily
    ADD CONSTRAINT marketplace_price_anomaly_daily_pkey PRIMARY KEY (marketplace,offer_id,observed_date,currency);
CREATE TABLE IF NOT EXISTS audit.marketplace_batch_run (
    run_id VARCHAR(64) PRIMARY KEY, as_of TIMESTAMPTZ NOT NULL, silver_uri TEXT NOT NULL, gold_run_uri TEXT,
    started_at TIMESTAMPTZ NOT NULL, completed_at TIMESTAMPTZ, status VARCHAR(16) NOT NULL CHECK (status IN ('RUNNING','GOLD_WRITTEN','SUCCEEDED','FAILED')),
    silver_rows BIGINT CHECK (silver_rows IS NULL OR silver_rows >= 0), gold_rows BIGINT CHECK (gold_rows IS NULL OR gold_rows >= 0),
    cache_published BOOLEAN NOT NULL DEFAULT FALSE, dataset_counts JSONB, error_message TEXT CHECK (length(error_message) <= 2000)
);
CREATE TABLE IF NOT EXISTS audit.marketplace_cache_version (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton), run_id VARCHAR(64) NOT NULL, published_at TIMESTAMPTZ NOT NULL, dataset_counts JSONB NOT NULL
);

-- ---------- Marketplace quality gate (Phase 7) ----------
-- Separate from audit.data_quality_result on purpose: that table belongs to the
-- legacy behavioural pipeline, its checked_at is a naive TIMESTAMP, and its key
-- has no room for severity or dataset. Sharing it would couple two unrelated
-- pipelines through one migration.
-- A row is written for every rule on every run, including runs that were
-- refused. A blocked publication with no stored evidence cannot be told apart
-- from a crash.
CREATE TABLE IF NOT EXISTS audit.marketplace_quality_result (
    run_id VARCHAR(64) NOT NULL, check_name VARCHAR(64) NOT NULL,
    severity VARCHAR(16) NOT NULL CHECK (severity IN ('MANDATORY','ADVISORY')),
    dataset_name VARCHAR(64) NOT NULL,
    status VARCHAR(8) NOT NULL CHECK (status IN ('PASS','FAIL','SKIPPED')),
    observed_value DOUBLE PRECISION, expectation TEXT NOT NULL, rule_version VARCHAR(64) NOT NULL,
    -- Identifiers only. Never a raw body, product title, URL query or traceback.
    failure_sample_json TEXT CHECK (failure_sample_json IS NULL OR length(failure_sample_json) <= 4000),
    checked_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, check_name)
);
CREATE INDEX IF NOT EXISTS ix_marketplace_quality_result_checked_at ON audit.marketplace_quality_result (checked_at DESC);

-- QUALITY_FAILED is deliberately distinct from FAILED: the run produced
-- complete, inspectable Gold and was refused, which is a different operational
-- situation from a crash. The two statements below are idempotent together and
-- migrate a database created before Phase 7.
ALTER TABLE audit.marketplace_batch_run
    ADD COLUMN IF NOT EXISTS quality_status VARCHAR(16),
    ADD COLUMN IF NOT EXISTS mandatory_failure_count BIGINT,
    ADD COLUMN IF NOT EXISTS manifest_uri TEXT,
    ADD COLUMN IF NOT EXISTS manifest_promoted BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE audit.marketplace_batch_run DROP CONSTRAINT IF EXISTS marketplace_batch_run_status_check;
ALTER TABLE audit.marketplace_batch_run ADD CONSTRAINT marketplace_batch_run_status_check
    CHECK (status IN ('RUNNING','GOLD_WRITTEN','QUALITY_FAILED','SUCCEEDED','FAILED'));

-- The serving cache names the manifest and the rule version that admitted it,
-- so "which Gold version is live, under which rules" needs no object listing.
ALTER TABLE audit.marketplace_cache_version
    ADD COLUMN IF NOT EXISTS quality_rule_version VARCHAR(64),
    ADD COLUMN IF NOT EXISTS manifest_uri TEXT;

-- ============================================================
-- PHASE 3 — crawl frontier, run/attempt audit, source circuit
-- Operational metadata only. Analytical truth stays in Bronze/Silver/Gold;
-- nothing here stores a raw body, cookie, authorization header or traceback.
-- ============================================================
CREATE TABLE IF NOT EXISTS audit.crawl_frontier (
    task_id VARCHAR(80) PRIMARY KEY,
    marketplace_code VARCHAR(32) NOT NULL CHECK (marketplace_code = lower(marketplace_code)),
    marketplace_id VARCHAR(128) NOT NULL,
    target TEXT NOT NULL CHECK (length(btrim(target)) > 0),
    resource_type VARCHAR(32) NOT NULL CHECK (resource_type IN ('LISTING_PAGE', 'PRODUCT_DETAIL')),
    tier VARCHAR(16) NOT NULL CHECK (tier IN ('ACTIVE', 'NORMAL', 'COLD')),
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority >= 0),
    scheduled_for TIMESTAMPTZ NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('READY', 'LEASED', 'RETRY_WAIT', 'SUCCEEDED', 'FAILED', 'DISABLED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts >= 1),
    lease_owner VARCHAR(128),
    lease_expires_at TIMESTAMPTZ,
    last_http_status INTEGER CHECK (last_http_status IS NULL OR last_http_status BETWEEN 100 AND 599),
    last_error_kind VARCHAR(32) CHECK (last_error_kind IS NULL OR last_error_kind IN (
        'RATE_LIMITED', 'TRANSIENT_NETWORK', 'SERVER_ERROR', 'CLIENT_ERROR',
        'ROBOTS_DENIED', 'STORAGE_ERROR', 'PARSE_ERROR', 'VALIDATION_ERROR', 'PUBLISH_ERROR', 'UNKNOWN')),
    last_error TEXT CHECK (last_error IS NULL OR length(last_error) <= 2000),
    last_success_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (attempts <= max_attempts),
    -- Only a LEASED row may carry lease fields, and it must carry both.
    CHECK ((status = 'LEASED') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_crawl_frontier_due
ON audit.crawl_frontier (status, scheduled_for, priority DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_frontier_schedule
ON audit.crawl_frontier (marketplace_code, resource_type, target, scheduled_for);

CREATE TABLE IF NOT EXISTS audit.crawl_run (
    crawl_run_id VARCHAR(128) PRIMARY KEY,
    marketplace_id VARCHAR(128) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status VARCHAR(16) NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'PARTIAL', 'FAILED')),
    requested BIGINT NOT NULL DEFAULT 0 CHECK (requested >= 0),
    succeeded BIGINT NOT NULL DEFAULT 0 CHECK (succeeded >= 0),
    failed BIGINT NOT NULL DEFAULT 0 CHECK (failed >= 0),
    adapter_version VARCHAR(64) NOT NULL,
    error_summary JSONB,
    CHECK (completed_at IS NULL OR completed_at >= started_at),
    CHECK ((status = 'RUNNING') = (completed_at IS NULL)),
    CHECK (succeeded + failed <= requested)
);

CREATE TABLE IF NOT EXISTS audit.crawl_request_attempt (
    attempt_id BIGSERIAL PRIMARY KEY,
    crawl_run_id VARCHAR(128) NOT NULL REFERENCES audit.crawl_run(crawl_run_id),
    task_id VARCHAR(80) NOT NULL REFERENCES audit.crawl_frontier(task_id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('SUCCEEDED', 'PARTIAL', 'FAILED')),
    http_status INTEGER CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    latency_ms BIGINT CHECK (latency_ms IS NULL OR latency_ms >= 0),
    raw_artifact_id VARCHAR(80),
    raw_uri TEXT,
    raw_bytes BIGINT NOT NULL DEFAULT 0 CHECK (raw_bytes >= 0),
    parsed_count BIGINT NOT NULL DEFAULT 0 CHECK (parsed_count >= 0),
    rejected_count BIGINT NOT NULL DEFAULT 0 CHECK (rejected_count >= 0),
    error_kind VARCHAR(32) CHECK (error_kind IS NULL OR error_kind IN (
        'RATE_LIMITED', 'TRANSIENT_NETWORK', 'SERVER_ERROR', 'CLIENT_ERROR',
        'ROBOTS_DENIED', 'STORAGE_ERROR', 'PARSE_ERROR', 'VALIDATION_ERROR', 'PUBLISH_ERROR', 'UNKNOWN')),
    error_message TEXT CHECK (error_message IS NULL OR length(error_message) <= 2000),
    CHECK (completed_at >= started_at)
);
CREATE INDEX IF NOT EXISTS idx_crawl_attempt_run ON audit.crawl_request_attempt (crawl_run_id, started_at);
CREATE INDEX IF NOT EXISTS idx_crawl_attempt_task ON audit.crawl_request_attempt (task_id, started_at);

-- Phase 8: PUBLISH_ERROR, a Kafka failure after a good fetch and parse. The
-- inline lists above cover a fresh volume; these re-add the two constraints
-- under the names PostgreSQL generated for them, so an existing volume ends
-- in the same state. Safe to run twice.
ALTER TABLE audit.crawl_frontier DROP CONSTRAINT IF EXISTS crawl_frontier_last_error_kind_check;
ALTER TABLE audit.crawl_frontier
    ADD CONSTRAINT crawl_frontier_last_error_kind_check CHECK (last_error_kind IS NULL OR last_error_kind IN (
        'RATE_LIMITED', 'TRANSIENT_NETWORK', 'SERVER_ERROR', 'CLIENT_ERROR',
        'ROBOTS_DENIED', 'STORAGE_ERROR', 'PARSE_ERROR', 'VALIDATION_ERROR', 'PUBLISH_ERROR', 'UNKNOWN'));
ALTER TABLE audit.crawl_request_attempt DROP CONSTRAINT IF EXISTS crawl_request_attempt_error_kind_check;
ALTER TABLE audit.crawl_request_attempt
    ADD CONSTRAINT crawl_request_attempt_error_kind_check CHECK (error_kind IS NULL OR error_kind IN (
        'RATE_LIMITED', 'TRANSIENT_NETWORK', 'SERVER_ERROR', 'CLIENT_ERROR',
        'ROBOTS_DENIED', 'STORAGE_ERROR', 'PARSE_ERROR', 'VALIDATION_ERROR', 'PUBLISH_ERROR', 'UNKNOWN'));

CREATE TABLE IF NOT EXISTS audit.crawl_source_state (
    marketplace_code VARCHAR(32) PRIMARY KEY CHECK (marketplace_code = lower(marketplace_code)),
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    -- The circuit is open only while opened_until > now(); there is no boolean.
    opened_until TIMESTAMPTZ,
    last_failure_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
