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
