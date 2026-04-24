-- ============================================================
-- Data Warehouse Schema — PostgreSQL
-- He thong TMDT - Kien truc Lambda
-- ============================================================

-- ── DIMENSION TABLES ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_products (
    product_id   VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category     VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_users (
    user_id      VARCHAR(20) PRIMARY KEY,
    created_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_time (
    date_key     DATE PRIMARY KEY,
    day_of_week  INTEGER,
    week_of_year INTEGER,
    month        INTEGER,
    quarter      INTEGER,
    year         INTEGER,
    is_weekend   BOOLEAN
);

-- Populate dim_time voi 2 nam du lieu
INSERT INTO dim_time (date_key, day_of_week, week_of_year, month, quarter, year, is_weekend)
SELECT
    d::DATE,
    EXTRACT(DOW FROM d)::INTEGER,
    EXTRACT(WEEK FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    EXTRACT(QUARTER FROM d)::INTEGER,
    EXTRACT(YEAR FROM d)::INTEGER,
    EXTRACT(DOW FROM d) IN (0, 6)
FROM generate_series('2025-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT DO NOTHING;

-- ── FACT TABLES ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_events (
    event_id        BIGSERIAL PRIMARY KEY,
    event_type      VARCHAR(30) NOT NULL,
    user_id         VARCHAR(20),
    product_id      VARCHAR(20),
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    quantity        INTEGER,
    price           DOUBLE PRECISION,
    order_id        VARCHAR(50),
    total_amount    DOUBLE PRECISION,
    payment_method  VARCHAR(30),
    rating          INTEGER,
    old_price       DOUBLE PRECISION,
    new_price       DOUBLE PRECISION,
    query_text      VARCHAR(500),
    event_time      TIMESTAMP,
    event_date      DATE,
    event_hour      INTEGER,
    day_of_week     INTEGER
);

-- Indexes cho truy van nhanh
CREATE INDEX IF NOT EXISTS idx_fact_events_type ON fact_events (event_type);
CREATE INDEX IF NOT EXISTS idx_fact_events_date ON fact_events (event_date);
CREATE INDEX IF NOT EXISTS idx_fact_events_user ON fact_events (user_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_product ON fact_events (product_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_category ON fact_events (category);
CREATE INDEX IF NOT EXISTS idx_fact_events_order ON fact_events (order_id);

-- ── AGGREGATION TABLES (Batch Layer output) ──────────────────

CREATE TABLE IF NOT EXISTS agg_daily_sales (
    event_date      DATE PRIMARY KEY,
    total_orders    BIGINT,
    total_revenue   DOUBLE PRECISION,
    unique_buyers   BIGINT
);

CREATE TABLE IF NOT EXISTS agg_product_stats (
    product_id      VARCHAR(20) PRIMARY KEY,
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    views           BIGINT DEFAULT 0,
    cart_adds       BIGINT DEFAULT 0,
    purchases       BIGINT DEFAULT 0,
    reviews         BIGINT DEFAULT 0,
    avg_rating      DOUBLE PRECISION,
    conversion_rate DOUBLE PRECISION DEFAULT 0
);

-- ── ML MODEL OUTPUT TABLES ───────────────────────────────────

-- (1) Trend Analysis
CREATE TABLE IF NOT EXISTS ml_trending_products (
    product_id          VARCHAR(20),
    product_name        VARCHAR(200),
    category            VARCHAR(100),
    recent_views        BIGINT,
    recent_purchases    BIGINT,
    prev_views          BIGINT,
    prev_purchases      BIGINT,
    view_growth_pct     DOUBLE PRECISION,
    purchase_growth_pct DOUBLE PRECISION,
    trend_score         DOUBLE PRECISION
);

-- (2) Anomaly Detection
CREATE TABLE IF NOT EXISTS ml_anomaly_zscore (
    event_time    TIMESTAMP,
    user_id       VARCHAR(20),
    order_id      VARCHAR(50),
    total_amount  DOUBLE PRECISION,
    z_score       DOUBLE PRECISION,
    anomaly_type  VARCHAR(50),
    severity      VARCHAR(20)
);

-- (3) Price Forecast
CREATE TABLE IF NOT EXISTS ml_price_volatility (
    product_id      VARCHAR(20),
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    num_changes     BIGINT,
    avg_change_pct  DOUBLE PRECISION,
    volatility      DOUBLE PRECISION,
    min_price       DOUBLE PRECISION,
    max_price       DOUBLE PRECISION,
    latest_price    DOUBLE PRECISION,
    trend_direction VARCHAR(20)
);

-- (4) Fraud Detection
CREATE TABLE IF NOT EXISTS ml_fraud_high_value (
    event_time      TIMESTAMP,
    user_id         VARCHAR(20),
    order_id        VARCHAR(50),
    total_amount    DOUBLE PRECISION,
    payment_method  VARCHAR(30),
    fraud_rule      VARCHAR(50),
    risk_score      DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS ml_user_features (
    user_id                     VARCHAR(20) PRIMARY KEY,
    total_events                BIGINT,
    purchase_count              BIGINT,
    view_count                  BIGINT,
    cart_count                  BIGINT,
    total_spent                 DOUBLE PRECISION,
    avg_order_value             DOUBLE PRECISION,
    max_order_value             DOUBLE PRECISION,
    active_days                 BIGINT,
    purchase_days               BIGINT,
    payment_methods_used        BIGINT,
    unique_products_interacted  BIGINT,
    purchase_rate               DOUBLE PRECISION,
    cart_abandonment_rate       DOUBLE PRECISION
);

-- ── GRANT (cho nguoi dung doc) ──────────────────────────────
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;

SELECT 'Data Warehouse schema khoi tao thanh cong!' AS status;
