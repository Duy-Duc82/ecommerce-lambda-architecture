# Mô hình dữ liệu marketplace: hiện tại và star schema đề xuất

Mở file này trong VS Code (Markdown Preview Mermaid Support), trên GitHub, hoặc dán từng khối ` ```mermaid ` vào https://mermaid.live. Cũng có thể mở `star-schema.html`, file này tự vẽ cả hai sơ đồ.

## Nguồn định nghĩa trong code (branch `phase-9-tiki-category-html`)

| Phần | File |
|---|---|
| Contract của observation, change, DLQ | `config/marketplace_schema.py`; mô tả ở `docs/DATA_MODEL.md` §1 |
| 10 mart Gold (cách tính từng bảng) | `batch_layer/marketplace_marts.py`: `build_offer_current`, `build_seller_current`, `build_offer_price_history_daily`, `build_offer_change_daily`, `build_offer_freshness`, `build_category_price_daily`, `build_source_coverage_daily`, `build_crawl_reliability_daily`, `build_counter_transitions` / `build_counter_delta_daily` |
| Phát hiện giá bất thường | `batch_layer/marketplace_anomaly.py` |
| Bảng Postgres phục vụ BI (`cache.marketplace_*`, `audit.*`) | `scripts/init_postgres.sql`; mô tả ở `docs/DATA_MODEL.md` §4–5 |
| Star schema legacy (Kaggle: `DIM_DATE`, `DIM_USER`, `DIM_PRODUCT`, `DIM_CATEGORY`, `FACT_BEHAVIOR_EVENT`) | `batch_layer/warehouse_job.py`; `docs/DATA_MODEL.md` §9 |

**Hiện chưa có star schema dim/fact cho marketplace.** Gold gồm 10 mart. Mỗi mart có grain riêng và nối với các mart khác bằng natural key (`offer_id`, `seller_id`, `category_path`, `marketplace`, `observed_date`). Không có bảng dim với surrogate key.

## 1. Mô hình hiện tại (đúng với code)

```mermaid
erDiagram
    SILVER_OFFER_OBSERVATION ||--o{ MARKETPLACE_OFFER_CURRENT : "latest per offer_id"
    SILVER_OFFER_OBSERVATION ||--o{ MARKETPLACE_OFFER_PRICE_HISTORY_DAILY : "per offer, day, currency"
    SILVER_OFFER_OBSERVATION ||--o{ MARKETPLACE_OFFER_CHANGE_DAILY : "per offer, day, currency"
    SILVER_OFFER_OBSERVATION ||--o{ MARKETPLACE_PRICE_ANOMALY_DAILY : "per offer, day, currency"
    SILVER_OFFER_OBSERVATION ||--o{ MARKETPLACE_COUNTER_DELTA_DAILY : "per offer, day, counter"
    SILVER_OFFER_OBSERVATION ||--o{ MARKETPLACE_CATEGORY_PRICE_DAILY : "per category, day"
    SILVER_OFFER_OBSERVATION ||--o{ MARKETPLACE_SELLER_CURRENT : "per seller"
    MARKETPLACE_OFFER_CURRENT ||--|| MARKETPLACE_OFFER_FRESHNESS : offer_id
    MARKETPLACE_OFFER_CURRENT ||--o{ MARKETPLACE_OFFER_PRICE_HISTORY_DAILY : offer_id
    MARKETPLACE_OFFER_CURRENT ||--o{ MARKETPLACE_OFFER_CHANGE_DAILY : offer_id
    MARKETPLACE_OFFER_CURRENT ||--o{ MARKETPLACE_PRICE_ANOMALY_DAILY : offer_id
    MARKETPLACE_OFFER_CURRENT ||--o{ MARKETPLACE_COUNTER_DELTA_DAILY : offer_id
    MARKETPLACE_SELLER_CURRENT ||--o{ MARKETPLACE_OFFER_CURRENT : seller_id
    MARKETPLACE_CATEGORY_PRICE_DAILY }o--o{ MARKETPLACE_OFFER_CURRENT : category_path
    AUDIT_CRAWL_REQUEST_ATTEMPT ||--o{ MARKETPLACE_CRAWL_RELIABILITY_DAILY : "per marketplace, day"
    AUDIT_CRAWL_REQUEST_ATTEMPT ||--o{ MARKETPLACE_SOURCE_COVERAGE_DAILY : "per marketplace, day"

    SILVER_OFFER_OBSERVATION {
        string observation_id PK
        string offer_id
        string marketplace
        string platform_listing_id
        string seller_id
        string category_path
        timestamp observed_at
        decimal current_price
        decimal list_price
        decimal discount_percent
        decimal rating_value
        bigint rating_count
        bigint review_count
        bigint sold_count
        string availability
        bigint ranking_position
        string crawl_run_id
        string raw_uri
    }
    MARKETPLACE_OFFER_CURRENT {
        string offer_id PK
        string marketplace
        string platform_listing_id
        string seller_id
        string product_title
        string brand
        string category_path
        string currency
        timestamp first_seen_at
        timestamp last_seen_at
        decimal current_price
        decimal list_price
        bigint sold_count
        string availability
        string raw_uri
    }
    MARKETPLACE_SELLER_CURRENT {
        string seller_id PK
        string marketplace PK
        timestamp first_seen_at
        timestamp last_seen_at
        bigint observed_offer_count
    }
    MARKETPLACE_OFFER_PRICE_HISTORY_DAILY {
        string marketplace PK
        string offer_id PK
        date observed_date PK
        string currency PK
        decimal first_price
        decimal last_price
        decimal min_price
        decimal max_price
        decimal avg_price
        bigint observation_count
        bigint distinct_price_count
    }
    MARKETPLACE_OFFER_CHANGE_DAILY {
        string marketplace PK
        string offer_id PK
        date observed_date PK
        string currency PK
        bigint price_change_count
        bigint price_drop_count
        bigint price_increase_count
        decimal max_price_drop
        bigint counter_change_count
        bigint availability_change_count
    }
    MARKETPLACE_PRICE_ANOMALY_DAILY {
        string marketplace PK
        string offer_id PK
        date observed_date PK
        string currency PK
        decimal evaluated_price
        decimal baseline_median
        decimal baseline_mad
        double robust_score
        string anomaly_status
        string anomaly_reason
    }
    MARKETPLACE_COUNTER_DELTA_DAILY {
        string marketplace PK
        string offer_id PK
        date observed_date PK
        string counter_name PK
        bigint first_value
        bigint last_value
        bigint valid_delta_sum
        double velocity_proxy_per_hour
        boolean counter_reset_or_invalid
    }
    MARKETPLACE_OFFER_FRESHNESS {
        string offer_id PK
        timestamp as_of
        timestamp last_observed_at
        bigint age_seconds
        string freshness_status
    }
    MARKETPLACE_CATEGORY_PRICE_DAILY {
        string marketplace PK
        string category_path PK
        date observed_date PK
        string currency PK
        bigint observed_offer_count
        decimal median_price
        decimal p25_price
        decimal p75_price
    }
    MARKETPLACE_SOURCE_COVERAGE_DAILY {
        string marketplace PK
        date observed_date PK
        bigint observed_offer_count
        bigint fresh_offer_count
        double coverage_rate
        double rejection_rate
    }
    MARKETPLACE_CRAWL_RELIABILITY_DAILY {
        string marketplace PK
        date request_date PK
        bigint request_count
        bigint succeeded_count
        double success_rate
        double p95_latency_ms
        bigint rate_limited_count
    }
    AUDIT_CRAWL_REQUEST_ATTEMPT {
        string attempt_id PK
        string crawl_run_id
        string task_id
        timestamp started_at
        timestamp completed_at
        string status
        int http_status
        bigint latency_ms
        bigint raw_bytes
        bigint parsed_count
        string raw_uri
    }
```

## 2. Star schema đề xuất (chưa có trong code)

Các dim lấy từ những cột đang lặp lại trong các mart. Fact giữ nguyên grain của các mart hiện có. Cột nào cũng đã có trong Silver hoặc audit, không cần crawl thêm dữ liệu.

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_OFFER_OBSERVATION : observed_on
    DIM_MARKETPLACE ||--o{ FACT_OFFER_OBSERVATION : listed_on
    DIM_OFFER ||--o{ FACT_OFFER_OBSERVATION : sighting_of
    DIM_SELLER ||--o{ FACT_OFFER_OBSERVATION : sold_by
    DIM_CATEGORY ||--o{ FACT_OFFER_OBSERVATION : classified_as

    DIM_DATE ||--o{ FACT_OFFER_DAILY : day
    DIM_OFFER ||--o{ FACT_OFFER_DAILY : offer
    DIM_SELLER ||--o{ FACT_OFFER_DAILY : seller
    DIM_CATEGORY ||--o{ FACT_OFFER_DAILY : category
    DIM_MARKETPLACE ||--o{ FACT_OFFER_DAILY : marketplace

    DIM_DATE ||--o{ FACT_COUNTER_DELTA_DAILY : day
    DIM_OFFER ||--o{ FACT_COUNTER_DELTA_DAILY : offer

    DIM_DATE ||--o{ FACT_CRAWL_ATTEMPT : requested_on
    DIM_MARKETPLACE ||--o{ FACT_CRAWL_ATTEMPT : source
    DIM_CATEGORY ||--o{ FACT_CRAWL_ATTEMPT : target_page

    DIM_DATE {
        int date_key PK "yyyymmdd"
        date full_date UK
        smallint day_of_week
        smallint month
        smallint quarter
        smallint year
        boolean is_weekend
    }
    DIM_MARKETPLACE {
        smallint marketplace_key PK
        string marketplace_code UK "tiki"
        string marketplace_id
        string country "VN"
    }
    DIM_OFFER {
        bigint offer_key PK
        string offer_id UK "deterministic id"
        string platform_listing_id
        string product_title
        string brand
        string source_url
        string currency
        timestamp valid_from "SCD2"
        timestamp valid_to "SCD2"
        boolean is_current
    }
    DIM_SELLER {
        bigint seller_key PK
        string seller_id UK
        timestamp first_seen_at
        timestamp last_seen_at
    }
    DIM_CATEGORY {
        int category_key PK
        string category_path UK "1/2/1789/1795"
        string leaf_category_id
        string parent_path
        smallint depth
        string category_slug "from frontier target"
    }
    FACT_OFFER_OBSERVATION {
        string observation_id PK "degenerate"
        int date_key FK
        smallint marketplace_key FK
        bigint offer_key FK
        bigint seller_key FK
        int category_key FK
        timestamp observed_at
        decimal current_price
        decimal list_price
        decimal discount_percent
        decimal rating_value
        bigint rating_count
        bigint review_count
        bigint sold_count
        bigint ranking_position
        string availability
        string crawl_run_id "lineage"
        string raw_uri "lineage"
    }
    FACT_OFFER_DAILY {
        int date_key PK
        bigint offer_key PK
        string currency PK
        smallint marketplace_key FK
        bigint seller_key FK
        int category_key FK
        decimal first_price
        decimal last_price
        decimal min_price
        decimal max_price
        decimal avg_price
        bigint observation_count
        bigint price_change_count
        bigint price_drop_count
        bigint price_increase_count
        string anomaly_status
        double robust_score
    }
    FACT_COUNTER_DELTA_DAILY {
        int date_key PK
        bigint offer_key PK
        string counter_name PK
        bigint first_value
        bigint last_value
        bigint valid_delta_sum
        double velocity_proxy_per_hour
        boolean counter_reset_or_invalid
    }
    FACT_CRAWL_ATTEMPT {
        string attempt_id PK
        int date_key FK
        smallint marketplace_key FK
        int category_key FK
        string status
        int http_status
        bigint latency_ms
        bigint raw_bytes
        bigint parsed_count
        bigint rejected_count
        string raw_uri "lineage"
    }
```

### Mỗi bảng mới lấy dữ liệu từ đâu

| Bảng đề xuất | Nguồn hiện có |
|---|---|
| `FACT_OFFER_OBSERVATION` | Silver `offer_observations`, mỗi quan sát một dòng |
| `FACT_OFFER_DAILY` | gộp `marketplace_offer_price_history_daily` + `offer_change_daily` + `price_anomaly_daily` (cả ba cùng grain `offer × ngày × tiền tệ`) |
| `FACT_COUNTER_DELTA_DAILY` | `marketplace_counter_delta_daily` |
| `FACT_CRAWL_ATTEMPT` | `audit.crawl_request_attempt` join `audit.crawl_frontier.target` |
| `DIM_OFFER` | thuộc tính mô tả trong `marketplace_offer_current`, thêm SCD2 vì tiêu đề và thương hiệu có thể đổi |
| `DIM_SELLER` | `marketplace_seller_current` |
| `DIM_CATEGORY` | các giá trị `category_path` khác nhau; slug lấy từ target `<slug>/c<id>` của frontier |
| Các mart tổng hợp (`category_price_daily`, `source_coverage_daily`, `crawl_reliability_daily`, `offer_freshness`) | giữ nguyên làm aggregate trên các fact |
