# E-commerce Behavioral Analytics — Lambda Architecture

An end-to-end data platform built around a **Lambda architecture**: one canonical
event contract feeds a *speed path* (low-latency, approximate) and a *batch path*
(accurate, replayable, dimensionally modelled), each with its own serving store
and its own BI surface.

The platform is deliberately **scope-honest**. The primary source is the public
Kaggle *Multi-Category Store* behavior dataset, which contains exactly three
facts — product **views**, **cart** additions and **purchases** — plus users,
products, categories and sessions. There are no orders, payments, reviews, fraud
signals or geography in the source, and no layer of this system invents them.
A second, independent source (a multi-site price crawler) was added later with
its own contract, because a crawler can observe public catalog/price state but
*cannot* observe real user behavior.

---

## 1. Architecture at a glance

```mermaid
flowchart TB
    subgraph SRC["Sources"]
        K[Kaggle CSV<br/>behavioral events]
        C["Multi-site crawler<br/>(Tiki — live)"]
    end

    subgraph ING["Ingestion"]
        P["producer.py<br/>normalize → canonical contract"]
        R["crawler/runner.py<br/>normalize → price-snapshot contract"]
        KAFKA{{"Kafka (KRaft)<br/>ecommerce_events · ecommerce_price_snapshots · *.dlq"}}
    end

    subgraph SPEED["Speed path — seconds"]
        SS["Spark Structured Streaming<br/>1-min windows · 2-min watermark"]
        ESI["es_indexer.py<br/>raw-event drill-down"]
        ES[("Elasticsearch")]
        RD[("Redis rt:kpi:*")]
        KB["Kibana"]
    end

    subgraph BATCH["Batch path — accurate & replayable"]
        WJ["warehouse_job.py — Spark EtLT<br/>Bronze → Silver (+quarantine) → Gold"]
        LAKE[("MinIO S3A<br/>authoritative warehouse")]
        QG{"Data-quality gate<br/>5 mandatory checks"}
        ML["ml_job.py<br/>N-BEATS · LSTM · AutoEncoder"]
        PG[("PostgreSQL<br/>BI cache + audit")]
        SUP["Superset"]
    end

    K --> P --> KAFKA
    C --> R --> KAFKA
    R -.raw JSON.-> LAKE
    KAFKA --> SS --> ES & RD
    KAFKA --> ESI --> ES
    ES --> KB
    K --> WJ --> LAKE
    WJ --> QG -->|pass| PG
    QG -->|fail| PG
    PG --> ML --> PG --> SUP
```

**Storage roles are explicit and enforced in code**

| Store | Role | Not |
|---|---|---|
| **MinIO (S3A)** | Authoritative warehouse — Bronze / Silver / Gold, replayable history | — |
| **PostgreSQL** | *BI cache only* — compact marts + ML outputs + audit trail | Not the system of record |
| **Elasticsearch** | Realtime search & time-series store behind Kibana | Not queried by the batch path |
| **Redis** | Latest realtime KPI counters for a low-latency API surface | Not a dashboard source |

---

## 2. Technology stack

| Concern | Choice | Version |
|---|---|---|
| Streaming bus | Apache Kafka (KRaft, no ZooKeeper) | 4.2.0 |
| Distributed processing | Apache Spark (batch + Structured Streaming) | 3.5.1 |
| Object store / lakehouse | MinIO (S3A) | 2025-09 release |
| BI serving DB | PostgreSQL | 18.3 |
| Realtime KPI cache | Redis | 8.6.2 |
| Search & realtime analytics | Elasticsearch + Kibana | 8.18.0 |
| Batch BI | Apache Superset | 4.1.1 |
| Forecasting | Darts (N-BEATS, LSTM/RNN) + PyTorch | ≥ 0.27 |
| Anomaly detection | PyOD AutoEncoder, scikit-learn fallback | ≥ 1.1 |
| Orchestration (current) | Idempotent PowerShell runner + Compose profiles | — |

---

## 3. What each layer actually does

### 3.0 Contract & configuration layer — `config/`

The foundation the rest of the system is built on: **one contract per data
domain, one place for runtime configuration.**

- **`schema.py` — canonical behavioral-event contract.** Nine fields
  (`event_time, event_type, user_id, user_session, product_id, category_id,
  category_code, brand, price`). `normalize_event()` maps raw Kaggle columns *and*
  already-canonical keys onto that shape, resolving event-type aliases
  (`page_view → view`, `add_to_cart → cart`). `validate_event()` rejects
  unsupported event types, missing identifiers and negative prices.
  The producer, the speed layer and the ES indexer all import this module, so
  **the layers cannot drift apart on event shape.** The Spark batch job
  implements the identical rules column-wise for scale, and a dedicated test
  pins the two implementations to the same behavior.
- **`schema.py` — price-snapshot contract (crawler).** Fourteen fields
  (`snapshot_time, site, product_id, price, list_price, rating, in_stock, …`),
  kept **deliberately separate** from the behavioral contract rather than bolted
  onto it — the two describe fundamentally different observations.
- **`settings.py`.** All runtime configuration is env-driven with safe local
  defaults, so identical code runs on a laptop, inside Docker and in CI.
  `DATA_LAKE_MODE` switches every lake write between local Parquet (dev/tests)
  and MinIO S3A (Docker) through a single `data_lake_uri(zone, dataset)` helper.

### 3.1 Ingestion layer — `data_ingestion/`

- **`producer.py`** streams the Kaggle CSV into Kafka at a configurable rate
  (`--eps`), with bounded connection retries (5 attempts), `acks=all`, batching
  and partition keying by `user_session` (falling back to `user_id`) so all events
  of one session land on the same partition and stay ordered. Supports
  `--loop` for continuous replay and `--test-mode` for a dry print.
- **`es_indexer.py`** is a separate consumer that bulk-indexes *raw* events into
  `ecommerce-events` (batch size 200, dedicated consumer group, deterministic
  `event_id`) so Kibana can drill down to individual events. This is intentionally
  split from the speed layer: aggregation and drill-down have different failure
  modes and different retention needs.
- **`schemas.py`** exposes the canonical contract as a Spark `StructType` used by
  the streaming reader.

### 3.2 Acquisition layer (multi-site crawler) — `crawler/`, `common/`

A second, live data source, engineered to the same standards as the rest of the
platform rather than as a throwaway script.

- **`crawler/base.py` — `SiteCrawler` (Template Method).** The base class owns
  everything every site must get right *identically*: **robots.txt compliance**
  (fail-closed — an unreadable robots.txt is treated as *disallowed*),
  **rate limiting with jitter** (default 2 s ± 1 s), a configurable research
  user-agent, and normalization/validation via the price-snapshot contract.
  Site adapters implement only `fetch_listing()` and `parse_product()`.
- **Error isolation at three levels.** A failed category fetch does not abort the
  site; a single unparseable product does not abort the category; a failed site
  does not abort the other sites. Every rejection is *yielded* as a quarantined
  record rather than swallowed.
- **`crawler/sites/tiki.py`** — Tiki adapter over the public listing JSON API.
  Field mapping was **verified against a live response**; because Tiki publishes
  no stable API contract, a shape change fails loudly through
  `normalize_price_snapshot()` (missing `product_id`/`price`) instead of silently
  corrupting data. Known gaps (e.g. seller display name requires an extra
  per-product call that would blow the rate-limit budget) are documented in code.
- **`crawler/runner.py`** — CLI (`python -m crawler.runner --site tiki [--dry-run]`)
  that writes the untouched raw JSON to Bronze (`crawl_raw/{site}/{date}/…`) *and*
  publishes the normalized snapshot to Kafka, so re-parsing history never requires
  re-crawling.
- **`common/dlq.py`** — every rejected record goes to `<topic>.dlq` with its error
  type, message and original payload. The DLQ publisher **never raises**: a DLQ
  outage must not take down the ingestion path it protects.
- **`common/object_store.py`** — plain-Python Bronze writes that mirror the
  local/S3A duality of `data_lake_uri()`, so a crawler does not need a Spark
  session just to land a JSON blob.

### 3.3 Speed layer — `speed_layer/`

Spark Structured Streaming over the canonical topic:

- **1-minute tumbling windows** grouped by `event_type`, with a **2-minute
  watermark** to bound state and tolerate late arrivals; `update` output mode on a
  10-second processing trigger.
- Metrics per window: event count, `approx_count_distinct(user_id)`
  (HyperLogLog — the right accuracy/cost trade-off for a speed view), and summed
  amount, with revenue attributed only to `purchase` events.
- A single `foreachBatch` sink fans out to **both** serving stores in one pass:
  bulk upsert into Elasticsearch (`ecommerce-metrics`, deterministic
  `window:event_type` document id → idempotent re-delivery) and a pipelined Redis
  write of `rt:kpi:{event_type}` hashes (1-hour TTL) plus a trimmed
  `rt:kpi:revenue_series` list (last 240 points).
- Checkpointing is configured for restart-safe offsets.

### 3.4 Batch layer — `batch_layer/warehouse_job.py`

The heart of the project: a Spark **EtLT** pipeline composed of small, pure,
independently testable stages orchestrated by `run_warehouse()`.

| Stage | What it guarantees |
|---|---|
| `read_source` | CSV or JSON/JSONL, with source-file lineage attached |
| `write_bronze` | Append-only raw landing, partitioned by ingest date + run id |
| `normalize_events` | Canonical contract applied column-wise; **deterministic `event_key = SHA-256(event_type ‖ user_id ‖ product_id ‖ session_id ‖ event_time)`** |
| `split_valid_events` | Valid rows deduplicated by `event_key`; rejects **quarantined with a reason** (`unsupported_event_type`, `missing_user_id`, `negative_price`, …) instead of dropped |
| `read_canonical_silver` | Reprocessing-safe read: latest record wins per `event_key` |
| `build_dimensions` | `dim_date`, `dim_user`, `dim_product`, `dim_category` (SCD-1; surrogate keys via `xxhash64`; category hierarchy split from the dotted code) |
| `build_fact` | `fact_behavior_event` — exactly one row per valid source event |
| `build_marts` | Five BI marts (below) |
| `quality_checks` | Five mandatory gates |
| `write_gold` | Star schema + marts written to the MinIO Gold zone |
| `publish_postgres` | Atomic staging → cache swap |

**Idempotency.** Because `event_key` is content-derived, replaying the same file
never duplicates a fact — the same run can be re-executed safely after a failure.

**Data-quality gate (blocking).** The job **refuses to publish** if any of these
fail: Silver non-empty · `event_key` unique · required identifiers/timestamps
complete · zero dimension orphans in the fact · product dimension non-empty.
Every check — pass or fail, with its observed value and its expectation — is
persisted to `audit.data_quality_result`, and every run to `audit.pipeline_run`
(status, row counts per zone, rejected rows, error message). **A failed run is
therefore as observable as a successful one.**

**Atomic publish.** Marts are written to a `staging` schema over JDBC, then a
single transaction truncates and refills `cache`. Superset always sees either
the last good version or the complete new version — **never a half-loaded
dashboard.**

**Marts produced:** `funnel_daily` (viewers → cart users → buyers with
view→cart, cart→purchase, conversion and abandonment rates), `product_daily`,
`category_daily`, `session_daily` (duration, counts, and an outcome label of
`converted` / `abandoned_cart` / `browsing`) and `daily_revenue` — the feature
table consumed by the ML job.

### 3.5 Batch ML layer — `batch_layer/analytics/`, `ml_job.py`

Two production concerns modelled as extensible strategies, not as notebook code.

- **Forecasting — Strategy + Template Method.** `Forecaster` (abstract) owns the
  shared workflow: history preparation, series construction, scaling, covariate
  building, prediction inverse-transform and fallback. Concrete strategies
  (`NBeatsForecaster`, `LstmForecaster`) implement only `_train()`.
  `TrendPredictor` orchestrates any set of strategies — **open for extension,
  closed for modification**: adding a model touches neither the orchestrator nor
  any caller.
- **Numerical engineering, documented in code.** Revenue is raw-scale, which made
  the RNN collapse toward zero on a short series; a Darts `Scaler` fit at train
  time and inverted at predict time fixes it. N-BEATS additionally enables
  **RevIN** for level/variance shift. Each strategy declares which covariate kind
  its Darts class actually supports (`past` vs `future`), resolved through a
  single helper so `fit()` and `predict()` cannot disagree.
- **Feature engineering under data scarcity.** Cyclical day-of-week
  (`sin`/`cos`) plus a weekend flag squeeze extra signal out of the ~30-day
  window rather than demanding a longer history.
- **Honest evaluation.** `backtest()` runs a **rolling-origin (walk-forward)**
  evaluation — expanding train window, origin moved forward per fold — and
  averages MAE / MSE / RMSE / MAPE per model across folds, degrading to a single
  split when history is too short. Per-fold detail is retained for inspection.
  This avoids drawing conclusions from one lucky split.
- **Anomaly detection.** PyOD **AutoEncoder** over
  `(revenue, purchase_events, avg_purchase_value)` with standardized features and
  a batch size capped for short daily series (PyOD's loader drops the last partial
  batch and would otherwise train on zero batches).
- **Graceful degradation as a design rule.** If `darts` / `pyod` / `torch` are
  unavailable, forecasting falls back to a naive baseline and detection to
  `IsolationForest`, and the active backend is reported. **The pipeline always
  completes and always says what it actually ran.**

### 3.6 Serving layer — `serving_layer/`, `batch_layer/postgres_cache.py`

Storage access sits behind small, intention-revealing gateways rather than being
scattered across jobs.

- `PostgresCacheSync` — reads the `daily_revenue` feature table, writes
  `predictions` / `anomalies` with truncate-and-insert so consumers never see a
  partially refreshed model output.
- `postgres_views.py` — reshaping views over the cache for common charts
  (`v_revenue_forecast` for actual-vs-forecast, `v_anomaly_days`, `v_top_products`),
  managed by a `create` / `drop` CLI.
- `redis_cache.py` — typed read helpers over `rt:kpi:*` for a low-latency KPI/API
  surface.

### 3.7 Presentation layer — `display/`

Dashboards are **provisioned as code**, not clicked together by hand.

- **Superset (batch BI)** — automated bootstrap (`setup_superset.sh`), runtime
  patching, declarative database + dataset registration (`datasources.yaml`),
  and scripted dashboard creation for both the descriptive marts and the ML
  outputs (forecast comparison + anomalies).
- **Kibana (realtime)** — data views and a speed-layer dashboard created through
  the Saved Objects API. Classic aggregation-based visualizations were chosen
  over Lens **deliberately**: their saved-object schema is stable across Kibana
  versions, avoiding migration-sensitive internal state.

### 3.8 Infrastructure & operations

- **`docker-compose.yml`** — the full stack with health checks and
  `depends_on: service_healthy` conditions so start-up ordering is real rather
  than hopeful; buckets auto-created by an init container; heavy one-shot jobs
  behind a `jobs` **profile** so they never run resident.
- **`docker/spark-warehouse/Dockerfile`** — a pinned Spark 3.5.1 image with the
  PostgreSQL JDBC and S3A jars baked in. Dependency resolution is kept *out* of
  application code, so batch runs are reproducible and work offline.
- **`scripts/init_postgres.sql`** — idempotent DDL for the `cache`, `staging` and
  `audit` schemas, applied at container init *and* before every publish.
- **`scripts/start_all.ps1`** — one-command runner with composable switches
  (`-RunEverything`, `-RunRealtime`, `-RunBatch`, `-RefreshBI`, `-SmokeCheck`,
  `-BuildWarehouseImage`, …), plus `smoke_fullstack.ps1` and
  `validate_warehouse.sql` for post-run verification.

### 3.9 Quality assurance — `tests/`

**27 tests, all passing** (`pytest tests/ -q`), covering the parts most likely to
break silently:

| Area | What is asserted |
|---|---|
| Canonical contract | Normalization, alias mapping, validation rules, wire serialization |
| Ingestion contract | The Kaggle mapping preserves **only** facts the source actually contains |
| Spark warehouse transforms | Normalization preserves source grain · star schema has **no orphans** · funnel and session marts are correct |
| Speed layer | Windowed aggregation by event type |
| Crawler | robots.txt gating, throttling, per-record error isolation, adapter parsing against a captured fixture |
| Object store | Local and S3A path resolution |
| ML | Forecast shape per strategy, backtest metrics per model, anomaly labelling |
| Cache gateway | Prediction/anomaly write contract |

---

## 4. Data model

Full detail — canonical contract, medallion zones, star-schema ERD, mart grains,
ML output tables and audit tables — lives in
**[docs/DATA_MODEL.md](docs/DATA_MODEL.md)**; architecture rationale and design
patterns in **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

Summary of the Gold star schema:

- `fact_behavior_event` — one row per valid source event, keyed by a
  content-derived `event_key`.
- `dim_date`, `dim_user`, `dim_product`, `dim_category` — SCD Type 1.

Semantics are stated precisely: `revenue` is the sum of `price` over purchase
events and is **never** described as an order metric; funnel rates are same-day
population ratios, **not** ordered-path attribution.

---

## 5. Quick start

Requires Docker Desktop (≥ 8 GB RAM) and the Kaggle CSV at
`data/data_kaggle/2019-Oct.csv`. Runtime configuration is read from `.env` at the
repository root; every value has a working local default in `config/settings.py`.

```powershell
# Full stack: infra + realtime + producer + batch EtLT + ML + BI
.\scripts\start_all.ps1 -RunEverything -Source .\data\data_kaggle\2019-Oct.csv -BuildWarehouseImage

# Batch + ML + BI only (warehouse image already built)
.\scripts\start_all.ps1 -RunBatch -RefreshBI -Source .\data\data_kaggle\2019-Oct.csv

# Crawler (live Tiki listing API; --dry-run performs no Kafka/MinIO writes)
.\.venv\Scripts\python.exe -m crawler.runner --site tiki --dry-run

# Test suite
.\.venv\Scripts\python.exe -m pytest tests\ -q
```

| Service | URL | Credentials |
|---|---|---|
| Superset (batch BI) | http://localhost:8088 | `admin` / `admin` |
| Kibana (realtime) | http://localhost:5601 | — |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Spark master UI | http://localhost:8080 | — |
| Elasticsearch | http://localhost:9200 | — |
| PostgreSQL | `localhost:5433` | `admin` / `admin123` |

---

## 6. Repository layout

```text
config/
  schema.py                 Canonical event contract + price-snapshot contract
  settings.py               Env-driven runtime configuration (local ⇄ S3A)
common/
  dlq.py                    Dead-letter publishing shared by all ingestion paths
  object_store.py           Spark-free Bronze writes (local ⇄ MinIO)
data_ingestion/
  producer.py               Kaggle CSV → Kafka (canonical events)
  schemas.py                Spark StructType for the contract
  es_indexer.py             Kafka → Elasticsearch raw events
crawler/
  base.py                   SiteCrawler: robots.txt, rate limit, error isolation
  sites/tiki.py             Tiki listing-API adapter (live-verified)
  runner.py                 CLI: crawl → Bronze raw + Kafka + DLQ
speed_layer/
  speed_layer.py            Structured Streaming → Elasticsearch + Redis
batch_layer/
  warehouse_job.py          Spark EtLT, quality gate, atomic cache publish
  ml_job.py                 Forecasting + anomaly detection orchestration
  postgres_cache.py         ML cache gateway
  analytics/                base.py · forecasters.py · trend_predictor.py · anomaly_detector.py
serving_layer/
  postgres_views.py         Convenience views over the BI cache
  redis_cache.py            Realtime KPI read helpers
display/
  superset/                 Superset provisioning + dashboards as code
  kibana/                   Kibana data views + speed dashboard
docker/spark-warehouse/     Pinned Spark 3.5.1 batch image (JDBC + S3A jars)
scripts/                    init_postgres.sql · start_all.ps1 · smoke & validation
docs/                       ARCHITECTURE.md · DATA_MODEL.md · PROGRESS.md
tests/                      Contract, Spark transform, streaming, crawler, ML tests
```

---

## 7. Status & roadmap

| # | Milestone | Status |
|---|---|---|
| — | Lambda core: ingestion, speed, batch EtLT, star schema, marts, quality gate, audit | ✅ Delivered |
| — | Batch ML: N-BEATS + LSTM forecasting, AutoEncoder anomaly detection, rolling-origin backtest | ✅ Delivered |
| — | BI: Superset + Kibana provisioned as code | ✅ Delivered |
| 1 | Multi-site price crawler | 🔶 Tiki live-verified; Shopee/Lazada (Playwright) pending |
| 2 | Ingestion hardening — retrofit DLQ into `producer.py` / `es_indexer.py` | ⏳ Planned |
| 3 | Speed-layer hardening | ⏳ Planned |
| 4 | Delta Lake gold zone + incremental processing + price warehouse job | ⏳ Planned |
| 5 | ML maturity — MLflow tracking, prediction history, price-anomaly models | ⏳ Planned |
| 6 | Orchestration — Airflow (LocalExecutor, reusing `postgres-dw`) | ⏳ Planned |
| 7 | Serving API — FastAPI over the cache + Redis | ⏳ Planned |

Detailed working state, decisions already settled and the exact resume point are
tracked in **[docs/PROGRESS.md](docs/PROGRESS.md)**.

---

## 8. Known limits (stated deliberately)

- **Delivery semantics are at-least-once, not exactly-once.** `kafka-python-ng`
  does not implement a true idempotent producer; the system compensates with
  content-derived keys (`event_key`, deterministic ES document ids) that make
  re-delivery harmless, plus an observable DLQ. The stronger claim is not made.
- **The crawler is a research crawler.** It respects `robots.txt`, rate-limits
  itself and identifies itself honestly. It does **not** attempt to evade blocking,
  and it is not intended for large-scale extraction.
- **Crawled `product_id` is site-scoped** — cross-site price comparison requires a
  separate product-matching step, which is out of scope for the current cut.
- **The real ML models require Python 3.10–3.12** (`darts`, `pyod`, `torch`).
  On any other interpreter the job runs its documented fallbacks rather than
  failing, and reports which backend was actually used.
- **Sessionization is source-provided.** `user_session` comes from the dataset;
  no session-stitching heuristic is applied.

---

## 9. Dataset

Kaggle — *eCommerce behavior data from multi-category store*. Columns:
`event_time, event_type, product_id, category_id, category_code, brand, price,
user_id, user_session`.

One purchase row represents one **purchase event/item** — never an order.
