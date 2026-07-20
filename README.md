# E-commerce Behavioral Analytics — Lambda Architecture

A data engineering project that processes the public Kaggle **Multi-Category
Store** behavior dataset through a batch path and a realtime path. Scope is
limited to the facts the source actually contains: product **views**, **cart**
additions and **purchases**, plus users, products, categories and sessions.
There are no orders, payments, reviews, fraud or geography — the dataset has
none, and the code never invents them.

## Architecture at a glance

```text
Kaggle CSV → Kafka (ecommerce_events)
   ├─ SPEED  Spark Structured Streaming → Elasticsearch + Redis → Kibana
   └─ BATCH  Spark EtLT on MinIO (Bronze→Silver→Gold star schema + marts + ML)
             → PostgreSQL `cache` → Superset
```

- **MinIO (S3A)** is the authoritative warehouse. **PostgreSQL** is only a BI
  cache so Superset always has data.
- Batch is visualized in **Superset**; realtime in **Kibana**.
- One **canonical event contract** (`config/schema.py`) is shared by every layer.

Full detail: **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** and
**[docs/DATA_MODEL.md](docs/DATA_MODEL.md)** (contract, medallion zones, star
schema ERD, marts, ML outputs).

## Quick start

Requires Docker Desktop (≥ 8 GB RAM) and the Kaggle CSV at
`data/data_kaggle/2019-Oct.csv`.

```powershell
# Copy config
Copy-Item .env.example .env

# Full stack: infra + realtime + producer + batch EtLT + ML + BI
.\scripts\start_all.ps1 -RunEverything -Source .\data\data_kaggle\2019-Oct.csv -BuildWarehouseImage

# Batch + ML + BI only (image already built)
.\scripts\start_all.ps1 -RunBatch -RefreshBI -Source .\data\data_kaggle\2019-Oct.csv
```

## Machine learning (batch)

`batch_layer/ml_job.py` reads `cache.daily_revenue` and produces:

- **Revenue forecast** — Darts **N-BEATS** and **LSTM** (Strategy pattern:
  `analytics/forecasters.py` + `analytics/trend_predictor.py`).
- **Anomaly detection** — PyOD **AutoEncoder** (`analytics/anomaly_detector.py`).

> `darts`, `pyod` and `torch` require **Python 3.10–3.12**. On another
> interpreter the models degrade to a naive forecast / IsolationForest so the
> pipeline still completes. Run the ML step in a 3.11 venv for the real models.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest tests\ -q
```

Covers the canonical contract, the producer, the Spark warehouse transforms
(normalization, star schema, marts, quality gate), the speed-layer aggregation,
the cache publisher and the ML strategies.

## Services

| Service | URL / endpoint |
|---|---|
| Superset (batch BI) | http://localhost:8088 (`admin` / `admin`) |
| Kibana (realtime) | http://localhost:5601 |
| MinIO console | http://localhost:9001 |
| Spark master | http://localhost:8080 |
| Elasticsearch | http://localhost:9200 |
| PostgreSQL | localhost:5433 |

## Repository layout

```text
config/
  schema.py                 Canonical behavioral-event contract
  settings.py               Env-driven runtime configuration
data_ingestion/
  producer.py               Kaggle CSV → Kafka (canonical events)
  schemas.py                Spark schema for the contract
  es_indexer.py             Kafka → Elasticsearch raw events
speed_layer/
  speed_layer.py            Spark Structured Streaming → ES + Redis
batch_layer/
  warehouse_job.py          Spark EtLT on MinIO + quality gate + cache publish
  ml_job.py                 Orchestrates forecasting + anomaly detection
  postgres_cache.py         ML cache reader/writer (gateway)
  analytics/                base.py, forecasters.py, trend_predictor.py, anomaly_detector.py
serving_layer/
  postgres_views.py         Convenience views over the cache
  redis_cache.py            Redis KPI access
scripts/
  init_postgres.sql         cache + audit DDL (idempotent)
  start_all.ps1             One-command runner
  run_warehouse.ps1         Batch warehouse helper
  validate_warehouse.sql    Post-run cache/audit checks
display/
  superset/                 Superset provisioning
  kibana/                   Kibana data-view provisioning
docker/spark-warehouse/     Pinned Spark 3.5.1 batch image (JDBC + S3A jars)
docs/                       ARCHITECTURE.md, DATA_MODEL.md
tests/                      Unit + Spark transform tests
```

## Dataset

Source: Kaggle `ecommerce-behavior-data-from-multi-category-store`. Columns:
`event_time, event_type, product_id, category_id, category_code, brand, price,
user_id, user_session`. One purchase row is one **purchase event/item**, never
an order.
