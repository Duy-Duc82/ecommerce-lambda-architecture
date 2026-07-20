# Architecture

A textbook **Lambda architecture** over the Kaggle store behavior dataset: one
ingestion source feeds a **batch path** (accurate, replayable) and a **speed
path** (low-latency), each with its own serving store and dashboard.

```text
                              Kaggle CSV
                                  │
                     data_ingestion/producer.py
                                  │
                        Kafka  ecommerce_events        ← one canonical contract
                 ┌────────────────┴───────────────────┐
        SPEED    │                                     │   BATCH
  ┌──────────────┴───────────────┐      ┌──────────────┴──────────────────────┐
  │ speed_layer  (Spark Struct.  │      │ batch_layer/warehouse_job.py (Spark) │
  │              Streaming)      │      │  Bronze → Silver(+quarantine)        │
  │  → Elasticsearch  metrics    │      │        → Gold star schema + marts    │
  │  → Redis  rt:kpi:*           │      │  quality gate → publish cache        │
  │ es_indexer → ES raw events   │      │ batch_layer/ml_job.py                │
  └──────────────┬───────────────┘      │  N-BEATS/LSTM (Darts) + AutoEncoder  │
                 │                       └──────────────┬──────────────────────┘
              Kibana                        MinIO gold (authoritative warehouse)
                                                        │
                                     PostgreSQL `cache` (compact marts + ML)
                                                        │
                                                    Superset
```

## Layers

| Layer | Package | Responsibility |
|---|---|---|
| Ingestion | `data_ingestion/` | Kaggle CSV → Kafka (producer); Kafka → ES raw events (es_indexer) |
| Speed | `speed_layer/` | Windowed aggregates → Elasticsearch + Redis |
| Batch | `batch_layer/` | Spark EtLT on MinIO; quality gate; publish to cache |
| Batch ML | `batch_layer/analytics/`, `ml_job.py` | Revenue forecast + anomaly detection |
| Serving | `serving_layer/` | Cache views (Postgres) + KPI access (Redis) |
| Config | `config/` | Settings + canonical event contract |
| Display | `display/` | Superset & Kibana provisioning |

## Storage roles

- **MinIO (S3A)** — the authoritative big-data warehouse (Bronze/Silver/Gold).
- **PostgreSQL** — a *BI cache* only (`cache` + `audit` schemas). It is not the
  system of record; the lake is. This keeps Superset populated even when the
  pipeline is idle and keeps the BI DB small.
- **Elasticsearch** — realtime search/analytics store, read by Kibana.
- **Redis** — latest realtime KPI counters for a low-latency API surface.

## Processing model: EtLT

`Extract → transform (validate) → Load Silver → transform (dimensional model) →
Load cache`. Raw history stays replayable in Bronze/Silver; the BI database
never becomes the source of truth. See [DATA_MODEL.md](DATA_MODEL.md).

## Design patterns

- **Single canonical contract** — `config/schema.py` defines the one event
  shape; producer, speed and batch all conform, so layers never disagree.
- **Pipeline of pure stages** — `warehouse_job.py` is a sequence of small,
  independently testable functions (`read_source`, `normalize_events`,
  `build_dimensions`, `build_fact`, `build_marts`, `quality_checks`,
  `write_gold`, `publish_postgres`) orchestrated by `run_warehouse`.
- **Strategy + Template Method** — `analytics/base.Forecaster` is the abstract
  strategy; `NBeatsForecaster` / `LstmForecaster` implement only `_train`, while
  the base owns history prep, series building and the naive fallback.
  `TrendPredictor` is an orchestrator that runs any set of strategies — open for
  extension, closed for modification.
- **Gateway / repository** — `postgres_cache.PostgresCacheSync` and
  `serving_layer` isolate storage access behind small, intention-revealing APIs.
- **Atomic publish (staging → swap)** — see DATA_MODEL "Publish consistency".
- **Graceful degradation** — if `darts`/`pyod`/`torch` are missing, ML falls
  back to naive/IsolationForest so the pipeline still completes.

## Configuration

All runtime config is centralised in `config/settings.py`, read from the
environment (`.env`) with safe local defaults. `DATA_LAKE_MODE` switches Spark
between local parquet (dev/CI) and MinIO S3A (Docker). See `.env.example`.

## Runtime notes

- **ML interpreter**: `darts`/`pyod`/`torch` require Python 3.10–3.12. Run
  `ml_job` in such a venv for the real models; otherwise it degrades gracefully.
- **Spark connector**: align `SPARK_KAFKA_PACKAGE` with the actual Spark
  version running the speed layer (the pinned batch image uses Spark 3.5.1).
