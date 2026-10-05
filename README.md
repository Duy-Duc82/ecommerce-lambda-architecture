# Marketplace Price Intelligence — Lambda Architecture

An end-to-end data engineering platform that **tracks public catalog and price
state on Vietnamese marketplaces** (Tiki today), detects changes in real time,
and builds a versioned, quality-gated price history for BI.

It is a **Lambda architecture**: one raw-first crawl feeds a *speed path*
(change events within seconds, served from Elasticsearch and Redis) and a
*batch path* (an `as_of`-cut temporal warehouse on MinIO, published to
PostgreSQL only after it passes a quality gate). The two paths read the same
Kafka topic and share no state.

The platform is deliberately **scope-honest**. It observes what a public
listing page shows: price, list price, availability, rating and the counters
the page displays. It sees no orders, carts, payments or users. A change in a
displayed "sold" counter is a change in a number on a page, not a sale, and
nothing here calls it one.

> **Status (`develop`).** Phases 1–8 of the plan are done and merged:
> acquisition, scheduler and audit, canonical Kafka and Silver, speed layer,
> temporal warehouse, quality, anomaly and replay, and reliability and
> operations (eleven failure drills, backup and restore). Phase 9
> (evaluation and feature freeze: benchmarks, 30-day live collection, demo) is
> in progress on `phase-9-*` branches. Progress and decisions:
> [`docs/PROGRESS.md`](docs/PROGRESS.md). Phase map:
> [`docs/PHASE_INDEX.md`](docs/PHASE_INDEX.md).

---

## 1. Data source

Tiki's public category listing API, crawled by a **research crawler**:
`robots.txt` is honoured, requests are throttled (2 s plus jitter, a delay we
chose ourselves, because Tiki sets no `Crawl-delay`), and there is no attempt
to evade blocking. The endpoint is undocumented, so field mappings are
verified against live responses, and a page shape that drifts is quarantined
as `PARSE_ERROR` rather than guessed at.

What one full crawl looked like (9 categories × up to 50 pages, 2026-08-17):

| Measure | Value |
|---|---|
| Unique products | **14,117** |
| Requests | 356 succeeded, 0 failed |
| Duration | 17 m 22 s, mostly the self-imposed throttle |
| Ceiling of the endpoint | ~2,000 products per category, whatever `total` it reports |

Properties of the real data that shaped the design:

- the endpoint is a **recommender**: ordering changes between requests and
  pages overlap, so deduplication by product is required, not defensive;
- categories form a **ragged tree 4–8 levels deep** (964 distinct paths), so
  `primary_category_path` is kept whole rather than forced into fixed level
  columns;
- `rating = 0` with `review_count = 0` on ~47 % of products means *no reviews
  yet*, not a zero rating, and `brand` is empty on ~14 % (unbranded goods);
- 66 % of products are not discounted at any given moment, so price changes are
  sparse, which is why changes get their own event stream and mart instead of
  being recomputed from snapshots.

`ops/stub_source.py` serves recorded pages offline, so the smoke test, the
drills and the benchmarks never contact Tiki.

## 2. Architecture

```text
              Tiki listing API (or ops/stub_source.py, offline)
                                  │
       crawler/service.py ── frontier lease · robots · rate limit · circuit breaker
                                  │
       Bronze (MinIO)   raw body + metadata sidecar          ← written first, append-only
                                  │
       Kafka  marketplace.observations.v1      (bad records → .dlq)
          ┌───────────────────────┴────────────────────────┐
   SPEED  │                                                │  BATCH
 speed_layer/marketplace_speed_service.py       data_ingestion/marketplace_silver_service.py
  Spark Structured Streaming, stateful            Kafka → Silver, one object per observation
  observation → 7 change types                                     │
   → Kafka marketplace.changes.v1               batch_layer/marketplace_scheduler.py
   → Elasticsearch (changes, offers)             → marketplace_warehouse.py
   → Redis rt:*                                    Silver ─(as_of)→ Gold run → 17 quality rules
          │                                        → publish PostgreSQL cache → promote current.json
   ops/es_projector.py                                             │
   (audit, DLQ → ES)                            MinIO Gold (authoritative) · PostgreSQL cache
          │                                                        │
       Kibana                                                  Superset
```

Read twice:

- **Bronze is written before Kafka.** If Kafka is down after a good fetch,
  the attempt is audited as `PUBLISH_ERROR` and the raw body stays in Bronze.
  Nothing fetched is lost because a downstream was down.
- **Delivery is at-least-once, never claimed as exactly-once.** Identity is
  deterministic everywhere (`observation_id`, `offer_id`, `event_id`, every
  Elasticsearch `_id`), so a redelivery overwrites instead of duplicating, and
  only undecodable or contract-violating records reach the DLQ.

The full design (`as_of` semantics, the manifest pointer, failure boundaries
per component, storage roles) is in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

### What each path produces

| Output | Where | What |
|---|---|---|
| Change events | Kafka `marketplace.changes.v1`, Elasticsearch | exactly seven types: `NEW_OFFER`, `PRICE_CHANGED`, `LARGE_PRICE_DROP`, `RATING_CHANGED`, `COUNTER_CHANGED`, `AVAILABILITY_CHANGED`, `OFFER_STALE` |
| Realtime state | Redis `rt:*`, Elasticsearch | current state per offer, recent changes, each source's last observation |
| Operational state | Elasticsearch, via `ops/es_projector.py` | source health, crawl attempts, speed micro-batches, the DLQ |
| Gold | MinIO, one directory per run, plus manifests and `current.json` | the authoritative history, every run kept |
| BI marts | PostgreSQL `cache.marketplace_*` (10 tables) | current offers and sellers, daily price history and changes, freshness, category price distribution, source coverage, crawl reliability, counter deltas, price anomalies |
| Audit | PostgreSQL `audit.*` | frontier, crawl runs and attempts, batch runs, all 17 quality results, speed micro-batches |

Every contract, table and index is documented in [`docs/DATA_MODEL.md`](docs/DATA_MODEL.md).

### Guarantees the batch path enforces

- **One instant defines a run.** `as_of` closes the window. The run reads only
  Silver rows observed at or before it, and the scheduler cuts a window only
  after a settle lag, so late crawl runs can still be reconciled.
- **Quality gate before publication.** 17 rules (13 mandatory, 4 advisory) run
  against Silver, the audit and Gold. A mandatory failure gives
  `QUALITY_FAILED`: Gold and its manifest are written and inspectable, but
  nothing is published.
- **Atomic, ordered publish.** Marts go to `staging`, then one transaction
  swaps `cache`, and only then is the pointer promoted, by compare-and-swap.
  Superset never sees half a version, and the pointer never names a broken run.
- **One batch at a time.** A PostgreSQL advisory lock is held for the whole
  run. A second run exits 75 and writes nothing.
- **Verdicts carry their rule version.** Freshness, counter deltas and price
  anomalies store the parameters they were judged with. A price anomaly
  compares an offer only with **its own** recent history (robust MAD/IQR),
  and `INSUFFICIENT_HISTORY` is reported as "no verdict", not as "normal".

## 3. Technology

| Concern | Choice |
|---|---|
| Messaging | Apache Kafka 4.2 (KRaft) |
| Processing | Apache Spark 4.0 (PySpark 4.0.4): Structured Streaming with `applyInPandasWithState` for speed, batch for the warehouse |
| Lake | MinIO over S3A: Bronze, Silver, Gold |
| Serving | PostgreSQL 18 (BI cache and audit), Elasticsearch 8.18, Redis 8.6 |
| Dashboards | Superset 4.1 (batch marts, quality), Kibana 8.18 (realtime and operations) |
| Orchestration | a plain scheduler loop with an advisory lock, not Airflow (decision recorded in the Phase 8 plan) |
| Packaging | Docker Compose with profiles, two pinned images: `ecommerce/marketplace-python`, `ecommerce/spark-marketplace:4.0.1` |
| Language | Python 3.12 |

Every image and jar is pinned to an exact version (MinIO by digest).

## 4. Running it

### Prerequisites

- Docker Desktop. For scale: the live collection stack (10 containers,
  without the dashboards) used about 6 GiB in Phase 9; Kibana and Superset
  add to that;
- Windows PowerShell for `scripts/mp.ps1`;
- Python 3.12 and `pip install -r requirements.txt` for the tests and the
  drills. PySpark 4 is required: 3.5 cannot run on Python 3.12.

### Start, smoke-test, check

```powershell
copy .env.example .env                                 # once; change *_HOST_PORT if a port is taken
.\scripts\mp.ps1 up -With crawl,ingest,speed,batch     # core plus the pipeline services
.\scripts\mp.ps1 smoke                                 # the whole slice against the offline stub
.\scripts\mp.ps1 validate -Json validate.json          # 11 read-only checks; exit 1 if any fails
.\scripts\mp.ps1 status                                # containers, then the last run of each component
```

`mp smoke` never contacts Tiki. It seeds its own made-up categories, waits for
crawl → Silver → speed changes, runs one batch, validates, and parks its tasks
so that a later live run never crawls them.

To collect from Tiki for real, seed the frontier, then start the crawler:

```powershell
.\scripts\mp.ps1 migrate                               # idempotent
.\scripts\mp.ps1 seed --category 1846 --pages 2
.\scripts\mp.ps1 up -With crawl,ingest,speed,batch,serve
```

### Where to look

| UI | Default address | Shows |
|---|---|---|
| Superset | http://localhost:8088 | price history, changes, freshness, coverage, quality results |
| Kibana | http://localhost:5601 | realtime changes and offers, source health, crawl attempts, speed batches, DLQ |
| MinIO console | http://localhost:9001 | Bronze, Silver, Gold, `current.json` |
| PostgreSQL | `localhost:5433` | `cache.*`, `audit.*` |

### Profiles

`docker compose up -d` alone starts only the core (Kafka, MinIO, PostgreSQL,
Redis, Elasticsearch). Everything else is opt-in: `crawl`, `ingest`, `speed`,
`batch`, `serve` (Kibana, Superset), `ops` (tool container, ES projector),
`smoke` (stub source), and `legacy` / `jobs` for the old Kaggle demo.

### Operations

| Command | What it does |
|---|---|
| `mp batch -AsOf <instant> [-AllowBackfill] [-QualityOnly]` | one operator batch run; the scheduler itself never backfills |
| `mp drill d1` … `d11`, `mp drill all` | inject a failure (source down, Kafka down, MinIO down, ES/Redis down, kill mid-stream, lease loss, page drift, reconciliation mismatch, publish refused, concurrent batch, backup/restore) and assert what it leaves behind |
| `mp backup`, `mp restore -BackupId <id> -Project <name>` | copy the stack out, restore it into a separate Compose project |
| `mp down [-Volumes]` | stop; `-Volumes` asks before deleting data |

Procedures, recovery steps and measured outcomes are in
[`docs/RUNBOOK.md`](docs/RUNBOOK.md).

## 5. Tests

```powershell
python -m pytest                          # default suite: offline, no Docker needed
python -m pytest tests/drills -m drill    # the eleven drills, against a running stack
```

The default suite (1,006 tests on `develop`) drives every edge through fakes:
Kafka, MinIO, PostgreSQL, Elasticsearch and Redis each sit behind a small
`Protocol`. Spark tests run on a local session. Drills are excluded by
default in `pytest.ini`.

## 6. Repository layout

```text
crawler/          frontier, leases, scheduling, robots/rate limit, Bronze raw store, reparse; sites/tiki.py
config/           frozen contracts (marketplace_schema, marketplace_wire, DLQ), quality rules, settings
data_ingestion/   observation and change producers, Kafka → Silver sink service
speed_layer/      change rules, the streaming query, Kafka/ES/Redis sinks with micro-batch audit
batch_layer/      scheduler, advisory lock, warehouse runner, marts, quality gate, anomaly, manifest, publish
ops/              mp's tool container: validate, smoke, drills, backup/restore, ES projector, stub source
display/          Kibana index templates and dashboards, Superset dashboards and config
scripts/          mp.ps1, init_postgres.sql (idempotent migrations)
docker/           the two pinned images
tests/            offline suite; tests/drills for the live-stack drills
docs/             architecture, data model, runbook, phase plans, progress
```

Files without the `marketplace_` prefix in `data_ingestion/`, `speed_layer/`
and `batch_layer/` belong to the legacy demo (section 8).

## 7. Documentation

| Document | Read it for |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | the design, failure boundaries, storage roles, patterns |
| [DATA_MODEL.md](docs/DATA_MODEL.md) | contracts, lake zones, manifest, marts, audit, ES and Redis layouts |
| [RUNBOOK.md](docs/RUNBOOK.md) | operating the stack: start, seed, validate, drills, backfill, backup |
| [PHASE_INDEX.md](docs/PHASE_INDEX.md) | the phase map, frozen contracts, branching model |
| `PHASE_*_IMPLEMENTATION_PLAN.md` | each phase's plan and acceptance criteria |
| [PROGRESS.md](docs/PROGRESS.md) | what was done, measured and decided, session by session (Vietnamese) |
| [CLOUD_MIGRATION.md](docs/CLOUD_MIGRATION.md) | the plan for moving storage to the cloud (Vietnamese) |

## 8. Limitations

- **One marketplace.** The acquisition layer is adapter-based, but only Tiki
  is implemented. Shopee and Lazada were dropped for anti-bot risk.
- **Single machine.** Everything runs in one Docker Compose project. No
  number here describes a cluster.
- **Undocumented source.** The listing API can change without notice. Drift
  is detected and quarantined, not prevented.
- **Counters are not demand.** Sold, review and rating counters are displayed
  values; deltas are reported with their resets and invalid transitions, never
  as sales.
- **No performance claims before Phase 9.** Throughput and latency are being
  measured on purpose in Phase 9; see `docs/PROGRESS.md`.

### Legacy: the Kaggle behavior demo

The repository's original pipeline over the Kaggle *eCommerce behavior data
from multi-category store* (views, carts, purchases) is kept as a demo under
the `legacy` and `jobs` profiles (`.\scripts\start_all.ps1`). It is
independent of the marketplace platform and pins its own Spark 3.5.1 image.
See [ARCHITECTURE.md §11](docs/ARCHITECTURE.md) and
[DATA_MODEL.md §9](docs/DATA_MODEL.md).
