# Architecture

A **Lambda architecture** over a multi-marketplace price crawler. One crawl
feeds a **batch path** (accurate, replayable, versioned) and a **speed path**
(low-latency change events), each with its own serving store and dashboard.

The system observes **public catalog and price state**. It cannot see orders,
payments, carts, or any user. A counter the marketplace displays — sold count,
review count — is a number on a page; a change in it is not a sale and not a
demand signal, and nothing here describes it as one.

A second, older pipeline over a Kaggle behavior dataset still lives in the
repository as a demo. It is described at the end and runs only under the
`legacy` profile.

## 1. The data flow

```text
                      a marketplace listing API (or ops/stub_source.py)
                                        │
                    crawler/service.py ─┴─ frontier lease, robots, rate limit
                                        │
                    Bronze  raw body + metadata sidecar      ← append-only truth
                                        │
                       Kafka  marketplace.observations.v1
                 ┌──────────────────────┴───────────────────────┐
        SPEED    │                                               │   BATCH
 ┌───────────────┴─────────────────┐        ┌───────────────────┴──────────────┐
 │ speed_layer/                     │        │ data_ingestion/                  │
 │  marketplace_speed_service.py    │        │  marketplace_silver_service.py   │
 │  (Spark Structured Streaming)    │        │   → Silver, one object per       │
 │  observation → change event      │        │     observation (+ DLQ)          │
 │   7 frozen change types          │        │                                  │
 │  → Kafka marketplace.changes.v1  │        │ batch_layer/                     │
 │  → Elasticsearch  changes/offers │        │  marketplace_scheduler.py        │
 │  → Redis  rt:*                   │        │   → marketplace_warehouse.py     │
 └───────────────┬─────────────────┘        │     Silver →(as_of)→ Gold run    │
                 │                           │     → quality gate → cache      │
                 │                           │     → promote current.json      │
                 │                           └───────────────────┬──────────────┘
                 │                                               │
     ops/es_projector.py ── audit + DLQ ──► Elasticsearch   MinIO gold (authoritative)
                 │                                               │
              Kibana                              PostgreSQL `cache` (BI marts)
                                                                 │
                                                             Superset
```

Two things are worth reading twice:

- **Bronze is written before Kafka.** A publish failure after a good fetch and
  parse is audited as `PUBLISH_ERROR` with `parsed_count = acknowledged`, and
  the raw body stays in Bronze. Nothing that was fetched is ever lost because
  a downstream was down.
- **The speed path and the batch path never share state.** They read the same
  Kafka topic and agree only through the frozen contracts, so neither can
  corrupt the other.

## 2. Components and profiles

`docker compose up -d` starts only the core. Everything else is behind a
profile, so a laptop runs what it needs and nothing more.

| Profile | Service | What it is |
|---|---|---|
| *(none)* | `kafka`, `kafka-init`, `minio`, `minio-init`, `postgres-dw`, `redis`, `elasticsearch` | the core: brokers and stores |
| `crawl` | `crawl-worker` | `crawler/service.py`, leases frontier tasks and crawls them |
| `ingest` | `silver-sink` | `data_ingestion/marketplace_silver_service.py`, Kafka → Silver |
| `speed` | `speed` | `speed_layer/marketplace_speed_service.py`, the streaming query |
| `batch` | `batch-scheduler`, `batch-once` | the scheduled batch, and the operator's one-shot |
| `serve` | `kibana`, `kibana-marketplace-setup`, `superset`, `superset-init` | dashboards and their importers |
| `ops` | `ops`, `es-projector` | the `run --rm` tool container, and the operational projector |
| `smoke` | `stub-source` | an offline stand-in for the listing API |
| `legacy` | `spark`, `spark-worker`, `kibana`, `kibana-setup` | the Kaggle demo |
| `jobs` | `warehouse-job` | the Kaggle demo's batch job |

Two images, deliberately: `ecommerce/marketplace-python` for the plain-Python
services and the ops tools, `ecommerce/spark-marketplace` for anything that
needs a Spark session. Every image and jar is pinned to an exact version.

Each long-running service touches a heartbeat file once per loop, and its
container healthcheck fails when that file goes stale — so a loop that *hangs*
goes unhealthy while a process that is merely waiting out an outage does not.

## 3. The `as_of` cut

A batch run is defined by one instant. `as_of` closes the window; the run reads
only Silver rows **observed at or before** it, and audits only crawl runs that
finished at or before it.

Two consequences that are easy to get wrong:

- A rebuild of the same `as_of` is not guaranteed to produce the same row
  counts. A row observed *inside* the window can land in Silver *after* the
  run read it, and a later rebuild legitimately sees more. Anything that
  asserts an exact count across two runs of the same window is wrong.
- The scheduler cuts a window only once the **lag** has passed
  (`MARKETPLACE_BATCH_AS_OF_LAG_SECONDS`), which leaves crawl runs time to
  settle before the reconciliation gate compares them against Silver.

The reconciliation lookback must exceed the batch interval plus the settle
delay, or a crawl run could settle and age out between two batches without
ever being reconciled. `validate_marketplace_settings()` refuses a
configuration where it does not.

## 4. The quality gate and the manifest pointer

Every run writes a Gold run directory of its own and a **run manifest** — even
a run the gate refuses. A refused run that left no inspectable record cannot
be told apart from a crash.

```text
gold/marketplace/runs/run_id=<run>/<dataset>/...      the run's own Gold
gold/marketplace/manifests/run_id=<run>/manifest.json the record of that run
gold/marketplace/current.json                          the serving pointer
```

Seventeen rules run against Silver, the audit and the Gold datasets: thirteen
**mandatory**, and a failure in any one of them refuses publication; four
**advisory**, stored as evidence but never blocking. A mandatory failure gives
the run status
`QUALITY_FAILED`, which is deliberately distinct from `FAILED`: Gold exists,
is complete and is inspectable, and publication was refused.

The pointer is the single definition of "the last good published version". It
is replaced by one write of the whole manifest document, never a reference to
another object, so a reader never sees a torn pointer and `current.json` is
byte-identical to the run manifest it promoted. Promotion is a
compare-and-swap: a promoter whose pointer moved underneath it returns
`PROMOTION_CONFLICT` and writes nothing.

**The pointer is promoted after the cache is published, never before.**
Promoting first means a failed publish rolls the cache back correctly while
the pointer has already moved to a broken run.

Two batch runs can never interleave: a PostgreSQL session advisory lock is
held for the whole run, and a run refused by it writes no audit row and exits
75.

## 5. Failure boundaries

What each component does when its dependency goes away, and what its restart
repairs. Every row was established by running it — the drill that proves it is
named, and `PROGRESS.md` sections 17 and 18 carry the measurements.

| Component | Dependency lost | What happens | What the restart does | Drill |
|---|---|---|---|---|
| crawl-worker | the source errors or rate-limits | attempt audited with its `error_kind`; the circuit opens after N consecutive failures and the source is left alone until `opened_until` | tasks come back when the circuit closes | D1 |
| crawl-worker | Kafka | attempts settle `PUBLISH_ERROR` with `parsed_count = acknowledged`; **the raw body is still in Bronze** | the window reconciles with no mismatch | D2 |
| silver-sink | MinIO | the sink stops committing offsets, consumer lag grows, **no DLQ record is produced** — a storage outage is not a bad record | lag returns to 0 and Silver matches the acknowledged count | D3 |
| speed | Elasticsearch or Redis | the micro-batch audit records `FAILED`; the query retries the same batch | the batch succeeds; ES change IDs stay unique and Redis has no duplicate member | D4 |
| speed | killed mid-stream | the checkpoint holds the offsets | no change event lost or duplicated; a fresh checkpoint version replays from `earliest` to the same document set | D5 |
| crawl-worker | killed holding leases | tasks stay `LEASED` until `lease_expires_at` | another worker recovers them; a late completion from the dead worker raises `LeaseLostError` | D6 |
| crawler | the page shape drifts | attempt `PARSE_ERROR`, terminal, raw kept in Bronze, **the circuit does not move** — drift is not an outage | `crawler.reparse` reports `PARSE_FAILED` and writes nothing | D7 |
| batch | Silver and the audit disagree | run `QUALITY_FAILED`; cache version and pointer unchanged; the 17 results stored | resume the same run once the discrepancy is gone | D8 |
| batch | the cache publish is refused | run `FAILED` at publish; the previous cache version and pointer both intact, because publish truncates and refills in one transaction | resume → the pointer and cache move together | D9 |
| batch | a second run starts | exactly one runs; the other exits 75 with `ALREADY_RUNNING` and writes **no audit row** | nothing to repair | D10 |
| es-projector | anything | the pass is logged and the loop carries on | the overlap window re-reads whatever the outage hid | — |

Two invariants hold across all of them:

- **a valid observation can never reach the DLQ.** The DLQ is for a record
  that could not be decoded or failed its contract. A write that failed is
  retried at the same offset, forever if need be;
- **an offset is committed only after its record was processed.** A crash
  between the write and the commit re-delivers the record, which is safe
  because the Silver path is a function of `observation_id`.

## 6. Storage roles

- **MinIO (S3A)** — the authoritative warehouse. Bronze is raw truth and
  cannot be recreated; Silver is canonical observations; Gold is one directory
  per run plus the manifests and the pointer.
- **PostgreSQL** — two roles, neither of them "system of record". `cache` is
  the compact BI marts Superset reads; `audit` is the operational record —
  the crawl frontier, run and attempt history, the batch runs, the quality
  results, the speed micro-batches and the cache version.
- **Elasticsearch** — recent operational and realtime state, read by Kibana.
  Two indices from the speed layer (changes, current offers) and four written
  by `ops/es_projector.py` (source health, crawl attempts, speed batches, the
  DLQ).
- **Redis** — the latest realtime state: recent changes, per-offer state, and
  each source's last observation instant.

Gold-level freshness and coverage stay in **Superset**, on the cache marts.
Kibana holds recent operational state. The two are not interchangeable, and
the dashboards say which is which.

## 7. Design patterns

- **Frozen contracts, defined once.** `config/marketplace_schema.py` and
  `config/marketplace_wire.py` own the observation, change and DLQ shapes, and
  validate them field for field. A wire record with an unexpected field is
  refused, so the contract cannot drift without a schema version bump.
- **Deterministic identity everywhere.** `observation_id`, `offer_id`,
  `event_id`, `dlq_id` and every projected Elasticsearch `_id` are derived
  from the thing they name. Reprocessing overwrites rather than duplicates,
  which is what makes replay and restore safe.
- **Pipeline of pure stages.** `marketplace_warehouse.py` is a sequence of
  small testable functions orchestrated by one runner; the Spark session is a
  parameter, not a global.
- **Ports and adapters at every edge.** `ops/validate.py`, `ops/backup.py` and
  `ops/es_projector.py` each define a small `Protocol` for the stack and a
  `Live*` implementation. The default test suite drives them with fakes, which
  is how it stays offline.
- **Atomic publish (staging → swap).** Spark writes each mart to `staging`,
  then one transaction truncates and refills `cache`. Superset sees the last
  good version or the complete new one, never half of either.
- **Audit before action.** The frontier, the attempt, the batch run and the
  micro-batch are all recorded before and after the work, so every claim in
  this document is checkable against a table.

## 8. Configuration

`config/settings.py` reads everything from the environment with safe local
defaults, and `validate_marketplace_settings()` refuses a configuration that
cannot work — a reconciliation lookback that does not exceed the interval plus
the settle delay, a projector overlap that does not exceed its interval, a
currency code that is not ISO-4217.

Host ports come from `*_HOST_PORT` in `.env`, so a machine whose ports are
taken needs no override file. Container-side addresses stay literal in
`docker-compose.yml`: a developer's `.env` holds host-side values such as
`localhost:9010`, which must never reach a container.

`DATA_LAKE_PROFILE` chooses between a local parquet root (tests, dev) and
MinIO. The Compose services always run against MinIO.

## 9. Operations

`scripts/mp.ps1` is the one command. Docker itself (up, down, the Spark jobs)
is driven from the host; everything that reads or writes the stack runs as
`python -m ops ...` inside the `ops` container, so it works from any shell.

- `mp smoke` runs the whole slice against `stub-source` — it never contacts a
  live marketplace;
- `mp validate` is eleven read-only checks over the running stack, each
  derived from data the stack recorded itself rather than from a wall clock;
- `mp drill d1 … d10` injects each failure above and asserts what it leaves
  behind, restoring a passing `validate` before it exits;
- `mp backup` / `mp restore` copy the stack into a directory and load it into
  a separate Compose project.

See [RUNBOOK.md](RUNBOOK.md).

## 10. What Phase 8 established, and what it did not

Established by running it: the four services run unattended and stop
gracefully; the batch runs against MinIO through `s3a://`; ten failure drills
pass against the real stack; a backup restores into a fresh project with
pointer and cache in agreement and a reparse sample `IDENTICAL`.

Not established, deliberately:

- **no performance number is a Phase 8 result.** Throughput and latency belong
  to Phase 9, measured on purpose;
- **"micro-batch duration" is not end-to-end latency.** The change contract
  carries no processed time and Phase 8 did not add one;
- **nothing here contacted a live marketplace.** Every Phase 8 run used the
  stub source.

## 11. Legacy: the Kaggle behavior demo

The original pipeline over the Kaggle Multi-Category Store dataset is kept as
a demo and runs only under the `legacy` and `jobs` profiles: `producer.py` →
Kafka `ecommerce_events` → `speed_layer/speed_layer.py` (Elasticsearch, Redis)
and `batch_layer/warehouse_job.py` (Bronze → Silver → a star schema and marts
→ `cache`), plus `batch_layer/ml_job.py` for a revenue forecast and anomaly
detection. Its data model is in [DATA_MODEL.md](DATA_MODEL.md) section 9.

It pins `apache/spark:3.5.1` in `docker/spark-warehouse/Dockerfile`. That pin
does **not** apply to the marketplace branch, which uses
`ecommerce/spark-marketplace:4.0.1` to match pyspark 4.0.4. The two images
exist side by side on purpose.
