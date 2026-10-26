# Phase 8 implementation plan — Reliability and operations

> Status: accepted (PR #6, merged 2026-10-01 after PR #5). WP1 in progress on
> `phase-8-wp1-crawl-service`; Section 6.1 carries the amendments WP1 made.
>
> Intended implementer: a low-capability coding model working one work package
> at a time. It must follow the fixed contracts and stop between packages.
>
> Parent plan: `Brief để xây dựng plan sơ bộ cho việc cải tiến ecommerce-lambda-architecture.md`
>
> Phase boundary: `docs/PHASE_INDEX.md` §1 — Phase 8 owns **P1-12** and the
> **Kibana half of P1-11**. It must not take P2-01 … P2-07 (Phase 9) or any P3
> item. Section 2.1 records where this boundary needed an explicit decision.

## 1. Objective

Implement the parent plan's Week 8 slice (Brief §21 "Tuần 8 — Reliability và
operations") and backlog P1-12 plus the Kibana half of P1-11:

```text
crawl worker service  -> Bronze -> marketplace.observations.v1
Silver sink service   -> Silver (+ DLQ for bad records only)
speed service         -> marketplace.changes.v1 -> Elasticsearch / Redis
batch scheduler       -> Gold -> quality gate -> PostgreSQL cache -> manifest pointer
ops projector         -> audit + DLQ -> Elasticsearch -> Kibana source health
one command           -> start / smoke / validate / drill / backup / restore
```

This phase answers **"does the whole pipeline run unattended, and does it
recover from each failure the Brief names without losing or duplicating
data?"** Every claim must be shown on the running Compose stack, not only with
fakes.

The survey behind this plan (2026-10-01) found that several components marked
done in Phases 2–5 exist only as functions exercised by unit tests with fakes:

- the crawler never publishes to Kafka — `publish_observation` has no
  production caller (`data_ingestion/marketplace_producer.py:15`);
- `CrawlWorker.run_once()` has no loop or CLI (`crawler/worker.py:113`);
- the Silver sink is `process_record()` only, with no consumer loop
  (`data_ingestion/marketplace_silver_sink.py:24`);
- the marketplace speed layer has no `writeStream`, no checkpointed query and
  no entrypoint (`speed_layer/marketplace_speed_layer.py`);
- the marketplace Kibana dashboard is an empty saved object with no panels
  (`display/kibana/create_marketplace_speed_dashboard.py:17`);
- no recovery or failure test touches a real dependency.

Phase 8 closes these as its first work package (Section 2.1, D1), because
Compose services cannot exist without entrypoints.

## 2. Mandatory dependency gate

Before editing, record and verify:

1. PR #5 is merged into `develop`. `batch_layer/marketplace_warehouse.py`
   exposes `observations_as_of()` and `crawl_audit_as_of()`, and
   `MARKETPLACE_QUALITY_RULE_VERSION` defaults to `quality-rules.v2`.
2. `run_marketplace_warehouse()` keeps the Phase 7 order (evaluate → persist
   results → write run manifest → act on the verdict → publish → promote last)
   and returns `MarketplaceBatchResult` with `promotion_reason`.
3. `crawler/worker.py` exposes `CrawlWorker.run_once(crawl_run_id_for=...)`
   returning `CycleResult`; `crawler/frontier.py` exposes
   `PostgresCrawlFrontier.enqueue()`, `lease_due()` with `FOR UPDATE SKIP
   LOCKED` and `recover_expired_leases()`.
4. `crawler/scheduling.py` classifies failures into the error kinds allowed by
   the `audit.crawl_request_attempt.error_kind` and
   `audit.crawl_frontier.last_error_kind` CHECK constraints.
5. `data_ingestion/marketplace_silver_sink.process_record()` and
   `speed_layer/marketplace_speed_layer.write_marketplace_batch()` exist with
   their Phase 4/5 signatures, and `speed_layer/marketplace_sinks.py` writes
   Elasticsearch with deterministic `_id`s.
6. The default suite passes and stays offline:
   `tests/test_crawl_worker.py::test_no_default_test_opens_network_kafka_minio_or_postgresql`.

If any item is absent or differs, stop and report the exact mismatch.

### 2.1 Decisions, reviewed 2026-10-01

Each was put to the user and accepted. A reviewer may still overrule one
before code is written; record that here if so.

- **D1 — Service wiring lives in Phase 8.** Writing the four service
  entrypoints closes Phase 2–5 wiring that was never built. Brief §21 Week 8
  says "Compose profiles/**services** cho crawler, sink, speed và batch", so it
  is in scope. `docs/PHASE_INDEX.md` §5 must say so rather than leave Phases 4
  and 5 looking fully live.
- **D2 — A plain loop service schedules the batch; no Airflow.** This
  reverses the Airflow LocalExecutor line in `PROGRESS.md` §5. Brief §27 allows
  no infrastructure that does not serve an acceptance criterion, and a
  periodic loop with a PostgreSQL advisory lock meets the only one here. Record
  the reversal in `PROGRESS.md` §5.
- **D3 — An ops projector feeds Kibana from PostgreSQL audit and the DLQ.**
  Brief §17 places source last-success, fetch/parse/DLQ errors and processing
  latency in Kibana. That data lives only in PostgreSQL audit and the Kafka DLQ
  today. Elasticsearch stays a derived projection that can be rebuilt.
- **D4 — Legacy Kaggle services move to a `legacy` profile.** The default
  `docker compose up` stops starting Spark 3.5.1 master/worker, the legacy
  Kibana setup and the warehouse job. `scripts/start_all.ps1` passes
  `--profile legacy`.
- **D5 — Boundary with P2-05/P2-06.** Brief §21 Week 8 lists "Compose
  profiles" and "one-command start/smoke/validate"; Brief §22 also has
  P2-05 "Compose application profiles" and P2-06 "One-command demo", which
  `PHASE_INDEX.md` gives to Phase 9. Resolution: Phase 8 builds the
  **operational** profiles and the start/smoke/validate/drill/backup commands
  that P1-12 needs to run drills. Phase 9 keeps the **demo** packaging: the
  scripted thesis demo, the recorded fallback demo (Brief §24 "Demo phụ thuộc
  internet") and the evaluation profile for benchmarks. Add this line to
  `PHASE_INDEX.md` §1 "Ranh giới dễ nhầm".
- **D6 — A new error kind `PUBLISH_ERROR`.** A Kafka publish failure inside a
  crawl attempt is neither transport, storage nor parse. It is retryable. Both
  CHECK constraints gain it through an idempotent migration.
- **D7 — Marketplace Elasticsearch indices get explicit index templates.**
  Decimals are serialised as strings (`common/serialization.py:25`), so dynamic
  mapping makes prices text and blocks numeric charts. The existing `*-v1`
  indices are projections: the procedure in Section 11.4 recreates them from
  Kafka instead of migrating them in place.
- **D8 — A separate Spark 4 image for marketplace jobs.** The batch and speed
  services run on `apache/spark:4.0.1`, matching `pyspark==4.0.4` and the image
  already proven by the 2026-10-01 end-to-end runs. The legacy
  `docker/spark-warehouse` image (3.5.1) stays unchanged, under `legacy`.

## 3. Scope

### 3.1 In scope

- Long-running service entrypoints for crawl, Silver sink, speed and batch
  scheduling, each with graceful shutdown.
- Crawl attempts that publish observations to Kafka, with honest audit counts
  when publication fails part-way.
- Batch exclusivity and a compare-and-swap on the manifest pointer (Phase 7
  debt: "chưa có khóa giữa các batch run chạy đồng thời").
- The `s3a://` path for the marketplace batch (Phase 7 debt: `build_spark()`
  never applies `spark_hadoop_options()`).
- Container images and Compose profiles; ports configurable from `.env`.
- An offline stub source serving frozen fixtures, so smoke and drills never
  touch the live marketplace.
- One command for start, smoke, validate, drills, backup and restore.
- Eleven failure drills against the running stack (Section 10).
- Kibana: index templates, the realtime-changes dashboard Phase 5 left empty,
  and a source-health and freshness dashboard.
- A backup, export and restore procedure that preserves the manifest pointer.
- Rewritten `docs/ARCHITECTURE.md` and `docs/DATA_MODEL.md`, and a new
  `docs/RUNBOOK.md`.

### 3.2 Out of scope

- Performance benchmarks, latency percentiles, storage-growth reports,
  evaluation runs and the evidence bundle — Phase 9 (P2-01 … P2-04, P2-07).
- The scripted thesis demo and the recorded fallback demo — Phase 9 (P2-06).
- Airflow, Prometheus, Grafana or any new resident system (Brief §19, §27).
- A second marketplace adapter, or crawling more of the live source.
- Changing any frozen contract (`PHASE_INDEX.md` §3): Kafka topic names, the
  seven change types, Kafka keys, `observation_id` derivation, the
  `MarketplaceObservationV1` / `MarketplaceChangeV1` wire fields.
- Superset provisioning beyond what the quality and batch dashboards already
  specify. Superset is P1-11's other half and stays as Phase 6/7 left it.
- Cross-market matching (P3).
- Detecting a crawl run absent from Silver altogether (Phase 7 §7.1 item 24
  still holds; changing it is a separate decision).

## 4. Allowed file changes

Create:

```text
crawler/service.py
crawler/seed_frontier.py
data_ingestion/marketplace_silver_service.py
speed_layer/marketplace_speed_service.py
batch_layer/marketplace_scheduler.py
batch_layer/marketplace_lock.py
ops/__init__.py
ops/__main__.py
ops/stub_source.py
ops/validate.py
ops/smoke.py
ops/drills.py
ops/backup.py
ops/es_projector.py
display/kibana/marketplace_index_templates.py
display/kibana/setup_marketplace_kibana.py
display/kibana/saved_objects/marketplace_realtime.ndjson
display/kibana/saved_objects/marketplace_source_health.ndjson
docker/marketplace-python/Dockerfile
docker/spark-marketplace/Dockerfile
requirements-marketplace.txt
.env.example
scripts/mp.ps1
docs/RUNBOOK.md
tests/test_crawl_service.py
tests/test_marketplace_silver_service.py
tests/test_marketplace_speed_service.py
tests/test_marketplace_scheduler.py
tests/test_marketplace_lock.py
tests/test_ops_validate.py
tests/test_ops_backup.py
tests/test_es_projector.py
tests/test_kibana_saved_objects.py
tests/drills/test_drills.py
pytest.ini            # the repo has none yet; registers the `drill` marker and excludes it by default
```

Modify only:

```text
crawler/worker.py
crawler/scheduling.py
crawler/contracts.py      (ObservationPublishError, beside the other boundary errors)
crawler/base.py           (robots.txt URL as an overridable property)
crawler/sites/tiki.py
data_ingestion/marketplace_silver_sink.py
batch_layer/marketplace_warehouse.py
batch_layer/marketplace_manifest.py
config/settings.py
scripts/init_postgres.sql
scripts/start_all.ps1
docker-compose.yml
.gitignore
display/kibana/create_marketplace_speed_dashboard.py
docs/ARCHITECTURE.md
docs/DATA_MODEL.md
docs/PHASE_INDEX.md
docs/PROGRESS.md
docs/PHASE_6_TEMPORAL_WAREHOUSE_IMPLEMENTATION_PLAN.md   (§16 smoke wording only)
docs/PHASE_7_QUALITY_ANOMALY_REPLAY_IMPLEMENTATION_PLAN.md (§18 smoke wording only)
the matching existing test files for the modules above
```

Do not touch the legacy pipeline (`batch_layer/warehouse_job.py`,
`speed_layer/speed_layer.py`, `data_ingestion/producer.py`,
`data_ingestion/es_indexer.py`, `display/kibana/setup_kibana.py`) beyond moving
its services into the `legacy` profile.

## 5. Configuration

### 5.1 New settings in `config/settings.py`

Register every positive integer in `validate_settings()` and every version
string in its non-empty list, as Phases 6 and 7 did.

```python
# Crawl service
CRAWL_SERVICE_WORKER_ID = os.getenv("CRAWL_SERVICE_WORKER_ID", "")   # default: hostname
CRAWL_SERVICE_IDLE_SECONDS = int(os.getenv("CRAWL_SERVICE_IDLE_SECONDS", "30"))
TIKI_LISTING_URL = os.getenv("TIKI_LISTING_URL", "https://tiki.vn/api/personalish/v1/blocks/listings")
# No TIKI_ROBOTS_URL (amended in WP1): robots.txt is read from the host of
# TIKI_LISTING_URL, because RFC 9309 scopes it to the host actually fetched.

# Silver sink service
MARKETPLACE_SILVER_POLL_TIMEOUT_MS = int(os.getenv("MARKETPLACE_SILVER_POLL_TIMEOUT_MS", "1000"))
MARKETPLACE_SILVER_RETRY_BASE_SECONDS = int(os.getenv("MARKETPLACE_SILVER_RETRY_BASE_SECONDS", "2"))
MARKETPLACE_SILVER_RETRY_MAX_SECONDS = int(os.getenv("MARKETPLACE_SILVER_RETRY_MAX_SECONDS", "60"))

# Speed service
MARKETPLACE_SPEED_TRIGGER_SECONDS = int(os.getenv("MARKETPLACE_SPEED_TRIGGER_SECONDS", "30"))
MARKETPLACE_SPEED_CHECKPOINT_ROOT = os.getenv("MARKETPLACE_SPEED_CHECKPOINT_ROOT", "")  # default: CHECKPOINTS_DIR

# Batch scheduler
MARKETPLACE_BATCH_INTERVAL_SECONDS = int(os.getenv("MARKETPLACE_BATCH_INTERVAL_SECONDS", "86400"))
MARKETPLACE_BATCH_AS_OF_LAG_SECONDS = int(os.getenv("MARKETPLACE_BATCH_AS_OF_LAG_SECONDS", "1800"))
MARKETPLACE_BATCH_LOCK_KEY = int(os.getenv("MARKETPLACE_BATCH_LOCK_KEY", "820801"))

# Ops projector
MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS = int(os.getenv("MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS", "60"))
MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS = int(os.getenv("MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS", "900"))
KAFKA_DLQ_PROJECTOR_GROUP = os.getenv("KAFKA_DLQ_PROJECTOR_GROUP", "marketplace-dlq-projector-v1")
ES_INDEX_MARKETPLACE_SOURCE_HEALTH = os.getenv("ES_INDEX_MARKETPLACE_SOURCE_HEALTH", "marketplace-source-health-v1")
ES_INDEX_MARKETPLACE_CRAWL_ATTEMPTS = os.getenv("ES_INDEX_MARKETPLACE_CRAWL_ATTEMPTS", "marketplace-crawl-attempts-v1")
ES_INDEX_MARKETPLACE_SPEED_BATCHES = os.getenv("ES_INDEX_MARKETPLACE_SPEED_BATCHES", "marketplace-speed-batches-v1")
ES_INDEX_MARKETPLACE_DLQ = os.getenv("ES_INDEX_MARKETPLACE_DLQ", "marketplace-dlq-v1")
```

Change one existing default: `SPARK_KAFKA_PACKAGE` becomes
`org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1`. The `_2.12:3.5.1` value
cannot load on Spark 4 (Scala 2.13).

`validate_settings()` must also refuse
`MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS <=
MARKETPLACE_BATCH_INTERVAL_SECONDS + MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS`.
Otherwise a crawl run can settle and age out between two scheduled batches
without ever being reconciled (`PROGRESS.md` §11.2). With the defaults,
172800 > 86400 + 900 holds.

### 5.2 Ports from `.env`, not an override file

Every published port in `docker-compose.yml` becomes `${NAME:-default}`, with
the current defaults, for example `"${POSTGRES_HOST_PORT:-5433}:5432"`. This
removes the need for the untracked `docker-compose.override.yml` and its
`!override` trap (`PROGRESS.md` §10.2). Commit `.env.example` listing every
variable a developer may set. The real `.env` stays gitignored.

Client-side settings and the host port must agree: `.env.example` sets
`POSTGRES_PORT` equal to `POSTGRES_HOST_PORT`, with a comment saying so.

### 5.3 Image pins

- The pinned MinIO tags in `docker-compose.yml` no longer pull
  (`PROGRESS.md` §10.2). Pin both `minio/minio` and `minio/mc` to tags or
  digests that pull **today**. Record the command used to verify the pull in
  `docs/RUNBOOK.md`. If no pullable tag exists, pin the locally cached image by
  digest and say so in the runbook. Never use `latest`.
- `docker/spark-marketplace/Dockerfile` pins `apache/spark:4.0.1` and every
  jar by exact version. Take the versions of `spark-sql-kafka-0-10_2.13`,
  `spark-token-provider-kafka-0-10_2.13`, `kafka-clients`, `commons-pool2`,
  `hadoop-aws` and the AWS SDK bundle from Spark 4.0.1's own dependency
  manifest; Spark 4.0.1 ships Hadoop 3.4.x, so `hadoop-aws` is 3.4.x and needs
  AWS SDK v2. Record each version and its source in the Dockerfile comment.
  The smoke in Section 9 is the proof that the set loads.

## 6. Service entrypoints (WP1–WP3)

Every service:

- has a `main()` behind `if __name__ == "__main__"` and an argparse `--help`;
- runs a loop with an injected clock and an injected `sleep`, so a unit test
  drives N iterations without waiting;
- stops gracefully on SIGTERM and SIGINT: it finishes the unit of work in
  hand (a crawl cycle, a record, a micro-batch, a batch run), takes no new one,
  closes its clients and exits 0. For the crawler the unit is the whole cycle,
  not one task: the cycle leased its tasks up front, and a task leased but
  never started would sit `LEASED` until its lease expired. Compose's
  `stop_grace_period` for the crawler must therefore cover one cycle (WP4);
- logs one structured JSON line per unit of work, with no secrets;
- never reads a wall clock inside business semantics. The loop clock decides
  *when* to work, never *what* a result contains.

### 6.1 Crawl service — `crawler/service.py`

```text
python -m crawler.service [--worker-id ID] [--max-cycles N]
```

1. Build `PostgresCrawlFrontier`, `CrawlAuditRepository`, the observation
   producer (`create_marketplace_producer()`), and the listing-page executor
   from `crawler.runner.listing_page_executor`, wrapped by
   `publishing_executor` (below).
2. Loop: `result = worker.run_once(crawl_run_id_for=...)`. If
   `result.leased == 0`, wait `CRAWL_SERVICE_IDLE_SECONDS`; otherwise wait
   `CRAWL_WORKER_POLL_SECONDS`. The wait is on the stop signal, so SIGTERM
   ends it at once. No wait follows the last cycle of a bounded run.
3. `--max-cycles` exists for tests and the smoke only.
4. One JSON log line per cycle (`"event": "crawl_cycle"` plus the
   `CycleResult` fields).

`crawl_run_id_for` derives one ID per attempt from the task ID, the attempt
number and the lease expiry, so neither a retry nor a re-lease after a crash
reuses a crawl run ID. The default worker ID is `<hostname>:<pid>`, so two
processes on one host never share a lease owner.

**`publishing_executor(inner, producer)`** returns an executor with the same
signature. It calls `inner(...)`, which persists raw Bronze **before** parsing
(Phase 2 contract, unchanged). It then publishes every parsed observation
through `publish_observation()`, in order, each blocking on its acknowledgement.

- All acknowledged → return the report unchanged. The worker records
  `parsed_count = len(report.observations)`, as today.
- A publish fails after `k` acknowledgements → raise
  `ObservationPublishError(report=report, acknowledged=k, cause=error)`.

`crawler/scheduling.classify_failure()` maps `ObservationPublishError` to
`PUBLISH_ERROR`, which is **retryable**. `CrawlWorker._on_failure` records the
attempt with `status='FAILED'`, `error_kind='PUBLISH_ERROR'` and
**`parsed_count = acknowledged`**, not `len(report.observations)`.

Why `acknowledged`: Phase 7 check 8 compares `parsed_count` with Silver rows per
crawl run. Only the acknowledged observations can ever reach Silver. Recording
the parsed total would leave a permanent mismatch for that crawl run, and
recording zero would hide the `k` rows that did land. The retry refetches, so
it produces new raw, new `observation_id`s and a new crawl run ID.
`rejected_count` keeps its meaning.

**Migration** in `scripts/init_postgres.sql`, idempotent and safe to run
twice: drop and re-add the two error-kind CHECK constraints with
`PUBLISH_ERROR` added, using `DROP CONSTRAINT IF EXISTS` / `ADD CONSTRAINT`, the
pattern Phase 7 used for the anomaly key. Constraint names must be explicit;
if the existing constraints are unnamed, look up the generated names and name
the new ones.

**Configurable source URLs.** `crawler/sites/tiki.py` reads `TIKI_LISTING_URL`
and `TIKI_ROBOTS_URL` from settings instead of the module constant, so the
stub source (Section 8) can stand in. Robots compliance is unchanged: the stub
serves its own `robots.txt`.

**Frontier seeding — `crawler/seed_frontier.py`.**

```text
python -m crawler.seed_frontier --marketplace tiki --category 1846 [--category ...] [--tier NORMAL] [--max-pages N]
```

It calls `enqueue()` once per listing page target and prints how many tasks
were new.

Idempotency needs care, because each frontier row is **one scheduled
occurrence**: `mark_succeeded` closes it and inserts the next occurrence under
a new `task_id`. Seeding with `scheduled_for = now` would therefore start a
second chain for a target already being crawled. Instead, the first occurrence
of every seeded target is anchored at a fixed instant,
`SEED_SCHEDULED_FOR = 1970-01-01T00:00:00Z`, so its `task_id` depends only on
the target. A re-seed hits `ON CONFLICT DO NOTHING` and `enqueue()` returns
`False`. That anchored row stays in the table whatever its status, so a re-seed
can never revive or duplicate a chain; reviving a target that ended `FAILED`
is a deliberate operator action and needs `--scheduled-for`. An anchored task
is due at once, which is what a first seed wants.

### 6.2 Silver sink service — `data_ingestion/marketplace_silver_service.py`

```text
python -m data_ingestion.marketplace_silver_service [--max-records N]
```

- A `KafkaConsumer` on `marketplace.observations.v1`, group
  `KAFKA_SILVER_CONSUMER_GROUP`, `enable_auto_commit=False`,
  `auto_offset_reset="earliest"`.
- For each record: `process_record(...)`, then commit **that record's offset
  + 1** only after it returns. A crash between write and commit re-delivers
  the record, which is safe: the Silver path is deterministic per
  `observation_id`, and rewriting identical bytes is a no-op
  (`test_landing_the_same_record_twice_writes_the_same_object`).
- If `process_record` raises, do not commit. Back off
  (`MARKETPLACE_SILVER_RETRY_BASE_SECONDS`, doubling, capped at
  `..._MAX_SECONDS`), `seek` back to the failed offset and retry. Never skip a
  record.

**Production fix, its own test-then-fix commit pair.**
`process_record()` currently wraps the Silver `writer(...)` call in the same
`except` as decoding and validation. A MinIO outage therefore quarantines a
perfectly valid observation to the DLQ as `CONTRACT_VALIDATION`, and the
observation never reaches Silver. Restructure it:

- decode and contract validation failures → quarantine + DLQ, as today;
- a failure while writing a **valid** event to Silver → raise
  `SilverWriteError`. No quarantine, no DLQ. The service retries it.
- a failure while writing the quarantine object or publishing the DLQ →
  propagate, as today.

### 6.3 Speed service — `speed_layer/marketplace_speed_service.py`

```text
spark-submit ... -m speed_layer.marketplace_speed_service    (inside the Spark 4 image)
```

1. `read_marketplace_observations(spark)` → `decode_observation_stream` →
   `build_change_stream(valid, config)`.
2. `writeStream.foreachBatch(write_marketplace_batch)`, with:
   - `queryName(MARKETPLACE_SPEED_QUERY_NAME)`;
   - `option("checkpointLocation", checkpoint_path())`, where
     `checkpoint_path()` honours `MARKETPLACE_SPEED_CHECKPOINT_ROOT` and still
     ends in `MARKETPLACE_STREAM_CHECKPOINT_VERSION`;
   - `trigger(processingTime=MARKETPLACE_STREAM_TRIGGER)`, the setting Phase 5
     already had (amended in WP2; no `MARKETPLACE_SPEED_TRIGGER_SECONDS`);
   - `spark.sql.shuffle.partitions = MARKETPLACE_SPEED_SHUFFLE_PARTITIONS`
     (default 4), set on the session. Spark fixes it in the checkpoint on the
     first run, so changing it later needs a new checkpoint version.
3. `awaitTermination()`. On SIGTERM, call `query.stop()`. On Spark 4.0.1 this
   cancels a micro-batch in progress; that is safe, because its offsets were
   not committed and every sink is idempotent.

The checkpoint lives on a named Docker volume (`speed_checkpoints`). It is
derived state: losing it means replaying from `earliest`, which the
deterministic sinks already make safe. That is drill D5 (Section 10).

The speed image needs `pandas` and `pyarrow`, because `build_change_stream`
uses a pandas-based stateful operator.

### 6.4 Batch scheduler — `batch_layer/marketplace_scheduler.py`

```text
python -m batch_layer.marketplace_scheduler [--max-ticks N]
```

Each tick:

1. `as_of = floor(now - MARKETPLACE_BATCH_AS_OF_LAG_SECONDS, MARKETPLACE_BATCH_INTERVAL_SECONDS)`
   in UTC. The lag leaves crawl runs time to settle (Phase 7 check 8) before
   the window is cut.
2. `run_id = "mp-" + as_of.strftime("%Y%m%dT%H%MZ")`. The ID is deterministic,
   so a scheduler restart never invents a second run for the same window.
3. Look the run up in `audit.marketplace_batch_run`:
   - `SUCCEEDED` → nothing to do;
   - absent → run it;
   - any other status → run it with `resume=True`.
4. Call `run_marketplace_warehouse(context, ...)` with the serving Gold root.
5. `QualityGateFailure` is logged with its failing checks and the loop goes
   on. A refused window must not stop later windows; the evidence is already in
   PostgreSQL. Any other exception is logged and the loop goes on.
6. Sleep until the next interval boundary **plus the lag** (amended before
   WP3). Waking at the bare boundary computes `now - lag` inside the previous
   interval, so `floor` returns the window just run: that tick is a no-op and
   every window is published one interval late instead of one lag late.

The scheduler never passes `--allow-backfill`. A backfill stays a deliberate
operator command (Section 12, runbook).

### 6.5 Batch exclusivity and pointer compare-and-swap

`batch_layer/marketplace_lock.py`:

```python
@contextmanager
def exclusive_batch(connection_factory, *, key: int = MARKETPLACE_BATCH_LOCK_KEY): ...
class BatchAlreadyRunning(RuntimeError): ...
```

- Opens a **dedicated** connection and calls `pg_try_advisory_lock(key)`. If it
  returns false, raise `BatchAlreadyRunning` and touch nothing.
- Holds the lock on that connection for the whole run and releases it in
  `finally`. If the process dies, PostgreSQL releases a session lock when the
  connection drops.
- `run_marketplace_warehouse` takes the lock **before** `start_run`, including
  under `--skip-postgres`. That run still reads the crawl audit over JDBC, so
  PostgreSQL is reachable anyway. A refused second run therefore leaves no audit
  row at all, because it never started.
- The CLI prints `{"status":"ALREADY_RUNNING",...}` and exits 75
  (`EX_TEMPFAIL`). The scheduler logs it and waits for the next tick.

Compare-and-swap, in `batch_layer/marketplace_manifest.py`:
`promote_manifest(manifest, *, writer, reader, allow_backfill, expected_current_run_id)`
re-reads the pointer immediately before writing it. If the pointer's run ID is
no longer `expected_current_run_id`, return
`PromotionResult(False, "PROMOTION_CONFLICT", ...)` without writing. The
orchestrator passes the run ID it read at step 9 of Phase 7 §14. With the lock
in place this should never fire. It is the guard for a run started outside
the lock, for example by a future caller.

`mark_promotion` records `promoted=False` on a conflict, and the result carries
`promotion_reason="PROMOTION_CONFLICT"`.

Amended in WP3 review: the orchestrator also re-reads the pointer **before
publishing the cache**. The lock's connection idles through the whole Spark
run and can drop, and a conflict found only at promotion would leave the cache
serving this run, the pointer another, and the audit row `SUCCEEDED`, so the
scheduler would skip the window for good. A pointer that moved by then holds
the run as `GOLD_WRITTEN` with `PROMOTION_CONFLICT`, which stays resumable.

### 6.6 The `s3a://` path

`build_spark()` applies `config.storage.spark_hadoop_options()` whenever the
active storage profile is not `local`. This is the first time the marketplace
batch can read or write MinIO (`PROGRESS.md` §10.6 item 2). The Compose `batch`
profile runs against MinIO by default. A test asserts that the S3A options
reach the builder for a `minio` profile and are absent for `local`.

The options alone are not enough (amended before WP3): `apache/spark:4.0.1`
ships no `hadoop-aws`. A real `s3a://` run needs `hadoop-aws` matching the
image's Hadoop (3.4.x) and the AWS SDK v2 bundle it depends on. WP3 verifies
the path by loading them through `spark.jars.packages`; WP4 bakes them into
`docker/spark-marketplace/Dockerfile`.

## 7. Images and Compose profiles (WP4)

### 7.1 Images

| Image | Base | Contents | Used by |
|---|---|---|---|
| `docker/marketplace-python` | `python:3.12-slim` (pinned patch) | `requirements-marketplace.txt`: kafka-python-ng, minio, psycopg2-binary, redis, elasticsearch, python-dotenv, pyyaml | crawl, silver-sink, ops, stub-source |
| `docker/spark-marketplace` | `apache/spark:4.0.1` | the same Python deps plus pandas and pyarrow; jars from §5.3 | speed, batch, batch-scheduler |

`requirements-marketplace.txt` pins exact versions that already appear in
`requirements.txt`. It does not add a package that `requirements.txt` lacks,
except `pyyaml` if the ops CLI needs it.

Neither image declares an `ENTRYPOINT` that swallows arguments. The legacy
warehouse image does, which is why `run_warehouse.ps1 -Marketplace` never
worked (`ENTRYPOINT spark-submit warehouse_job.py`). Each Compose service sets
`command:` explicitly.

### 7.2 Profiles

| Profile | Services | Notes |
|---|---|---|
| *(none)* | kafka, kafka-init, minio, minio-init, postgres-dw, redis, elasticsearch | the core; `docker compose up` starts only this |
| `crawl` | crawl-worker | needs a seeded frontier |
| `ingest` | silver-sink | |
| `speed` | speed | Spark 4, checkpoint volume |
| `batch` | batch-scheduler | Spark 4; `batch-once` is a `run --rm` service for operator commands |
| `serve` | kibana, kibana-marketplace-setup, superset, superset-init | |
| `ops` | ops-projector, ops (a `run --rm` tool container) | |
| `smoke` | stub-source | offline fixture source, Section 8 |
| `legacy` | spark, spark-worker, warehouse-job, kibana-setup (legacy) | Section 2.1 D4 |

- `kafka-init` creates the three frozen topics with the configured partition
  count, idempotently. It replaces the topic creation in `start_all.ps1` for
  the marketplace path.
- `postgres-dw` keeps mounting `init_postgres.sql` for a fresh volume. The ops
  command `migrate` re-applies it to an existing volume. Every statement in it
  is already idempotent.
- Every long-running service declares a healthcheck. Python services touch a
  heartbeat file per loop iteration, and the healthcheck fails if it is older
  than three intervals. Spark services check that the driver process is alive.
- `depends_on` uses `condition: service_healthy` for core services, and
  `service_completed_successfully` for the init containers.
- The repository mount stays read-only for services (`./:/app:ro`). Data goes
  to named volumes or MinIO, never into the source tree.

## 8. Offline stub source (WP5)

`ops/stub_source.py` is a small HTTP server built on the standard library. It
serves:

- `/robots.txt`, allowing the listing path;
- the listing API path with **frozen fixture pages** from
  `tests/fixtures/marketplace_raw/` (the real Tiki artifact Phase 7 already
  froze), paginated deterministically;
- `/_stub/mode` (POST), which switches the response mode for drills: `ok`,
  `429` (with `Retry-After`), `500`, `timeout` (sleeps past
  `CRAWL_HTTP_TIMEOUT_SECONDS`) and `drift` (a payload whose shape the adapter
  must reject).

The smoke and every drill set `TIKI_LISTING_URL` to the stub, and robots.txt
then follows from its host. **Nothing in Phase 8 automation contacts the live marketplace.** This
answers Brief §24 "Demo phụ thuộc internet" for operations, and keeps drills
from loading the real source (Brief §24 "Crawler ảnh hưởng source").

Fixture prices must differ between pages and between successive serves of the
same page, so the speed layer has real `PRICE_CHANGED` and
`LARGE_PRICE_DROP` events to emit. The stub derives each served price from
`(fixture price, serve counter)` by a fixed table, never at random.

## 9. One command (WP5)

`scripts/mp.ps1` is a thin wrapper. All logic lives in `python -m ops`, run in
the `ops` container, so the same commands work from any host shell.

```text
mp up [--with crawl,ingest,speed,batch,serve,ops]   compose up for the listed profiles
mp down [--volumes]                                  --volumes asks for confirmation
mp status                                            services, health, last run per component
mp migrate                                           re-apply init_postgres.sql
mp seed --category ...                               crawler.seed_frontier
mp smoke                                             Section 9.1
mp validate [--json PATH]                            Section 9.2
mp drill <name>|all                                  Section 10
mp backup [--dest PATH]                              Section 12
mp restore --from PATH --project NAME                Section 12
mp batch --as-of ISO [--allow-backfill] [--quality-only]   one-shot operator run
```

### 9.1 Smoke

1. `up` with every marketplace profile plus `smoke`, pointing the crawler at
   the stub.
2. `migrate`, then seed a fixed fixture universe (e.g. 3 categories × 2 pages).
3. Wait, polling with a bounded timeout, until: the frontier tasks have
   `SUCCEEDED` at least twice each; Silver holds the expected observation
   count; the speed audit shows at least one `SUCCEEDED` micro-batch with
   `change_rows > 0`.
4. Trigger one batch with `mp batch --as-of <now - settle>`.
5. Run `validate`. The smoke passes only if `validate` passes.

Within its timeout, the smoke runs the slice no earlier phase ran end to end:
`crawl → Bronze → Kafka → Silver → speed → ES/Redis` and
`Silver → Gold → quality → PostgreSQL → pointer`.

### 9.2 Validate

`ops/validate.py` runs read-only checks and emits one JSON report
(`check`, `status`, `observed`, `expected`), sorted by check name. It exits
non-zero if any check fails.

| Check | Passes when |
|---|---|
| `bronze_present` | every attempt with a `raw_uri` in the window has that object in Bronze |
| `kafka_to_silver_lag` | the Silver consumer group lag is 0, or under a bound, after a quiet period |
| `silver_reconciles_with_audit` | the Phase 7 check 8 query over the window returns no mismatch |
| `dlq_only_bad_records` | every DLQ record has stage `DECODE` or `CONTRACT_VALIDATION` |
| `speed_last_batch_succeeded` | the latest `audit.marketplace_speed_batch` row is `SUCCEEDED` |
| `es_changes_unique` | ES change document count equals the distinct `event_id` count |
| `redis_offer_state_present` | `rt:offer:<id>` exists for every offer in `marketplace-offers-current-v1` |
| `batch_latest_terminal` | the latest scheduled run is `SUCCEEDED`, or `QUALITY_FAILED` with results stored |
| `pointer_matches_cache` | `current.json` `run_id` equals `audit.marketplace_cache_version.run_id` |
| `pointer_gold_exists` | every dataset URI in `current.json` exists with its recorded row count |
| `quality_results_complete` | the pointer's run has one stored result per registered rule |
| `source_health_projected` | `marketplace-source-health-v1` has a document per seeded marketplace, no older than three projector intervals |

`validate` reads nothing from a wall clock for its verdicts except the
staleness bound in the last check, which is stated in seconds in the report.

## 10. Failure drills — P1-12 (WP6–WP7; D11 in WP9)

`ops/drills.py` runs each drill against the live stack. `tests/drills/` wraps
them as pytest tests marked `drill`. That marker is excluded from the default
suite, so the offline-suite rule in Section 2 item 6 still holds. Each drill
writes a JSON record with inject, observe, recover and verify steps, and
timestamps. These records are Phase 9's evidence input; Phase 8 does not
measure performance.

Every drill follows one shape:

```text
baseline: validate passes
inject:   one fault, by a documented mechanism
observe:  the system's state while faulted, asserted
recover:  remove the fault; no manual data repair
verify:   validate passes again, plus the drill-specific invariant
```

| # | Drill | Inject | Observed while faulted | Invariant after recovery |
|---|---|---|---|---|
| D1 | Source 429 / 5xx / timeout | stub mode `429`, then `500`, then `timeout` | attempts `RATE_LIMITED` / `SERVER_ERROR` / `TRANSIENT_NETWORK`; `Retry-After` honoured; circuit opens at the threshold | tasks complete after the mode returns to `ok`; no task `FAILED` while attempts remain |
| D2 | Kafka unavailable during crawl | stop `kafka` mid-cycle | attempts `PUBLISH_ERROR` with `parsed_count = acknowledged`; raw still in Bronze | after restart, check 8 passes over the window — no reconciliation mismatch from the partial publishes |
| D3 | MinIO unavailable during the Silver sink | stop `minio` | sink stops committing; the consumer lag grows; **no DLQ record is produced** | after restart lag returns to 0; Silver count equals acknowledged observations; DLQ unchanged (proves the §6.2 fix) |
| D4 | Elasticsearch / Redis unavailable during speed | stop `elasticsearch`, then `redis` | speed batch audit `FAILED`; query retries the same batch | after restart the batch `SUCCEEDED`; `es_changes_unique` holds; Redis sorted set has no duplicate members |
| D5 | Speed restart from checkpoint | `docker kill` the speed container mid-stream, restart | — | no change event lost or duplicated: ES change count = distinct `event_id`; second variant deletes the checkpoint volume and bumps `MARKETPLACE_STREAM_CHECKPOINT_VERSION` → full replay from `earliest` yields the same ES document set |
| D6 | Expired crawl lease | `docker kill` crawl-worker A while it holds leases; start worker B | tasks stay `LEASED` until `lease_expires_at` | B recovers and completes them; no task processed twice concurrently; A's late completion (if any) raises `LeaseLostError` |
| D7 | Parser schema drift | stub mode `drift` | attempts `PARSE_ERROR`, terminal; circuit **not** counted; raw kept in Bronze | `crawler.reparse` on a drifted artifact reports `PARSE_FAILED`, writes nothing |
| D8 | Quality failure | append to `audit.crawl_request_attempt` a settled attempt whose `parsed_count` disagrees with Silver, inside the lookback | batch `QUALITY_FAILED`; cache version and pointer unchanged; results stored | remove the bad row; `mp batch --as-of <same>` resumes → `SUCCEEDED`, pointer advances |
| D9 | PostgreSQL publish failure | add a temporary CHECK constraint that rejects one cache row (the method used for run `e2e-f` on 2026-10-01) | run `FAILED`; previous cache version and previous pointer both intact | drop the constraint; resume → `SUCCEEDED` |
| D10 | Concurrent batch runs | start two `mp batch` at once | exactly one runs; the other exits 75 with `ALREADY_RUNNING` and writes no audit row | pointer and cache consistent (`pointer_matches_cache`) |
| D11 | Backup and restore | `mp backup`, then `mp restore` into a fresh Compose project | — | Section 12.3 restore checks all pass |

Rules for drill code:

- Inject only through Compose (stop, kill, start), the stub's mode endpoint,
  or one documented SQL statement whose reverse is in the same drill. Never
  edit a data file by hand.
- A drill that cannot reach its baseline fails. It never "skips green".
- A drill restores the stack to a passing `validate` before it exits, pass or
  fail, so `drill all` cannot cascade.
- A drill that reveals a production bug gets a failing unit test (fakes are
  fine) and a fix, in two commits, before the drill is marked passing — the
  same rule as every earlier phase.

## 11. Kibana — the P1-11 half (WP8)

### 11.1 Index templates

`display/kibana/marketplace_index_templates.py` installs composable index
templates with explicit mappings for every `marketplace-*` index:

- prices and amounts (`current_value`, `previous_value` where numeric,
  `current_price`, `list_price`) as `scaled_float` with `scaling_factor` 1000000,
  the same six decimal places as the PostgreSQL `decimal(38,6)` columns;
- identifiers as `keyword`, timestamps as `date`, counts as `long`;
- `dynamic: strict` for the projector indices, so a stray field fails loudly.

`previous_value` / `current_value` hold a scalar for most change types but the
whole offer, an object, for `NEW_OFFER`, and no Elasticsearch mapping takes
both. Amended in WP2: the sink's projection stores an object value as its
canonical JSON string (`_change_document`), so the template maps both fields
as `keyword`, plus `scaled_float` subfields with `ignore_malformed: true` for
numeric charts. The wire contract is unchanged. Without the projection, the
first `NEW_OFFER` fixed the field as an object and the first price change was
refused (`PROGRESS.md` §13.2).

### 11.2 Ops projector — `ops/es_projector.py`

A loop service (profile `ops`). Every `MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS`:

| Target index | Source | `_id` |
|---|---|---|
| `marketplace-source-health-v1` | `audit.crawl_source_state`, plus derived `circuit_open = opened_until > now` and the latest Redis `rt:source:<mkt>:last_observation` | `marketplace_code` |
| `marketplace-crawl-attempts-v1` | `audit.crawl_request_attempt` joined to the frontier's `marketplace_code`, rows with `completed_at >= watermark - overlap` | `attempt_id` |
| `marketplace-speed-batches-v1` | `audit.marketplace_speed_batch`, same watermark rule on `completed_at` | `query_name:batch_id` |
| `marketplace-dlq-v1` | Kafka `marketplace.observations.v1.dlq`, consumer group `KAFKA_DLQ_PROJECTOR_GROUP`, commit after index | `dlq_id` |

- Deterministic `_id`s make every projection idempotent. The overlap re-indexes
  the recent window each pass, so a late `UPDATE` or a crash mid-pass is
  repaired by the next pass.
- The watermark is in-process state only. On restart, the projector resumes
  from `now - overlap`. A full rebuild is `--rebuild`, which re-indexes
  everything.
- A projector failure never touches PostgreSQL or Kafka beyond its own
  consumer offsets. It is read-only towards the pipeline.

### 11.3 Dashboards

Saved objects are committed as `.ndjson` and imported by
`display/kibana/setup_marketplace_kibana.py`, run by the
`kibana-marketplace-setup` init container (profile `serve`). Import uses
`overwrite=true`, so it is idempotent. `create_marketplace_speed_dashboard.py`
becomes a thin call into the same importer.

**Marketplace — realtime changes** (fills the empty Phase 5 shell):
recent changes table; `LARGE_PRICE_DROP` count over time; `NEW_OFFER` and
`OFFER_STALE` over time; changes by `change_type`; offers by availability.

**Marketplace — source health and freshness:**

| Panel | Index | Brief §17 item |
|---|---|---|
| Source last success, circuit state, consecutive failures | source-health | source last-success time |
| Attempts by `error_kind` over time | crawl-attempts | fetch/parse errors |
| HTTP status distribution, latency p50/p95 per day | crawl-attempts | (Brief §19 crawl metrics) |
| Parsed vs rejected per day | crawl-attempts | parse rejection |
| DLQ records by stage over time | dlq | DLQ errors |
| Observation rate (parsed per hour) | crawl-attempts | observation rate |
| Speed micro-batch duration and rows per batch | speed-batches | processing latency |
| Freshness: now − last observation per source | source-health | freshness |

Honest labelling, required on the panels and in the runbook:

- "Processing latency" here is **micro-batch duration**
  (`completed_at − started_at`). End-to-end observation-to-change latency is a
  Phase 9 measurement (P2-01). The change contract carries no processed time,
  and Phase 8 does not add one.
- Kibana shows operational, recent state. Gold-level freshness and coverage
  stay in Superset (`cache.marketplace_offer_freshness`,
  `cache.marketplace_source_coverage_daily`).

### 11.4 Recreating the existing `*-v1` indices

The existing `marketplace-changes-v1` and `marketplace-offers-current-v1`
indices were mapped dynamically. The templates apply only to new indices.
Procedure, also in the runbook:

1. stop `speed`;
2. delete both indices; the templates are already installed;
3. bump `MARKETPLACE_STREAM_CHECKPOINT_VERSION` (for example to `v2`), so the
   query starts from `earliest` with a fresh checkpoint;
4. start `speed`. The deterministic `_id`s rebuild the same documents.

Redis is rebuilt by the same replay. This is the D5 replay drill run once on
purpose.

The replay relies on the speed audit keying a batch by
`(query_name, query_id, batch_id)` (fixed in WP2). Batch IDs restart at 0
under a new checkpoint, and with the old `(query_name, batch_id)` key the
audit skipped every replayed batch as already `SUCCEEDED`, so the indices were
never rebuilt.

## 12. Backup, export and restore (WP9)

### 12.1 What is backed up

| Store | Backed up | Why |
|---|---|---|
| Bronze (all) | yes | raw truth; cannot be recreated (Brief §20) |
| Silver (all) | yes | canonical observations; recreatable only by replaying Kafka, which retention does not keep |
| Gold manifests and `current.json` | yes | the single definition of the serving version (Phase 7 §23) |
| Gold run named by `current.json` | yes | the pointer must resolve after restore |
| other Gold runs | no | rebuildable from Silver |
| PostgreSQL `audit`, `cache`, `staging` schema DDL | yes, `pg_dump -Fc` of `audit` and `cache` | run history, quality evidence, crawl audit, frontier |
| Kafka, Elasticsearch, Redis, speed checkpoints | no | derived or transient; rebuilt by replay or new crawls |
| adapter fixtures and versions | already in git | Brief §20 "Lưu adapter version và fixtures" |

### 12.2 `mp backup`

1. Take the batch lock (§6.5) for the duration, so no batch promotes while the
   pointer and the cache are being copied. The crawl, sink and speed services
   keep running: Bronze and Silver are append-only, so a copy taken a few
   seconds earlier than the dump is merely older, never torn.
2. Copy `current.json` **first**, then the Gold run it names, then the manifests.
3. `pg_dump` the `audit` and `cache` schemas.
4. Mirror Bronze and Silver (`mc mirror`, or a filesystem copy for the local
   profile).
5. Write `backup-manifest.json` with: backup ID; pointer `run_id`;
   `audit.marketplace_cache_version.run_id` at dump time; for each copied
   object its path, size and SHA-256; counts per zone; and the tool versions.
6. Release the lock.

The two run IDs in step 5 must be equal. If they differ, the backup fails,
because a backup whose pointer and cache disagree restores into a state
`validate` rejects.

### 12.3 `mp restore` and the restore checks

Restore into a **separate** Compose project name and fresh volumes. Never
restore over the running stack.

1. Verify every SHA-256 in `backup-manifest.json` before anything is written.
2. Load PostgreSQL, then MinIO, writing `current.json` **last**.
3. Checks:
   - `current.json` `run_id` equals the restored `audit.marketplace_cache_version.run_id`;
   - every dataset in the restored pointer exists with its manifest row count;
   - `crawler.reparse` on a deterministic sample of N restored raw artifacts
     returns `IDENTICAL` against restored Silver;
   - a `--quality-only` batch at the pointer's `as_of`, under a new run ID,
     passes the quality gate on the restored Silver and audit. Row counts are
     reported next to the pointer's, but not asserted equal: the `as_of` cut
     excludes observations made after the pointer's window, yet a row observed
     inside it that landed late in Silver legitimately changes a rebuild
     (`PHASE_7` §13). An exact-count check would fail on correct data.
4. ES and Redis start empty and refill from new crawls. The runbook says so
   rather than pretending they were restored.

## 13. Documentation (WP10)

- **`docs/ARCHITECTURE.md`** — rewrite around the marketplace pipeline:
  - components and their profiles;
  - the data flow in Section 1;
  - failure boundaries per component, and what each one's restart does (from the drills);
  - the `as_of` cut;
  - the quality gate and the manifest pointer.

  Keep the Kaggle pipeline as a short "legacy demo" section.
- **`docs/DATA_MODEL.md`** — add:
  - the observation, change and DLQ contracts, plus the Silver and Gold layouts;
  - every `cache.marketplace_*` table, including the anomaly key with `currency`;
  - `audit.marketplace_batch_run` with `QUALITY_FAILED` and the manifest columns,
    plus `audit.marketplace_cache_version`, `audit.marketplace_quality_result`
    and `audit.marketplace_speed_batch`;
  - the crawl audit tables, including `PUBLISH_ERROR`;
  - the manifest and pointer;
  - the Elasticsearch indices and Redis keys, with their `_id`/member rules.

  The legacy model moves to a section of its own.
- **`docs/RUNBOOK.md`** — new:
  - first start;
  - seeding;
  - daily operational check (Brief §20) mapped to `validate`;
  - each drill's recovery steps;
  - backfill (`mp batch --allow-backfill`);
  - recreating the ES indices;
  - backup and restore;
  - the `--skip-postgres` warning (it can advance the pointer past the cache, `PROGRESS.md` §10.6);
  - the lookback-versus-interval constraint.
- **Phase 6 §16 and Phase 7 §18** — replace the "offline smoke with
  `--skip-postgres`" sentences. That smoke cannot run, because the crawl audit
  is read over JDBC unconditionally. Point to `mp smoke` instead.
- **`PHASE_INDEX.md`**: add the D5 boundary line to §1 and update the Phase 8
  row in §5. Also add one line to §5 that Phase 4/5 service wiring was
  completed in Phase 8 (D1).
- **`PROGRESS.md`**: record D2's reversal of the Airflow decision in §5, and
  add a Phase 8 session section.

## 14. Required tests

Default suite: offline, fakes allowed, with Spark where it is already used.
Drill suite: marker `drill`, real stack.

### Services — default suite

1. the crawl service runs `run_once` N times under `--max-cycles` and sleeps the idle interval only when nothing was leased;
2. SIGTERM during a cycle finishes the task in hand and leases no new one;
3. `publishing_executor` publishes in order and returns the report unchanged when every publish is acknowledged;
4. a publish failure after `k` acknowledgements raises `ObservationPublishError` with `acknowledged=k`;
5. `PUBLISH_ERROR` is retryable, and the attempt records `parsed_count = acknowledged`;
6. `crawl_run_id_for` differs between two attempts of the same task;
7. `seed_frontier` is idempotent: a second seed of the same targets enqueues nothing;
8. the Tiki adapter takes its listing and robots URLs from settings;
9. the Silver service commits an offset only after `process_record` returns;
10. a raising `process_record` leaves the offset uncommitted and is retried at the same offset;
11. **a Silver writer failure on a valid event raises and produces no DLQ record** (the §6.2 fix; test commit before fix commit);
12. decode and contract failures are still quarantined and sent to the DLQ;
13. the speed service builds its query with the configured name, checkpoint location (honouring the root override) and trigger;
14. the scheduler derives `as_of` and `run_id` deterministically from the clock, interval and lag;
15. the scheduler skips a `SUCCEEDED` window, starts an absent one, resumes any other status, and never passes `allow_backfill`;
16. a `QualityGateFailure` in one tick does not stop the next tick;
17. `exclusive_batch` raises `BatchAlreadyRunning` when the lock is held and releases the lock on exit and on error;
18. a run refused by the lock writes no audit row and exits 75;
19. `promote_manifest` returns `PROMOTION_CONFLICT` and writes nothing when the pointer moved;
20. `build_spark()` applies the S3A options for a non-local profile and omits them for `local`;
21. `validate_settings()` rejects a lookback not exceeding interval plus settle;
22. the migration adding `PUBLISH_ERROR` runs twice on the DDL text without error, verified as Phase 7 verified the anomaly-key migration.

### Ops — default suite

23. every `validate` check reports `PASS` and `FAIL` correctly from fake clients, and the report is sorted and deterministic;
24. the backup manifest records the SHA-256 of every copied object, and a single changed byte fails restore verification;
25. a backup whose pointer and cache run IDs differ fails;
26. restore writes `current.json` last;
27. the projector builds documents with the documented deterministic `_id`s, and two passes over the same rows yield identical bulk bodies;
28. the projector's overlap window re-indexes the recent rows each pass;
29. every committed `.ndjson` saved object parses, references only index patterns installed by the templates, and contains at least one panel;
30. the index templates map price fields as `scaled_float` and identifiers as `keyword`;
31. the stub source serves deterministic prices per `(fixture, serve counter)` and honours each mode.

### Drills — marker `drill`

32. one test per drill D1–D11, asserting the Section 10 invariants;
33. `mp smoke` passes on a fresh stack;
34. `validate` passes after `drill all`.

## 15. Verification commands

```powershell
python -m pytest tests -q                       # default suite, offline
python -m pytest tests -q -m drill              # needs the running stack
python -m crawler.service --help
python -m crawler.seed_frontier --help
python -m data_ingestion.marketplace_silver_service --help
python -m batch_layer.marketplace_scheduler --help
python -m ops --help
docker compose config --profiles                # lists the eight profiles
docker compose --profile legacy config -q       # legacy still resolves
.\scripts\mp.ps1 smoke
.\scripts\mp.ps1 validate --json validate.json
.\scripts\mp.ps1 drill all
git diff --check
```

The default suite must stay offline:
`test_no_default_test_opens_network_kafka_minio_or_postgresql` still passes.

## 16. Commit and work-package sequence

Each package ends with its focused tests green and the full default suite
green. Stop after each package and show the focused tests plus
`git diff --stat`. Keep a production fix in its own commit, separate from the
test that found it.

1. `feat: add the crawl service with Kafka publication and frontier seeding`
   - `crawler/service.py`, `crawler/seed_frontier.py`, `publishing_executor`,
     `PUBLISH_ERROR` (enum + migration), the configurable Tiki URLs; tests 1–8, 22.
2. `feat: add the Silver sink and speed services`
   - test commit, then fix commit, for the §6.2 misclassification;
   - then both services; tests 9–13.
3. `feat: schedule the batch under an exclusive lock`
   - scheduler, lock, pointer compare-and-swap, S3A wiring; tests 14–21.
4. `feat: add marketplace images and compose profiles`
   - two Dockerfiles, `requirements-marketplace.txt`, Compose profiles and
     healthchecks, `.env.example`, the `legacy` profile and `start_all.ps1`.
5. `feat: add the stub source and the one-command ops CLI`
   - `ops/` (stub, validate, smoke), `scripts/mp.ps1`; tests 23, 31, 33.
6. `test: add failure drills D1-D6`
7. `test: add failure drills D7-D10`
   - any production bug a drill reveals: test commit, then fix commit, inside
     the package that found it.
8. `feat: project audit and DLQ into Elasticsearch and add Kibana dashboards`
   - templates, projector, saved objects, setup; tests 27–30; index
     recreation run once.
9. `feat: add backup, export and restore`
   - `ops/backup.py`, D11; tests 24–26.
10. `docs: rewrite architecture and data model, add the runbook`
    - Section 13 in full, with status rows in `PHASE_INDEX.md` and `PROGRESS.md`.

Do not start Phase 9 work inside these commits.

## 17. Definition of Done

- [ ] the dependency gate is recorded; D1–D8 stand, or overrulings are recorded in §2.1;
- [ ] each of the four services runs unattended under Compose and stops gracefully;
- [ ] the crawler publishes every parsed observation, and a partial publish is audited with `parsed_count = acknowledged`;
- [ ] a Silver write failure is retried, never sent to the DLQ;
- [ ] the speed query is checkpointed and restarts without loss or duplication;
- [ ] two batch runs can never interleave, and the pointer cannot be overwritten by a stale promoter;
- [ ] the marketplace batch runs against MinIO through `s3a://`;
- [ ] `docker compose up` starts only the core; every component has a profile; legacy runs only under `legacy`;
- [ ] no Phase 8 automation contacts the live marketplace;
- [ ] `mp smoke` passes on a fresh stack, and `mp validate` passes on a running one;
- [ ] all eleven drills pass against the real stack and leave `validate` green;
- [ ] Kibana has a populated realtime dashboard and a source-health and freshness dashboard, each panel labelled with what it actually measures;
- [ ] a backup restores into a fresh project, with pointer and cache in agreement and reparse `IDENTICAL`;
- [ ] `ARCHITECTURE.md`, `DATA_MODEL.md` and `RUNBOOK.md` describe the marketplace system as built;
- [ ] the default suite is green and offline.

## 18. Mandatory rejection conditions

Reject the work package if any of these is true:

- any frozen contract in `PHASE_INDEX.md` §3 changes;
- a drill passes by skipping, by retrying until green without asserting the faulted state, or by editing data by hand;
- a service commits a Kafka offset before its write succeeded;
- a valid observation can reach the DLQ;
- `parsed_count` for a partially published attempt is anything but the acknowledged count;
- the default test suite opens a network connection;
- any automation contacts the live marketplace;
- Airflow, Prometheus, Grafana or another resident system is added;
- an image or jar is pinned to `latest` or left unpinned;
- the legacy pipeline's behaviour changes other than its profile;
- a panel labelled "latency" shows anything other than the quantity the runbook names;
- a restore writes over the running stack, or writes `current.json` before the data it names;
- performance numbers are reported as Phase 8 results (they belong to Phase 9).

## 19. Handoff to Phase 9

Phase 9 (Week 9, P2-01 … P2-07) takes evaluation and feature freeze. It
inherits from Phase 8:

- the drill records (Section 10) as reliability evidence;
- the running stack and `mp smoke` as the base of the end-to-end integration
  run and the benchmark harness;
- the stub source as the controlled load generator for throughput
  measurement. Phase 9 must label any number measured against it as replayed
  fixtures, not new marketplace observations (Brief §23);
- the projector indices as the source of operational time series;
- the operational profiles, on which P2-05 and P2-06 build the demo profile
  and the one-command demo (Section 2.1 D5).
