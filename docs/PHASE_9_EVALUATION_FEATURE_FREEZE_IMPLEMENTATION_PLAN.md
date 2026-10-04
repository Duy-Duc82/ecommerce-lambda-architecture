# Phase 9 implementation plan — Evaluation and feature freeze

> Status: **draft, awaiting review** (2026-10-04, branch
> `phase-9-evaluation-plan`). No code may be written until the decisions in
> Section 2.1 are accepted or overruled, and the result is recorded there.
>
> Intended implementer: a low-capability coding model working one work package
> at a time. It must follow the fixed contracts and stop between packages.
>
> Parent plan: `Brief để xây dựng plan sơ bộ cho việc cải tiến ecommerce-lambda-architecture.md`
>
> Phase boundary: `docs/PHASE_INDEX.md` §1 — Phase 9 owns **P2-01 … P2-07**.
> It must not take any P3 item (Brief §27: P3 opens only after P0–P2 meet the
> Definition of Done), and it must not reopen P1 work beyond the
> instrumentation that P2-01 needs (Section 2.1, D3).

## 1. Objective

Implement the parent plan's Week 9 slice (Brief §21 "Tuần 9 — Evaluation và
feature freeze") and backlog P2-01 … P2-07:

```text
live collection stack   -> 30+ days of real Tiki observations (Brief §23)
isolated bench stack    -> replayed-fixture load -> throughput and latency (P2-01)
crawl audit + drills    -> crawl reliability evaluation (P2-02)
Gold freshness/coverage -> freshness and coverage evaluation (P2-03)
storage snapshots       -> storage growth and volume projection (P2-04)
demo / bench profiles   -> application profiles (P2-05)
mp demo                 -> scripted, offline-capable thesis demo (P2-06)
mp evidence             -> one reproducible evidence bundle for the report (P2-07)
feature freeze          -> tag; afterwards only correctness, reliability, report and demo blockers
```

This phase answers **"how well does the system actually perform, on what
data, on what hardware — and can someone rerun the measurement?"** Every
number it produces must name its dataset (live observations or replayed
fixtures), its time window, its hardware and the commit it ran on.

### 1.1 The gap this plan must close first

The 2026-10-04 survey found that **no continuous collection on the real
marketplace has ever run**. `audit.crawl_request_attempt` on the running
stack holds 476 attempts over two days (2026-10-03 and 2026-10-04), all of
them smoke and drill traffic against the stub source. Phase 8 kept every
automation away from the live site on purpose (Phase 8 plan §8).

Brief §23 makes "the main source collected continuously for at least 30 days"
the first item of the core Definition of Done, and Brief §24 rates "not
enough days of data" as a very high risk whose control is "start collection in
Week 2". Phase 9 starts in Week 9. Each day without collection is a day the
report cannot recover. That is why WP0 (Section 4) comes before everything
else and is only a few hours of work.

P2-02 and P2-03 evaluate *collection*. Run on fixture traffic, they would
evaluate the stub. They are therefore built in Phase 9 and run on whatever
live window exists, and their reports are rerun in Weeks 10–12 as the window
grows. The generator is the deliverable; the numbers refresh.

### 1.2 What the survey found about measurability

Measured against the code at `develop` `8e6a19e`:

- **No end-to-end latency can be computed from stored data today.**
  - `MarketplaceChangeV1.detected_at` is the observation's `produced_at`, not
    the processing time (`speed_layer/marketplace_change_rules.py:103-124`).
  - Silver stores the event as received, with no write time
    (`data_ingestion/marketplace_silver_sink.py:43,59`).
  - The speed stream reads the Kafka timestamp as `kafka_timestamp` and then
    drops it (`speed_layer/marketplace_speed_layer.py:67,91`).
  - The only interval on record is `produced_at - fetched_at`.
- **No Spark streaming progress is recorded.** There is no `lastProgress`,
  `recentProgress` or listener anywhere. `audit.marketplace_speed_batch` has
  `started_at`/`completed_at`, but those bracket only the sink write
  (`ops/es_projector.py:183`).
- **Nothing measures storage.** No code sums MinIO object sizes, Parquet
  sizes, table or index sizes. Only `raw_bytes` per crawl attempt exists.
- **The stub cannot generate load.** The fixture holds three rows, one of them
  invalid on purpose, so a page yields two valid offers. Compose runs it with
  `last_page = 2`. Its listing-ID formula
  `fixture_id*10000 + crc(cat)%100*100 + page` (`ops/stub_source.py:49`)
  collides once `page > 99`, or when two categories share `crc32 % 100`.
- **Two stacks cannot run side by side.** `docker-compose.yml` sets
  `container_name` on 21 services, and container names are global. A second
  Compose project therefore cannot start while the first runs; D11 had to take
  the stack down to restore (`scripts/mp.ps1:159`). This collides head-on
  with 1.1: a benchmark, a demo or a drill would stop the live collection.

## 2. Mandatory dependency gate

Before editing, record and verify:

1. Phase 8 is merged into `develop` (`8e6a19e` or later). `PROGRESS.md` §23.6
   checks off the Phase 8 Definition of Done.
2. `python -m ops --help` lists `validate`, `status`, `backup` and `restore`,
   and `scripts/mp.ps1` has `up`, `smoke`, `drill` and `batch`.
3. `audit.crawl_request_attempt`, `audit.marketplace_speed_batch`,
   `audit.marketplace_batch_run` and the Gold marts `offer_freshness`,
   `source_coverage_daily` and `crawl_reliability_daily` exist with the
   columns listed in Section 1.2's sources.
4. The drill records `data/ops/drills/d1.json … d11.json` exist and each has
   `"passed": true`.
5. The default suite passes and stays offline:
   `tests/test_crawl_worker.py::test_no_default_test_opens_network_kafka_minio_or_postgresql`.

If any item is absent or differs, stop and report the exact mismatch.

### 2.1 Decisions — proposed 2026-10-04, **not yet reviewed**

Each decision below needs an explicit accept or overrule before its work
package starts. Record the outcome next to it.

- **D1 — Live collection starts now, on its own Compose project, and nothing
  else ever runs there.** This is the first time any Phase 8+ automation
  contacts the live marketplace, so it needs explicit approval. Proposed
  universe and cadence, inside Brief §20 ("start with 100–500 offers"):
  - five frozen Tiki categories, three listing pages each: 15 targets of about
    40 offers, so 500–600 offers;
  - `ACTIVE` tier (60-minute cadence) and `CRAWL_REQUEST_DELAY_SECONDS = 2.0`.
    That is 15 requests an hour, which is about 360 a day;
  - expected volume: about 14,000 observations a day, and about 430,000 in
    30 days. This sits on the Brief §5 projection's first row.

  The project is named `mp-live`, and the rule is absolute: **no smoke, drill,
  benchmark or demo ever runs in `mp-live`**. Each of those injects stub
  traffic or faults, and either would contaminate the audit that P2-02 reads.
  Owner: the user, for the categories and for keeping the host running.
- **D2 — Two stacks may coexist.** `container_name` takes a prefix,
  `${MP_CONTAINER_PREFIX:-}`. The prefix defaults to empty, so the existing
  names and every runbook command stay valid. Each isolated project also gets
  its own env file (`env/bench.env`, `env/demo.env`) with distinct host ports.
  Every script that addresses a container by name reads the prefix. The
  alternative was to pause collection for each benchmark. It was rejected:
  every pause shows up as a gap in the reliability data the report rests on.
- **D3 — Measurement instrumentation is allowed; behaviour changes are not.**
  Phase 9 may add *audit columns and audit tables* that record timing. It may
  not change a frozen contract (`PHASE_INDEX.md` §3). In particular:
  - no field is added to `MarketplaceObservationV1` or `MarketplaceChangeV1`;
  - no output any consumer reads changes shape;
  - every new column is nullable, and is added by an idempotent migration.

  The two additions are in Section 6.
- **D4 — The benchmark load is the frozen fixture, synthesised at scale, and
  it is labelled as such everywhere.** Brief §5 and §23 allow replaying
  observations for throughput and forbid presenting them as new
  observations. Every benchmark artefact therefore carries
  `"dataset": "replayed_fixture"`, and the report generator refuses to put a
  `replayed_fixture` number and a `live` number in the same table without the
  label column. Once `mp-live` holds real Silver, the load generator can take
  an exported Silver sample as its source instead (`--source silver-export`).
  The label then becomes `replayed_live`, which is still a replay.
- **D5 — The demo runs offline, from the stub, in its own project.** The demo
  never depends on Tiki or on the network (Brief §24 "Demo phụ thuộc
  internet"). Its recorded fallback is two things:
  - a **restorable demo backup**, taken with Phase 8's `mp backup`, so the
    stack can be restored in place of a live run;
  - a screen recording that the user makes by following `docs/DEMO_SCRIPT.md`.

  No browser automation or screenshot tooling enters the stack (Brief §27).
- **D6 — Feature freeze is a tag, and it has a gate.** At the end of Phase 9,
  `develop` is tagged `feature-freeze-w9`, but only after the full suite, `mp
  smoke` and `drill all` have passed in an isolated project. After the tag,
  only commits whose message starts `fix:`, `docs:`, `test:` or `report:` may
  enter `develop`. Brief §27 lists what may still change.

## 3. Scope

### 3.1 In scope

- Starting and monitoring live collection (WP0), and the isolation that lets
  it keep running (WP1).
- Timing instrumentation for the speed path and a storage snapshot (WP2).
- A load-capable stub and a deterministic load generator (WP3).
- The benchmark runner, P2-01 (WP4).
- The crawl reliability, freshness and coverage evaluations, P2-02 and P2-03
  (WP5).
- The storage growth and volume report, P2-04 (WP6).
- The `demo` and `bench` profiles, `mp demo`, the demo script and the offline
  fallback, P2-05 and P2-06 (WP7).
- The evidence bundle and the feature freeze, P2-07 (WP8).

### 3.2 Out of scope

- Any P3 item: product or variant schemas, matching, comparison tables.
- Any frozen contract change (`PHASE_INDEX.md` §3).
- Prometheus, Grafana, Airflow, a browser or screenshot service, or any other
  new resident system (Brief §19, §27).
- Crawling more of the live source than D1 fixes, a second marketplace
  adapter, or a higher cadence to inflate volume (Brief §5: "Không tăng crawl
  frequency chỉ để tạo volume").
- Writing the thesis chapters. Brief §21 Week 9 lists "draft Chương 1–5";
  that is the user's work. Phase 9 supplies its inputs through the evidence
  bundle, and nothing in this plan writes report prose.
- Tuning for speed. Phase 9 measures the system as built. A bottleneck it
  finds is reported, and fixed only if it is a correctness or reliability
  defect.

## 4. Live collection — WP0 (do this first)

1. Agree the five categories with the user and freeze them in
   `config/settings.py` as `TIKI_CATEGORIES`. Then record them, with the date,
   in `PROGRESS.md`.
2. Create `env/live.env` with `COMPOSE_PROJECT_NAME=mp-live` and the live
   `TIKI_LISTING_URL`. Commit it; it holds no secrets.
3. On the host, bring the stack up and seed it:

   ```powershell
   .\scripts\mp.ps1 up -With crawl,ingest,speed,batch,ops
   .\scripts\mp.ps1 migrate
   .\scripts\mp.ps1 seed --tier ACTIVE --pages 3   # categories from settings
   ```

4. Make sure no smoke task is `READY` (`ops park-smoke`). A smoke category
   that is still active would send the crawler to the live site for
   categories that do not exist there.
5. Run the daily check from `RUNBOOK.md` once a day and keep the JSON:
   `mp validate -Json data/ops/live/validate-<date>.json`.
6. Record the start instant in `PROGRESS.md`. Day 30 is the earliest date on
   which the Brief §23 item can be checked off.

Acceptance: within two hours, `mp status` shows successful live attempts;
Silver gains observations; one scheduled batch succeeds the next day; and
`validate` passes.

WP0 ships before WP1 lands, so its first run uses the existing unprefixed
container names. Until WP1 is merged, **no smoke, drill or benchmark may run
on this machine**.

## 5. Isolation — WP1

- Every `container_name: X` in `docker-compose.yml` becomes
  `container_name: ${MP_CONTAINER_PREFIX:-}X`.
- `env/bench.env` and `env/demo.env` each set:
  - `COMPOSE_PROJECT_NAME`;
  - `MP_CONTAINER_PREFIX` (`bench-`, `demo-`);
  - a full set of `*_HOST_PORT` values that does not overlap the default set
    or each other;
  - `TIKI_LISTING_URL` pointing at the stub.
- `scripts/mp.ps1` gains `-Env <file>`. It loads the file into the process
  environment before any `docker compose` call. Without `-Env`, it keeps
  today's behaviour.
- `mp.ps1` refuses `smoke`, `drill` and `demo` when `COMPOSE_PROJECT_NAME` is
  `mp-live`, with exit code 2. This is the mechanical form of D1's rule.
- `ops/drills.py` and every script that runs `docker exec <name>` or
  `docker kill <name>` resolves names through one helper that applies the
  prefix.

Acceptance: with `mp-live` running, `mp -Env env/bench.env smoke` passes, and
the `mp-live` containers report the same `StartedAt` before and after.

## 6. Instrumentation — WP2 (D3)

### 6.1 Speed progress and latency

New table `audit.marketplace_stream_progress`, one row per completed
micro-batch per query:

| Column | Source |
|---|---|
| `query_name`, `query_id`, `batch_id` | the query; primary key, same as `marketplace_speed_batch` |
| `recorded_at` | the wall clock when the row is written |
| `num_input_rows`, `input_rows_per_second`, `processed_rows_per_second` | `StreamingQueryProgress` |
| `trigger_execution_ms`, `add_batch_ms`, `get_batch_ms`, `query_planning_ms`, `wal_commit_ms` | `progress.durationMs` |
| `latency_p50_ms`, `latency_p95_ms`, `latency_max_ms` | see below |

- The service polls `query.lastProgress` once per trigger and inserts each
  batch it has not seen yet, with `ON CONFLICT DO NOTHING`. Polling replaces a
  `StreamingQueryListener` on purpose: PySpark 4.0 listeners run on the driver
  in a separate thread, and the service already owns a loop.
- **Latency** means processing completion minus the Kafka record timestamp,
  measured per row, in milliseconds. Inside `foreachBatch`, after the sinks
  succeed, the batch computes `approxQuantile` of
  `(completion_instant - kafka_timestamp)` at 0.5 and 0.95 with relative error
  0.01, plus the max. It writes them to the `marketplace_speed_batch` row as
  three new nullable columns, and the progress row copies them.
- This is the "Spark streaming p50/p95 processing latency" of Brief §25,
  defined precisely. `RUNBOOK.md` must state the definition, including that it
  includes Kafka queueing time while the query was idle.

### 6.2 Silver write time

The sink is not changed. The benchmark derives Silver latency from MinIO
object `LastModified` minus the Kafka record timestamp, and it reads that
timestamp from the topic for the same `(partition, offset)`. Second-level
resolution is enough for a drain measurement, and the report must say that
resolution is one second.

### 6.3 Storage snapshot

- New table `audit.storage_snapshot`:
  `(captured_at, component, scope, bytes, objects)`, where `component` is one
  of `minio`, `postgres`, `elasticsearch` or `kafka`.
  - **minio:** `scope` is the bucket and top-level prefix (`bronze/tiki`,
    `silver/marketplace/offer_observations`, `gold/.../runs`, `quarantine`).
    `bytes` and `objects` are summed from a listing.
  - **postgres:** `scope` is the schema. `bytes` comes from
    `pg_total_relation_size`, summed per schema.
  - **elasticsearch:** `scope` is the index. `bytes` is
    `store.size_in_bytes`, and `objects` is `docs.count`.
  - **kafka:** `scope` is the topic. `bytes` is the log size from
    `kafka-log-dirs.sh --describe` in the kafka container.
- `python -m ops storage-snapshot` writes one snapshot. The es-projector loop
  calls it once a day, keyed on the UTC date, idempotently. It runs in
  `mp-live` as well: a snapshot only reads.

## 7. Load generation — WP3

- **Stub fix, as two commits: a test, then the fix.** A test first shows two
  pages and two categories that collide under the current formula. The fix
  then derives the ID from
  `sha256(f"{fixture_id}:{category}:{page}:{row}")[:8]` as an integer, which
  is stable and collision-free at the scale used here.
- **Stub scale.**
  - `--rows-per-page N` (default 3, which keeps today's behaviour) repeats
    the fixture rows with distinct IDs. The one invalid row stays invalid in
    each repetition.
  - Compose passes `STUB_LAST_PAGE` and `STUB_ROWS_PER_PAGE`, with defaults
    equal to today's values.
- **`ops/bench_load.py`** produces `MarketplaceObservationV1` events straight
  into `marketplace.observations.v1` at a target rate, for a fixed count.
  - Each event's listing ID, `observed_at` and price come from
    `(seed, index)` through a fixed table, so two runs with one seed produce
    byte-identical events.
  - `observation_id` follows the frozen derivation, so Silver dedup behaves as
    it does in production.
  - `--duplicate-ratio r` re-sends a deterministic fraction, which measures
    the dedup path.
  - `--source silver-export PATH` replays an exported live Silver sample
    instead (D4).

## 8. Benchmark runner — P2-01, WP4

`python -m ops bench <scenario> [--repeat 3] [--json PATH]`, run only in a
`bench` project. It refuses to run when `COMPOSE_PROJECT_NAME` is `mp-live`.

| Scenario | Load | Measured | Source of the number |
|---|---|---|---|
| `crawl` | stub, `last_page` 20, 6 categories | requests/s, parsed offers/s, request latency p50/p95/p99, bytes/s | `audit.crawl_request_attempt` |
| `ingest` | `bench_load`, N ∈ {10k, 50k, 200k} at max rate | Silver records/s, lag drain time, Silver write latency p50/p95 | consumer-group lag polled each second; MinIO `LastModified` (§6.2) |
| `speed` | `bench_load` at fixed rates R ∈ {50, 200, 1000}/s for 5 min each | input/processed rows/s, micro-batch duration, latency p50/p95/max, rate where lag stops draining | `audit.marketplace_stream_progress` |
| `batch` | Silver from `ingest` at three sizes | duration, Silver rows/s, Gold rows, per-mart row counts | `audit.marketplace_batch_run` |
| `resources` | runs alongside any scenario | CPU %, memory, block I/O and net I/O per container, at 5 s intervals | `docker stats --no-stream` |

Rules:

- Each scenario starts from a fresh `bench` project (`down --volumes`, then
  `up`), so one run cannot warm the next. Then it waits for `validate`.
- The crawl scenario runs with the production `CRAWL_REQUEST_DELAY_SECONDS`
  and again with it set to `0`, and labels both. The first is the crawler as
  deployed, bounded by politeness. The second is its parse and publish
  capacity against a local source. Only the first describes the real system.
- `--repeat 3` reports the median plus the min and max. One run is never
  reported as a result.
- Every result file embeds:
  - the `environment` block (Section 12.1);
  - `"dataset": "replayed_fixture" | "replayed_live"`;
  - the scenario parameters and the generator seed.
- `bench all` runs every scenario. `bench report` renders the JSON files as
  Markdown tables, one per scenario, each with the dataset label in a column.

## 9. Crawl reliability, freshness and coverage — P2-02, P2-03, WP5

`python -m ops evaluate reliability|freshness [--from ISO --to ISO] [--json PATH]`.
It reads only. The default window is from the WP0 start to the newest attempt.

Reliability (P2-02), computed from `audit.crawl_request_attempt`,
`crawl_frontier` and `crawl_source_state`:

- attempts, success rate, and the distributions of `error_kind` and HTTP
  status, per day and in total;
- request latency p50/p95/p99, per day;
- retries per task, and the share of tasks that needed one, two or three or
  more attempts;
- schedule adherence: the distribution of
  `first attempt started_at - scheduled_for`;
- **collection gaps:** every interval of more than twice the cadence with no
  attempt, listed with its start, its end and, where known, the cause
  (`circuit open`, `host down`, `unknown`). Gaps are a finding, not something
  to clean up;
- circuit-breaker episodes: count and duration;
- drill recovery: for each of D1–D11, `recover` step `at` minus `inject` step
  `at`, taken from the drill records. These are labelled `stub stack, fault
  injected`.

Freshness and coverage (P2-03), computed from the Gold marts of every
`SUCCEEDED` run in the window, read from the cache tables:

- the share of `FRESH`, `STALE` and `FUTURE` offers at each run's `as_of`,
  and the distribution of `age_seconds`;
- `coverage_rate`, `rejection_rate` and `missing_offer_count` per day, from
  `source_coverage_daily`;
- inter-observation interval per offer against the configured cadence: the
  median and p95 of the ratio;
- crawl-to-Kafka delay: `produced_at - fetched_at`, the only pipeline interval
  on record in live data (Section 1.2);
- the monitored-universe size over time: distinct offers per day.

Both reports state the window, the day count, the number of attempts and
observations, and whether the Brief §23 30-day threshold is met. They name
themselves `dataset: live`, and they refuse to run against a project whose
audit holds any attempt with a stub `request_url`. That refusal is how D1's
isolation is checked in the data itself.

Pure functions take rows and return the report, and are tested with fixed
rows. The SQL that feeds them is tested against the DDL text, the way Phase 7
verified its migration.

## 10. Storage growth — P2-04, WP6

`python -m ops evaluate storage [--json PATH]` reads `audit.storage_snapshot`
and reports, per component and scope:

- bytes and objects at each snapshot, plus daily growth;
- bytes per observation per layer: raw Bronze, Silver JSON, Gold Parquet, the
  cache, and the ES indices. The denominator is Silver observation count at
  the same snapshot;
- the Bronze-to-Silver and Silver-to-Gold size ratios;
- a projection to 30, 45 and 60 days, and to the four Brief §5 rows (1,000 and
  5,000 offers; 30-minute to 4-hour cadence). The projection is computed from
  measured bytes per observation, and labelled as a projection;
- the remaining disk on the host volume, and the day it would run out at the
  measured rate.

It needs at least two snapshots, and refuses to project from fewer. Like WP5,
it runs on the live data and is rerun as the window grows.

## 11. Profiles and the one-command demo — P2-05, P2-06, WP7

### 11.1 Profiles (P2-05)

Compose profiles are labels, so the two new ones reuse existing services:

| Profile | Adds | Purpose |
|---|---|---|
| `demo` | stub-source, crawl-worker, silver-sink, speed, batch-scheduler, es-projector, kibana, kibana-marketplace-setup, superset, superset-init | everything the demo shows, from one flag |
| `bench` | stub-source, silver-sink, speed, batch-once, and a `bench-load` `run --rm` service | the benchmark scenarios |

`RUNBOOK.md` gets one table that maps each of the four stacks — `live`,
`smoke` (and drills), `bench`, `demo` — to its env file, its profiles, and
what may run in it.

### 11.2 `mp demo` (P2-06)

`mp demo [-Step N] [-Reset]` uses `env/demo.env`, and runs a fixed sequence.
Each step prints what to look at and where, then waits for Enter, unless
`-Auto` is passed:

1. **Start:** bring up the demo project, migrate, and seed a demo universe of
   three stub categories. Wait until `validate` passes.
2. **Raw first:** show one attempt's `raw_uri`, its Bronze object and
   checksum, and the Silver observation that names it.
3. **Realtime:** switch the stub to a fixed price-drop table, then wait for
   `LARGE_PRICE_DROP` to reach ES. Point to the Kibana realtime dashboard.
4. **Failure and recovery:** stop `minio`, show the Silver lag growing with
   the DLQ unchanged, then start `minio` and show the lag draining. This is
   drill D3, narrated.
5. **Batch truth:** run one batch, and show the quality results, the pointer
   and the Superset price-history chart.
6. **Quality gate:** inject the D8 fault, run a batch and show
   `QUALITY_FAILED` with the cache unchanged. Then remove the fault.
7. **Evaluation:** open the latest `bench report` and `evaluate` outputs from
   the evidence bundle.

`-Reset` takes the demo project down with its volumes. `docs/DEMO_SCRIPT.md`
holds the spoken script, the expected screen per step, and a timing budget
(target: 12 minutes).

### 11.3 Offline fallback

- `mp demo -Snapshot` runs steps 1–6 and then takes `mp backup` of the demo
  project, into `data/ops/demo-backup/`.
- `mp demo -FromSnapshot` restores that backup into the demo project with
  Phase 8's restore. This shows the same state with no crawling at all.
- The screen recording follows `DEMO_SCRIPT.md`. Its path is recorded in the
  evidence bundle index; the video itself stays outside git.

## 12. Evidence bundle and feature freeze — P2-07, WP8

### 12.1 `mp evidence`

`python -m ops evidence --out data/ops/evidence/<UTC stamp>/` collects, and
never measures anew:

- **`environment.json`:**
  - CPU model and cores, RAM, disk, OS;
  - Docker version, and every image with its digest;
  - Python, pyspark and Java versions;
  - `git rev-parse HEAD` and `git status --porcelain`, so a dirty tree is
    visible.
- **`tests/`:** the junit XML of the full default suite, plus the quality
  suite, run by the command itself.
- **`integration/`:** `smoke-validate.json` and the eleven drill records from
  the latest isolated run.
- **`bench/`:** every benchmark JSON file, plus `bench report`.
- **`evaluation/`:** reliability, freshness and storage, as JSON and Markdown.
- **`samples/`**, one per item in Brief §25 that has a data form:
  - a crawl-run audit sample;
  - a raw → Silver → Gold lineage trace for one observation;
  - a schema-drift example (the D7 artefact);
  - quality results and a quarantine sample;
  - price-change and anomaly examples from the cache.
- **`INDEX.md`:** the Brief §25 checklist. Each item links its file, or reads
  `MISSING`, or reads `MANUAL` for items a person supplies, such as the
  screenshots and the demo recording. Each item carries its dataset label.

The command exits non-zero when any non-`MANUAL` item is `MISSING`. It can be
rerun at any time. Each bundle is a new directory, and older bundles are never
overwritten.

### 12.2 The end-to-end integration run

Brief §21 Week 9 "End-to-end integration run" and "Full automated tests" are,
concretely:

1. a full rebuild of a fresh `bench` project;
2. `mp smoke`, then `mp drill all`, then `mp validate`;
3. the default and quality suites;
4. `mp evidence`.

All four run on one commit, and the bundle records that commit. It is the
bundle the thesis cites.

### 12.3 Freeze (D6)

When 12.2 passes:

1. Tag `develop` `feature-freeze-w9`, and push the tag.
2. Add a `PHASE_INDEX.md` §5 row and a `PROGRESS.md` section that name the
   tag, the bundle path and the live-collection day count at that moment.

## 13. Allowed file changes

Create:

```text
env/live.env
env/bench.env
env/demo.env
ops/bench.py
ops/bench_load.py
ops/evaluate.py
ops/storage.py
ops/evidence.py
ops/demo.py
docs/DEMO_SCRIPT.md
tests/test_stub_source_ids.py
tests/test_bench_load.py
tests/test_bench_report.py
tests/test_evaluate_reliability.py
tests/test_evaluate_freshness.py
tests/test_storage_snapshot.py
tests/test_evidence_index.py
tests/test_stream_progress.py
tests/test_compose_isolation.py
```

Modify only:

```text
docker-compose.yml          (container_name prefix, demo/bench profiles, stub scale env)
scripts/mp.ps1              (-Env, the mp-live guard, demo/bench/evidence/evaluate verbs)
scripts/init_postgres.sql   (audit.marketplace_stream_progress, audit.storage_snapshot, three nullable latency columns)
ops/__main__.py
ops/stub_source.py
ops/drills.py               (prefix-aware container names only)
ops/es_projector.py         (the daily storage-snapshot call only)
speed_layer/marketplace_speed_service.py   (progress polling)
speed_layer/marketplace_speed_layer.py     (latency quantiles inside foreachBatch, after the sinks)
speed_layer/marketplace_sinks.py           (write the three latency columns)
config/settings.py          (TIKI_CATEGORIES freeze, new settings registered in validate_settings)
.env.example
.gitignore                  (data/ops/evidence, data/ops/demo-backup)
docs/RUNBOOK.md
docs/ARCHITECTURE.md        (the four stacks; the measurement definitions)
docs/PHASE_INDEX.md
docs/PROGRESS.md
the matching existing test files for the modules above
```

Do not touch the crawler, the Silver sink, the batch warehouse or the
change rules. A measurement that would need one of them changed goes back to
the user as a decision.

## 14. Required tests

Default suite, offline:

1. stub listing IDs are unique across 200 pages × 50 categories × rows
   (the test commit precedes the fix);
2. the stub with default flags serves byte-identical responses to today's;
3. `bench_load` with one seed yields identical events twice; every event
   passes the observation contract; `observation_id` matches the frozen
   derivation;
4. `--duplicate-ratio` re-sends exactly the expected events;
5. the progress poller inserts each batch once, across repeated polls of the
   same `lastProgress`;
6. the latency quantiles are computed after the sinks succeed, and a sink
   failure writes none;
7. the migration for the new table and columns runs twice on the DDL text
   without error;
8. reliability: success rate, error distribution, retries and gaps are
   computed correctly from fixed rows, including a gap that crosses midnight;
9. reliability refuses a window that contains a stub `request_url`;
10. freshness: shares, interval ratios and `produced_at - fetched_at` are
    correct on fixed rows;
11. storage: growth, bytes per observation and the projections are correct on
    fixed snapshots; fewer than two snapshots are refused;
12. a snapshot taken twice on one UTC date writes one row per scope;
13. `bench report` puts the dataset label in every table and refuses to merge
    two labels into one column;
14. a benchmark and a demo both refuse `COMPOSE_PROJECT_NAME=mp-live`;
15. `docker compose config` with `MP_CONTAINER_PREFIX=bench-` and
    `env/bench.env` yields container names and host ports disjoint from the
    defaults;
16. the evidence `INDEX.md` lists every Brief §25 item, and marks absent files
    `MISSING`;
17. the evidence command exits non-zero when a non-`MANUAL` item is missing.

On a stack (marker `drill`, not in the default suite):

18. `mp -Env env/bench.env smoke` passes while the default project runs, and
    leaves it untouched;
19. `bench crawl --repeat 1` and `bench ingest --repeat 1 --n 10000` complete
    and write labelled JSON;
20. `mp demo -Auto` runs steps 1–6 and ends with `validate` passing.

## 15. Verification commands

```powershell
python -m pytest tests -q
python -m pytest tests -q -m drill
python -m ops --help                     # lists bench, evaluate, storage-snapshot, evidence
docker compose --env-file env/bench.env config -q
.\scripts\mp.ps1 -Env env/bench.env smoke
.\scripts\mp.ps1 -Env env/bench.env bench all
.\scripts\mp.ps1 -Env env/live.env evaluate reliability
.\scripts\mp.ps1 -Env env/demo.env demo -Auto
.\scripts\mp.ps1 evidence
git diff --check
```

## 16. Work-package sequence

Each package ends with its focused tests green and the full default suite
green. Stop after each package and show the focused tests plus
`git diff --stat`. Keep a production fix in its own commit, separate from the
test that found it.

0. `ops: start live collection` — Section 4. Config and docs only; it runs
   today.
1. `feat: let isolated stacks run beside the live one` — Section 5; test 15
   and drill test 18.
2. `feat: record streaming progress, latency and storage snapshots` —
   Section 6; tests 5–7 and 12.
3. `test:` then `fix:` for the stub ID collision; then `feat: scale the stub
   and add the load generator` — Section 7; tests 1–4.
4. `feat: add the benchmark runner` — Section 8; tests 13–14 and drill test 19.
5. `feat: evaluate crawl reliability, freshness and coverage` — Section 9;
   tests 8–10.
6. `feat: report storage growth and volume projection` — Section 10; test 11.
7. `feat: add demo and bench profiles and the one-command demo` — Section 11;
   drill test 20.
8. `feat: assemble the evidence bundle`, then the integration run and the
   freeze tag — Section 12; tests 16–17.

WP0 must land first. WP1 must land before any smoke, drill or benchmark runs
on this machine again. WP2–WP3 may proceed in either order; WP4 needs both.

## 17. Definition of Done

- [ ] the dependency gate is recorded; D1–D6 are accepted, or the overrulings
      are recorded in §2.1;
- [ ] live collection runs in `mp-live`; its start instant and universe are
      recorded; the daily `validate` files exist;
- [ ] a bench or demo project runs while `mp-live` keeps running, untouched;
- [ ] `audit.marketplace_stream_progress` and the latency columns fill on a
      running stack, and the RUNBOOK states what "latency" means;
- [ ] `bench all` produces labelled, repeated results for the crawl, ingest,
      speed and batch scenarios, plus a resource trace;
- [ ] the reliability, freshness and storage reports run on the live window;
      each states its window, and whether 30 days are reached;
- [ ] `mp demo` runs offline end to end; a demo backup restores; and
      `DEMO_SCRIPT.md` exists;
- [ ] `mp evidence` produces a bundle with no `MISSING` item, on the freeze
      commit;
- [ ] `develop` is tagged `feature-freeze-w9`;
- [ ] the default suite is green and offline.

The Brief §23 item "collected continuously for at least 30 days" **cannot**
be closed in Week 9 if WP0 starts on 2026-10-04. The earliest date is
2026-11-03. Phase 9 closes without it; the bundle is regenerated once the
window reaches 30 days, and that regeneration is a Week 10–12 task, not new
scope.

## 18. Mandatory rejection conditions

Reject the work package if any of these is true:

- any frozen contract in `PHASE_INDEX.md` §3 changes;
- a smoke, drill, benchmark or demo can run in `mp-live`, or stub traffic
  appears in its audit;
- a benchmark number appears without its dataset label, or a replayed number
  is presented as a live observation (Brief §23);
- a reported figure comes from a single run when `--repeat` was available;
- the crawler, Silver sink, warehouse or change rules change behaviour;
- a metric labelled "latency" measures anything other than its RUNBOOK
  definition;
- the default suite opens a network connection;
- a new resident system is added (Brief §19, §27);
- the cadence or universe grows beyond D1 without a recorded decision;
- after the freeze tag, a commit enters `develop` whose type is not `fix`,
  `docs`, `test` or `report`.

## 19. Handoff to Weeks 10–12

- **Week 10** (buffer, `PHASE_INDEX.md` §1): rerun `evaluate` and
  `mp evidence` as the live window grows. P3 opens only if every Brief §23
  item except the 30-day window is met, and the advisor agrees.
- **Week 11**: the bundle feeds the report's Experiments and Evaluation
  chapter, and its Limitations chapter. The collection gaps in P2-02 belong
  in Threats to Validity.
- **Week 12**: rehearse `mp demo -FromSnapshot`, and do a final `mp evidence`
  on the commit that ships.
