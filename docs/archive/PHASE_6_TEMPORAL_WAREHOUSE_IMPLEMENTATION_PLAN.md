# Phase 6 implementation plan — Marketplace temporal warehouse and serving cache

> Status: ready for implementation after Phase 5 acceptance
>
> Intended implementer: a low-capability coding model working one work package
> at a time. It must follow the fixed grains and stop between packages.
>
> Parent plan: `Brief để xây dựng plan sơ bộ cho việc cải tiến ecommerce-lambda-architecture.md`

## 1. Objective

Implement the parent plan's Week 6 vertical slice and the temporal/serving part
of backlog P1-07 and P1-10:

```text
Phase 4 canonical Silver observations + Phase 3 crawl audit
-> deterministic deduplication and temporal ordering
-> offer/seller current projections
-> price/change/freshness/coverage/reliability/counter marts
-> run-scoped Gold Parquet
-> run-scoped PostgreSQL staging
-> one transactional cache refresh
-> Superset batch draft
```

This phase answers **“what does the observed history show?”** It does not use
the speed layer as historical truth and does not infer real sales or demand.

## 2. Mandatory dependency gate

Before editing, record and verify:

1. Phase 4 Silver observations exist at the accepted deterministic layout and
   retain complete raw URI, checksum, adapter version and crawl run lineage.
2. The accepted Phase 4 flat Silver schema names/types are known. Import-path
   changes may be adapted, but field semantics may not be guessed.
3. Phase 3 crawl run and request-attempt tables are accepted and expose source,
   request status, timestamps, latency, raw/parsed/rejected counts and errors.
4. Phase 5 is accepted, but Phase 6 can run when Phase 5 services are stopped.
5. PostgreSQL remains a serving cache; object storage remains authoritative.

If seller descriptive attributes are not present in the accepted observation
envelope, do not invent them. The current v1 envelope contains `seller_id` on
the offer but not a nested Seller object; this plan therefore builds only an
observed seller reference projection. A future schema version may enrich it.

If any required Silver or audit field is absent, stop and report the exact
mismatch. Do not silently fill operational counts with zero.

## 3. Scope

### 3.1 In scope

- A separate marketplace batch job; the legacy behavioral warehouse remains.
- Recursive read and deterministic flattening of Phase 4 Silver JSON.
- Deduplication by observation ID and deterministic per-offer ordering.
- Current offer and observed seller reference projections.
- Daily offer price history with first/last/min/max and observation count.
- Daily offer change summaries recomputed from Silver.
- Freshness/stale projection at a caller-supplied `as_of` time.
- Source/category coverage with explicit eligible/missing semantics.
- Crawl reliability from Phase 3 audit tables.
- Public counter deltas with validity flags and unclamped negative values.
- Category/source price distributions without cross-source equivalence claims.
- Run-scoped Parquet datasets in Gold.
- PostgreSQL cache DDL and all-table transactional refresh.
- Batch run audit and a Superset marketplace dashboard draft.
- Offline Spark/gateway tests and gated PostgreSQL/object-store integration tests.

### 3.2 Out of scope

- Robust price anomaly (median/MAD/IQR), anomaly reasons or rule version; Phase 7.
- Mandatory data-quality gate and `audit.data_quality_result` extensions; Phase 7.
- Gold current-version manifest/promotion and last-good Gold pointer; Phase 7.
- Raw reparse/replay workflow; Phase 7.
- Cross-market product/variant matching or comparisons.
- Currency conversion or grouping different currencies into one distribution.
- Full seller name/rating/official status when absent from v1 events.
- Forward-filling observations beyond the freshness threshold without a stale flag.
- Reading historical facts from Redis, Elasticsearch or `marketplace.changes.v1`.
- Airflow/Compose profiles, backup and one-command operations; Phase 8.
- Replacing or rewriting `batch_layer/warehouse_job.py`.

Phase 6 may enforce structural preconditions needed to avoid corrupt database
keys. It must not claim the complete blocking quality framework scheduled for
Phase 7.

## 4. Allowed file changes

Create:

```text
batch_layer/marketplace_warehouse.py
batch_layer/marketplace_marts.py
batch_layer/marketplace_postgres.py
config/counter_semantics.py
display/superset/create_marketplace_batch_dashboard.py
tests/fixtures/marketplace_silver/observations.jsonl
tests/test_marketplace_marts.py
tests/test_marketplace_postgres.py
tests/test_marketplace_warehouse.py
tests/test_counter_semantics.py
```

Modify only:

```text
config/settings.py
scripts/init_postgres.sql
display/superset/datasources.yaml
scripts/run_warehouse.ps1
```

Do not add marketplace branches throughout the legacy warehouse job. Shared
helpers may be extracted only in a separate reviewed refactor after both test
suites pass.

## 5. Configuration and deterministic run context

Add environment-backed defaults:

```python
MARKETPLACE_SILVER_DATASET = "marketplace/offer_observations"
MARKETPLACE_GOLD_DATASET = "marketplace"
MARKETPLACE_FRESHNESS_SECONDS = 21600
MARKETPLACE_FRESHNESS_RULE_VERSION = "freshness-rules.v1"
MARKETPLACE_COUNTER_RULE_VERSION = "counter-rules.v1"
MARKETPLACE_COUNTER_MAX_GAP_SECONDS = 86400
MARKETPLACE_PERCENTILE_ACCURACY = 10000
MARKETPLACE_BATCH_SHUFFLE_PARTITIONS = 8
MARKETPLACE_BATCH_APP_NAME = "MarketplaceTemporalWarehouse"
```

Define a frozen context in `batch_layer/marketplace_warehouse.py`:

```python
@dataclass(frozen=True)
class MarketplaceBatchContext:
    run_id: str
    as_of: datetime
    silver_uri: str
    gold_root_uri: str
    freshness_seconds: int
    freshness_rule_version: str
    counter_rule_version: str
    counter_max_gap_seconds: int
```

Rules:

- caller supplies `run_id` and timezone-aware `as_of`; normalize UTC;
- `run_id` matches `[a-zA-Z0-9_-]{1,64}` and is never interpolated raw into SQL;
- thresholds/partition counts are positive;
- rule versions and URIs are non-empty;
- no transform calls `datetime.now()` or `current_timestamp()` for business
  semantics;
- CLI may create run ID/as-of once, then pass them to every stage.

## 6. Canonical input and deduplication

Create the orchestration/source functions in
`batch_layer/marketplace_warehouse.py`:

```python
def build_spark() -> SparkSession: ...

def read_marketplace_silver(
    spark: SparkSession,
    silver_uri: str,
) -> DataFrame: ...

def flatten_marketplace_observations(wire: DataFrame) -> DataFrame: ...

def deduplicate_marketplace_observations(flat: DataFrame) -> DataFrame: ...

def read_crawl_audit(
    spark: SparkSession,
) -> tuple[DataFrame, DataFrame]: ...
```

Read recursively from the accepted Phase 4 observation layout with the
explicit wire schema; never rely on JSON schema inference. Ignore the
quarantine prefix by selecting only the valid dataset URI.

Flatten one row per observation and preserve at least:

- envelope event/schema/type/occurred/produced/marketplace/partition key;
- crawl run and raw URI;
- all offer attributes;
- all observation values;
- raw checksum, adapter version and fetched/observed times;
- UTC `observed_date`.

Recheck structural lineage and non-negative numeric fields before transforms.
Invalid inputs are reported as a Phase 6 run failure; Phase 7 later persists
the complete quality result set. Do not silently drop them.

Deduplicate by `observation_id`. When physically duplicated rows are identical,
keep one. When the same ID has different canonical content, fail with
`ConflictingObservationError` rather than choosing an arbitrary row. Do this by
hashing a canonical projection of all semantic/lineage columns and requiring
one distinct hash per observation ID.

All per-offer windows use this total order:

```text
(observed_at ascending, fetched_at ascending, observation_id ascending)
```

## 7. Fixed dataset grains and columns

All dates are derived in UTC. All money uses Spark `DecimalType(38, 6)` and
PostgreSQL `NUMERIC(38, 6)`. Ratios may use `DOUBLE PRECISION` only after their
numerator/denominator meanings are fixed.

### 7.1 `offer_current`

Grain: exactly one row per `offer_id`.

Select the maximum ordering tuple and retain:

```text
offer_id, marketplace, marketplace_id, platform_listing_id, seller_id,
product_title, brand, category_path, source_url, currency, active_status,
first_seen_at, last_seen_at,
current_observation_id, observed_at, fetched_at,
current_price, list_price, shipping_price,
rating_value, rating_scale, rating_count, review_count, sold_count,
availability, ranking_position,
raw_uri, raw_sha256, adapter_version, crawl_run_id
```

`first_seen_at` is the minimum observed offer first-seen timestamp; last seen
and mutable descriptive attributes come from the latest ordered observation.
No arbitrary `first()` aggregation without ordering is allowed.

### 7.2 `seller_current`

Grain: one row per non-null `seller_id`.

Columns:

```text
seller_id, marketplace, marketplace_id,
first_seen_at, last_seen_at, observed_offer_count
```

This is explicitly an **observed seller reference**, not a complete seller
dimension. Do not add fake name, URL, rating or official-status defaults.

### 7.3 `offer_price_history_daily`

Grain: `(marketplace, offer_id, observed_date, currency)`.

Columns:

```text
marketplace, offer_id, observed_date, currency,
first_observed_at, last_observed_at,
first_price, last_price, min_price, max_price, avg_price,
first_list_price, last_list_price,
observation_count, distinct_price_count,
last_availability, last_observation_id
```

First/last values use the total order, not unordered `first()`/`last()`.
`observation_count` counts deduplicated observations.

### 7.4 `offer_change_daily`

Grain: `(marketplace, offer_id, observed_date, currency)`.

Recompute transitions from consecutive Silver observations; do not read Phase
5 change output. Columns:

```text
marketplace, offer_id, observed_date, currency,
transition_count, price_change_count, price_drop_count, price_increase_count,
absolute_price_change_sum, signed_price_change_sum,
max_price_drop, max_price_increase,
rating_change_count, counter_change_count, availability_change_count
```

The first observation has no transition and does not count as a price change.
Null comparisons use null-safe equality. Availability changes count only
between two non-`UNKNOWN` values, matching Phase 5 semantics.

### 7.5 `offer_freshness`

Grain: one row per `offer_id` at `context.as_of`.

Columns:

```text
as_of, marketplace, offer_id, last_observation_id, last_observed_at,
age_seconds, freshness_status, stale_after_seconds,
freshness_rule_version
```

Statuses are exactly:

```text
FRESH  when 0 <= age_seconds <= threshold
STALE  when age_seconds > threshold
FUTURE when age_seconds < 0
```

Do not clamp future times or call them fresh. Phase 7 quality gates decide the
allowed future tolerance.

### 7.6 `category_price_daily`

Grain:
`(marketplace, category_path, observed_date, currency)`.

Columns:

```text
marketplace, category_path, observed_date, currency,
observed_offer_count, observation_count,
min_price, p25_price, median_price, p75_price, max_price, avg_price
```

Use exact source `category_path`; null becomes a documented `__UNKNOWN__`
bucket only in this aggregate. Never claim category equivalence between sources
and never combine currencies. Compute quartiles with Spark
`percentile_approx(..., accuracy=10000)` from the configured positive accuracy;
record this approximation in dataset/dashboard descriptions.

### 7.7 `source_coverage_daily`

Grain: `(marketplace, observed_date)`.

Columns:

```text
marketplace, observed_date,
eligible_offer_count, observed_offer_count, missing_offer_count,
fresh_offer_count, stale_offer_count,
observation_count, parsed_count, rejected_count,
coverage_rate, rejection_rate,
freshness_rule_version
```

Fixed definitions:

- an offer is eligible on a date from its first observed date through the
  lesser of `as_of` date and input maximum date, unless latest known
  `active_status` before that day is `INACTIVE`;
- observed means at least one deduplicated observation on that UTC date;
- missing means eligible minus observed, never “source has no product”;
- fresh/stale at day end use the most recent observation at or before that day
  and the fixed freshness threshold; for the current partial UTC day, evaluate
  at `min(day_end, context.as_of)` rather than a future day-end;
- parsed/rejected come from Phase 3 audit and must reconcile there; they are not
  inferred from Silver row counts;
- `coverage_rate = observed_offer_count / eligible_offer_count`;
- `rejection_rate = rejected_count / (parsed_count + rejected_count)`;
- rates return null when their denominator is zero, not fabricated zero.

Generate eligible dates with a bounded Spark `sequence` from first-seen date to
the batch end date. Do not unbounded-cross-join every offer to calendar history.

### 7.8 `crawl_reliability_daily`

Grain: `(marketplace, request_date)`.

Columns:

```text
marketplace, request_date,
request_count, succeeded_count, failed_count,
success_rate, avg_latency_ms, p95_latency_ms,
raw_bytes, parsed_count, rejected_count,
rate_limited_count, transport_error_count,
server_error_count, parse_error_count, validation_error_count
```

Use Phase 2/3 accepted status/failure enums. `SUCCEEDED` and `PARTIAL` are
successful requests for transport/reliability purposes; `FAILED` is failed,
while PARTIAL rejected rows remain visible in the rejection metrics. Join
attempts to crawl runs/tasks by their accepted keys to obtain marketplace.
Requests without one of these terminal statuses remain in `request_count` but
are neither silently successful nor silently failed.

### 7.9 `counter_delta_daily`

Grain:
`(marketplace, offer_id, observed_date, counter_name)` where counter name is one
of `rating_count`, `review_count`, `sold_count` and is enabled by the semantics
registry.

Columns:

```text
marketplace, offer_id, observed_date, counter_name,
first_value, last_value, raw_delta_sum, valid_delta_sum,
valid_transition_count, invalid_transition_count,
elapsed_seconds_valid, velocity_proxy_per_hour,
counter_reset_or_invalid, invalid_reasons_json,
counter_rule_version
```

`raw_delta_sum` retains signed deltas including negatives. `valid_delta_sum`
includes only valid transitions. `invalid_reasons_json` is a sorted canonical
JSON array encoded as text so Spark JDBC and PostgreSQL preserve it without an
array-driver dependency. Velocity is null when no valid positive elapsed time
exists.

## 8. Counter semantics registry and transition validity

Create `config/counter_semantics.py`:

```python
@dataclass(frozen=True)
class CounterSemantic:
    marketplace: str
    field_name: str
    enabled: bool
    monotonic_expected: bool
    semantic_version: str

def counter_semantics_for(marketplace: str) -> tuple[CounterSemantic, ...]: ...
```

Only explicitly registered marketplace/field pairs produce trusted deltas.
Unknown semantics may still be retained as invalid evidence, but must not enter
`valid_delta_sum` or velocity.

For each consecutive observation and counter, classify in this precedence:

```text
MISSING_VALUE             previous or current is null
SEMANTICS_UNREGISTERED     registry entry absent/disabled
SEMANTICS_VERSION_CHANGED  adapter version or semantic version changed
NON_POSITIVE_INTERVAL      elapsed seconds <= 0
GAP_TOO_LONG               elapsed seconds > configured maximum
COUNTER_DECREASED          monotonic expected and delta < 0
VALID                      otherwise
```

Never clamp a negative delta to zero. Preserve previous/current values, raw
delta, elapsed time and reason in an internal transition DataFrame before daily
aggregation. A valid velocity is:

```text
delta / elapsed_seconds * 3600
```

and must be named a public-counter velocity proxy, not sales velocity.

## 9. Public transform API

Create pure DataFrame transforms in `batch_layer/marketplace_marts.py`:

```python
def build_offer_current(observations: DataFrame) -> DataFrame: ...
def build_seller_current(observations: DataFrame) -> DataFrame: ...
def build_offer_price_history_daily(observations: DataFrame) -> DataFrame: ...
def build_offer_change_daily(observations: DataFrame) -> DataFrame: ...
def build_offer_freshness(
    observations: DataFrame, *, as_of: datetime,
    stale_after_seconds: int, rule_version: str,
) -> DataFrame: ...
def build_category_price_daily(observations: DataFrame) -> DataFrame: ...
def build_source_coverage_daily(
    observations: DataFrame, attempts: DataFrame, runs: DataFrame, *,
    as_of: datetime, stale_after_seconds: int, rule_version: str,
) -> DataFrame: ...
def build_crawl_reliability_daily(
    attempts: DataFrame, runs: DataFrame,
) -> DataFrame: ...
def build_counter_transitions(
    observations: DataFrame, *, max_gap_seconds: int, rule_version: str,
) -> DataFrame: ...
def build_counter_delta_daily(transitions: DataFrame) -> DataFrame: ...
def build_marketplace_marts(
    observations: DataFrame, attempts: DataFrame, runs: DataFrame,
    context: MarketplaceBatchContext,
) -> dict[str, DataFrame]: ...
```

The returned dictionary keys exactly match the nine dataset names in Section
7. No function reads storage, opens a database, starts Spark or uses global
clock state.

Cache shared ordered observations/transitions once in orchestration and uncache
in `finally`; avoid recomputing all windows once per mart.

## 10. Run-scoped Gold write

Implement:

```python
def write_run_scoped_gold(
    marts: Mapping[str, DataFrame],
    context: MarketplaceBatchContext,
) -> dict[str, GoldWriteResult]: ...
```

Path layout:

```text
gold/marketplace/runs/run_id=<url-escaped-run-id>/offer_current/
gold/marketplace/runs/run_id=<url-escaped-run-id>/seller_current/
gold/marketplace/runs/run_id=<url-escaped-run-id>/<mart-name>/
```

Write Parquet with explicit schemas. Partition daily datasets by
`marketplace` and date; do not partition small current projections into tiny
files by offer. Record URI and row count per dataset in the batch run audit.

Rerunning the same `run_id` uses overwrite on that exact run-scoped directory
and produces the same logical rows. Never overwrite another run directory.

Phase 6 deliberately does **not** create or advance a `current` manifest. A
Phase 6 run-scoped output is inspectable data, not the accepted published Gold
version. Phase 7 adds quality results and promotes a manifest only after all
mandatory checks pass.

## 11. PostgreSQL cache schema

Append non-destructive DDL for these exact cache tables:

```text
cache.marketplace_offer_current
cache.marketplace_seller_current
cache.marketplace_offer_price_history_daily
cache.marketplace_offer_change_daily
cache.marketplace_offer_freshness
cache.marketplace_category_price_daily
cache.marketplace_source_coverage_daily
cache.marketplace_crawl_reliability_daily
cache.marketplace_counter_delta_daily
```

Use the grains in Section 7 as primary keys. Important types:

- UTC instants: `TIMESTAMPTZ`;
- dates: `DATE`;
- money/rating: `NUMERIC(38, 6)`;
- counts: `BIGINT`;
- ratios/percentiles/velocity: `DOUBLE PRECISION`;
- canonical invalid-reason JSON: `TEXT`;
- source/raw URIs: `TEXT`;
- IDs: `VARCHAR(128)` unless an accepted upstream size is smaller.

Add:

```sql
CREATE TABLE IF NOT EXISTS audit.marketplace_batch_run (
    run_id             VARCHAR(64) PRIMARY KEY,
    as_of              TIMESTAMPTZ NOT NULL,
    silver_uri         TEXT NOT NULL,
    gold_run_uri       TEXT,
    started_at         TIMESTAMPTZ NOT NULL,
    completed_at       TIMESTAMPTZ,
    status             VARCHAR(16) NOT NULL,
    silver_rows        BIGINT,
    gold_rows          BIGINT,
    cache_published    BOOLEAN NOT NULL DEFAULT FALSE,
    dataset_counts     JSONB,
    error_message      TEXT
);

CREATE TABLE IF NOT EXISTS audit.marketplace_cache_version (
    singleton          BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    run_id             VARCHAR(64) NOT NULL,
    published_at       TIMESTAMPTZ NOT NULL,
    dataset_counts     JSONB NOT NULL
);
```

Batch statuses are exactly `RUNNING`, `GOLD_WRITTEN`, `SUCCEEDED`, `FAILED`.
Counts are non-negative; completion fields must match terminal status.
`GOLD_WRITTEN` is terminal only for an intentional `--skip-postgres` run and
must set `completed_at` while leaving `cache_published = FALSE`. Store no raw
event body, secret or stack trace.

`batch_layer/marketplace_postgres.py` also exposes the audit gateway used by
orchestration:

```python
class MarketplaceBatchAudit:
    def __init__(self, connection_factory: Callable[[], Any]): ...
    def start_run(self, context: MarketplaceBatchContext, started_at: datetime) -> None: ...
    def mark_gold_written(
        self, *, run_id: str, gold_run_uri: str,
        silver_rows: int, dataset_counts: Mapping[str, int],
        completed_at: datetime | None,
    ) -> None: ...
    def mark_failed(
        self, *, run_id: str, completed_at: datetime, error: Exception,
    ) -> None: ...
```

`completed_at` is non-null in `mark_gold_written()` only for
`--skip-postgres`; otherwise publication completes the run inside the cache
transaction. Starting an already SUCCEEDED run ID fails. Re-entering a
RUNNING/FAILED run is allowed only through an explicit CLI `--resume` flag and
must preserve the same context values.

## 12. Transactional PostgreSQL publisher

Create `batch_layer/marketplace_postgres.py`:

```python
@dataclass(frozen=True)
class StagedDataset:
    dataset_name: str
    staging_table: str
    row_count: int

class MarketplaceCachePublisher:
    def __init__(self, connection_factory: Callable[[], Any]): ...
    def stage(
        self, marts: Mapping[str, DataFrame], *, run_id: str,
    ) -> tuple[StagedDataset, ...]: ...
    def publish(
        self, staged: Sequence[StagedDataset], *,
        run_id: str, published_at: datetime,
    ) -> None: ...
    def cleanup(self, staged: Sequence[StagedDataset]) -> None: ...
```

### 12.1 Run-scoped staging

Each Spark DataFrame writes through JDBC to a unique table:

```text
staging.mp_<dataset>_<run_token>
```

Derive `run_token` from SHA-256 of run ID, not raw user text. Validate dataset
names against the fixed allowlist. Staging table identifiers use psycopg SQL
identifier composition; values use parameters.

Stage every mart before touching cache. Count the staged SQL table and require
it equals the Spark-recorded count. Missing/extra columns, duplicate primary
keys or count mismatch fail before publish.

### 12.2 One atomic cache transaction

In one PostgreSQL transaction:

1. acquire a transaction advisory lock for marketplace cache publication;
2. revalidate all nine staging tables and counts;
3. truncate all nine cache tables;
4. insert each staging table into its exact cache target with explicit column
   lists;
5. upsert the singleton cache-version row with run ID/time/counts;
6. mark the batch run SUCCEEDED and `cache_published = TRUE`;
7. commit.

On any failure, roll back. Consumers see either the complete prior cache or all
nine new datasets. Cleanup run-scoped staging tables after success; cleanup is
best-effort after rollback so publish failure cannot mask the original error.

Never use Spark `overwrite` directly on `cache.*`. Never truncate cache before
all staging data exists. Phase 7 will add a quality-gate prerequisite to this
publisher; Phase 6 does not claim that gate yet.

## 13. Batch orchestration and CLI

Expose:

```python
def run_marketplace_warehouse(
    context: MarketplaceBatchContext,
    *,
    publish_cache: bool = True,
) -> MarketplaceBatchResult: ...
```

Algorithm:

1. record RUNNING batch audit;
2. read/flatten/validate/deduplicate Silver;
3. read Phase 3 audit tables;
4. build/cache shared temporal intermediates and all nine marts;
5. run structural checks needed for deterministic primary keys;
6. write run-scoped Gold Parquet;
7. mark audit GOLD_WRITTEN with URIs/counts;
8. when enabled, stage all marts and transactionally refresh cache;
9. otherwise finish GOLD_WRITTEN with `cache_published = FALSE`;
10. unpersist frames and stop owned Spark resources in `finally`;
11. on error, record FAILED best-effort and re-raise for non-zero CLI exit.

CLI:

```powershell
python -m batch_layer.marketplace_warehouse `
  --run-id <id> `
  --as-of <timezone-aware-iso> `
  [--silver-uri <uri>] `
  [--resume] `
  [--skip-postgres]
```

`--as-of` is required for reproducibility. `scripts/run_warehouse.ps1` may add a
new explicit `-Marketplace` switch; existing behavior job invocation remains
the default and must not change.

## 14. Superset batch draft

Register the nine cache datasets in `display/superset/datasources.yaml` and
create `display/superset/create_marketplace_batch_dashboard.py` using current
provisioning conventions.

Minimum charts:

- observed/eligible offer coverage by source/day;
- fresh versus stale offers at selected `as_of`;
- daily first/last/min/max price for a selected offer;
- category/source price distribution, always showing currency;
- price-change count and magnitude;
- crawl success rate, p95 latency and rejected rows;
- invalid public-counter transition rate;
- seller/offer current detail table.

Labels must say:

- “observed offer”, not marketplace inventory;
- “missing from scheduled observation coverage”, not unavailable product;
- “public counter delta/velocity proxy”, not sales/demand;
- “price change”, not anomaly.

Do not add anomaly panels until Phase 7 and do not add source comparison without
matched variants.

## 15. Required tests

Default tests use a small frozen Silver JSONL fixture, a local Spark session,
fake DB connections and no network/service containers.

### Input/current-state tests

1. reader uses explicit schema and valid dataset path only;
2. flatten preserves all raw lineage and Decimal fields;
3. identical duplicate observation IDs collapse to one;
4. conflicting duplicate ID fails loudly;
5. ordering is stable for equal observed timestamps;
6. current offer chooses latest ordered observation;
7. current offer first/last seen values are deterministic;
8. current offer has exactly one row per offer;
9. seller projection excludes null seller IDs and invents no attributes.

### Temporal mart tests

10. price daily first/last/min/max/count values are exact;
11. unordered input yields the same price mart;
12. first observation is not a transition;
13. increases/drops/change magnitudes are exact Decimal values;
14. null-safe rating/counter comparisons are correct;
15. ambiguous availability does not count as a change;
16. freshness boundary is inclusive for FRESH and over-boundary is STALE;
17. future observation is FUTURE, not clamped;
18. category mart separates marketplace, category and currency;
19. category null uses only the documented aggregate bucket;
20. source coverage eligible/observed/missing counts reconcile;
21. zero denominator rates are null;
22. parsed/rejected counts come from audit, not inferred Silver counts;
23. crawl reliability status/error/latency aggregation is exact.

### Counter tests

24. registry rejects unknown fields/duplicate entries;
25. null counter creates MISSING_VALUE evidence;
26. unregistered semantics cannot enter valid delta;
27. adapter/semantic version change is invalid;
28. non-positive interval and excessive gap are invalid;
29. negative monotonic delta is retained and flagged, never clamped;
30. valid delta and per-hour velocity are exact;
31. daily invalid-reason JSON is sorted/deterministic;
32. the mart never names a counter proxy as sale/demand.

### Gold/publisher/orchestration tests

33. all nine expected mart keys and explicit schemas exist;
34. Gold paths contain escaped run ID and never overwrite another run;
35. rerunning same run ID produces identical logical rows;
36. stage table names derive from safe hash token and fixed allowlist;
37. all staging writes finish before cache mutation;
38. staged/Spark count mismatch blocks publish;
39. duplicate cache primary key blocks publish;
40. publisher uses one transaction and explicit columns;
41. a failure on any table rolls back every cache change;
42. cache version updates in the same transaction;
43. skip-Postgres still writes run-scoped Gold;
44. SUCCEEDED run ID cannot restart and resume requires identical context;
45. orchestration records FAILED and propagates error;
46. no default test opens MinIO/PostgreSQL/network;
47. legacy warehouse transformation tests remain unchanged and pass.

Optional tests marked `integration` may write local Parquet and publish to a
temporary PostgreSQL database only when `TEST_POSTGRES_URL` exists. Add one
rollback test proving a forced insert failure preserves the previous cache
version.

## 16. Verification commands

Run after each work package, then:

```powershell
python -m pytest `
  tests/test_counter_semantics.py `
  tests/test_marketplace_marts.py `
  tests/test_marketplace_postgres.py `
  tests/test_marketplace_warehouse.py `
  tests/test_marketplace_change_rules.py `
  tests/test_marketplace_silver_sink.py `
  tests/test_crawl_worker.py `
  tests/test_warehouse_transform.py `
  -q
python -m pytest tests -q
python -m batch_layer.marketplace_warehouse --help
git diff --check
git diff --stat
```

> **Superseded by Phase 8 (2026-10-04).** This step used to ask for an offline
> local-file smoke with the frozen Silver fixture and `--skip-postgres`. That
> smoke cannot run: `read_crawl_audit()` is called unconditionally
> (`marketplace_warehouse.py`), because the coverage and reliability marts need
> crawl evidence over JDBC, and `--skip-postgres` only skips the *publish*.
> Use **`.\scripts\mp.ps1 smoke`** instead — it runs the whole slice on a real
> stack with `ops/stub_source.py` standing in for the marketplace, so nothing
> contacts a live site. Verify there that all nine run-scoped Parquet datasets
> exist and that their reported counts equal Spark reads. Giving the two marts
> a way to read the audit from the lake would make a PostgreSQL-free smoke
> possible again; nobody has needed it.

## 17. Commit/work-package sequence

1. `feat: add marketplace silver batch reader`
   - context, explicit reader, flatten/dedup and tests only.
2. `feat: build marketplace current and price marts`
   - offer/seller current, price daily, category daily and focused tests.
3. `feat: build marketplace temporal change and freshness marts`
   - offer changes/freshness and focused tests.
4. `feat: add validity-aware public counter deltas`
   - semantics registry, transitions/mart and tests only.
5. `feat: add marketplace coverage and reliability marts`
   - audit read, coverage/reliability and focused tests.
6. `feat: write run-scoped marketplace gold`
   - Parquet writer, orchestration audit and local smoke only.
7. `feat: add atomic marketplace cache publish`
   - DDL, run-scoped staging, publisher and rollback tests.
8. `feat: add marketplace superset batch draft`
   - dataset registration/dashboard only.
9. `test: verify temporal warehouse replay and compatibility`
   - full Phase 6 matrix and compatibility fixes only.

The model must stop after each package and show focused tests plus
`git diff --stat`. Do not implement Phase 7 inside these commits.

## 18. Definition of Done

- [ ] dependency gate and exact accepted Silver/audit schemas are recorded;
- [ ] Silver is the only analytical observation-history source;
- [ ] conflicting duplicate observation identities fail loudly;
- [ ] total per-offer ordering is deterministic;
- [ ] current offer has at most one row per offer;
- [ ] seller projection contains no invented attributes;
- [ ] all nine mart grains and primary keys match Section 7;
- [ ] money remains Decimal/NUMERIC through Spark and PostgreSQL;
- [ ] price daily first/last/min/max/count reconcile with Silver fixture;
- [ ] freshness uses caller-supplied as-of and explicit FUTURE status;
- [ ] coverage definitions distinguish eligible, observed and missing;
- [ ] crawl reliability uses persistent Phase 3 audit evidence;
- [ ] counter negatives/gaps/semantic changes remain visible and invalid;
- [ ] no counter proxy is described as a real sale or demand;
- [ ] Gold output is run-scoped and does not advance a current manifest;
- [ ] all nine cache tables refresh in one transaction;
- [ ] failed publication preserves the prior complete cache version;
- [ ] Superset labels reflect observational limitations;
- [ ] legacy behavioral warehouse remains runnable;
- [ ] focused tests pass without external services;
- [ ] full tests pass or environment failures are documented.

## 19. Mandatory rejection conditions

Reject the implementation if it:

- reads Redis, Elasticsearch or the change topic as batch history;
- infers schemas or silently drops malformed/conflicting Silver rows;
- uses unordered `first()`/`last()` for current/daily values;
- converts money to float before persistence;
- combines currencies or raw categories as comparable markets;
- fabricates missing seller attributes;
- calls missing daily observation a delisted/unavailable offer;
- clamps negative counter deltas to zero;
- treats public counters as real orders, sales or demand;
- uses runtime `now()` independently in multiple transforms;
- writes directly over a stable Gold/current path;
- advances a Gold manifest before Phase 7 quality gates;
- truncates any cache table before all nine staging tables validate;
- commits a partial multi-table cache refresh;
- starts anomaly, product matching or orchestration work early;
- breaks the legacy behavioral warehouse/tests.

## 20. Copy-ready prompts for a low-capability model

### Prompt A — reader and dedup

```text
Implement only Sections 5–6 of
docs/PHASE_6_TEMPORAL_WAREHOUSE_IMPLEMENTATION_PLAN.md. Read the accepted Phase
4 schema/layout first. Create the marketplace warehouse context, explicit
reader, flatten/dedup and frozen fixture tests. Preserve Decimal and raw
lineage; fail conflicting IDs. Do not build marts or touch PostgreSQL. Run
focused tests, show diff stat and stop.
```

### Prompt B — current and price marts

```text
Implement only Sections 7.1–7.3, 7.6 and corresponding APIs/tests from the
Phase 6 plan. Use deterministic ordered windows and exact grains. Do not use
unordered first/last, invent seller fields, combine currencies or build other
marts. Run focused tests and stop.
```

### Prompt C — changes and freshness

```text
Implement only Sections 7.4–7.5 and focused tests from the Phase 6 plan.
Recompute from consecutive Silver observations, use caller-supplied as_of and
the exact FRESH/STALE/FUTURE boundary. Do not read Phase 5 outputs or implement
anomaly rules. Run focused tests and stop.
```

### Prompt D — counters

```text
Implement Sections 7.9 and 8 plus counter tests from the Phase 6 plan. Create
the explicit semantics registry and validity precedence. Retain negative raw
deltas, exclude invalid transitions from valid sums/velocity and never use
sales/demand wording. Run focused tests and stop.
```

### Prompt E — coverage and reliability

```text
Implement Sections 7.7–7.8 from the Phase 6 plan. Read the accepted Phase 3
audit schema first and fail on mismatch. Use bounded eligible dates and exact
missing/fresh/stale definitions; obtain parsed/rejected from audit. Run focused
tests and stop.
```

### Prompt F — run-scoped Gold

```text
Implement Sections 9–10 and orchestration portions that end at GOLD_WRITTEN.
Write all nine explicit Parquet datasets below one run-scoped path and add a
local-file smoke. Do not create/advance a current manifest and do not publish
PostgreSQL. Run focused tests, show diff stat and stop.
```

### Prompt G — PostgreSQL publisher

```text
Implement Sections 11–13 PostgreSQL portions only. Add exact cache/audit DDL,
safe run-scoped staging and one transaction for all nine cache tables plus
version. Use fake DB and optional rollback integration tests. Never overwrite
cache from Spark directly. Run focused tests and stop.
```

### Prompt H — Superset and final verification

```text
Implement Section 14 and final Phase 6 verification. Register the exact cache
datasets and charts with observational labels. Run the full matrix and offline
Gold smoke, document environment-only skips, show diff stat and stop. Do not
add anomaly panels, manifest promotion or Phase 7 quality gates.
```

## 21. Handoff to Phase 7

Phase 7 adds the mandatory quality checks from the parent plan, robust rolling
median/MAD/IQR price anomaly, persisted quality results, raw reparse/replay and
the Gold publish manifest. It must gate both manifest promotion and the Phase 6
cache publisher: on quality failure, the new run remains inspectable under its
run-scoped path while the last good Gold manifest and PostgreSQL cache remain
unchanged.
