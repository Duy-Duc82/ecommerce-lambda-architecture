# Phase 6 implementation plan — Batch temporal warehouse and Gold marts

> Status: ready for implementation after Phases 4 and 5 are accepted
>
> Intended implementer: a low-capability coding model working one work package
> at a time. It must follow the fixed contracts below and stop between packages.
>
> Parent plan: `Brief để xây dựng plan sơ bộ cho việc cải tiến ecommerce-lambda-architecture.md`

## 1. Objective

Implement the parent plan's Week 6 vertical slice and backlog P1-09 through
P1-12:

```text
silver/marketplace/offer_observations (canonical JSON envelopes)
-> deduplicated, typed, compacted Silver
-> silver/offers + silver/sellers dimensions
-> run-scoped Gold temporal marts
-> transactional PostgreSQL BI cache
-> Superset batch dashboard draft
```

The batch layer answers a different question from Phase 5: **what does the full
history say, computed correctly rather than quickly?** It recomputes price
movement, freshness and coverage from every observation, so the batch view can
correct anything the speed layer approximated or missed while a sink was down.
That correction is the entire point of the Lambda architecture here, and it is
why Phase 6 must never read Phase 5 output as an input.

## 2. Mandatory dependency gate

Before editing, record and verify:

1. Phase 4 lands canonical observation envelopes at the documented Silver path
   and exposes `MARKETPLACE_OBSERVATION_WIRE_SCHEMA` and
   `MARKETPLACE_SILVER_SCHEMA`.
2. Phase 4 quarantine objects exist at the documented quarantine path, so
   rejected rows can be counted rather than guessed.
3. Phase 3 audit tables `audit.crawl_run` and `audit.crawl_request_attempt`
   exist and hold at least one recorded run.
4. Phase 1 contracts and identity helpers pass.
5. `config/settings.py::data_lake_uri()` resolves `silver` and `gold` zones for
   the active storage profile.

If any dependency is missing, stop. If Phase 3 audit is unavailable at run
time, the crawl-reliability mart must be **skipped with a recorded reason**, not
fabricated from Silver.

Phase 5 is a dependency of the phase order, not of the data flow. Phase 6 must
not read Redis, Elasticsearch or `marketplace.changes.v1`.

## 3. Scope

### 3.1 In scope

- A Silver reader that decodes the nested envelopes with the frozen wire
  schema, flattens to `MARKETPLACE_SILVER_SCHEMA`, deduplicates by
  `observation_id` and compacts to Parquet.
- `silver/offers` and `silver/sellers` dimensions resolved from observation
  history with first/last-seen semantics.
- Pure temporal rules: freshness classification, counter-delta validity,
  representative daily price selection, price-movement classification.
- Eight run-scoped Gold marts (Section 7).
- Three structural publish assertions.
- A `marketplace_gold` PostgreSQL cache with staging tables and a single
  transactional swap.
- A Superset dashboard draft over the cache.
- A recorded Gold run row and reconciliation results.
- Offline unit tests for every pure rule and for the publish SQL sequence.

### 3.2 Out of scope

- Robust price anomaly detection: rolling median, MAD, IQR — Phase 7.
- The full mandatory data-quality gate battery — Phase 7.
- The Gold publish manifest and version promotion — Phase 7.
- The raw reparse/replay workflow — Phase 7.
- `price_anomaly_daily`, `variant_market_daily`,
  `cross_market_offer_comparison`.
- Product matching, variant resolution, a second marketplace.
- Any change to crawler, Bronze, parser, retry, Kafka topics, Silver landing
  format or Phase 5 change detection.
- Machine learning of any kind. The existing `batch_layer/analytics/*` and
  `ml_job.py` belong to the legacy behavioral pipeline and stay untouched.
- Replacing the legacy `batch_layer/warehouse_job.py` medallion pipeline.

Phase 6 may claim deterministic, re-runnable batch marts. It may not claim
validated or gate-approved Gold: that word belongs to Phase 7.

## 4. Allowed file changes

Create:

```text
batch_layer/marketplace_silver_reader.py
batch_layer/marketplace_rules.py
batch_layer/marketplace_gold_contracts.py
batch_layer/marketplace_gold_job.py
batch_layer/marketplace_postgres_cache.py
display/superset/create_marketplace_dashboard.py
tests/test_marketplace_rules.py
tests/test_marketplace_gold_contracts.py
tests/test_marketplace_silver_reader.py
tests/test_marketplace_gold_job.py
tests/test_marketplace_postgres_cache.py
```

Modify only:

```text
config/settings.py
scripts/init_postgres.sql
scripts/validate_warehouse.sql
```

`batch_layer/warehouse_job.py`, `batch_layer/ml_job.py`,
`batch_layer/postgres_cache.py`, `batch_layer/analytics/*` and
`serving_layer/postgres_views.py` are the legacy behavioral path. Do not edit
or delete them. `tests/test_warehouse_transform.py` and
`tests/test_postgres_cache.py` must keep passing unchanged, or their
pre-existing environment failures must be documented unchanged.

## 5. Configuration

Add to `config/settings.py`:

```python
MARKETPLACE_FRESHNESS_THRESHOLD_MINUTES = 360
MARKETPLACE_STALE_THRESHOLD_MINUTES = 1440
COUNTER_DELTA_MAX_GAP_MINUTES = 2880
GOLD_MIN_OBSERVATIONS_PER_DAY = 1
GOLD_DEFAULT_WINDOW_DAYS = 30
GOLD_QUANTILE_ACCURACY = 10000
GOLD_RULE_VERSION = "marketplace-gold-rules.v1"
POSTGRES_MARKETPLACE_SCHEMA = "marketplace_gold"
POSTGRES_MARKETPLACE_STAGING_SCHEMA = "marketplace_gold_staging"
MARKETPLACE_DECIMAL_PRECISION = 38
MARKETPLACE_DECIMAL_SCALE = 6
```

Keep every legacy setting unchanged.

## 6. Fixed architectural decisions

### 6.1 No floating point anywhere in a money path

- Spark: `DecimalType(MARKETPLACE_DECIMAL_PRECISION, MARKETPLACE_DECIMAL_SCALE)`.
- Python: `Decimal`.
- PostgreSQL: `NUMERIC(38, 6)`.

`DoubleType`, `FloatType`, `float()` and `DOUBLE PRECISION` are forbidden in
every price, list price, discount, rating and derived-magnitude column. The one
documented exception is `GOLD_QUANTILE_ACCURACY` in Section 6.6.

### 6.2 Reproducibility: the job has no clock

`run_marketplace_gold()` takes `as_of: datetime` and `window_days: int`. Every
freshness, staleness and gap computation uses `as_of`, never
`datetime.now()`, `current_timestamp()` or `F.now()` inside a transform.

Consequences that must hold: running the job twice with the same `as_of`,
`window_days` and Silver input produces byte-comparable mart contents and
identical row counts. A test asserts this.

Rerunning with a later `as_of` may legitimately change freshness columns. That
is why `as_of` is stored on the Gold run row and on every freshness mart row.

### 6.3 Silver is input and history; Gold is derived and disposable

- Phase 6 never rewrites, compacts in place, moves or deletes a Phase 4
  observation object. The compacted Parquet is written to a **new** dataset,
  `silver/marketplace/offer_observations_compacted`, and the JSON envelopes
  remain the landing truth.
- `silver/offers` and `silver/sellers` are derived dimensions and may be
  overwritten per run.
- Gold is written under run-scoped paths and can be deleted at any time without
  data loss.

### 6.4 Run-scoped Gold layout, no promotion yet

```text
gold/marketplace/<mart_name>/gold_run_id=<run_id>/
```

Phase 6 writes only run-scoped paths. There is no `latest` pointer, no symlink
and no manifest: publish promotion belongs to Phase 7, after the mandatory gate
battery exists. Writing a `latest` marker in this phase would advertise
unvalidated Gold as current.

The PostgreSQL cache is the only consumer-facing surface in this phase, and its
refresh is transactional (Section 12), so Superset never reads a half-written
mart.

### 6.5 Deduplication and ordering

- Deduplicate observations by `observation_id`. Two objects with one
  `observation_id` are the same observation by construction, since the ID
  derives from marketplace, listing, observed time and raw body hash.
- The survivor is chosen deterministically: order by
  `(observation_id, produced_at, raw_uri)` and keep the first. Never
  `dropDuplicates()` without an explicit order — its survivor is
  nondeterministic and would break Section 6.2.
- Per-offer temporal ordering is `(observed_at, observation_id)` everywhere. A
  tie on `observed_at` is broken by ID so windows are stable across runs.
- `observed_date` is the UTC calendar date of `observed_at`. Never local time.

### 6.6 Exact aggregates versus approximate quantiles

- Exact, and must reconcile with Silver: row counts, observation counts,
  distinct offer counts, `first`/`last`/`min`/`max` price.
- Approximate, and must declare itself: median, p25 and p75 in
  `category_price_daily`, computed with `percentile_approx` at
  `GOLD_QUANTILE_ACCURACY`.

Every mart row carrying a quantile also carries `quantile_method` and
`quantile_accuracy` columns. A distribution panel that cannot say how its
median was computed is not evidence.

### 6.7 Representative daily price

`offer_price_history_daily` stores `price_open`, `price_close`, `price_min`,
`price_max` and `observation_count`. The representative price is
`price_close`: the last observation of that UTC day by
`(observed_at, observation_id)`.

This is a documented choice, not an average. A mean of unevenly spaced crawl
observations weights crawl scheduling, not market behavior, so no mean price
column is produced in this phase.

### 6.8 Freshness and staleness, never silent forward-fill

Three statuses, from `as_of` minus `last_observation_at`:

| Status | Condition |
|---|---|
| `FRESH` | age <= `MARKETPLACE_FRESHNESS_THRESHOLD_MINUTES` |
| `AGING` | age <= `MARKETPLACE_STALE_THRESHOLD_MINUTES` |
| `STALE` | age > `MARKETPLACE_STALE_THRESHOLD_MINUTES` |

`offer_current` carries the last observed values **plus** `freshness_status`,
`age_minutes`, `as_of` and `rule_version`. A stale row is never dropped and
never presented as current without its flag. No column is forward-filled
across days: a day with no observation for an offer produces no
`offer_price_history_daily` row for that offer, and the absence is reported by
`source_coverage_daily`, not hidden by interpolation.

### 6.9 Public counter deltas

```text
observed_delta  = current_counter - previous_counter
elapsed_minutes = observed_at - previous_observed_at
velocity_proxy  = observed_delta / (elapsed_minutes / 60)   -- per hour
```

Validity rules, all mandatory:

- `previous_counter IS NULL` -> `counter_reset_or_invalid = true`, reason
  `NO_PREVIOUS`;
- `observed_delta < 0` -> `true`, reason `NEGATIVE_DELTA`;
- `elapsed_minutes > COUNTER_DELTA_MAX_GAP_MINUTES` -> `true`, reason
  `GAP_TOO_LONG`;
- `elapsed_minutes <= 0` -> `true`, reason `NON_POSITIVE_ELAPSED`.

A negative delta is stored with its true value and flagged. It is never clamped
to zero, never dropped and never summed into a total. `velocity_proxy` is
`NULL` whenever the row is invalid. Aggregations over counter deltas must
filter on `counter_reset_or_invalid = false` and must report how many rows they
excluded.

The column is `sold_count`. Nothing in this phase — column, table, view, chart
title or log line — may call it sales, orders, revenue or demand. It is a public
counter whose semantics the marketplace does not document.

### 6.10 Batch recomputes what the speed layer approximated

`offer_change_daily` is derived from consecutive Silver observations with a
window `lag`, not from `marketplace.changes.v1`. The rule thresholds are read
from the same settings Phase 5 uses, so the two views are comparable.

Any difference between the batch count and the Phase 5 count is a real
operational signal — a sink outage, a consumer gap, a rule-version skew — and
the dashboards may show both. Neither is corrected into the other, and the
batch number is the one used for analysis.

## 7. Gold marts

Exactly eight, all partitioned by `gold_run_id`:

| Mart | Grain | Purpose |
|---|---|---|
| `offer_current` | one row per `offer_id` | latest observed state with freshness flags |
| `offer_price_history_daily` | `offer_id` x `observed_date` | open/close/min/max price, observation count |
| `offer_change_daily` | `offer_id` x `observed_date` | change counts and magnitudes recomputed in batch |
| `offer_freshness` | one row per `offer_id` | last observation, age, status, rule version |
| `category_price_daily` | `marketplace` x `category_path` x `observed_date` | offer count, min/max, declared quantiles |
| `source_coverage_daily` | `marketplace` x `observed_date` | offers observed, missing, stale, rejected |
| `crawl_reliability_daily` | `marketplace` x `observed_date` | requests, success rate, latency, error mix |
| `counter_delta_daily` | `offer_id` x `observed_date` | observed deltas with validity flags |

Every mart carries `gold_run_id`, `as_of` and `rule_version`. `offer_current`
and `offer_freshness` are point-in-time snapshots; the six daily marts are
recomputed for the whole `window_days` window on each run, so a late-arriving
observation corrects the affected day rather than only today.

## 8. Work package GOLD-01 — Silver reader

### Files

```text
batch_layer/marketplace_silver_reader.py
tests/test_marketplace_silver_reader.py
```

### 8.1 Required API

```python
SILVER_OBSERVATIONS_DATASET = "marketplace/offer_observations"
SILVER_OBSERVATIONS_COMPACTED = "marketplace/offer_observations_compacted"
SILVER_QUARANTINE_DATASET = "quarantine/offer_observations"


def observation_partition_paths(
    *,
    marketplaces: Sequence[str],
    start_date: date,
    end_date: date,
) -> tuple[str, ...]: ...


def flatten_observations(nested_df) -> "DataFrame": ...


def deduplicate_observations(flat_df) -> "DataFrame": ...


def read_silver_observations(
    spark,
    *,
    marketplaces: Sequence[str],
    start_date: date,
    end_date: date,
) -> "DataFrame": ...


def compact_observations(flat_df, *, gold_run_id: str) -> int: ...


def count_quarantined(spark, *, start_date: date, end_date: date) -> int: ...
```

Rules:

- read with `spark.read.schema(MARKETPLACE_OBSERVATION_WIRE_SCHEMA).json(paths)`.
  Never infer a schema: inference would sample files, guess money as double and
  change types between runs.
- build explicit partition path globs from `marketplace=` and `observed_date=`
  rather than scanning the dataset root. Phase 4 writes one small JSON object
  per observation, so an unpruned scan of the whole prefix is the predictable
  way to make this job unusable. State that in the module docstring.
- `flatten_observations()` produces exactly `MARKETPLACE_SILVER_SCHEMA`:
  wire strings cast to `TimestampType`, money strings cast to
  `DecimalType(38, 6)`, counts to `LongType`, `promotion` preserved as
  `promotion_json`, `observed_date` derived from UTC `observed_at`.
- a cast that would silently produce `NULL` from a non-null wire value is a
  failure, not a skip: assert that the non-null wire count equals the non-null
  typed count for every required column and raise with the column name.
- `read_silver_observations()` = read, flatten, deduplicate, in that order.
- `compact_observations()` writes Parquet to
  `SILVER_OBSERVATIONS_COMPACTED`, partitioned by `marketplace` and
  `observed_date`, with `mode("overwrite")` and
  `partitionOverwriteMode=dynamic` so only touched days are replaced.
- missing partitions are normal. A requested date with no objects contributes
  zero rows and is recorded, not an error.

### Exit criteria

- flatten output schema equals `MARKETPLACE_SILVER_SCHEMA` field for field;
- deduplication is deterministic;
- a silent-null cast raises with the column name;
- path globs are pruned by marketplace and date.

## 9. Work package GOLD-02 — Pure temporal rules

### Files

```text
batch_layer/marketplace_rules.py
tests/test_marketplace_rules.py
```

### 9.1 Required API

No Spark, Kafka, Redis, Elasticsearch or PostgreSQL import anywhere in this
module. Every function is a pure scalar or sequence function over `Decimal`,
`int`, `datetime` and `str`.

```python
class FreshnessStatus(str, Enum):
    FRESH = "FRESH"
    AGING = "AGING"
    STALE = "STALE"


class CounterValidity(str, Enum):
    VALID = "VALID"
    NO_PREVIOUS = "NO_PREVIOUS"
    NEGATIVE_DELTA = "NEGATIVE_DELTA"
    GAP_TOO_LONG = "GAP_TOO_LONG"
    NON_POSITIVE_ELAPSED = "NON_POSITIVE_ELAPSED"


@dataclass(frozen=True)
class FreshnessThresholds:
    fresh_after: timedelta
    stale_after: timedelta


@dataclass(frozen=True)
class CounterDeltaResult:
    observed_delta: int | None
    elapsed_minutes: int | None
    velocity_per_hour: Decimal | None
    validity: CounterValidity
    counter_reset_or_invalid: bool


@dataclass(frozen=True)
class PriceMovement:
    delta_absolute: Decimal | None
    delta_percent: Decimal | None
    is_price_change: bool
    is_large_drop: bool


def classify_freshness(
    *,
    last_observation_at: datetime,
    as_of: datetime,
    thresholds: FreshnessThresholds,
) -> tuple[FreshnessStatus, int]: ...


def counter_delta(
    *,
    previous_counter: int | None,
    current_counter: int | None,
    previous_observed_at: datetime | None,
    current_observed_at: datetime,
    max_gap: timedelta,
) -> CounterDeltaResult: ...


def price_movement(
    *,
    previous_price: Decimal | None,
    current_price: Decimal,
    large_drop_absolute: Decimal,
    large_drop_percent: Decimal,
) -> PriceMovement: ...


def daily_representative(
    observations: Sequence[tuple[datetime, str, Decimal]],
) -> tuple[Decimal, Decimal, Decimal, Decimal, int]: ...
```

Rules:

- `classify_freshness()` returns the status and the integer age in minutes,
  truncated toward zero. A future `last_observation_at` relative to `as_of`
  returns `FRESH` with age `0` and must not return a negative age.
- `counter_delta()` implements Section 6.9 exactly. It returns the true
  `observed_delta` even when invalid, and `velocity_per_hour = None` for every
  non-`VALID` result.
- `price_movement()` reuses the Phase 5 threshold semantics: percent branch
  skipped when `previous_price == 0`, drop measured as `previous - current`,
  and `is_large_drop` never true without `is_price_change`.
- `daily_representative()` returns `(open, close, min, max, count)` from
  observations ordered by `(observed_at, observation_id)`. It raises on an
  empty sequence rather than returning zeros.
- `Decimal` results are quantized to `MARKETPLACE_DECIMAL_SCALE` with
  `ROUND_HALF_UP`, so the same inputs always give the same stored value.

These rules are the phase's testable core. The Spark job composes them; it does
not restate them in SQL. Where a rule must run as a column expression for
performance, the Spark expression and the pure function must be covered by one
shared table of cases, and a test asserts they agree on every case.

### Exit criteria

- every enum member reachable;
- no forbidden import;
- Decimal quantization deterministic;
- Spark expression and pure rule agree on the shared case table.

## 10. Work package GOLD-03 — Gold contracts

### Files

```text
batch_layer/marketplace_gold_contracts.py
tests/test_marketplace_gold_contracts.py
```

### 10.1 Required API

```python
@dataclass(frozen=True)
class MartSpec:
    name: str
    grain: tuple[str, ...]
    schema: "StructType"
    partition_by: tuple[str, ...] = ()


OFFER_CURRENT: MartSpec
OFFER_PRICE_HISTORY_DAILY: MartSpec
OFFER_CHANGE_DAILY: MartSpec
OFFER_FRESHNESS: MartSpec
CATEGORY_PRICE_DAILY: MartSpec
SOURCE_COVERAGE_DAILY: MartSpec
CRAWL_RELIABILITY_DAILY: MartSpec
COUNTER_DELTA_DAILY: MartSpec

MARKETPLACE_MART_SPECS: tuple[MartSpec, ...]


def gold_dataset_path(mart: MartSpec, *, gold_run_id: str) -> str: ...

def make_gold_run_id(as_of: datetime, window_days: int, rule_version: str) -> str: ...
```

Rules:

- `MartSpec.schema` is declared explicitly. Do not derive a mart schema from
  whatever the transform happened to produce; the schema is the contract and
  the transform must satisfy it.
- validate at import: unique names, non-empty grain, grain columns present in
  the schema, every money column `DecimalType(38, 6)`, no `DoubleType` or
  `FloatType` anywhere, and `gold_run_id`, `as_of`, `rule_version` present in
  every schema.
- `make_gold_run_id()` uses `deterministic_id("goldrun", as_of, window_days,
  rule_version)`, so re-running the same logical window targets the same
  partition and overwrites instead of accumulating duplicate Gold.
- `gold_dataset_path()` builds `gold/marketplace/<name>/gold_run_id=<id>` via
  `data_lake_uri("gold", ...)`, with URL-escaped components.

### Exit criteria

- all eight specs validate;
- a `DoubleType` money column fails at import;
- run IDs deterministic;
- paths escaped and run-scoped.

## 11. Work package GOLD-04 — Dimensions, current state and marts

### Files

```text
batch_layer/marketplace_gold_job.py
tests/test_marketplace_gold_job.py
```

### 11.1 Dimensions

```python
def build_offer_dimension(observations) -> "DataFrame": ...
def build_seller_dimension(observations) -> "DataFrame": ...
```

- `silver/offers`: one row per `offer_id`. `first_seen_at` is the minimum
  `observed_at`, `last_seen_at` the maximum. Descriptive attributes
  (`product_title`, `brand`, `category_path`, `source_url`, `currency`) come
  from the **latest** observation by `(observed_at, observation_id)`, because a
  title or category correction should win over its older value.
- `silver/sellers`: one row per non-null `seller_id`, same first/last-seen
  rule. Observations with a null `seller_id` contribute to no seller row and
  are counted as `offers_without_seller` in coverage.
- Duplicate `(marketplace, platform_listing_id)` in the offer dimension is a
  structural failure (Section 13 assertion), not something to deduplicate away.
- Both dimensions are written under `silver/` with dynamic partition overwrite
  by `marketplace`.

### 11.2 Marts

```python
def build_offer_current(observations, *, as_of, thresholds) -> "DataFrame": ...
def build_offer_freshness(observations, *, as_of, thresholds) -> "DataFrame": ...
def build_price_history_daily(observations) -> "DataFrame": ...
def build_offer_change_daily(observations, *, thresholds) -> "DataFrame": ...
def build_category_price_daily(observations, *, accuracy) -> "DataFrame": ...
def build_counter_delta_daily(observations, *, max_gap) -> "DataFrame": ...
def build_source_coverage_daily(
    observations, *, quarantined, as_of, thresholds
) -> "DataFrame": ...
def build_crawl_reliability_daily(audit_df) -> "DataFrame": ...
```

Fixed details:

- all per-offer temporal columns use
  `Window.partitionBy("offer_id").orderBy("observed_at", "observation_id")`.
  A window without the ID tiebreak is nondeterministic on same-instant
  observations.
- `build_offer_change_daily()` counts, per offer per day:
  `price_changes`, `large_price_drops`, `availability_changes`,
  `counter_changes`, plus `max_abs_price_delta`, `avg_abs_price_delta` and
  `max_drop_percent`. Availability transitions involving `UNKNOWN` are excluded,
  matching Phase 5 Section 6.5, and the exclusion count is a column so the two
  views stay comparable.
- `build_counter_delta_daily()` aggregates only `VALID` rows into
  `total_valid_delta` and reports `invalid_rows` and a per-reason breakdown.
  Never `greatest(delta, 0)`.
- `build_source_coverage_daily()` computes `offers_observed`,
  `observations`, `offers_stale`, `offers_without_seller`, `rows_rejected`
  (from `count_quarantined()`) and `offers_missing`: offers observed in the
  preceding `GOLD_DEFAULT_WINDOW_DAYS` window but absent on that day. Missing
  is a count of absence, never an imputed row.
- `build_crawl_reliability_daily()` reads Phase 3
  `audit.crawl_request_attempt` through an injected loader returning a
  DataFrame, and produces `requests`, `succeeded`, `failed`, `success_rate`
  (`NUMERIC`, not float), `p50_latency_ms`, `p95_latency_ms` and a per-
  `FailureKind` error map. When the loader reports the audit source is
  unavailable, the mart is written empty with `skipped_reason` populated and the
  Gold run row records the skip.
- every builder ends by selecting its `MartSpec.schema` columns in schema
  order, so a drifted transform fails loudly at the contract boundary.

### 11.3 Orchestration

```python
@dataclass(frozen=True)
class GoldRunReport:
    gold_run_id: str
    as_of: datetime
    window_days: int
    rule_version: str
    started_at: datetime
    completed_at: datetime
    observations_read: int
    observations_deduplicated: int
    quarantined_rows: int
    mart_row_counts: dict[str, int]
    assertions: tuple["AssertionResult", ...]
    skipped_marts: dict[str, str]
    status: str            # "SUCCEEDED" | "FAILED"
    failure_stage: str | None


def run_marketplace_gold(
    *,
    as_of: datetime,
    window_days: int = GOLD_DEFAULT_WINDOW_DAYS,
    marketplaces: Sequence[str] | None = None,
    publish: bool = False,
    spark=None,
    audit_loader=None,
    clock: Callable[[], datetime],
) -> GoldRunReport: ...
```

- `pyspark` imports live inside functions, so the module imports and unit-tests
  without pyspark installed.
- stage order: read Silver -> compact -> dimensions -> marts -> structural
  assertions -> record the Gold run -> PostgreSQL publish only when
  `publish=True` and every assertion passed.
- a failure at any stage records a `FAILED` run row with the stage, then
  re-raises. It must not publish.

### Exit criteria

- every mart matches its declared schema;
- windows deterministic with the ID tiebreak;
- two identical runs produce identical row counts;
- module imports without pyspark.

## 12. Work package GOLD-05 — PostgreSQL cache

### Files

```text
batch_layer/marketplace_postgres_cache.py
scripts/init_postgres.sql
scripts/validate_warehouse.sql
tests/test_marketplace_postgres_cache.py
```

### 12.1 DDL

Two schemas, `marketplace_gold` and `marketplace_gold_staging`, with identical
table definitions for the eight marts, plus in `marketplace_gold`:

```sql
CREATE TABLE IF NOT EXISTS marketplace_gold.gold_run (
    gold_run_id       TEXT         PRIMARY KEY,
    as_of             TIMESTAMPTZ  NOT NULL,
    window_days       INTEGER      NOT NULL,
    rule_version      TEXT         NOT NULL,
    started_at        TIMESTAMPTZ  NOT NULL,
    completed_at      TIMESTAMPTZ,
    observations_read BIGINT       NOT NULL DEFAULT 0,
    quarantined_rows  BIGINT       NOT NULL DEFAULT 0,
    status            TEXT         NOT NULL,
    failure_stage     TEXT,
    skipped_marts     JSONB        NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS marketplace_gold.gold_assertion (
    gold_run_id     TEXT        NOT NULL REFERENCES marketplace_gold.gold_run(gold_run_id),
    assertion_name  TEXT        NOT NULL,
    observed_value  TEXT        NOT NULL,
    expectation     TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    rule_version    TEXT        NOT NULL,
    PRIMARY KEY (gold_run_id, assertion_name)
);
```

Money columns are `NUMERIC(38, 6)`. Counter columns are `BIGINT`. Validity
flags are `BOOLEAN NOT NULL`. Reasons and statuses are `TEXT` holding the
enum values from Section 9, never free text.

`scripts/init_postgres.sql` must remain idempotent and must not alter any
existing legacy table. `scripts/validate_warehouse.sql` gains read-only checks
for the new schema; it must not mutate anything.

### 12.2 Transactional publish

```python
def publish_marts(
    reports: Mapping[str, "DataFrame"],
    *,
    gold_run_id: str,
    connect: Callable[[], Any],
) -> dict[str, int]: ...
```

Exact sequence:

1. write each mart to its staging table, replacing staging content;
2. open one transaction;
3. for each target table: `DELETE FROM <target> WHERE gold_run_id = %s` for the
   current run, then `TRUNCATE`-free `INSERT INTO <target> SELECT * FROM
   <staging>`;
4. upsert the `gold_run` row and insert the assertion rows;
5. commit once.

Rules:

- exactly one commit. Readers must never observe a partially refreshed set of
  marts, which is the whole reason Superset points at these tables rather than
  at run-scoped object storage.
- on any error, roll back and re-raise. Never leave a half-published cache and
  never catch-and-log.
- no `autocommit`. No `TRUNCATE` of a target outside the transaction.
- the delete-then-insert per run makes a re-run of the same logical window
  idempotent instead of doubling rows.

### Exit criteria

- one transaction, one commit, rollback on error;
- re-running the same run ID does not duplicate rows;
- no float column;
- legacy DDL untouched.

## 13. Structural assertions

Three assertions, evaluated before publish. These are not the Phase 7 quality
gate battery; they are the minimum that catches a broken job.

```python
@dataclass(frozen=True)
class AssertionResult:
    name: str
    observed_value: str
    expectation: str
    passed: bool
```

1. `offer_current_unique`: `offer_current` has at most one row per `offer_id`.
2. `offer_dimension_unique`: `silver/offers` has no duplicate
   `(marketplace, platform_listing_id)`.
3. `daily_counts_reconcile`: the sum of `observation_count` over
   `offer_price_history_daily` equals the deduplicated Silver observation count
   for the same window.

A failed assertion blocks publish, is recorded with observed value and
expectation, and fails the run. It is never downgraded to a warning.

## 14. Required tests

No default test opens Spark, PostgreSQL, MinIO, Kafka or the network. Spark-
dependent transform tests are marked `integration` and skip unless an explicit
environment variable is set; the pure rules and the publish SQL sequence are
covered offline and unconditionally.

### Pure rules

1. `FRESH`, `AGING` and `STALE` boundaries, tested exactly on the threshold;
2. a future `last_observation_at` returns `FRESH` with age zero, never
   negative;
3. `counter_delta()` returns each `CounterValidity` member;
4. a negative delta keeps its true value and sets the flag;
5. a gap beyond the maximum invalidates even a positive delta;
6. non-positive elapsed time invalidates;
7. `velocity_per_hour` is `None` for every invalid result;
8. `price_movement()` matches the Phase 5 threshold semantics case for case;
9. `previous_price == 0` skips the percent branch without dividing;
10. `is_large_drop` is never true without `is_price_change`;
11. `daily_representative()` returns open/close/min/max/count in observation
    order and raises on empty input;
12. Decimal quantization is stable and half-up;
13. `marketplace_rules.py` imports no engine or client library.

### Contracts

14. all eight mart specs validate at import;
15. a `DoubleType` or `FloatType` money column fails validation;
16. a grain column absent from a schema fails validation;
17. `gold_run_id`, `as_of` and `rule_version` are required in every schema;
18. `make_gold_run_id()` is deterministic and changes with rule version;
19. `gold_dataset_path()` is run-scoped and URL-escaped.

### Silver reader

20. flatten output equals `MARKETPLACE_SILVER_SCHEMA` field for field
    (`integration`);
21. duplicate `observation_id` survives once, deterministically
    (`integration`);
22. a non-null wire value casting to null raises with the column name
    (`integration`);
23. path globs prune by marketplace and date (offline, pure string test);
24. a missing partition contributes zero rows and is recorded (offline).

### Job

25. every builder output matches its declared schema (`integration`);
26. windows tiebreak on `observation_id` for same-instant observations
    (`integration`);
27. two runs with identical `as_of` produce identical row counts and contents
    (`integration`);
28. a later `as_of` changes freshness columns but not history columns
    (`integration`);
29. `offer_change_daily` excludes `UNKNOWN` availability transitions and
    reports the exclusion count (`integration`);
30. `counter_delta_daily` never clamps a negative delta and reports invalid
    rows per reason (`integration`);
31. `source_coverage_daily` counts missing offers without imputing rows
    (`integration`);
32. an unavailable audit source writes an empty reliability mart with a
    `skipped_reason` and records the skip (offline, fake loader);
33. no transform calls `datetime.now()` or `current_timestamp()` — assert by
    source inspection;
34. `marketplace_gold_job.py` imports successfully with pyspark absent.

### Assertions and publish

35. duplicate `offer_current` rows fail `offer_current_unique` and block
    publish;
36. duplicate `(marketplace, platform_listing_id)` fails
    `offer_dimension_unique`;
37. a daily-count mismatch fails `daily_counts_reconcile`;
38. a failed assertion is recorded with observed value and expectation and the
    run status is `FAILED`;
39. `publish_marts()` issues exactly one commit for a full refresh (fake
    connection asserting call order);
40. an error mid-publish rolls back and re-raises;
41. re-publishing the same `gold_run_id` does not duplicate rows;
42. `publish=False` performs no database call at all;
43. the generated DDL contains no `DOUBLE PRECISION` and no `FLOAT`.

## 15. Verification commands

```powershell
python -m pytest `
  tests/test_marketplace_rules.py `
  tests/test_marketplace_gold_contracts.py `
  tests/test_marketplace_silver_reader.py `
  tests/test_marketplace_gold_job.py `
  tests/test_marketplace_postgres_cache.py `
  tests/test_marketplace_schema.py `
  tests/test_serialization.py `
  tests/test_identity.py `
  -q

python -m pytest tests -q

python -m data_ingestion.producer `
  --source tests/fixtures/events.csv --test-mode -n 2

git diff --check
git diff --stat
```

Also run one no-service smoke over a hand-built list of five observation rows
for two offers across two UTC days, using the pure rules only, a fake audit
loader and a fixed `as_of`. Assert:

1. day-one and day-two price history rows carry the correct open/close;
2. a negative counter delta is present, flagged and excluded from the valid
   total;
3. a stale offer is flagged, not dropped;
4. re-running with the same `as_of` reproduces identical results;
5. no Spark, PostgreSQL, MinIO or network object is constructed.

When a local Spark and PostgreSQL are available, run the integration matrix
once and record: `as_of`, window, observations read, deduplicated count,
per-mart row counts, assertion results, publish duration and any skipped mart
with its reason.

## 16. Commit/work-package sequence

1. `feat: add marketplace silver reader and compaction` — GOLD-01 only.
2. `feat: add pure marketplace temporal rules` — GOLD-02 only.
3. `feat: add marketplace gold mart contracts` — GOLD-03 only.
4. `feat: build marketplace offer dimensions and current state` — GOLD-04
   dimensions, `offer_current`, `offer_freshness` only.
5. `feat: build marketplace daily temporal marts` — price history, change
   daily, category price daily, counter delta daily only.
6. `feat: build marketplace coverage and reliability marts` — coverage,
   reliability and the structural assertions only.
7. `feat: add transactional marketplace postgres cache` — GOLD-05 only.
8. `feat: add marketplace superset dashboard draft` — Superset assets only.
9. `test: verify gold determinism and legacy compatibility` — test and
   compatibility fixes only.

Stop after each numbered package and show focused tests plus
`git diff --stat`.

## 17. Definition of Done

- [ ] dependency gate recorded and green;
- [ ] Silver JSON envelopes never mutated, moved or deleted;
- [ ] schema never inferred; the frozen wire schema is always supplied;
- [ ] silent null casts raise with the offending column;
- [ ] deduplication and every window are deterministic with an ID tiebreak;
- [ ] the job takes `as_of` and calls no clock inside a transform;
- [ ] two runs with the same `as_of` are reproducible;
- [ ] a later `as_of` changes only freshness, not history;
- [ ] no float or double in any money path, in Spark, Python or PostgreSQL;
- [ ] quantile columns declare method and accuracy;
- [ ] representative daily price is documented as close, not a mean;
- [ ] stale rows are flagged, never dropped, never forward-filled;
- [ ] missing days are counted as absence, never imputed;
- [ ] negative counter deltas are stored, flagged and excluded from totals;
- [ ] no column, table, view or chart calls a public counter a sale;
- [ ] `offer_change_daily` is recomputed from Silver, not from Phase 5 events;
- [ ] Gold is written only to run-scoped paths, with no `latest` pointer;
- [ ] all three structural assertions block publish on failure;
- [ ] PostgreSQL refresh is one transaction with one commit and rollback on
      error;
- [ ] re-running one logical window does not duplicate cache rows;
- [ ] pure rules import no engine or client library;
- [ ] job module imports without pyspark installed;
- [ ] legacy behavioral warehouse, ML and serving code untouched;
- [ ] offline tests pass with no external service;
- [ ] full tests pass or environment-only failures are documented.

## 18. Mandatory rejection conditions

Reject the implementation if it:

- infers a Silver schema or reads observations without the frozen wire schema;
- rewrites, compacts in place or deletes Phase 4 Silver objects;
- calls `dropDuplicates()` without a deterministic order;
- orders a per-offer window by `observed_at` alone;
- calls `datetime.now()`, `current_timestamp()` or `F.now()` inside a
  transform;
- stores any money value as double, float or `DOUBLE PRECISION`;
- reports a median without declaring the method and accuracy;
- forward-fills a price across a day with no observation;
- imputes a row for a missing offer/day;
- clamps a negative counter delta, drops it, or sums invalid deltas into a
  total;
- names a public counter as sales, orders, revenue or demand;
- derives `offer_change_daily` from `marketplace.changes.v1`, Redis or
  Elasticsearch;
- writes a `latest` pointer, manifest or promotion marker;
- publishes with a failed structural assertion;
- publishes across more than one transaction, uses autocommit, or swallows a
  publish error;
- computes anomaly detection, quality gates, a manifest or a replay workflow
  in this phase;
- modifies the legacy behavioral warehouse, ML jobs or their tests;
- claims the Gold output is validated or gate-approved.

## 19. Copy-ready prompts for a low-capability model

### Prompt A — Silver reader

```text
Implement only Section 8 and tests 20-24 of
docs/PHASE_6_BATCH_TEMPORAL_WAREHOUSE_IMPLEMENTATION_PLAN.md. Read the Phase 4
plan's Silver layout and data_ingestion/schemas.py first. Create
batch_layer/marketplace_silver_reader.py. Always supply
MARKETPLACE_OBSERVATION_WIRE_SCHEMA, build pruned partition globs, flatten to
MARKETPLACE_SILVER_SCHEMA, deduplicate deterministically and raise on a silent
null cast. Keep pyspark imports inside functions. Run focused tests, show diff
stat and stop.
```

### Prompt B — pure rules

```text
Implement only Section 9 and tests 1-13 of the Phase 6 plan. Create
batch_layer/marketplace_rules.py with no pyspark, psycopg2, redis or
elasticsearch import. Implement Section 6.9 counter validity exactly: return
true deltas even when invalid, never clamp, and return None velocity for every
invalid result. Match the Phase 5 price threshold semantics. Run focused tests
and stop.
```

### Prompt C — contracts

```text
Implement only Section 10 and tests 14-19 of the Phase 6 plan. Create
batch_layer/marketplace_gold_contracts.py with eight explicitly declared mart
schemas, import-time validation rejecting DoubleType and FloatType, and
deterministic run IDs and run-scoped paths. Do not derive a schema from a
transform. Run focused tests and stop.
```

### Prompt D — dimensions and current state

```text
Implement the Section 11.1 dimensions plus build_offer_current and
build_offer_freshness from the Phase 6 plan. Use
Window.partitionBy("offer_id").orderBy("observed_at", "observation_id") for
every latest-value lookup. Take as_of as an argument and call no clock. Select
the declared MartSpec schema columns at the end of each builder. Run focused
tests and stop. Do not build daily marts yet.
```

### Prompt E — daily marts

```text
Implement build_price_history_daily, build_offer_change_daily,
build_category_price_daily and build_counter_delta_daily from Section 11.2 of
the Phase 6 plan. Representative price is the day's close, not a mean. Exclude
UNKNOWN availability transitions and report the exclusion count. Never clamp a
negative counter delta and aggregate only VALID rows. Declare quantile method
and accuracy. Run focused tests and stop.
```

### Prompt F — coverage, reliability and assertions

```text
Implement build_source_coverage_daily, build_crawl_reliability_daily,
Section 13 assertions and the run orchestration from the Phase 6 plan. Count
missing offers as absence without imputing rows. Read Phase 3 audit through an
injected loader and, when unavailable, write an empty reliability mart with a
skipped_reason and record the skip. A failed assertion blocks publish and fails
the run. Run focused tests and stop.
```

### Prompt G — PostgreSQL cache

```text
Implement Section 12 and tests 35-43 of the Phase 6 plan. Create
batch_layer/marketplace_postgres_cache.py and add idempotent DDL to
scripts/init_postgres.sql without altering legacy tables. Stage every mart,
then refresh all targets in exactly one transaction with one commit, deleting
the current gold_run_id before insert. Roll back and re-raise on error. Use
NUMERIC(38,6) and no float column anywhere. Test with a fake connection that
asserts call order. Run focused tests and stop.
```

### Prompt H — Superset draft and final verification

```text
Implement display/superset/create_marketplace_dashboard.py over the
marketplace_gold cache, then run the Section 15 verification of the Phase 6
plan. Reuse existing display/superset patterns without editing
create_batch_ml_dashboard.py. No chart may label a public counter as sales, and
every median panel must show its method and accuracy. Run the full test matrix
and the no-service smoke, document environment-only failures, show diff stat
and stop. Do not implement anomaly detection, quality gates or a manifest.
```

## 20. Handoff to Phase 7

Phase 7 adds the mandatory quality-gate battery from the parent plan's
Section 16, robust price anomaly detection using rolling median, MAD and IQR
fallback, the Gold publish manifest with version promotion, and the raw reparse
and replay workflow. It builds on this phase's run-scoped Gold, structural
assertions and recorded run rows, and it must not loosen the no-float,
no-imputation or counter-semantics rules established here.

The Phase 6 product is not "a warehouse with charts". It is a reproducible
temporal derivation: given the same observations and the same `as_of`, it
produces the same marts, and every number it publishes can be traced to a
declared schema, a named rule version and a recorded run.
