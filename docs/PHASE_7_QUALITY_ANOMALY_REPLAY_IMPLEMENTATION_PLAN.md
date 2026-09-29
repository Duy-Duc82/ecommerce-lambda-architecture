# Phase 7 implementation plan — Quality gates, robust price anomaly and replay

> Status: ready for implementation after Phase 6 acceptance
>
> Intended implementer: a low-capability coding model working one work package
> at a time. It must follow the fixed grains and stop between packages.
>
> Parent plan: `Brief để xây dựng plan sơ bộ cho việc cải tiến ecommerce-lambda-architecture.md`
>
> Phase boundary: `docs/PHASE_INDEX.md` §1 — Phase 7 owns **P1-08** and
> **P1-09** only. It must not take P1-11 Kibana, P1-12 recovery drills or any
> P2 item.

## 1. Objective

Implement the parent plan's Week 7 slice and backlog P1-08 and P1-09:

```text
Phase 6 run-scoped Gold (nine marts)
-> robust rolling median/MAD/IQR price anomaly mart
-> mandatory data-quality gate over Silver, audit and Gold
-> persisted quality results with rule version and run ID
-> Gold publish manifest written and promoted only when the gate passes
-> PostgreSQL cache refresh gated by the same decision
-> raw reparse workflow proving the same raw yields the same observation
```

This phase answers **“is this run publishable, and can it be reproduced from
raw?”** It does not add new sources, new speed-layer behaviour, new
orchestration services or any operational drill.

The Phase 6 handoff (§21 of that plan) fixes the required outcome: on quality
failure the new run stays inspectable under its run-scoped path while the last
good Gold manifest and the last good PostgreSQL cache remain unchanged.

## 2. Mandatory dependency gate

Before editing, record and verify:

1. Phase 6 is accepted. `batch_layer/marketplace_warehouse.py` exposes
   `MarketplaceBatchContext`, `run_marketplace_warehouse()`,
   `write_run_scoped_gold()` and the nine-dataset contract.
2. `batch_layer/marketplace_marts.py` exposes `build_marketplace_marts()`
   returning exactly nine keys, and `build_offer_price_history_daily()` already
   produces deterministic `first_price`/`last_price`/`min_price`/`max_price`/
   `observation_count` per `(marketplace, offer_id, observed_date)`.
3. `batch_layer/marketplace_postgres.py` exposes `DATASET_COLUMNS`,
   `MarketplaceBatchAudit`, `MarketplaceCachePublisher` and the single-
   transaction publish over nine cache tables.
4. Phase 2 raw artifacts exist at the accepted layout
   `marketplace/raw/marketplace=<code>/observed_date=<date>/hour=<hh>/crawl_run_id=<id>/raw_artifact_id=<id>/{body.bin,metadata.json}`
   with a recorded SHA-256 and `RAW_METADATA_SCHEMA_VERSION`.
5. Phase 3 `audit.crawl_run` and `audit.crawl_request_attempt` expose parsed and
   rejected counts; Phase 7 reconciliation reads them, it does not recompute
   them from Silver.
6. `observation_id` derivation is unchanged and still derives from marketplace,
   listing ID, observed time and raw hash (PHASE_INDEX §3). The reparse
   determinism claim in Section 12 depends on this and on nothing else.

If any of these is absent or differs, stop and report the exact mismatch. Do
not guess a field meaning and do not fill an operational count with zero.

### 2.1 Two deliberate deviations, flagged for review

These are design decisions inside Phase 7's scope, recorded here so a reviewer
can overrule them before code is written.

- **Quality results go to a new `audit.marketplace_quality_result`**, not into
  the legacy `audit.data_quality_result`. The Phase 6 plan §3.2 called the
  latter's extension a Phase 7 item. The legacy table is written by the legacy
  behavioural pipeline, its `checked_at` is `TIMESTAMP` while every marketplace
  audit column is `TIMESTAMPTZ`, and its primary key has no room for severity
  or dataset. Sharing it couples two unrelated pipelines through a schema
  migration. Every other marketplace audit artifact is already namespaced
  `marketplace_*`; this follows that. `audit.data_quality_result` is left
  untouched.
- **`MarketplaceCachePublisher.publish()` gains a required `quality`
  argument.** This deliberately breaks the three Phase 6 publisher call sites
  in `tests/test_marketplace_postgres.py`, which Section 4 lists as modified. A
  keyword with a permissive default would let a future caller publish an
  ungated run, which is the exact failure this phase exists to prevent.

## 3. Scope

### 3.1 In scope

- A tenth Gold mart `price_anomaly_daily` built from robust rolling statistics.
- A declarative quality-rule registry with fixed check names and rule version.
- Thirteen mandatory and four advisory checks evaluated as pure transforms.
- Quality results persisted with observed value, expectation, status, severity,
  rule version and run ID — persisted on failure as well as on success.
- A canonical Gold publish manifest per run and a promoted `current` pointer.
- Manifest promotion and PostgreSQL publication both gated on the same
  decision object.
- A raw reparse workflow that verifies checksum, re-parses a stored artifact
  and proves the canonical observation is unchanged.
- Batch replay and idempotency tests over rerun, promotion and publication.
- A Superset data-quality and audit dashboard reading run history.

### 3.2 Out of scope

- Kibana source-health and freshness dashboards — Phase 8 (P1-11 Kibana half).
- Compose profiles, one-command start/smoke/validate, failure drills,
  backup/export — Phase 8 (P1-12).
- Performance benchmarks, storage-growth reports, evidence bundle — Phase 9.
- Cross-market product/variant matching or comparison — P3, gated on P0–P2.
- ML anomaly detection of any kind. The Brief fixes the MVP at robust
  statistics (§15): rolling median, MAD, IQR fallback, minimum sample size.
- Adapter upgrades that legitimately change parsed content. Section 12 reports
  such a divergence and refuses to write; superseding a Silver version needs a
  schema version bump and is not a Phase 7 change.
- Rewriting any Phase 6 mart grain. Phase 7 adds one mart and reads the other
  nine; it does not redefine them.
- Anomaly detection over the speed layer, Redis, Elasticsearch or
  `marketplace.changes.v1`. Batch anomaly reads Gold and Silver only.
- Re-deriving parsed or rejected counts from Silver instead of Phase 3 audit.
- Replacing or rewriting `batch_layer/warehouse_job.py`.

## 4. Allowed file changes

Create:

```text
batch_layer/marketplace_anomaly.py
batch_layer/marketplace_quality.py
batch_layer/marketplace_manifest.py
config/quality_rules.py
crawler/reparse.py
display/superset/create_marketplace_quality_dashboard.py
tests/fixtures/marketplace_raw/tiki_listing_body.json
tests/fixtures/marketplace_raw/tiki_listing_metadata.json
tests/test_marketplace_anomaly.py
tests/test_marketplace_quality.py
tests/test_marketplace_manifest.py
tests/test_marketplace_reparse.py
```

Modify only:

```text
batch_layer/marketplace_marts.py
batch_layer/marketplace_warehouse.py
batch_layer/marketplace_postgres.py
common/object_store.py
config/settings.py
scripts/init_postgres.sql
display/superset/datasources.yaml
tests/test_marketplace_marts.py
tests/test_marketplace_warehouse.py
tests/test_marketplace_postgres.py
docs/PHASE_INDEX.md
```

`docs/PHASE_INDEX.md` may be touched only to move the Phase 7 status row. Any
change to a phase boundary requires the Brief §27 change-control path first.

Do not extend the legacy warehouse job, the speed layer or the crawler worker
with quality branches. Shared helpers may be extracted only in a separate
reviewed refactor after both suites pass.

## 5. Configuration and deterministic run context

Add environment-backed defaults to `config/settings.py`:

```python
MARKETPLACE_ANOMALY_RULE_VERSION = "anomaly-rules.v1"
MARKETPLACE_ANOMALY_WINDOW_DAYS = 14
MARKETPLACE_ANOMALY_MIN_SAMPLES = 7
MARKETPLACE_ANOMALY_MAD_THRESHOLD = "3.5"
MARKETPLACE_ANOMALY_IQR_MULTIPLIER = "1.5"
MARKETPLACE_QUALITY_RULE_VERSION = "quality-rules.v1"
MARKETPLACE_QUALITY_FUTURE_TOLERANCE_SECONDS = 300
MARKETPLACE_QUALITY_SAMPLE_LIMIT = 10
MARKETPLACE_ALLOWED_CURRENCIES = "VND,USD"
MARKETPLACE_MANIFEST_SCHEMA_VERSION = "marketplace-gold-manifest.v1"
```

Thresholds use `_env_decimal` like the existing speed-layer drop thresholds, so
no money or fence value ever becomes a float before comparison. Register every
new positive integer in the existing `validate_settings()` positivity map and
every new version string in its non-empty list.

Extend `MarketplaceBatchContext` with these frozen fields, keeping existing
field order and defaults so Phase 6 construction sites still work:

```python
anomaly_window_days: int = MARKETPLACE_ANOMALY_WINDOW_DAYS
anomaly_min_samples: int = MARKETPLACE_ANOMALY_MIN_SAMPLES
anomaly_mad_threshold: Decimal = Decimal(MARKETPLACE_ANOMALY_MAD_THRESHOLD)
anomaly_iqr_multiplier: Decimal = Decimal(MARKETPLACE_ANOMALY_IQR_MULTIPLIER)
anomaly_rule_version: str = MARKETPLACE_ANOMALY_RULE_VERSION
quality_rule_version: str = MARKETPLACE_QUALITY_RULE_VERSION
future_tolerance_seconds: int = MARKETPLACE_QUALITY_FUTURE_TOLERANCE_SECONDS
allowed_currencies: tuple[str, ...] = ("VND", "USD")
```

Additional `__post_init__` rules:

- `anomaly_window_days`, `anomaly_min_samples` and `future_tolerance_seconds`
  are positive;
- `anomaly_min_samples <= anomaly_window_days`, otherwise no row can ever be
  evaluated and the mart would silently be all `INSUFFICIENT_HISTORY`;
- `anomaly_mad_threshold` and `anomaly_iqr_multiplier` are positive Decimals;
- `allowed_currencies` is a non-empty tuple of distinct `^[A-Z]{3}$` codes,
  normalised and sorted so the manifest is deterministic;
- `anomaly_rule_version` and `quality_rule_version` are non-empty.

The Phase 6 rule still binds: **no transform calls `datetime.now()` or
`current_timestamp()` for business semantics.** Phase 7 adds one corollary —
see Section 11.4 — the manifest carries no wall clock at all, so rerunning the
same context produces byte-identical manifest bytes.

## 6. Robust price anomaly — `price_anomaly_daily`

### 6.1 Input and window

Input is the Phase 6 `offer_price_history_daily` mart, not raw Silver. The
daily mart already collapses intraday noise deterministically, and reusing it
means the anomaly mart and the price mart can never disagree.

The evaluated value is `last_price`: the offer's last observed price that day,
under the Phase 6 total order. Document this in the module docstring; do not
switch to `avg_price`, because an average blends two genuinely different
observed prices into a value the marketplace never showed.

Baseline window, per offer:

```text
PARTITION BY (marketplace, offer_id, currency)
ORDER BY observed_date
ROWS BETWEEN <anomaly_window_days> PRECEDING AND 1 PRECEDING
```

Three properties this frame must have, each covered by a test:

- the frame excludes the current day, so a single extreme price cannot pull the
  median toward itself and hide;
- the frame counts **observed days**, not calendar days; a gap in observation
  must not inject a fabricated price;
- a currency change restarts the partition, so an offer that switches currency
  reports `INSUFFICIENT_HISTORY` rather than comparing VND against USD.

### 6.2 Exact order statistics, not approximations

Collect the frame with `F.collect_list` and sort with `F.array_sort`. Window
sizes are bounded by `anomaly_window_days`, so this is cheap and, unlike
`percentile_approx`, exact and reproducible.

For a sorted array `a` of length `n`, define one helper used everywhere:

```text
median(a) = a[(n-1)//2]                      when n is odd
median(a) = (a[n//2 - 1] + a[n//2]) / 2      when n is even
p25(a)    = a[floor(0.25 * (n - 1))]
p75(a)    = a[ceil (0.75 * (n - 1))]
```

Use one `_ordered_percentile()` helper for all three so the definitions cannot
drift apart. All arithmetic stays `DecimalType(38, 6)`; the even-count median
divides by an exact `Decimal("2")`.

MAD is the median of absolute deviations, computed from the same frame:

```text
deviations = transform(a, x -> abs(x - median(a)))
mad        = median(array_sort(deviations))
```

### 6.3 Decision precedence

Evaluate in this exact order and stop at the first match:

1. `baseline_sample_size < anomaly_min_samples`
   → status `INSUFFICIENT_HISTORY`, reason `MIN_SAMPLE_NOT_MET`,
   method `NONE`, all score and fence columns null.
2. `mad > 0`
   → method `ROLLING_MAD`.
   `robust_score = 0.6745 * (evaluated_price - baseline_median) / mad`.
   Status is `ANOMALOUS_HIGH` when `robust_score > +threshold`,
   `ANOMALOUS_LOW` when `robust_score < -threshold`, else `NORMAL`.
   Reason is `MAD_SCORE_EXCEEDED` or `WITHIN_TOLERANCE`.
3. `mad = 0` and `iqr > 0`
   → method `IQR_FALLBACK`.
   `lower_fence = p25 - multiplier * iqr`, `upper_fence = p75 + multiplier * iqr`.
   Status is `ANOMALOUS_LOW` below the lower fence, `ANOMALOUS_HIGH` above the
   upper fence, else `NORMAL`. Reason is `IQR_FENCE_EXCEEDED` or
   `WITHIN_TOLERANCE`.
4. `mad = 0` and `iqr = 0`
   → status `INSUFFICIENT_DISPERSION`, reason `ZERO_DISPERSION`,
   method `NONE`. A perfectly flat baseline makes every deviation infinitely
   significant; reporting that as an anomaly would flag the first price change
   of every stable offer.

The `0.6745` constant is the standard normal consistency factor that makes the
MAD score comparable to a z-score; the default threshold `3.5` is the
conventional companion. Both live in config so a reviewer can retune without a
code change.

### 6.4 Grain and columns

Grain: exactly one row per `(marketplace, offer_id, observed_date)`, matching
`offer_price_history_daily` one-to-one. Currency is carried, not keyed, because
the source mart already holds one currency per offer-day.

```text
marketplace              VARCHAR(64)     not null
offer_id                 VARCHAR(128)    not null
observed_date            DATE            not null
currency                 VARCHAR(8)      not null
evaluated_price          NUMERIC(38,6)   not null
baseline_sample_size     BIGINT          not null, >= 0
baseline_median          NUMERIC(38,6)   null when sample size is 0
baseline_mad             NUMERIC(38,6)   null when sample size is 0
baseline_p25             NUMERIC(38,6)   null when sample size is 0
baseline_p75             NUMERIC(38,6)   null when sample size is 0
baseline_iqr             NUMERIC(38,6)   null when sample size is 0
deviation_amount         NUMERIC(38,6)   evaluated_price - baseline_median
deviation_percent        DOUBLE          null when baseline_median = 0
robust_score             DOUBLE          null outside ROLLING_MAD
lower_fence              NUMERIC(38,6)   null outside IQR_FALLBACK
upper_fence              NUMERIC(38,6)   null outside IQR_FALLBACK
anomaly_method           VARCHAR(16)     ROLLING_MAD | IQR_FALLBACK | NONE
anomaly_status           VARCHAR(24)     see Section 6.3
anomaly_reason           VARCHAR(32)     see Section 6.3
window_days              BIGINT          not null
min_samples              BIGINT          not null
mad_threshold            NUMERIC(38,6)   not null
iqr_multiplier           NUMERIC(38,6)   not null
anomaly_rule_version     VARCHAR(64)     not null
```

Carrying the four parameters and the rule version per row is deliberate: a
stored anomaly verdict is meaningless without the rule that produced it, and
the Brief requires an anomaly reason and rule version (§15).

### 6.5 Language constraints

These are contract, not style:

- the mart, the cache table, the dashboard and every log line say
  **“statistical price outlier relative to this offer's own recent observed
  price history”**;
- never `scam`, `fraud`, `fake`, `incorrect price`, `wrong price`, `mispricing`;
- never claim a cross-offer, cross-seller or cross-marketplace comparison; the
  baseline is the offer's own history and nothing else;
- `ANOMALOUS_LOW` is not “a deal” and `ANOMALOUS_HIGH` is not “price gouging”.

A grep for the forbidden words across the new modules, the DDL and the
dashboard script is part of Section 18.

## 7. Quality rule registry

Create `config/quality_rules.py`, mirroring the shape of
`config/counter_semantics.py`:

```python
@dataclass(frozen=True)
class QualityRule:
    check_name: str
    severity: str          # "MANDATORY" | "ADVISORY"
    dataset_name: str      # the dataset the check reads, or "silver"
    expectation: str       # human-readable, stored verbatim with the result

QUALITY_RULES: tuple[QualityRule, ...] = (...)

def quality_rule(check_name: str) -> QualityRule: ...
def mandatory_rule_names() -> tuple[str, ...]: ...
```

Registry invariants, each covered by a test:

- check names are unique and match `^[a-z][a-z0-9_]{0,62}$`;
- severity is exactly `MANDATORY` or `ADVISORY`;
- expectation is non-empty;
- `dataset_name` is `silver`, `audit`, or one of the ten Gold dataset names;
- the registry is the only place a check name is spelled; the evaluator looks
  each one up and fails on an unregistered name.

### 7.1 The thirteen mandatory checks

These are the Brief §16 mandatory list, one row each. The evaluator must
produce exactly these thirteen with severity `MANDATORY`, no more and no fewer.

| # | `check_name` | Expectation |
|---|---|---|
| 1 | `raw_artifact_checksum_and_uri_present` | every observation carries a non-empty `raw_uri` and a 64-hex `raw_sha256` |
| 2 | `silver_observation_raw_lineage_complete` | every observation carries `crawl_run_id`, `adapter_version` and `fetched_at` |
| 3 | `observation_id_unique` | `count(distinct observation_id) = count(*)` |
| 4 | `offer_key_and_price_complete` | `offer_id`, `marketplace`, `observed_at` and `current_price` are all non-null |
| 5 | `price_non_negative` | `current_price >= 0` and `list_price` is null or `>= 0` |
| 6 | `observed_at_within_future_tolerance` | `observed_at <= as_of + future_tolerance_seconds` |
| 7 | `currency_valid` | `currency` matches `^[A-Z]{3}$` and is in `allowed_currencies` |
| 8 | `silver_parse_attempt_reconciliation` | Silver observation count equals audit `parsed_count` for the same crawl runs |
| 9 | `offer_listing_key_unique` | no `(marketplace_id, platform_listing_id)` maps to more than one `offer_id` |
| 10 | `offer_current_single_row_per_offer` | `offer_current` has exactly one row per `offer_id` |
| 11 | `gold_daily_row_count_reconciles` | `sum(offer_price_history_daily.observation_count)` equals the deduplicated Silver row count |
| 12 | `gold_daily_price_aggregates_reconcile` | per `(offer_id, observed_date)`, the mart's `min_price`/`max_price` equal a direct recomputation from Silver |
| 13 | `freshness_rule_version_and_as_of_applied` | every `offer_freshness` row carries the context `as_of` and `freshness_rule_version`, and a non-null status |

Notes that change the implementation:

- Check 8 reads `audit.crawl_request_attempt` and `audit.crawl_run`. It
  reconciles only over crawl run IDs actually present in this run's Silver
  input; a crawl run whose observations are outside the read window is not a
  discrepancy. The check is `SKIPPED` — not `PASS` — when the audit frames are
  empty, so an unavailable audit database can never be read as a green gate.
- Check 12 recomputes from Silver rather than trusting the mart, which is the
  entire point; comparing the mart against itself would always pass.
- Check 6 uses `context.as_of`, never a wall clock.

### 7.2 The four advisory checks

Advisory checks are persisted and shown on the dashboard but never block
publication:

| `check_name` | Expectation |
|---|---|
| `counter_invalid_transition_rate` | invalid public-counter transitions stay a reported minority; observed value is the rate |
| `price_anomaly_evaluation_coverage` | the share of offer-days actually evaluated (not `INSUFFICIENT_*`) is reported |
| `source_coverage_rate_denominator_present` | `source_coverage_daily.coverage_rate` is non-null wherever the eligible denominator is positive |
| `crawl_reliability_evidence_present` | `crawl_reliability_daily` is non-empty whenever Silver is non-empty |

An advisory failure must never change the gate decision. A test asserts that a
run with every advisory check failing and every mandatory check passing still
publishes.

## 8. Quality evaluation API

Create `batch_layer/marketplace_quality.py`:

```python
@dataclass(frozen=True)
class QualityResult:
    run_id: str
    check_name: str
    severity: str
    dataset_name: str
    status: str                    # PASS | FAIL | SKIPPED
    observed_value: float | None
    expectation: str
    rule_version: str
    failure_sample_json: str | None
    checked_at: datetime

@dataclass(frozen=True)
class QualityDecision:
    run_id: str
    passed: bool
    rule_version: str
    evaluated_at: datetime
    mandatory_total: int
    mandatory_failures: int
    advisory_failures: int
    skipped: int

class QualityGateFailure(RuntimeError):
    def __init__(self, decision: QualityDecision, results: Sequence[QualityResult]): ...

def evaluate_quality_gates(
    observations: DataFrame,
    marts: Mapping[str, DataFrame],
    attempts: DataFrame,
    runs: DataFrame,
    context: MarketplaceBatchContext,
) -> tuple[QualityResult, ...]: ...

def decide(
    results: Sequence[QualityResult],
    context: MarketplaceBatchContext,
) -> QualityDecision: ...
```

Rules:

- `evaluate_quality_gates` opens no database, touches no object store, starts no
  Spark session and reads no clock. `checked_at` is `context.as_of`.
- It returns one result per registered rule, always — a check that cannot run
  returns `SKIPPED` with a reason in `failure_sample_json`, never a silent
  omission. A missing result is itself a gate failure in `decide()`.
- `observed_value` is always a number: a violation count for boolean-shaped
  checks, a rate in `[0, 1]` for rate-shaped checks. It is cast to `float` only
  at the persistence boundary, never inside a comparison.
- `failure_sample_json` holds at most `MARKETPLACE_QUALITY_SAMPLE_LIMIT` keys,
  sorted, as canonical JSON. It carries **identifiers only** — offer ID,
  observation ID, date, marketplace. Never a raw body, a product title, a URL
  with a query string, a password, a connection string or a stack trace.
- `decide()` returns `passed=False` when any `MANDATORY` result is `FAIL` or
  `SKIPPED`, or when any registered mandatory rule has no result at all. A
  skipped mandatory check is not a pass.
- Evaluation is a fixed number of Spark actions. Compute the per-check counts in
  as few passes as the checks allow and document the count; do not trigger one
  full scan per check on a frame that was cached once.

## 9. Quality result persistence

Add to `batch_layer/marketplace_postgres.py`:

```python
class MarketplaceQualityRepository:
    def __init__(self, connection_factory: Callable[[], Any]): ...
    @classmethod
    def from_settings(cls): ...
    def record(self, results: Sequence[QualityResult], *, run_id: str) -> None: ...
    def latest_decision(self, *, run_id: str) -> QualityDecision | None: ...
```

`record()` runs in its own transaction, **before** any publication decision is
acted on, and is idempotent per `(run_id, check_name)` via upsert. This is the
one thing that must survive a failed run: a blocked publication with no stored
evidence is indistinguishable from a crash.

DDL appended to `scripts/init_postgres.sql`:

```sql
CREATE TABLE IF NOT EXISTS audit.marketplace_quality_result (
    run_id              VARCHAR(64) NOT NULL,
    check_name          VARCHAR(64) NOT NULL,
    severity            VARCHAR(16) NOT NULL CHECK (severity IN ('MANDATORY','ADVISORY')),
    dataset_name        VARCHAR(64) NOT NULL,
    status              VARCHAR(8)  NOT NULL CHECK (status IN ('PASS','FAIL','SKIPPED')),
    observed_value      DOUBLE PRECISION,
    expectation         TEXT NOT NULL,
    rule_version        VARCHAR(64) NOT NULL,
    failure_sample_json TEXT CHECK (length(failure_sample_json) <= 4000),
    checked_at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, check_name)
);

CREATE INDEX IF NOT EXISTS ix_marketplace_quality_result_checked_at
    ON audit.marketplace_quality_result (checked_at DESC);
```

Extend `audit.marketplace_batch_run` non-destructively:

```sql
ALTER TABLE audit.marketplace_batch_run
    ADD COLUMN IF NOT EXISTS quality_status          VARCHAR(16),
    ADD COLUMN IF NOT EXISTS mandatory_failure_count BIGINT,
    ADD COLUMN IF NOT EXISTS manifest_uri            TEXT,
    ADD COLUMN IF NOT EXISTS manifest_promoted       BOOLEAN NOT NULL DEFAULT FALSE;
```

Batch statuses become exactly `RUNNING`, `GOLD_WRITTEN`, `QUALITY_FAILED`,
`SUCCEEDED`, `FAILED`. `QUALITY_FAILED` is terminal, sets `completed_at`, and
leaves `cache_published = FALSE` and `manifest_promoted = FALSE`. It is
distinct from `FAILED`: the run produced complete, inspectable Gold and was
refused, which is a different operational situation from a crash.

Add `mark_quality_failed()` to `MarketplaceBatchAudit`:

```python
def mark_quality_failed(
    self, *, run_id: str, completed_at: datetime,
    decision: QualityDecision, manifest_uri: str | None,
) -> None: ...
```

## 10. Gated PostgreSQL publication

`DATASET_COLUMNS` gains `price_anomaly_daily` with the Section 6.4 column list,
so `DATASETS` becomes ten. Every “all nine” message, guard and comment in
`batch_layer/marketplace_postgres.py` becomes ten.

New cache table:

```sql
CREATE TABLE IF NOT EXISTS cache.marketplace_price_anomaly_daily (
    marketplace VARCHAR(64) NOT NULL,
    offer_id VARCHAR(128) NOT NULL,
    observed_date DATE NOT NULL,
    currency VARCHAR(8) NOT NULL,
    evaluated_price NUMERIC(38,6) NOT NULL,
    baseline_sample_size BIGINT NOT NULL CHECK (baseline_sample_size >= 0),
    baseline_median NUMERIC(38,6), baseline_mad NUMERIC(38,6),
    baseline_p25 NUMERIC(38,6), baseline_p75 NUMERIC(38,6),
    baseline_iqr NUMERIC(38,6),
    deviation_amount NUMERIC(38,6), deviation_percent DOUBLE PRECISION,
    robust_score DOUBLE PRECISION,
    lower_fence NUMERIC(38,6), upper_fence NUMERIC(38,6),
    anomaly_method VARCHAR(16) NOT NULL
        CHECK (anomaly_method IN ('ROLLING_MAD','IQR_FALLBACK','NONE')),
    anomaly_status VARCHAR(24) NOT NULL CHECK (anomaly_status IN
        ('NORMAL','ANOMALOUS_HIGH','ANOMALOUS_LOW',
         'INSUFFICIENT_HISTORY','INSUFFICIENT_DISPERSION')),
    anomaly_reason VARCHAR(32) NOT NULL,
    window_days BIGINT NOT NULL, min_samples BIGINT NOT NULL,
    mad_threshold NUMERIC(38,6) NOT NULL, iqr_multiplier NUMERIC(38,6) NOT NULL,
    anomaly_rule_version VARCHAR(64) NOT NULL,
    PRIMARY KEY (marketplace, offer_id, observed_date)
);
```

`publish()` gains a required keyword:

```python
def publish(
    self, staged: Sequence[StagedDataset], *,
    run_id: str, published_at: datetime, quality: QualityDecision,
) -> None: ...
```

Before acquiring the advisory lock it raises `QualityGateFailure` when
`quality.run_id != run_id` or `not quality.passed`. The existing ordering is
otherwise unchanged and still holds: lock, revalidate all ten staging tables
and counts, truncate all ten cache tables, insert with explicit column lists,
upsert the cache-version singleton, mark the run `SUCCEEDED` with
`cache_published = TRUE`, commit.

`audit.marketplace_cache_version` gains `quality_rule_version VARCHAR(64)` and
`manifest_uri TEXT` via `ADD COLUMN IF NOT EXISTS`, written in that same
transaction, so the published cache always names the manifest and the rule
version that admitted it.

`stage()` must not run at all when the decision failed. Orchestration enforces
this; the publisher's own guard is the second line of defence, not the first.

## 11. Gold publish manifest

Create `batch_layer/marketplace_manifest.py`.

### 11.1 Layout

```text
gold/marketplace/runs/run_id=<escaped>/<dataset>/          # Phase 6, unchanged
gold/marketplace/manifests/run_id=<escaped>/manifest.json  # Phase 7, per run
gold/marketplace/current.json                              # Phase 7, pointer
```

Run manifests are append-only. A promotion never deletes or rewrites a previous
manifest, so the last good version is always recoverable by reading the pointer
history recorded in the batch audit table.

### 11.2 Manifest document

Canonical JSON: UTF-8, sorted keys, `separators=(",", ":")`, no trailing
newline — the same canonicalisation `crawler/raw_store.py` already uses, so the
two never disagree about what “canonical” means.

```json
{
  "manifest_schema_version": "marketplace-gold-manifest.v1",
  "run_id": "<run id>",
  "as_of": "<ISO-8601 UTC>",
  "created_at": "<ISO-8601 UTC, equal to as_of>",
  "silver_uri": "<uri>",
  "gold_run_uri": "<uri>",
  "rule_versions": {
    "anomaly": "...", "counter": "...",
    "freshness": "...", "quality": "..."
  },
  "datasets": [
    {"dataset_name": "...", "uri": "...", "row_count": 0,
     "partition_columns": ["marketplace", "observed_date"]}
  ],
  "quality": {
    "status": "PASS",
    "rule_version": "...",
    "mandatory_total": 13,
    "mandatory_failures": 0,
    "advisory_failures": 0,
    "skipped": 0
  },
  "previous_run_id": "<run id or null>"
}
```

`datasets` is sorted by `dataset_name` and must contain exactly the ten Gold
datasets. `previous_run_id` is read from the current pointer before promotion,
which gives every manifest a backward chain without a separate index.

### 11.3 API

```python
@dataclass(frozen=True)
class ManifestDataset:
    dataset_name: str
    uri: str
    row_count: int
    partition_columns: tuple[str, ...]

@dataclass(frozen=True)
class GoldManifest:
    manifest_schema_version: str
    run_id: str
    as_of: datetime
    silver_uri: str
    gold_run_uri: str
    rule_versions: Mapping[str, str]
    datasets: tuple[ManifestDataset, ...]
    quality: Mapping[str, Any]
    previous_run_id: str | None

@dataclass(frozen=True)
class PromotionResult:
    promoted: bool
    reason: str          # PROMOTED | ALREADY_CURRENT | QUALITY_FAILED | BACKFILL_REFUSED
    manifest_uri: str
    previous_run_id: str | None

def build_gold_manifest(
    writes: Mapping[str, GoldWriteResult],
    decision: QualityDecision,
    context: MarketplaceBatchContext,
    *, previous_run_id: str | None,
) -> GoldManifest: ...

def serialize_manifest(manifest: GoldManifest) -> bytes: ...
def parse_manifest(payload: bytes) -> GoldManifest: ...

def write_run_manifest(
    manifest: GoldManifest, *, writer: Callable[[str, str, bytes], str],
) -> str: ...

def read_current_manifest(
    *, reader: Callable[[str, str], bytes | None],
) -> GoldManifest | None: ...

def promote_manifest(
    manifest: GoldManifest, *,
    writer: Callable[[str, str, bytes], str],
    reader: Callable[[str, str], bytes | None],
    allow_backfill: bool = False,
) -> PromotionResult: ...
```

`writer` and `reader` keep the `(zone, relative_path, data) -> uri` and
`(zone, relative_path) -> bytes | None` shapes already used by
`common/object_store.put_bytes`, so tests inject a dict-backed fake and no
default test opens MinIO. Add the matching `get_bytes(zone, relative_path)` to
`common/object_store.py`, returning `None` on a missing key and raising on
every other error — a transport failure must not be mistaken for “no current
manifest”, which would silently re-promote over a good pointer.

### 11.4 Promotion rules

1. **Write the run manifest first, always** — including when the gate failed.
   A refused run must leave a readable record of what it produced and why it
   was refused. Only the pointer is gated.
2. Promote only when `decision.passed`; otherwise return
   `PromotionResult(False, "QUALITY_FAILED", ...)` and leave `current.json`
   byte-identical.
3. The pointer is replaced by a single write of the full manifest document, not
   a reference — one object-store PUT, so a reader never observes a torn
   pointer. `current.json` and the run manifest are byte-identical for the
   promoted run.
4. Promoting a run that is already current is idempotent: return
   `PromotionResult(False, "ALREADY_CURRENT", ...)` and write nothing.
5. Refuse to move the pointer backwards in time. If the current manifest's
   `as_of` is strictly later than this manifest's `as_of`, return
   `BACKFILL_REFUSED` unless `allow_backfill=True`. A reprocessed old window
   must not silently become the serving version.
6. The manifest carries **no wall clock**: `created_at` equals `context.as_of`.
   Rerunning the same context therefore yields byte-identical manifest bytes,
   which is what makes replay verifiable in Section 13. Wall-clock timestamps
   belong in `audit.marketplace_batch_run` only.

## 12. Raw reparse workflow

Create `crawler/reparse.py`. The Brief's requirement is precise (§13, §19):
reparsing the same raw artifact must not create a duplicate or divergent
canonical observation.

### 12.1 API

```python
@dataclass(frozen=True)
class RawArtifactRef:
    marketplace_code: str
    observed_date: str
    hour: str
    crawl_run_id: str
    raw_artifact_id: str

    @property
    def body_path(self) -> str: ...
    @property
    def metadata_path(self) -> str: ...

@dataclass(frozen=True)
class ReparseOutcome:
    raw_artifact_id: str
    status: str              # see 12.3
    observation_ids: tuple[str, ...]
    content_hashes: Mapping[str, str]
    diverged_observation_ids: tuple[str, ...]
    error_type: str | None
    error_message: str | None

def load_raw_artifact(
    ref: RawArtifactRef, *, reader: Callable[[str, str], bytes | None],
) -> tuple[Mapping[str, Any], bytes]: ...

def reparse_raw_artifact(
    ref: RawArtifactRef, *, adapter, reader, clock: Callable[[], datetime],
) -> ReparseOutcome: ...

def diff_reparsed_observations(
    reparsed: Mapping[str, str], existing: Mapping[str, str],
) -> tuple[str, ...]: ...

def reparse_batch(
    refs: Sequence[RawArtifactRef], *, adapter_for, reader, clock,
) -> tuple[ReparseOutcome, ...]: ...
```

`diff_reparsed_observations` maps `observation_id -> canonical content hash` on
both sides and returns the sorted diverged IDs. It is a pure dict comparison —
no Spark, no storage — so the determinism claim is testable in milliseconds.
The CLI supplies the existing side by reading the matching Silver records.

### 12.2 Mandatory order

1. Read `metadata.json`; reject a missing or unknown
   `RAW_METADATA_SCHEMA_VERSION`.
2. Read `body.bin` and recompute SHA-256.
3. Compare against the checksum in the metadata sidecar. On mismatch, stop with
   `CHECKSUM_MISMATCH` and **do not parse**. A corrupted artifact must never be
   re-parsed into Silver-shaped output; that would launder corruption into
   canonical data.
4. Rebuild the `ListingPageRequest` from the metadata, never from a caller
   argument, so a reparse cannot be pointed at the wrong target.
5. Compare the metadata `adapter_version` against the supplied adapter. If they
   differ, stop with `ADAPTER_VERSION_CHANGED`. Reparse under a new adapter is
   out of scope (Section 3.2) and needs a Silver version bump.
6. Call `adapter.parse_listing_page()` on the stored body.
7. Derive canonical observations and their `observation_id`s exactly as the
   original acquisition did.
8. Compute one canonical content hash per observation using the same
   `serialize_for_wire` + sorted-key JSON + SHA-256 chain as
   `deduplicate_marketplace_observations`, so the reparse comparison and the
   batch dedup agree by construction.

The clock is injected and is used only for a reparse report timestamp — never
for an observation field. An observed time comes from the stored artifact or
the run fails; this is the same defect the `fix-tiki-clock` commit already
corrected once in the Tiki adapter, and a test pins it here.

### 12.3 Outcome statuses

- `IDENTICAL` — every observation ID and content hash matches the existing
  Silver record. This is the expected result and the one the Brief requires.
- `DIVERGED` — same IDs, different content. Report and write nothing.
- `NEW_OBSERVATIONS` — the reparse produced IDs absent from Silver, which means
  the original ingest lost rows. Report; writing them is a Phase 8 recovery
  decision, not an automatic one.
- `CHECKSUM_MISMATCH`, `PARSE_FAILED`, `ADAPTER_VERSION_CHANGED` — refuse.

### 12.4 CLI

```powershell
python -m crawler.reparse `
  --marketplace tiki `
  --observed-date 2026-09-20 `
  [--crawl-run-id <id>] `
  [--raw-artifact-id <id>] `
  [--silver-uri <uri>] `
  [--report-path <path>]
```

It prints one canonical JSON report — counts per status and, for divergences,
the bounded sorted list of observation IDs. It exits non-zero when any artifact
is not `IDENTICAL`. It never writes to Silver, Kafka, Gold or PostgreSQL.
Read-only by construction is what makes it safe to run against production data.

## 13. Batch replay and idempotency

No new module. These are behaviours the existing code must exhibit, each with a
test in Section 17:

- rerunning the same `run_id` with the same Silver produces identical Gold rows
  (Phase 6 already), identical quality results, and byte-identical manifest
  bytes;
- a rerun after a `QUALITY_FAILED` run is allowed with `--resume` and the same
  context, and can promote if the underlying data was fixed;
- a `SUCCEEDED` run ID still cannot restart;
- a publication failure leaves the previous cache version **and** the previous
  manifest pointer untouched — both are asserted in one test, because a
  rollback that restores the cache but advances the pointer is a worse state
  than either failure alone;
- promoting a manifest whose `as_of` is older than the current pointer is
  refused without `--allow-backfill`;
- reparsing the same raw artifact twice yields the same outcome, and neither
  run writes anything.

## 14. Orchestration and CLI

`run_marketplace_warehouse()` keeps its signature and gains behaviour:

```python
@dataclass(frozen=True)
class MarketplaceBatchResult:
    run_id: str
    status: str
    silver_rows: int
    dataset_counts: Mapping[str, int]
    quality_status: str                # PASS | FAIL
    mandatory_failure_count: int
    manifest_uri: str | None
    manifest_promoted: bool
```

Algorithm, replacing Phase 6 steps 6–9:

1. record `RUNNING`;
2. read, flatten, validate and deduplicate Silver; cache once;
3. read Phase 3 audit tables;
4. build all ten marts, including `price_anomaly_daily`;
5. write run-scoped Gold for all ten datasets;
6. mark `GOLD_WRITTEN` with URIs and counts;
7. evaluate quality gates and `decide()`;
8. **persist every quality result, pass or fail**;
9. read the current manifest pointer for `previous_run_id`; build and write the
   run manifest;
10. if the decision failed: mark `QUALITY_FAILED` with the manifest URI, do not
    promote, do not stage, do not publish, raise `QualityGateFailure`;
11. otherwise promote the pointer, then stage all ten marts and publish in one
    transaction with the decision, then clean up staging;
12. unpersist frames and stop owned Spark resources in `finally`;
13. on any other error, record `FAILED` best-effort and re-raise.

Step 8 precedes step 10 deliberately. Step 9 precedes step 10 deliberately.
Both exist so a refused run is fully documented in both PostgreSQL and object
storage before the refusal propagates.

New CLI flags on top of Phase 6's:

```powershell
python -m batch_layer.marketplace_warehouse `
  --run-id <id> --as-of <tz-aware-iso> `
  [--silver-uri <uri>] [--gold-root-uri <uri>] `
  [--resume] [--skip-postgres] `
  [--allow-backfill] `
  [--quality-only]
```

- `--allow-backfill` is passed through to `promote_manifest`.
- `--quality-only` runs steps 1–9 and then stops at `GOLD_WRITTEN`, printing the
  decision. It never promotes and never publishes. This is the flag an operator
  uses to inspect a suspect window without touching the serving version.
- `--skip-postgres` keeps its Phase 6 meaning and additionally skips quality
  persistence, since there is no database; the manifest is still written and,
  when the gate passes, promoted.

The process exits non-zero on `QualityGateFailure`, and the printed JSON names
the failing mandatory check names so a CI log is enough to diagnose the refusal.

## 15. Superset data-quality and audit dashboard

Create `display/superset/create_marketplace_quality_dashboard.py` following the
existing provisioning conventions, and register the new sources in
`display/superset/datasources.yaml`.

Sources: `cache.marketplace_price_anomaly_daily`, plus — as a documented
exception to the “Superset reads `cache`” convention —
`audit.marketplace_quality_result` and `audit.marketplace_batch_run`. The
exception is the point of the dashboard: when the gate blocks publication
nothing reaches `cache`, so a quality dashboard fed only from `cache` would go
blank exactly when it matters most. Register both audit tables read-only.

Minimum charts:

- quality run history: pass/fail per check over time;
- mandatory failures for the latest run, with expectation and observed value;
- batch run timeline by status, including `QUALITY_FAILED` as its own colour;
- published cache version versus latest run, showing the lag when a run is
  refused;
- price outlier counts per marketplace and day, split by method;
- outlier detail table: offer, day, evaluated price, baseline median, MAD or
  fence, status, reason, rule version;
- evaluation coverage: share of offer-days that were evaluable.

Labels must say “statistical price outlier versus this offer's own recent
observed history”, “mandatory quality check”, “publication blocked”, “last good
published version”. They must not say scam, fraud or incorrect price, and must
not claim a cross-offer or cross-marketplace comparison.

Kibana panels are Phase 8 and must not be added here.

## 16. Work that Phase 7 explicitly does not touch

Restating, because these are the boundaries that were violated in the
2026-09 incident:

- the nine Phase 6 mart grains, the Phase 6 total order, and the Phase 6
  `DATASET_COLUMNS` entries for those nine datasets;
- the Phase 5 speed layer, its change rules and its sinks;
- the Phase 4 wire schema and Silver sink;
- the Phase 3 scheduler, frontier and worker;
- `batch_layer/warehouse_job.py` and the legacy behavioural marts.

## 17. Required tests

Default tests use the frozen Silver JSONL fixture, a new frozen raw-artifact
fixture, a local Spark session, dict-backed fake object storage, fake DB
connections, and no network or service container.

### Anomaly tests — `tests/test_marketplace_anomaly.py`

1. exact median for odd and even sample counts, as Decimal;
2. MAD equals the median of absolute deviations on a hand-computed fixture;
3. p25/p75/IQR match the documented order-statistic definition;
4. the baseline frame excludes the evaluated day;
5. the baseline frame counts observed days, not calendar days, across a gap;
6. fewer than `min_samples` prior days yields `INSUFFICIENT_HISTORY` with null
   score and fences;
7. `mad > 0` selects `ROLLING_MAD` and the score sign is correct both ways;
8. a score exactly at the threshold is `NORMAL`; strictly beyond is anomalous;
9. `mad = 0` with `iqr > 0` selects `IQR_FALLBACK` and the fences are exact;
10. `mad = 0` and `iqr = 0` yields `INSUFFICIENT_DISPERSION`, never an anomaly;
11. a currency change restarts the baseline instead of mixing currencies;
12. every row carries window, min samples, thresholds and rule version;
13. money columns stay Decimal end to end; no float appears before persistence;
14. the mart has exactly one row per `(marketplace, offer_id, observed_date)`
    and joins one-to-one with `offer_price_history_daily`;
15. shuffling the input row order does not change any output value;
16. no module string contains scam, fraud, incorrect price or mispricing.

### Quality tests — `tests/test_marketplace_quality.py`

17. the registry rejects duplicate, malformed and unregistered check names;
18. exactly thirteen `MANDATORY` and four `ADVISORY` rules are registered;
19. every registered rule yields exactly one result, always;
20. each of the thirteen mandatory checks fails on a targeted corrupted fixture
    and passes on the clean fixture — thirteen focused cases, not one blanket
    assertion;
21. check 6 uses `context.as_of` and its tolerance boundary is inclusive;
22. check 7 rejects a currency outside the allowlist and a malformed code;
23. check 8 reports `SKIPPED` when the audit frames are empty, and `decide()`
    treats that skip as a gate failure;
24. check 8 reconciles only over crawl run IDs present in this run's Silver;
25. check 12 recomputes from Silver and catches a mart aggregate that was
    tampered with;
26. an advisory failure alone leaves `decision.passed` true;
27. a missing mandatory result makes `decide()` fail;
28. `observed_value` is numeric for every check, and rates stay in `[0, 1]`;
29. failure samples are bounded, sorted, identifier-only, and contain no raw
    body, title, URL query, credential or stack trace;
30. evaluation opens no database, no object store and no network;
31. evaluation reads no wall clock; `checked_at` equals `context.as_of`.

### Manifest tests — `tests/test_marketplace_manifest.py`

32. the manifest lists exactly the ten datasets, sorted, with URIs and counts;
33. serialisation is canonical and byte-stable across two identical builds;
34. `parse_manifest(serialize_manifest(m)) == m` round-trips;
35. a failed decision writes the run manifest and does not touch the pointer;
36. a passed decision writes the pointer byte-identical to the run manifest;
37. `previous_run_id` chains to the pointer that was current before promotion;
38. promoting the already-current run is idempotent and writes nothing;
39. an older `as_of` is refused with `BACKFILL_REFUSED` and allowed with
    `allow_backfill=True`;
40. `created_at` equals `as_of`, and no wall clock appears anywhere in the bytes;
41. `get_bytes` returning `None` means “no current manifest”; a transport error
    propagates and does not become an implicit first promotion.

### Reparse tests — `tests/test_marketplace_reparse.py`

42. a checksum mismatch stops before parsing and the adapter is never called;
43. an unknown metadata schema version is rejected;
44. an adapter version change is reported, not parsed into Silver shape;
45. reparsing the frozen raw fixture reproduces the original observation IDs;
46. reparsing twice yields identical content hashes and writes nothing;
47. `diff_reparsed_observations` returns sorted diverged IDs and empty on match;
48. a new observation ID absent from Silver is reported as `NEW_OBSERVATIONS`,
    not written;
49. the injected clock never reaches an observation field;
50. the CLI exits non-zero when any artifact is not `IDENTICAL`.

### Gated publication and replay — existing test files

51. `publish()` raises when the decision failed, before any lock or truncate;
52. `publish()` raises when the decision's run ID does not match;
53. a failed gate means `stage()` is never called at all;
54. quality results are persisted even when the gate fails;
55. `QUALITY_FAILED` is recorded with `cache_published` and `manifest_promoted`
    both false and `completed_at` set;
56. a publication failure preserves both the previous cache version and the
    previous manifest pointer;
57. all ten staging tables validate before any cache table is truncated;
58. rerunning the same context reproduces identical quality results and
    byte-identical manifest bytes;
59. `--quality-only` writes Gold and the manifest, and never promotes or
    publishes;
60. a `SUCCEEDED` run ID still cannot restart, and `--resume` still requires an
    identical context;
61. Phase 6's nine mart grains and the legacy behavioural warehouse tests are
    unchanged and pass.

Optional tests marked `integration` may publish to a temporary PostgreSQL
database only when `TEST_POSTGRES_URL` exists. Add one integration test proving
a forced insert failure preserves the previous cache version and the previous
pointer together.

## 18. Verification commands

Run after each work package, then:

```powershell
python -m pytest `
  tests/test_marketplace_anomaly.py `
  tests/test_marketplace_quality.py `
  tests/test_marketplace_manifest.py `
  tests/test_marketplace_reparse.py `
  tests/test_marketplace_marts.py `
  tests/test_marketplace_postgres.py `
  tests/test_marketplace_warehouse.py `
  tests/test_counter_semantics.py `
  tests/test_warehouse_transform.py `
  -q
python -m pytest tests -q
python -m batch_layer.marketplace_warehouse --help
python -m crawler.reparse --help
git diff --check
git diff --stat
```

Language check — must print nothing:

```powershell
Select-String -Pattern 'scam|fraud|fake price|incorrect price|mispric' `
  -Path batch_layer/marketplace_anomaly.py, batch_layer/marketplace_quality.py, `
        display/superset/create_marketplace_quality_dashboard.py, scripts/init_postgres.sql
```

Also run an offline local-file smoke with the frozen Silver fixture and
`--skip-postgres`, then rerun the identical command and diff the two manifest
files; they must be byte-identical.

## 19. Commit/work-package sequence

1. `feat: add robust rolling price anomaly mart`
   - `marketplace_anomaly.py`, config, mart wiring to ten keys, anomaly tests.
2. `feat: add marketplace data quality rule registry`
   - `config/quality_rules.py` and registry tests only.
3. `feat: evaluate mandatory marketplace quality gates`
   - `marketplace_quality.py`, the seventeen checks and their tests.
4. `feat: persist marketplace quality results`
   - DDL, `MarketplaceQualityRepository`, `mark_quality_failed`, tests.
5. `feat: add gold publish manifest and promotion`
   - `marketplace_manifest.py`, `common/object_store.get_bytes`, tests.
6. `feat: gate cache publication on the quality decision`
   - tenth dataset in `DATASET_COLUMNS`, cache DDL, gated `publish()`,
     updated Phase 6 publisher tests.
7. `feat: wire quality gate into batch orchestration`
   - orchestration steps 7–11, new CLI flags, result fields, replay tests.
8. `feat: add raw reparse verification workflow`
   - `crawler/reparse.py`, raw fixture, CLI, reparse tests.
9. `feat: add marketplace quality and audit dashboard`
   - datasource registration and dashboard script only.
10. `test: verify phase 7 replay, promotion and compatibility`
    - the full Phase 7 matrix, the language check and compatibility fixes only.

Stop after each package and show the focused tests plus `git diff --stat`. Do
not implement Phase 8 inside these commits. Keep a production fix in its own
commit, separate from the test that found it.

## 20. Definition of Done

- [ ] the dependency gate is recorded and both Section 2.1 deviations are
      reviewed and accepted or overruled;
- [ ] `price_anomaly_daily` exists as the tenth Gold mart with the Section 6.4
      grain and columns;
- [ ] the baseline excludes the evaluated day and never mixes currencies;
- [ ] median/MAD/p25/p75 are exact order statistics, not approximations;
- [ ] IQR fallback triggers only when MAD is zero, and zero dispersion is not
      reported as an anomaly;
- [ ] minimum sample size, thresholds and rule version are stored per row;
- [ ] no anomaly wording claims scam, fraud or incorrect price;
- [ ] exactly thirteen mandatory and four advisory checks are registered and
      always produce a result;
- [ ] a skipped mandatory check fails the gate;
- [ ] quality results persist with observed value, expectation, status,
      severity, rule version and run ID — including on failure;
- [ ] failure samples carry identifiers only;
- [ ] the run manifest is written for every run and is byte-stable across an
      identical rerun;
- [ ] the pointer advances only on a passing decision, is idempotent, and
      refuses a backward `as_of` without `--allow-backfill`;
- [ ] `publish()` cannot be called without a matching passing decision;
- [ ] a refused run leaves the previous cache and the previous pointer
      untouched, and is recorded as `QUALITY_FAILED`, not `FAILED`;
- [ ] reparsing a raw artifact verifies the checksum before parsing and proves
      the same observation IDs and content hashes;
- [ ] the reparse CLI writes nothing anywhere;
- [ ] the Superset dashboard shows a blocked run without depending on `cache`;
- [ ] Phase 6's nine marts, the Phase 5 speed layer and the legacy behavioural
      warehouse are unchanged and still pass;
- [ ] focused tests pass without external services; full suite passes or the
      environment-only failures are documented.

## 21. Mandatory rejection conditions

Reject the implementation if it:

- uses `percentile_approx` or any approximate statistic for the baseline;
- computes the baseline from a window that includes the evaluated day;
- compares prices across offers, sellers, marketplaces or currencies;
- reports an anomaly when the baseline has zero dispersion or too few samples;
- describes a price outlier as scam, fraud, fake or an incorrect price;
- converts money to float before a comparison or before persistence;
- lets a mandatory check be skipped, omitted or defaulted into a pass;
- persists quality results only on success;
- writes a raw body, title, URL query, credential or stack trace into a failure
  sample or an error column;
- promotes the manifest pointer before the decision, or on a failed decision;
- deletes or rewrites a previous run manifest;
- publishes to `cache` without a matching passing `QualityDecision`;
- truncates any cache table before all ten staging tables validate;
- records a refused run as `FAILED` and loses the distinction from a crash;
- parses a raw artifact whose checksum does not match;
- lets the reparse workflow write to Silver, Kafka, Gold or PostgreSQL;
- uses a wall clock for any manifest field or any observation field;
- starts Kibana dashboards, Compose profiles, failure drills or benchmarks;
- changes a Phase 6 mart grain, the Phase 5 speed layer or the legacy warehouse.

## 22. Copy-ready prompts for a low-capability model

### Prompt A — anomaly mart

```text
Implement only Section 6 of
docs/PHASE_7_QUALITY_ANOMALY_REPLAY_IMPLEMENTATION_PLAN.md. Create
batch_layer/marketplace_anomaly.py building price_anomaly_daily from
offer_price_history_daily with an exact rolling median, MAD and IQR fallback.
Exclude the evaluated day from the baseline. Keep every money value Decimal.
Never use percentile_approx and never use scam/fraud/incorrect-price wording.
Add price_anomaly_daily to build_marketplace_marts so it returns ten keys. Run
focused tests, show diff stat and stop.
```

### Prompt B — rule registry

```text
Implement only Section 7 of the Phase 7 plan. Create config/quality_rules.py
with exactly thirteen MANDATORY and four ADVISORY rules using the table's check
names and expectations verbatim. Add registry validation and tests. Do not
write any evaluation logic yet. Run focused tests and stop.
```

### Prompt C — quality evaluation

```text
Implement Section 8 and the seventeen checks from Section 7 of the Phase 7
plan. Create batch_layer/marketplace_quality.py as pure transforms: no
database, no object store, no clock, checked_at equals context.as_of. Every
registered rule returns exactly one result; a check that cannot run returns
SKIPPED and decide() treats a skipped mandatory check as a failure. Bound
failure samples to identifiers only. Run focused tests and stop.
```

### Prompt D — quality persistence

```text
Implement Section 9 of the Phase 7 plan. Add audit.marketplace_quality_result
DDL, the ALTER statements for audit.marketplace_batch_run, the
MarketplaceQualityRepository upsert and mark_quality_failed. Persistence runs in
its own transaction before any publication decision is acted on. Use fake DB
connections in tests. Do not change the publisher yet. Run focused tests and
stop.
```

### Prompt E — manifest

```text
Implement Section 11 of the Phase 7 plan. Create
batch_layer/marketplace_manifest.py and add get_bytes to common/object_store.py.
Canonical JSON, created_at equals as_of, no wall clock anywhere. Write the run
manifest always; move the pointer only on a passing decision; idempotent
re-promotion; refuse a backward as_of without allow_backfill. Use a dict-backed
fake store in tests. Run focused tests and stop.
```

### Prompt F — gated publisher

```text
Implement Section 10 of the Phase 7 plan. Add price_anomaly_daily to
DATASET_COLUMNS and its cache table DDL, change every "nine" to "ten", and make
publish() require a QualityDecision that it validates before the advisory lock.
Update the three Phase 6 publisher tests to pass a passing decision. Never give
the quality argument a default. Run focused tests and stop.
```

### Prompt G — orchestration

```text
Implement Section 14 of the Phase 7 plan. Wire evaluation, persistence,
manifest write, promotion and gated publication into run_marketplace_warehouse
in the documented order, add the new result fields and the --allow-backfill and
--quality-only flags, and raise QualityGateFailure with a non-zero exit. A
refused run must persist results and write its manifest first. Run focused
tests and stop.
```

### Prompt H — reparse

```text
Implement Section 12 of the Phase 7 plan. Create crawler/reparse.py with the
mandatory order: metadata, body, checksum verification before any parse,
request rebuilt from metadata, adapter version match, then parse. The workflow
is read-only and writes nothing. Add the frozen raw fixture and the CLI. Run
focused tests and stop.
```

### Prompt I — dashboard and final verification

```text
Implement Section 15 and the Section 18 verification for Phase 7. Register the
anomaly cache table and the two audit tables, create the quality dashboard with
the required labels, run the full matrix, run the forbidden-wording check, run
the double-run manifest byte comparison, document environment-only skips, show
diff stat and stop. Do not add Kibana panels, Compose profiles or benchmarks.
```

## 23. Handoff to Phase 8

Phase 8 (Tuần 8, P1-12 plus the Kibana half of P1-11) takes reliability and
operations: Compose profiles for crawler, sink, speed and batch; one-command
start, smoke and validate; failure drills; the Kibana source-health and
freshness dashboard; a backup and export procedure; and refreshed Architecture
and Data Model docs.

Phase 8 inherits from Phase 7:

- `QUALITY_FAILED` as a first-class operational state that a drill must be able
  to produce deliberately and recover from;
- the manifest pointer as the single definition of “the last good published
  version”, which the backup and restore procedure must preserve;
- `crawler/reparse.py` as the read-only verification step in the recovery
  runbook.

Still owed from Phase 6: items 33–35 and 43 need a real end-to-end
`run_marketplace_warehouse` against MinIO and PostgreSQL rather than unit
tests. Phase 7 does not close them; run them together once Docker is available,
now with the gate and the manifest in place so one run verifies both phases.
