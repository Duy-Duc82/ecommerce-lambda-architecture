# Phase 3 implementation plan — Restart-safe crawl scheduling, retry and audit

> Status: ready for implementation after Phase 2 acceptance
>
> Intended implementer: a low-capability coding model working one work package
> at a time. It must not redesign the architecture or start Phase 4.
>
> Parent plan: `Brief để xây dựng plan sơ bộ cho việc cải tiến ecommerce-lambda-architecture.md`

## 1. Objective

Turn the accepted Phase 2 one-shot raw-first acquisition flow into a
restart-safe scheduled worker:

```text
configured target
-> persistent PostgreSQL frontier
-> transactional lease
-> Phase 2 raw-first acquisition
-> bounded retry or terminal result
-> crawl-run/request audit
-> next static schedule
```

This phase implements parent-plan Week 3 and backlog P0-08, P0-09 and P0-10.
It does not publish canonical observations to Kafka; that is Phase 4.

## 2. Mandatory dependency gate

Before editing, the implementer must verify all of the following:

1. Phase 1 serializer, identities and marketplace contracts exist and pass.
2. Phase 2 has an accepted raw-first acquisition API.
3. Phase 2 persists response bytes and metadata before parsing.
4. A Phase 2 outcome exposes enough information to audit:
   marketplace, target, page/resource, RawArtifact when a response existed,
   success/failure stage and canonical observation count.

If Phase 2 is absent, incomplete or its public names differ from the names used
below, stop and report the exact mismatch. Do not recreate Phase 2 inside this
phase. Only import-path adaptation is allowed after the reviewer identifies the
accepted Phase 2 API.

## 3. Scope

### 3.1 In scope

- PostgreSQL-backed crawl frontier.
- Deterministic scheduled task IDs.
- Transactional task leasing with `FOR UPDATE SKIP LOCKED`.
- Expired-lease recovery after process restart.
- Static ACTIVE/NORMAL/COLD cadence tiers.
- Persistent bounded retry scheduling; no worker `sleep()` for backoff.
- Error taxonomy for network, HTTP, robots, storage, parser and validation.
- `Retry-After` handling for integer seconds and HTTP-date values.
- A small persistent source circuit breaker.
- CrawlRun and per-attempt audit tables/repositories.
- A worker loop with injected clock/executor for offline unit tests.
- One source only. A second source remains gated by feasibility evidence.

### 3.2 Out of scope

- Kafka producer, topics, DLQ or Silver sink.
- Spark, Elasticsearch, Redis, Gold marts or dashboards.
- Adaptive/ML scheduling.
- Distributed crawler frameworks.
- Anti-bot bypass, proxy rotation or CAPTCHA handling.
- Changing the Phase 2 fetch/parse/raw-first ordering.
- Product matching or a second marketplace.
- Docker Compose application services and production daemon management.

## 4. Allowed file changes

Create:

```text
crawler/scheduling.py
crawler/frontier.py
crawler/audit.py
crawler/worker.py
tests/test_crawl_scheduling.py
tests/test_crawl_frontier.py
tests/test_crawl_worker.py
```

Modify only:

```text
config/settings.py
scripts/init_postgres.sql
crawler/runner.py
```

`crawler/runner.py` may receive only a thin adapter that lets the worker invoke
the accepted Phase 2 acquisition function. Do not move scheduling SQL into the
runner and do not rewrite Phase 2.

## 5. Fixed terminology and enums

Implement these exact string enums in `crawler/scheduling.py`:

```python
class CrawlTier(str, Enum):
    ACTIVE = "ACTIVE"
    NORMAL = "NORMAL"
    COLD = "COLD"

class CrawlTaskStatus(str, Enum):
    READY = "READY"
    LEASED = "LEASED"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DISABLED = "DISABLED"

class FailureKind(str, Enum):
    RATE_LIMITED = "RATE_LIMITED"
    TRANSIENT_NETWORK = "TRANSIENT_NETWORK"
    SERVER_ERROR = "SERVER_ERROR"
    CLIENT_ERROR = "CLIENT_ERROR"
    ROBOTS_DENIED = "ROBOTS_DENIED"
    STORAGE_ERROR = "STORAGE_ERROR"
    PARSE_ERROR = "PARSE_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    UNKNOWN = "UNKNOWN"
```

Do not add purchase, fraud, demand or anti-bot semantics.

## 6. Configuration

Add environment-backed values to `config/settings.py` with these defaults:

```python
CRAWL_ACTIVE_CADENCE_MINUTES = 60
CRAWL_NORMAL_CADENCE_MINUTES = 240
CRAWL_COLD_CADENCE_MINUTES = 720
CRAWL_LEASE_SECONDS = 300
CRAWL_MAX_ATTEMPTS = 5
CRAWL_RETRY_BASE_SECONDS = 30
CRAWL_RETRY_MAX_SECONDS = 1800
CRAWL_RETRY_JITTER_RATIO = 0.20
CRAWL_CIRCUIT_FAILURE_THRESHOLD = 5
CRAWL_CIRCUIT_OPEN_SECONDS = 900
CRAWL_WORKER_POLL_SECONDS = 5
CRAWL_WORKER_BATCH_SIZE = 10
```

Validate positive integer values and require jitter ratio from `0` through `1`
inside the policy dataclass, not at module import. Tests monkeypatch settings.

## 7. Scheduling contracts and pure functions

### 7.1 CrawlTask

Use a frozen dataclass:

```python
@dataclass(frozen=True)
class CrawlTask:
    task_id: str
    marketplace_code: str
    marketplace_id: str
    target: str
    resource_type: ResourceType
    tier: CrawlTier
    priority: int
    scheduled_for: datetime
    status: CrawlTaskStatus
    attempts: int
    max_attempts: int
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    last_http_status: int | None = None
    last_error_kind: FailureKind | None = None
    last_error: str | None = None
    last_success_at: datetime | None = None
```

Rules:

- all timestamps are aware and normalized to UTC;
- IDs, marketplace and target are required;
- priority, attempts and max attempts are non-negative integers, not bool;
- max attempts is at least one and attempts cannot exceed max attempts;
- LEASED requires lease owner and expiry;
- every non-LEASED state requires both lease fields to be None;
- status and enum fields require real enum values.

### 7.2 RetryPolicy and RetryDecision

Use frozen dataclasses:

```python
@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    base_seconds: int
    max_seconds: int
    jitter_ratio: Decimal

@dataclass(frozen=True)
class RetryDecision:
    retryable: bool
    failure_kind: FailureKind
    delay_seconds: int | None
    next_attempt_at: datetime | None
```

### 7.3 Public pure API

Implement exactly:

```python
def make_crawl_task_id(
    marketplace_code: str,
    resource_type: ResourceType,
    target: str,
    scheduled_for: datetime,
) -> str: ...

def cadence_minutes(tier: CrawlTier) -> int: ...

def classify_failure(
    error: Exception,
    http_status: int | None = None,
) -> FailureKind: ...

def parse_retry_after(value: str | None, now: datetime) -> int | None: ...

def decide_retry(
    *,
    error: Exception,
    http_status: int | None,
    retry_after: str | None,
    attempts: int,
    now: datetime,
    policy: RetryPolicy,
    jitter_unit: Decimal,
) -> RetryDecision: ...
```

Task identity is:

```python
deterministic_id(
    "task",
    marketplace_code.strip().lower(),
    resource_type,
    target.strip(),
    scheduled_for,
)
```

The scheduler must pass a cadence-bucket timestamp as `scheduled_for`; do not
truncate time inside `make_crawl_task_id()`.

### 7.4 Retry classification

Retryable:

- timeout, connection reset and temporary DNS/network errors;
- HTTP 408, 425, 429;
- HTTP 500 through 599;
- temporary object-store connection/service errors only.

Never retry automatically:

- robots denial;
- HTTP 400–499 except 408, 425 and 429;
- parser/schema-drift errors;
- canonical validation errors;
- programming errors and unknown exceptions.

Retry delay before jitter:

```text
min(max_seconds, base_seconds * 2 ** (attempts - 1))
```

`attempts` is the already-consumed attempt count and starts at one. Apply
deterministic testable jitter in range `[-jitter_ratio, +jitter_ratio]` using
the injected `jitter_unit` in `[0, 1]`. Round up to a whole second and never
return less than one second. A valid Retry-After value overrides exponential
delay but is capped at `max_seconds`.

## 8. PostgreSQL DDL

Append non-destructive DDL to `scripts/init_postgres.sql`. Use schema `audit`;
PostgreSQL remains operational metadata, not analytical truth.

### 8.1 `audit.crawl_frontier`

Required columns:

```sql
task_id VARCHAR(80) PRIMARY KEY,
marketplace_code VARCHAR(32) NOT NULL,
marketplace_id VARCHAR(128) NOT NULL,
target TEXT NOT NULL,
resource_type VARCHAR(32) NOT NULL,
tier VARCHAR(16) NOT NULL,
priority INTEGER NOT NULL DEFAULT 0,
scheduled_for TIMESTAMPTZ NOT NULL,
status VARCHAR(16) NOT NULL,
attempts INTEGER NOT NULL DEFAULT 0,
max_attempts INTEGER NOT NULL,
lease_owner VARCHAR(128),
lease_expires_at TIMESTAMPTZ,
last_http_status INTEGER,
last_error_kind VARCHAR(32),
last_error TEXT,
last_success_at TIMESTAMPTZ,
created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
```

Add CHECK constraints for enums, non-negative counters, HTTP status, and lease
field consistency. Add:

```sql
CREATE INDEX IF NOT EXISTS idx_crawl_frontier_due
ON audit.crawl_frontier (status, scheduled_for, priority DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_frontier_schedule
ON audit.crawl_frontier (marketplace_code, resource_type, target, scheduled_for);
```

### 8.2 `audit.crawl_run`

Mirror the Phase 1 CrawlRun fields using TIMESTAMPTZ and BIGINT. Primary key is
`crawl_run_id`. Store `error_summary` as JSONB. Add status CHECK for RUNNING,
COMPLETED, PARTIAL and FAILED.

### 8.3 `audit.crawl_request_attempt`

Required columns:

```sql
attempt_id BIGSERIAL PRIMARY KEY,
crawl_run_id VARCHAR(128) NOT NULL REFERENCES audit.crawl_run(crawl_run_id),
task_id VARCHAR(80) NOT NULL REFERENCES audit.crawl_frontier(task_id),
attempt_number INTEGER NOT NULL,
started_at TIMESTAMPTZ NOT NULL,
completed_at TIMESTAMPTZ NOT NULL,
status VARCHAR(16) NOT NULL,
http_status INTEGER,
latency_ms BIGINT,
raw_artifact_id VARCHAR(80),
raw_uri TEXT,
raw_bytes BIGINT NOT NULL DEFAULT 0,
parsed_count BIGINT NOT NULL DEFAULT 0,
rejected_count BIGINT NOT NULL DEFAULT 0,
error_kind VARCHAR(32),
error_message TEXT
```

Add non-negative and timestamp-order CHECK constraints. Do not store raw body,
cookies, authorization headers or stack traces in PostgreSQL.

### 8.4 `audit.crawl_source_state`

Use `marketplace_code` primary key plus `consecutive_failures`, `opened_until`,
`last_failure_at`, `last_success_at` and `updated_at`. Circuit state is open
only while `opened_until > now`.

## 9. Frontier repository

`crawler/frontier.py` must accept a DB-API connection factory; importing the
module must not open a database connection.

Implement this public class:

```python
class PostgresCrawlFrontier:
    def __init__(self, connection_factory: Callable[[], Any]): ...
    def enqueue(self, task: CrawlTask) -> bool: ...
    def lease_due(
        self, *, worker_id: str, now: datetime,
        lease_seconds: int, limit: int,
    ) -> list[CrawlTask]: ...
    def mark_succeeded(
        self, *, task_id: str, worker_id: str,
        completed_at: datetime, next_scheduled_for: datetime,
    ) -> None: ...
    def mark_retry(
        self, *, task_id: str, worker_id: str,
        decision: RetryDecision, http_status: int | None,
        error_message: str,
    ) -> None: ...
    def mark_failed(
        self, *, task_id: str, worker_id: str,
        completed_at: datetime, failure_kind: FailureKind,
        http_status: int | None, error_message: str,
    ) -> None: ...
    def recover_expired_leases(self, *, now: datetime) -> int: ...
```

Rules:

- every method owns one transaction and closes cursor/connection reliably;
- enqueue uses `ON CONFLICT DO NOTHING` and returns whether inserted;
- lease query selects READY/RETRY_WAIT tasks due at or before `now`, plus
  expired LEASED tasks, ordered by priority descending then scheduled time;
- lease uses one CTE with `FOR UPDATE SKIP LOCKED`, then UPDATE ... RETURNING;
- leasing increments attempts exactly once;
- completion UPDATE includes `WHERE task_id = %s AND status = 'LEASED' AND
  lease_owner = %s`; if rowcount is not one, raise `LeaseLostError`;
- success creates the next READY schedule as a new deterministic task and marks
  the leased occurrence SUCCEEDED in the same transaction;
- retry reuses the same task row with RETRY_WAIT and decision time;
- terminal failure clears lease fields;
- error messages are truncated to 2000 characters;
- SQL values are parameters. Never interpolate target/error text into SQL.

## 10. Audit repository

`crawler/audit.py` exposes:

```python
class CrawlAuditRepository:
    def start_run(self, run: CrawlRun) -> None: ...
    def record_attempt(self, attempt: CrawlAttemptAudit) -> None: ...
    def finish_run(self, run: CrawlRun) -> None: ...
    def source_is_open(self, marketplace_code: str, now: datetime) -> bool: ...
    def record_source_success(self, marketplace_code: str, at: datetime) -> None: ...
    def record_source_failure(
        self, marketplace_code: str, at: datetime,
        *, threshold: int, open_seconds: int,
    ) -> None: ...
```

Define frozen `CrawlAttemptAudit` in the same module with fields matching the
attempt table. Validate UTC, counts and completion ordering before SQL.

Source success resets consecutive failures and closes the circuit. Only
retryable acquisition failures increment the source failure streak. Parser and
validation errors are data-quality problems and must not open the source
circuit.

## 11. Worker behavior

`crawler/worker.py` must contain no source-specific parsing. Define an injected
executor protocol/callable around accepted Phase 2:

```python
class AcquisitionExecutor(Protocol):
    def __call__(
        self, *, task: CrawlTask, crawl_run_id: str
    ) -> Phase2AcquisitionReport: ...
```

The thin adapter in `crawler/runner.py` converts the accepted Phase 2 outcome(s)
to the report. Do not duplicate raw persistence or parser code.

Worker algorithm for one polling cycle:

1. recover expired leases;
2. lease at most configured batch size;
3. skip execution when source circuit is open; reschedule without incrementing
   source failure count;
4. start a CrawlRun using caller-supplied/injected ID and time;
5. call Phase 2 executor once for the leased task;
6. write attempt audit;
7. on success, mark task success and enqueue next tier cadence;
8. on retryable failure with attempts remaining, persist RETRY_WAIT;
9. otherwise mark terminal FAILED;
10. update source circuit state;
11. finish CrawlRun as COMPLETED, PARTIAL or FAILED.

Do not hold a database transaction open during network/storage acquisition.
Do not sleep inside `run_once()`. An outer CLI loop may sleep only for the poll
interval. Catching `KeyboardInterrupt` must finish no in-flight task falsely.

## 12. Required tests

All unit tests use fixed UTC times, fake connections/repositories/executors and
zero real sleep.

### Scheduling tests

1. task IDs are deterministic and have `task_<64 hex>` shape;
2. changing target/bucket/resource changes ID;
3. timezone-equivalent buckets create the same ID;
4. naive timestamps fail;
5. tier cadences use configured values;
6. transient network, 408/425/429 and 5xx are retryable;
7. ordinary 4xx, robots, parse and validation failures are terminal;
8. exponential delay is capped;
9. jitter lower/upper bounds are deterministic;
10. Retry-After seconds and HTTP date work and are capped;
11. exhausted attempts never retry.

### Frontier tests

12. enqueue is idempotent;
13. lease SQL contains `FOR UPDATE SKIP LOCKED` and increments attempts;
14. due ordering is priority then time;
15. expired leases can be leased again;
16. a non-expired lease cannot be stolen;
17. wrong worker completion raises LeaseLostError;
18. success marks occurrence and enqueues exactly one next task atomically;
19. retry clears lease and stores next attempt/error;
20. terminal failure clears lease;
21. hostile target/error values are passed as SQL parameters.

### Audit/worker tests

22. CrawlAttemptAudit rejects naive times, negative counts and reversed times;
23. successful execution writes raw/parse counts and completes the run;
24. storage/network failure follows retry policy;
25. parser/validation failure is terminal and does not open source circuit;
26. repeated retryable source failures open the circuit at the threshold;
27. source success closes/reset circuit;
28. open circuit prevents acquisition call;
29. one task failure does not prevent another leased task from running;
30. no test opens network, Kafka, MinIO or PostgreSQL.

Add one optional PostgreSQL integration test marked `@pytest.mark.integration`.
It may run only when `TEST_POSTGRES_URL` exists. Default unit test execution must
not require Docker.

## 13. Verification commands

Run after every work package:

```powershell
python -m pytest tests/test_crawl_scheduling.py -q
python -m pytest tests/test_crawl_frontier.py -q
python -m pytest tests/test_crawl_worker.py -q
python -m pytest `
  tests/test_serialization.py `
  tests/test_identity.py `
  tests/test_marketplace_schema.py `
  tests/test_crawler.py `
  tests/test_crawl_scheduling.py `
  tests/test_crawl_frontier.py `
  tests/test_crawl_worker.py `
  -q
python -m pytest tests -q
git diff --check
git diff --stat
```

Report missing declared dependencies as environment failures. Do not fix legacy
ML/Spark tests in this phase.

## 14. Commit/work-package sequence

1. `feat: add crawl scheduling and retry contracts`
   - `crawler/scheduling.py`, scheduling tests only.
2. `feat: add persistent crawl frontier`
   - DDL, `crawler/frontier.py`, frontier tests only.
3. `feat: add crawl run and source audit`
   - `crawler/audit.py`, audit portion of tests only.
4. `feat: add restart-safe crawl worker`
   - worker and thin Phase 2 adapter only.
5. `test: verify scheduler recovery and retry policy`
   - compatibility/integration test fixes only.

The model must stop after each numbered package. A reviewer decides whether to
start the next one.

## 15. Definition of Done

- [ ] dependency gate is recorded;
- [ ] frontier survives process restart;
- [ ] competing workers cannot lease the same task concurrently;
- [ ] expired leases are recoverable;
- [ ] attempts are bounded and persistent;
- [ ] backoff is scheduled, not implemented as blocking sleep;
- [ ] Retry-After is respected within the configured cap;
- [ ] terminal parse/validation/robots failures are not retried;
- [ ] source circuit breaker is persistent and reset by success;
- [ ] every attempt and CrawlRun is auditable without secrets/raw bodies in DB;
- [ ] Phase 2 raw-first ordering remains unchanged;
- [ ] no Kafka/Spark/downstream layer is added;
- [ ] all focused tests pass without external services;
- [ ] existing tests pass or environment failures are documented.

## 16. Copy-ready prompts for a low-capability model

### Prompt A — scheduling and retry

```text
Implement only Sections 5–7 and scheduling tests from
docs/PHASE_3_SCHEDULER_RETRY_AUDIT_IMPLEMENTATION_PLAN.md. Read the full plan
and accepted Phase 2 API first. Create only crawler/scheduling.py and
tests/test_crawl_scheduling.py; modify config/settings.py only for the listed
settings. Use fixed UTC times and injected jitter. Run the focused tests and
stop. Do not add SQL, worker, Kafka or Spark code.
```

### Prompt B — frontier

```text
Implement only Sections 8.1 and 9 of the Phase 3 plan. Read completed
crawler/scheduling.py first. Modify scripts/init_postgres.sql; create
crawler/frontier.py and tests/test_crawl_frontier.py. Use parameterized SQL,
one transaction per method and FOR UPDATE SKIP LOCKED. Use fake DB-API objects
for default tests. Run focused tests and stop. Do not implement audit or worker.
```

### Prompt C — audit

```text
Implement only Sections 8.2–8.4 and 10 of the Phase 3 plan. Create
crawler/audit.py and its tests; add only the specified idempotent audit DDL.
Use injected DB connections and fixed UTC times. Run focused tests and stop.
Do not modify Phase 2 acquisition or start the worker/Kafka work.
```

### Prompt D — worker and compatibility

```text
Implement Sections 11–15 of the Phase 3 plan using the completed scheduling,
frontier and audit modules. Add crawler/worker.py and only a thin integration
adapter in crawler/runner.py. Do not duplicate Phase 2 fetch, raw persistence
or parsing. Add all worker tests, run the full Phase 3 matrix and stop. Do not
create Kafka topics, publishers, Spark schemas or Silver writers.
```

## 17. Handoff to Phase 4

Phase 4 consumes only successful, validated Phase 2 observation envelopes from
the Phase 3 worker execution path. Scheduling/retry must not depend on Kafka
availability; a later publish failure is a Phase 4 sink failure and must be
audited without re-fetching an already persisted raw response.
