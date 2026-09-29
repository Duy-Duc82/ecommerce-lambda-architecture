# Phase 5 implementation plan — Stateful marketplace speed layer

> Status: ready for implementation after Phase 4 acceptance
>
> Intended implementer: a low-capability coding model working one work package
> at a time. It must follow the fixed contracts below and stop between packages.
>
> Parent plan: `Brief để xây dựng plan sơ bộ cho việc cải tiến ecommerce-lambda-architecture.md`

## 1. Objective

Implement the parent plan's Week 5 vertical slice and backlog P1-05 and P1-06:

```text
marketplace.observations.v1
-> schema/version/key validation
-> per-offer ordered state
-> bounded MarketplaceChangeV1 events
-> marketplace.changes.v1
-> deterministic Elasticsearch + Redis projections
-> checkpoint and micro-batch audit
-> Kibana realtime draft
```

The question answered by this phase is **“what just changed?”** It does not
replace immutable observations or build authoritative historical marts.

## 2. Mandatory dependency gate

Before editing, record and verify all of the following:

1. Phase 1 `MarketplaceObservationV1`, `MarketplaceChangeV1`, enums and
   deterministic IDs pass their accepted tests.
2. Phase 4 registers the exact observation and change topics.
3. Phase 4 exposes `MARKETPLACE_OBSERVATION_WIRE_SCHEMA` and its field
   names/nullability match the accepted event envelope.
4. Valid observations are keyed by
   `f"{marketplace.lower()}:{platform_listing_id}"`.
5. Phase 4 keeps raw/Silver observations as immutable history; Phase 5 is an
   independent Kafka consumer and cannot delete or rewrite that history.

If a public import differs, adapt only the import after a reviewer identifies
the accepted Phase 4 name. Do not recreate Phase 4 producer, Silver sink, DLQ
or schemas here.

## 3. Scope

### 3.1 In scope

- A strict wire decoder/serializer for the existing `MarketplaceChangeV1`.
- Pure, offline-testable previous/current comparison rules.
- Deterministic rules for new offer, price, rating, public counter,
  availability and stale changes.
- Spark Structured Streaming consumption of the Phase 4 observation topic.
- Per-offer state with deterministic event-time ordering and duplicate/late
  suppression.
- A versioned checkpoint location; no reuse of the legacy behavior checkpoint.
- Acknowledged publication to the registered change topic.
- Idempotent Elasticsearch documents keyed by change/offer ID.
- Idempotent Redis latest-state and recent-change projections.
- Persistent micro-batch audit in PostgreSQL.
- A small Kibana data view/dashboard draft for marketplace changes.
- Offline tests plus explicitly gated service/streaming integration tests.

### 3.2 Out of scope

- Reading Silver to build historical Gold marts; that is Phase 6.
- Price anomaly, MAD/IQR, data-quality gates or Gold publish manifest; those are
  Phase 7.
- Crawl scheduling, source retry, parser or raw persistence changes.
- Product/entity matching or cross-market comparisons.
- Purchase, demand, fraud, scam or inferred sales events.
- A custom frontend or FastAPI service.
- Replacing the legacy behavior speed layer.
- Compose profiles, one-command operations and failure drills; those are Phase 8.
- Claims of distributed exactly-once behavior across Kafka, ES, Redis and
  PostgreSQL.

The delivery model is at-least-once with deterministic logical identities and
idempotent serving projections.

## 4. Allowed file changes

Create:

```text
speed_layer/marketplace_change_rules.py
speed_layer/marketplace_sinks.py
speed_layer/marketplace_speed_layer.py
data_ingestion/marketplace_change_producer.py
display/kibana/create_marketplace_speed_dashboard.py
tests/test_marketplace_change_rules.py
tests/test_marketplace_change_wire.py
tests/test_marketplace_speed_sinks.py
tests/test_marketplace_speed_layer.py
```

Modify only:

```text
config/settings.py
config/marketplace_wire.py
data_ingestion/schemas.py
scripts/init_postgres.sql
requirements.txt
```

Do not rewrite `speed_layer/speed_layer.py`; it remains the legacy behavioral
stream until a later deprecation decision. Do not modify the Phase 4 Silver
consumer.

## 5. Configuration

Add environment-backed settings with these defaults:

```python
MARKETPLACE_CHANGE_RULE_VERSION = "speed-rules.v1"
MARKETPLACE_LARGE_DROP_ABSOLUTE = "100000"
MARKETPLACE_LARGE_DROP_RELATIVE = "0.20"
MARKETPLACE_STALE_AFTER_SECONDS = 21600
MARKETPLACE_STREAM_WATERMARK = "2 hours"
MARKETPLACE_STREAM_TRIGGER = "30 seconds"
MARKETPLACE_STREAM_CHECKPOINT_VERSION = "v1"
KAFKA_CHANGE_ACK_TIMEOUT_SECONDS = 30
ES_INDEX_MARKETPLACE_CHANGES = "marketplace-changes-v1"
ES_INDEX_MARKETPLACE_OFFERS = "marketplace-offers-current-v1"
REDIS_MARKETPLACE_OFFER_TTL_SECONDS = 86400
REDIS_MARKETPLACE_RECENT_CHANGES_MAX = 5000
MARKETPLACE_SPEED_QUERY_NAME = "marketplace-speed-v1"
```

Parse money/ratios through `Decimal`, never float. Validate:

- absolute threshold is non-negative;
- relative threshold is in `[0, 1]`;
- timeouts, TTL, result limit and acknowledgement timeout are positive;
- rule/checkpoint versions and query name are non-empty.

Add `pyarrow==24.0.0`, required by the pinned Spark 3.5.1 pandas state
operation and compatible with the repository's Python 3.11/NumPy 2 runtime. Do not
upgrade Spark, pandas or NumPy as part of this phase.

## 6. Fixed change semantics

### 6.1 Ordering and state replacement

For each `offer_id`, order observations by:

```text
(observed_at UTC ascending, observation_id ascending)
```

The observation ID is the deterministic tie-breaker for two raw artifacts with
the same observed timestamp. An incoming record is:

- `DUPLICATE` when its observation ID equals the current state ID;
- `LATE` when its ordering tuple is lower than current state;
- `APPLIED` otherwise.

Duplicate and late records emit no changes and never move state backward. They
remain available in Silver for batch reconciliation.

### 6.2 Deterministic detection time

Observation-derived changes use `event.produced_at` as `detected_at`. The
actual processing time belongs in micro-batch audit, not in the canonical
change payload. A stale change uses the deterministic boundary:

```text
last_observed_at + stale_after_seconds
```

This rule ensures replaying the same observation and rule version produces
byte-identical change events, not merely the same ID with a different payload.

### 6.3 Bounded emission rules

One applied observation emits no more than these events:

| Change type | Condition | `field_name` | Value shape |
|---|---|---|---|
| `NEW_OFFER` | no previous state | `None` | previous `None`; current canonical state mapping |
| `PRICE_CHANGED` | current price differs | `current_price` | decimal strings |
| `LARGE_PRICE_DROP` | price decreased and absolute **or** relative threshold met | `current_price` | mappings containing price/drop amount/drop ratio as decimal strings |
| `RATING_CHANGED` | rating value or rating count differs | `rating` | `{value, count}` mapping |
| `COUNTER_CHANGED` | review or sold count differs | `public_counters` | mapping containing only changed counters |
| `AVAILABILITY_CHANGED` | two known values differ | `availability` | enum strings |
| `OFFER_STALE` | timeout crosses freshness boundary once | `None` | previous last-observed ISO time; current stale-boundary ISO time |

Rules:

- `NEW_OFFER` is emitted alone for the first observation; do not also emit
  price/rating/counter changes against null.
- Emit `PRICE_CHANGED` and `LARGE_PRICE_DROP` together when both apply.
- Relative drop is `(previous - current) / previous`; when previous price is
  zero, relative drop is undefined and only the absolute threshold can match.
- Null-to-value and value-to-null are real changes for rating/counters, but the
  literal source null is preserved.
- Rating count is grouped with rating. `COUNTER_CHANGED` is limited to
  `review_count` and `sold_count` so one observation cannot produce multiple
  IDs with the same change type.
- `UNKNOWN` availability is insufficient source semantics. Emit availability
  change only when both previous and current are not `UNKNOWN`.
- No counter delta is labelled as a sale. Phase 6 computes validity-aware
  counter deltas.
- Attribute changes such as title, brand, URL and category update latest state
  but do not create a new event type.

The grouping is mandatory because Phase 1 change identity is derived from
`offer_id`, current observation ID, change type and rule version, and does not
contain `field_name`.

### 6.4 Stale convention under the existing v1 contract

`MarketplaceChangeV1` requires both a current observation ID and, for a stale
event, a previous observation ID. A stale timeout has no new observation.
Therefore use the last successful observation ID for **both** fields. This is a
documented v1 convention: it identifies the state that became stale, while the
previous/current values compare last-observed time with the stale boundary.

Emit at most one stale event per state version. A newer applied observation
clears the stale marker and schedules a new timeout.

## 7. Pure change contracts and rules

Create `speed_layer/marketplace_change_rules.py` with frozen dataclasses:

```python
class ObservationDisposition(str, Enum):
    APPLIED = "APPLIED"
    DUPLICATE = "DUPLICATE"
    LATE = "LATE"

@dataclass(frozen=True)
class ChangeRuleConfig:
    rule_version: str
    large_drop_absolute: Decimal
    large_drop_relative: Decimal
    stale_after_seconds: int

@dataclass(frozen=True)
class OfferState:
    marketplace: str
    marketplace_id: str
    offer_id: str
    platform_listing_id: str
    seller_id: str | None
    product_title: str
    brand: str | None
    category_path: str | None
    source_url: str
    currency: str
    active_status: str
    observation_id: str
    observed_at: datetime
    produced_at: datetime
    current_price: Decimal
    list_price: Decimal | None
    rating_value: Decimal | None
    rating_count: int | None
    review_count: int | None
    sold_count: int | None
    availability: str
    stale_emitted: bool = False

@dataclass(frozen=True)
class ChangeDetectionResult:
    disposition: ObservationDisposition
    next_state: OfferState
    changes: tuple[MarketplaceChangeV1, ...]
```

Public API:

```python
def state_from_observation(event: MarketplaceObservationV1) -> OfferState: ...

def detect_observation_changes(
    previous: OfferState | None,
    event: MarketplaceObservationV1,
    config: ChangeRuleConfig,
) -> ChangeDetectionResult: ...

def detect_stale_change(
    state: OfferState,
    config: ChangeRuleConfig,
) -> tuple[OfferState, MarketplaceChangeV1 | None]: ...

def offer_state_to_json(state: OfferState) -> str: ...
def offer_state_from_json(value: str) -> OfferState: ...
```

Use `make_change_id()` for every event. Never call `uuid`, `hash()` or wall
clock functions. Serialize state with sorted compact JSON through the universal
serializer. State round-trip must preserve Decimal strings, nulls and UTC.

## 8. Strict change wire contract

Extend `config/marketplace_wire.py` without weakening the Phase 4 observation
decoder:

```python
def marketplace_change_from_wire(
    value: Mapping[str, Any],
) -> MarketplaceChangeV1: ...
```

Require exactly the v1 change fields. Validate:

- exact `marketplace-change.v1` schema version and enum value;
- timezone-aware ISO `detected_at`, normalized to UTC;
- all required IDs and rule version;
- previous/current values contain only JSON null, strings, booleans, integers,
  lists and string-key mappings; JSON floats are rejected;
- Phase 1 field-name and previous-observation rules;
- recomputed `event_id == make_change_id(...)`.

Do not parse decimal-looking values into binary float. Re-serializing a decoded
event must produce the same canonical JSON value structure.

Append `MARKETPLACE_CHANGE_WIRE_SCHEMA` to `data_ingestion/schemas.py`. Keep
both legacy and Phase 4 schemas unchanged. Because previous/current values are
heterogeneous, store their canonical compact JSON in Spark output columns
`previous_value_json` and `current_value_json`; the Kafka envelope itself still
contains JSON values, not double-encoded strings.

## 9. Spark stateful processing

Create `speed_layer/marketplace_speed_layer.py`.

### 9.1 Source validation

Read only `MARKETPLACE_OBSERVATIONS.name` with Spark Kafka source. Preserve
Kafka topic, partition, offset, timestamp and raw key columns. Start new groups
at `earliest`; checkpoint state decides progress after the first run.

Decode with `MARKETPLACE_OBSERVATION_WIRE_SCHEMA` and require at least:

- exact schema version and event type;
- non-null event, offer and observation IDs;
- key equals canonical event partition key;
- offer ID joins the observation;
- event/observation ID, observed time, crawl run and raw URI agree;
- non-negative current price and complete raw lineage.

Phase 4 remains responsible for the strict DLQ. Invalid rows here are excluded
from state and counted in micro-batch audit; do not create a second competing
DLQ contract.

### 9.2 Stateful API

Expose testable builders:

```python
def read_marketplace_observations(spark: SparkSession) -> DataFrame: ...
def decode_observation_stream(raw: DataFrame) -> tuple[DataFrame, DataFrame]: ...
def build_change_stream(valid: DataFrame, config: ChangeRuleConfig) -> DataFrame: ...
def write_marketplace_batch(batch_df: DataFrame, batch_id: int) -> None: ...
```

The final stream unions the invalid-row projection with stateful output using
one explicit schema matching this driver-side frozen contract:

```python
class SpeedOutputKind(str, Enum):
    STATE = "STATE"
    CHANGE = "CHANGE"
    DUPLICATE = "DUPLICATE"
    LATE = "LATE"
    INVALID = "INVALID"

@dataclass(frozen=True)
class SpeedOutput:
    output_kind: SpeedOutputKind
    marketplace: str | None
    offer_id: str | None
    observation_id: str | None
    change_id: str | None
    event_time: datetime | None
    state_json: str | None
    change_json: str | None
    source_topic: str | None
    source_partition: int | None
    source_offset: int | None
    error_type: str | None
    error_message: str | None
```

`STATE` requires state JSON; `CHANGE` requires canonical change JSON and ID;
`DUPLICATE`/`LATE` require observation identity; `INVALID` requires Kafka
coordinates and a bounded error. Fields not relevant to the kind are null.
This keeps input dispositions in the same checkpointed micro-batch as its
sinks and makes audit counts reconcilable.

Use `groupBy("offer_id").applyInPandasWithState(...)` with explicit output and
state schemas. State is one canonical JSON string plus the fields needed for
timeout ordering. Use processing-time timeout so stale detection can fire when
no new event advances the Kafka watermark. Compute timeout duration from
`GroupState.getCurrentProcessingTimeMs()` to the deterministic stale boundary;
use at least one millisecond when the boundary is already past.

For a group with rows:

1. decode the prior state;
2. sort all rows by `(observed_at, observation_id)`;
3. reconstruct/validate each Phase 1 observation;
4. call the pure rule function;
5. emit one `STATE` output for each applied observation, one disposition-only
   `DUPLICATE`/`LATE` output for each suppressed row, and zero or more `CHANGE`
   outputs;
6. persist the final state and schedule its timeout.

For a timed-out group:

1. call `detect_stale_change()`;
2. emit at most one `CHANGE` row;
3. retain state with `stale_emitted=True` but do not schedule another timeout.

Do not remove timed-out state: a returning offer is not a new offer. State size
is one bounded record per monitored offer. Add state-store metrics to the
operational log/audit.

Use a checkpoint path containing the exact checkpoint version, for example:

```text
<CHECKPOINTS_DIR>/marketplace_speed/v1
```

Any incompatible state schema/rule migration requires `v2`; never point new
state code at an old checkpoint.

## 10. Change producer and deterministic sinks

### 10.1 Kafka change publisher

Create `data_ingestion/marketplace_change_producer.py`:

```python
def create_change_producer(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
) -> Any: ...

def publish_change(
    producer: Any,
    event: MarketplaceChangeV1,
    *,
    ack_timeout_seconds: int = KAFKA_CHANGE_ACK_TIMEOUT_SECONDS,
) -> Any: ...
```

Use canonical UTF-8 JSON, key exactly `offer_id`, topic exactly
`MARKETPLACE_CHANGES.name`, `acks="all"`, supported idempotent producer options
and bounded retries. Revalidate through the strict change decoder, wait for
future acknowledgement and propagate failure. Flush once per micro-batch, not
per event.

### 10.2 Sink gateway

Create `speed_layer/marketplace_sinks.py` with dependency-injected clients:

```python
@dataclass(frozen=True)
class BatchCounts:
    input_rows: int
    invalid_rows: int
    applied_rows: int
    duplicate_rows: int
    late_rows: int
    change_rows: int
    kafka_rows: int
    es_rows: int
    redis_rows: int

class MarketplaceSpeedSinks:
    def __init__(self, *, producer: Any, es: Any, redis: Any, audit: Any): ...
    def write_batch(self, outputs: Iterable[SpeedOutput], batch_id: int) -> BatchCounts: ...
```

For `CHANGE` outputs:

- publish canonical event to Kafka;
- index ES change document with `_id = event_id`;
- `ZADD rt:changes:recent` with member `event_id` and deterministic score from
  `detected_at` epoch microseconds;
- store canonical change JSON at `rt:change:<event_id>` with the documented TTL.

For `STATE` outputs:

- upsert ES current-offer document with `_id = offer_id`;
- write Redis hash `rt:offer:<offer_id>` using string/null-safe values;
- set offer TTL;
- update `rt:source:<marketplace>:last_observation` monotonically.

`DUPLICATE`, `LATE` and `INVALID` outputs affect audit counters only and never
touch Kafka, ES or Redis. A Redis offer hash stores `state_json` plus selected
non-null lookup fields; nullable values are represented inside canonical JSON,
never passed to Redis as Python `None`.

Trim the recent-change sorted set to the configured maximum using rank.
Replaying a batch must overwrite the same ES IDs, Redis hashes and sorted-set
members. Never use list push for recent changes because it duplicates on replay.

Required sink order inside a batch:

1. mark audit RUNNING;
2. publish all Kafka changes and receive acknowledgements;
3. bulk upsert ES with item-level errors treated as failure;
4. execute Redis idempotent pipeline;
5. mark audit SUCCEEDED with counts.

On any failure mark audit FAILED best-effort, close clients and re-raise so the
Spark batch/checkpoint is not committed. A retry may repeat Kafka records, so
downstream consumers must deduplicate by deterministic event ID. Do not claim
physical exactly-once Kafka output.

## 11. Micro-batch audit DDL

Append non-destructive DDL to `scripts/init_postgres.sql`:

```sql
CREATE TABLE IF NOT EXISTS audit.marketplace_speed_batch (
    query_name       VARCHAR(128) NOT NULL,
    batch_id         BIGINT NOT NULL,
    status           VARCHAR(16) NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL,
    completed_at     TIMESTAMPTZ,
    input_rows       BIGINT NOT NULL DEFAULT 0,
    invalid_rows     BIGINT NOT NULL DEFAULT 0,
    applied_rows     BIGINT NOT NULL DEFAULT 0,
    duplicate_rows   BIGINT NOT NULL DEFAULT 0,
    late_rows        BIGINT NOT NULL DEFAULT 0,
    change_rows      BIGINT NOT NULL DEFAULT 0,
    kafka_rows       BIGINT NOT NULL DEFAULT 0,
    es_rows          BIGINT NOT NULL DEFAULT 0,
    redis_rows       BIGINT NOT NULL DEFAULT 0,
    error_message    TEXT,
    PRIMARY KEY (query_name, batch_id)
);
```

Add CHECK constraints for statuses (`RUNNING`, `SUCCEEDED`, `FAILED`),
non-negative counts and completion consistency. Truncate error messages to
2000 characters and store no payload, credentials or stack trace.

For input-bearing batches require:

```text
input_rows = invalid_rows + applied_rows + duplicate_rows + late_rows
```

Timeout-only batches may have zero input rows and positive change rows.

When an existing batch row is SUCCEEDED, `begin_batch()` returns `SKIP`; the
foreachBatch callback does not resend it. A RUNNING/FAILED row is retried from
the beginning using deterministic sinks.

The audit gateway lives in `speed_layer/marketplace_sinks.py` and exposes:

```python
class MarketplaceSpeedAudit:
    def __init__(self, connection_factory: Callable[[], Any]): ...
    def begin_batch(
        self, *, query_name: str, batch_id: int, started_at: datetime,
    ) -> str: ...  # exactly "RUN" or "SKIP"
    def mark_succeeded(
        self, *, query_name: str, batch_id: int,
        completed_at: datetime, counts: BatchCounts,
    ) -> None: ...
    def mark_failed(
        self, *, query_name: str, batch_id: int,
        completed_at: datetime, error: Exception,
    ) -> None: ...
```

Every method owns and closes one DB transaction. SQL values are parameters;
errors are `str(error)`, not repr or traceback.

## 12. Kibana realtime draft

Create `display/kibana/create_marketplace_speed_dashboard.py` using the existing
Saved Objects/API conventions. Provision:

- data view `marketplace-changes-v1-*` or the exact configured index;
- recent changes table with marketplace, offer, type, old/new and time;
- large-price-drop count and recent detail;
- new/stale offers over time;
- observation/change processing lag from produced/detected/processed fields;
- source last-observation/status panel when the indexed state provides it.

Titles must say **public counter change**, not sale, order or demand. Price
change/anomaly are separate concepts; this phase has no anomaly chart.

## 13. Required tests

No default test opens Kafka, Elasticsearch, Redis, PostgreSQL, MinIO, network
or a long-running stream.

### Pure rule and wire tests

1. state JSON round-trip preserves Decimal, null and UTC values;
2. first observation emits exactly one deterministic `NEW_OFFER`;
3. duplicate observation emits nothing and does not replace state;
4. older tuple is LATE and cannot move state backward;
5. same-time observation ID tie-break is deterministic;
6. price increase emits only `PRICE_CHANGED`;
7. qualifying decrease emits price and large-drop events;
8. zero previous price does not divide by zero;
9. absolute/relative thresholds include the exact boundary;
10. rating value/count changes collapse into one event;
11. review/sold changes collapse into one counter event;
12. null transitions are preserved;
13. unknown availability suppresses ambiguous changes;
14. known availability transition emits one event;
15. stale emits once with the last ID in both observation fields;
16. newer observation clears stale state and schedules a new boundary;
17. all IDs use `make_change_id()` and payloads are replay-identical;
18. strict change decoder rejects floats, unknown fields and forged IDs;
19. strict decoder accepts canonical values and normalizes UTC;
20. legacy and Phase 4 wire/schema tests remain unchanged.

### Spark/state tests

21. decode accepts an exact valid Phase 4 observation row;
22. wrong schema version/key/lineage is excluded and counted invalid;
23. grouped rows are processed in deterministic order;
24. output schema separates STATE and CHANGE rows;
25. timeout branch emits no duplicate stale event;
26. checkpoint path contains the configured version;
27. state/output schemas are explicit and stable;
28. imports do not start Spark or open external clients.

### Sink/audit tests

29. Kafka key/topic and acknowledgement timeout are exact;
30. change publish failure propagates;
31. ES IDs are event ID and offer ID, never random;
32. Redis uses hashes/sorted-set deterministic members, not duplicate lists;
33. replaying identical outputs produces identical sink commands;
34. ES item failure prevents success audit;
35. Redis failure prevents success audit;
36. SUCCEEDED batch audit causes a safe skip;
37. FAILED/RUNNING audit causes an idempotent retry;
38. errors are truncated and no event body is written to audit.

Add an optional integration test marked `integration` that runs a finite input
through a temporary checkpoint, restarts with the same checkpoint and asserts
no duplicate logical ES/Redis changes. It may run only when explicit service
environment variables are present.

## 14. Verification commands

Run after every package, then run:

```powershell
python -m pytest `
  tests/test_marketplace_change_rules.py `
  tests/test_marketplace_change_wire.py `
  tests/test_marketplace_speed_sinks.py `
  tests/test_marketplace_speed_layer.py `
  tests/test_marketplace_topics.py `
  tests/test_marketplace_wire.py `
  tests/test_marketplace_producer.py `
  tests/test_marketplace_silver_sink.py `
  -q
python -m pytest tests -q
git diff --check
git diff --stat
```

Also run one finite fake-client smoke from a fixed Phase 1 observation through
change detection and all fake sinks. Assert the Kafka change key, ES ID and
Redis member all contain the same deterministic change identity.

## 15. Commit/work-package sequence

1. `feat: add deterministic marketplace change rules`
   - pure rule module and tests only.
2. `feat: add strict marketplace change wire contract`
   - decoder/schema and wire tests only.
3. `feat: add acknowledged marketplace change producer`
   - producer and fake-producer tests only.
4. `feat: add stateful marketplace speed transform`
   - Spark source/state transform and tests only.
5. `feat: add idempotent marketplace speed sinks`
   - ES/Redis gateway and sink tests only.
6. `feat: audit marketplace streaming batches`
   - DDL/repository integration and audit tests only.
7. `feat: add marketplace realtime kibana draft`
   - Kibana provisioning only.
8. `test: verify marketplace speed restart semantics`
   - focused compatibility/smoke fixes only.

The model must stop after each package and show focused tests plus
`git diff --stat`. Do not implement Phase 6 in these commits.

## 16. Definition of Done

- [ ] dependency gate is recorded;
- [ ] observation stream uses the registered Phase 4 topic/schema/key;
- [ ] previous/current comparison is deterministic per offer;
- [ ] duplicate and late observations cannot move state backward;
- [ ] only the seven accepted change types can be emitted;
- [ ] rating/counter grouping cannot collide under the v1 change ID;
- [ ] large-drop thresholds and zero-price behavior are tested;
- [ ] ambiguous availability does not create false changes;
- [ ] stale is emitted once per state version with documented v1 semantics;
- [ ] replay creates byte-identical canonical change payloads;
- [ ] change publication waits for Kafka acknowledgement;
- [ ] ES/Redis projections are deterministic and replay-idempotent;
- [ ] sink failure fails the micro-batch and is auditable;
- [ ] checkpoint path is versioned and restart behavior is tested;
- [ ] Kibana draft uses accurate public-observation semantics;
- [ ] immutable observations/Silver history remain untouched;
- [ ] legacy behavior speed layer remains runnable;
- [ ] focused tests pass without external services;
- [ ] full tests pass or environment failures are documented.

## 17. Mandatory rejection conditions

Reject the implementation if it:

- uses title, price, batch ID, Python `hash()` or UUID as state/change identity;
- compares an observation with arbitrary arrival order instead of the fixed
  event-time tuple;
- moves latest state backward for late data;
- emits one counter event per field and therefore collides IDs;
- converts canonical money to float;
- calls a public counter delta a sale/order/demand event;
- treats `UNKNOWN` availability as confirmed stock state;
- produces a different canonical payload for the same event ID on replay;
- writes random ES IDs or Redis list entries;
- ignores ES bulk item failures or Redis/Kafka errors;
- marks a failed/partial batch SUCCEEDED;
- claims cross-system exactly-once delivery;
- overwrites/replaces raw or Silver observations;
- starts Gold/anomaly/product-matching work early;
- breaks the legacy behavioral stream.

## 18. Copy-ready prompts for a low-capability model

### Prompt A — pure rules

```text
Implement only Sections 5–7 and pure rule tests from
docs/PHASE_5_MARKETPLACE_SPEED_IMPLEMENTATION_PLAN.md. Read the accepted Phase
1 dataclasses and common/identity.py first. Create only the rule module/tests
and add only listed settings. Use no Spark, Kafka, DB or wall clock. Run focused
tests, show diff stat and stop.
```

### Prompt B — change wire

```text
Implement only Section 8 and wire tests from the Phase 5 plan. Extend the
existing marketplace wire module without weakening observation validation and
append the exact Spark change schema without changing existing schemas. Reject
JSON floats and forged IDs. Run focused plus Phase 4 wire tests and stop.
```

### Prompt C — change producer

```text
Implement only Section 10.1 of the Phase 5 plan. Create the acknowledged change
producer with exact registered topic and offer_id key. Use fake producer tests;
do not create Spark or sink code. Run focused tests and stop.
```

### Prompt D — stateful transform

```text
Implement only Section 9 of the Phase 5 plan and its Spark/state tests. Use the
completed pure rules and Phase 4 schema. Keep explicit state/output schemas,
deterministic ordering, duplicate/late handling and versioned checkpoint. Do
not connect real external services. Run focused tests and stop.
```

### Prompt E — sinks and audit

```text
Implement Sections 10.2 and 11 of the Phase 5 plan. Use injected fake clients,
deterministic ES IDs and Redis sorted-set members, exact sink order and
RUNNING/SUCCEEDED/FAILED audit behavior. Add only the listed DDL. Run focused
tests, show diff stat and stop.
```

### Prompt F — Kibana and final verification

```text
Implement Section 12 and final compatibility verification from the Phase 5
plan. Follow existing Kibana Saved Objects/API style and use accurate public
counter semantics. Run the full Phase 5 matrix and fake-client smoke, document
environment-only skips, show diff stat and stop. Do not implement Phase 6.
```

## 19. Handoff to Phase 6

Phase 6 must read authoritative Phase 4 Silver observations, not reconstruct
history from the speed layer, Elasticsearch, Redis or the derived change topic.
It may independently recompute temporal changes with Spark windows and publish
run-scoped Parquet marts plus atomic PostgreSQL serving projections. Phase 5
change IDs remain operational/realtime evidence, not the batch source of truth.
