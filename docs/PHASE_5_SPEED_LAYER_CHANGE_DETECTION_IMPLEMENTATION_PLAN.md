# Phase 5 implementation plan — Speed layer change detection and realtime serving

> Status: ready for implementation after Phases 3 and 4 are accepted
>
> Intended implementer: a low-capability coding model working one work package
> at a time. It must follow the fixed contracts below and stop between packages.
>
> Parent plan: `Brief để xây dựng plan sơ bộ cho việc cải tiến ecommerce-lambda-architecture.md`

## 1. Objective

Implement the parent plan's Week 5 vertical slice and backlog P1-05 through
P1-08:

```text
marketplace.observations.v1
-> strict wire decode (Phase 4 schema)
-> previous offer state lookup
-> bounded MarketplaceChangeV1 detection
-> marketplace.changes.v1 + Elasticsearch + Redis latest state
-> micro-batch audit + versioned checkpoint
```

The speed layer answers exactly one question: **what just changed?** It is a
derived, lossy, low-latency view. Bronze raw artifacts and Silver observations
remain the only history. Gold marts, temporal analytics, robust anomaly
detection and PostgreSQL serving remain Phase 6 and Phase 7.

## 2. Mandatory dependency gate

Before editing, record and verify:

1. Phase 1 contracts pass, including `MarketplaceChangeV1`,
   `MarketplaceChangeType`, `CHANGE_SCHEMA_VERSION` and
   `make_change_id()`.
2. Phase 4 exposes `marketplace_observation_from_wire()` and
   `MARKETPLACE_OBSERVATION_WIRE_SCHEMA`.
3. Phase 4 registry exposes `MARKETPLACE_CHANGES` as a real `TopicSpec`
   created by the start script.
4. Phase 4 Silver landing works and is not modified by this phase.
5. Phase 3 audit tables exist, so a micro-batch audit row can be written next
   to crawl audit without inventing a second audit story.

If any dependency is missing, stop. Do not re-implement acquisition,
scheduling, wire decoding or Silver landing inside Phase 5.

`MarketplaceChangeV1` is **frozen**. Phase 5 must not add fields to it, must
not relax its validation and must not introduce a second change contract. Every
piece of information the dashboards need beyond that contract belongs in a
sink-side document, never in the Kafka event.

## 3. Scope

### 3.1 In scope

- Additive `create_change_event()` factory next to the existing Phase 1
  observation factory.
- Pure previous/current comparison producing bounded change events.
- Exactly the seven `MarketplaceChangeType` members. No new type.
- An offer-state store abstraction with a Redis implementation and an
  in-memory fake.
- Monotonic and duplicate guards for out-of-order and replayed observations.
- Kafka producer for `marketplace.changes.v1` keyed by `offer_id`.
- Deterministic Elasticsearch documents for observations and changes.
- Idempotent Redis latest-state, recent-change and source-freshness writes.
- A bounded `OFFER_STALE` sweep driven by the state store.
- Spark Structured Streaming driver using `foreachBatch`, a versioned
  checkpoint path and a per-micro-batch audit row.
- A Kibana provisioning draft for realtime change/lag/error views.
- Offline unit tests plus optional integration tests that skip by default.

### 3.2 Out of scope

- Gold marts, daily aggregation, price history and `offer_change_daily`.
- Robust anomaly detection (rolling median, MAD, IQR) — Phase 7.
- Mandatory data-quality gates and the Gold publish manifest — Phase 7.
- PostgreSQL BI cache and Superset — Phase 6.
- Product matching, variant resolution, cross-market comparison.
- A second marketplace adapter.
- Any change to crawler fetching, Bronze layout, parsing, retry or Silver.
- Spark stateful operators (`mapGroupsWithState`, `flatMapGroupsWithState`).
- Exactly-once claims across Kafka, Redis and Elasticsearch.

The implementation may claim deterministic, idempotent, replay-safe logical
change detection. It may not claim distributed exactly-once.

## 4. Allowed file changes

Create:

```text
speed_layer/change_rules.py
speed_layer/offer_state.py
speed_layer/marketplace_speed_layer.py
data_ingestion/marketplace_change_producer.py
serving_layer/marketplace_redis.py
serving_layer/marketplace_es.py
display/kibana/setup_marketplace_kibana.py
tests/test_change_rules.py
tests/test_offer_state.py
tests/test_marketplace_change_producer.py
tests/test_marketplace_serving.py
tests/test_marketplace_speed_layer.py
```

Modify only:

```text
config/marketplace_schema.py
config/settings.py
scripts/init_postgres.sql
scripts/start_all.ps1
```

`config/marketplace_schema.py` may receive **only** the additive
`create_change_event()` factory. Existing enums, dataclasses, validators and
factories must stay byte-identical in behavior, and
`tests/test_marketplace_schema.py` must pass unchanged.

`speed_layer/speed_layer.py`, `data_ingestion/es_indexer.py` and
`serving_layer/redis_cache.py` are the legacy behavioral path. Do not edit or
delete them in this phase.

## 5. Configuration

Add these environment-backed settings to `config/settings.py`:

```python
CHANGE_RULE_VERSION = "marketplace-change-rules.v1"

KAFKA_MARKETPLACE_SPEED_CONSUMER_GROUP = "marketplace-speed-v1"
SPEED_CHECKPOINT_DIR = CHECKPOINTS_DIR / "marketplace_speed_v1"
SPEED_MAX_OFFSETS_PER_TRIGGER = 5000
SPEED_TRIGGER_INTERVAL_SECONDS = 30

SPEED_LARGE_DROP_ABSOLUTE = Decimal("500000")   # currency minor-unit free, VND
SPEED_LARGE_DROP_PERCENT = Decimal("15")        # percent, 0 < p <= 100
SPEED_FRESHNESS_THRESHOLD_MINUTES = 360
SPEED_STALE_THRESHOLD_MINUTES = 1440

REDIS_MARKETPLACE_NAMESPACE = "rt"
SPEED_RECENT_CHANGES_MAX = 1000
SPEED_CHANGE_DOC_TTL_SECONDS = 604800   # 7 days, documented and asserted

ES_INDEX_MARKETPLACE_OBSERVATIONS = "marketplace-observations-v1"
ES_INDEX_MARKETPLACE_CHANGES = "marketplace-changes-v1"
```

Rules:

- `SPEED_CHECKPOINT_DIR` ends in an explicit `_v1`. A rule-version bump that
  changes emitted change identity requires a new checkpoint directory, because
  reusing the old one would silently mix rule versions in one stream.
- Threshold settings are `Decimal`, parsed from strings. Never `float`.
- Keep every legacy setting unchanged.

## 6. Fixed architectural decisions

These are not open questions. An implementation that reopens them is rejected.

### 6.1 Engine and testability split

The streaming driver uses Spark Structured Streaming reading Kafka with
`MARKETPLACE_OBSERVATION_WIRE_SCHEMA` and `foreachBatch`.

Hard split:

- `speed_layer/change_rules.py` and `speed_layer/offer_state.py` must not
  import `pyspark`, `kafka`, `redis`, `elasticsearch` or `psycopg2` at module
  level or anywhere else in `change_rules.py`.
- `speed_layer/marketplace_speed_layer.py` may use Spark, but every
  `pyspark` import must live **inside** the function that needs it, so the
  module imports and unit-tests without pyspark installed.
- Spark stateful operators are forbidden. Previous state comes from the state
  store, which is also the serving store. One state, one source of truth.

### 6.2 Mandatory sink ordering

For each observation, in this exact order:

1. read previous state for `offer_id`;
2. detect changes purely, with no I/O;
3. publish every change to `marketplace.changes.v1` and wait for ack;
4. index the observation document, then the change documents, to
   Elasticsearch;
5. upsert Redis latest state, recent-change entries and source freshness;
6. record the micro-batch audit row;
7. only then let Spark commit the batch/checkpoint.

State advance is **last**. A crash anywhere before step 5 leaves the stored
previous state untouched, so the replay recomputes the identical change set
with identical IDs and every sink write is an idempotent overwrite. This is the
speed-layer analogue of Phase 2's raw-first ordering: never advance the pointer
that makes the evidence unreproducible.

### 6.3 Deterministic change identity

```python
change_id = make_change_id(
    offer_id,
    current_observation_id,
    change_type,          # the MarketplaceChangeType value
    rule_version,         # CHANGE_RULE_VERSION
)
```

Consequences that must hold:

- reprocessing one observation yields the same change IDs;
- Elasticsearch `_id` and Redis sorted-set members are those IDs, so a replay
  overwrites instead of duplicating;
- two different change types on the same observation get different IDs;
- a rule-version bump produces different IDs by construction, which is why it
  also requires a new checkpoint directory.

Random UUIDs, wall-clock time, titles or prices must never enter an identity.

### 6.4 Duplicate and out-of-order guards

Let `stored` be the previous state and `current` the decoded observation.

- If `stored.observation_id == current.observation_id`: emit nothing, write
  nothing, count the record as `duplicate` in the audit row.
- If `stored.observed_at > current.observed_at`: emit nothing, write nothing,
  count it as `out_of_order`. Do not rewrite state backwards.
- If `stored.observed_at == current.observed_at` but the observation ID
  differs: treat it as `conflict`, emit nothing and count it. Two distinct raw
  bodies at the same observed instant is a source or clock problem, not a
  change.
- Otherwise detect changes normally.

Counting these three cases is mandatory: they are the only honest way to
explain a gap between observations consumed and changes emitted.

### 6.5 Change rules, exactly seven types

| Type | Condition | `field_name` | previous / current |
|---|---|---|---|
| `NEW_OFFER` | no stored state for `offer_id` | `None` | `None` / `None` |
| `PRICE_CHANGED` | `current_price != previous_price` (Decimal compare) | `current_price` | Decimal / Decimal |
| `LARGE_PRICE_DROP` | a price drop meeting either threshold | `current_price` | Decimal / Decimal |
| `RATING_CHANGED` | `rating_value` or `review_count` differs | `rating_value` or `review_count` | value / value |
| `COUNTER_CHANGED` | `sold_count` differs | `sold_count` | int / int |
| `AVAILABILITY_CHANGED` | both sides known and different | `availability` | enum value / enum value |
| `OFFER_STALE` | sweep finds age > stale threshold | `None` | last `observed_at` / sweep `now` |

Additional fixed rules:

- `NEW_OFFER` sets `previous_observation_id = None`. Every other type,
  including `OFFER_STALE`, must carry a previous observation ID — the frozen
  contract enforces this, so the sweep must read it from stored state.
- `LARGE_PRICE_DROP` is emitted **in addition to** `PRICE_CHANGED`, never
  instead of it. A consumer counting price changes must not have to know about
  the drop rule.
- Drop test: `drop = previous - current` where `drop > 0`, and the change fires
  when `drop >= SPEED_LARGE_DROP_ABSOLUTE` **or**
  `drop / previous * 100 >= SPEED_LARGE_DROP_PERCENT`. When
  `previous == 0`, the percent branch is skipped, not divided by zero.
- Decimal comparison is by value: `Decimal("100.0") == Decimal("100")` is not a
  change. Normalize with `==` on Decimals, never on strings or floats.
- `RATING_CHANGED` may fire for either field. When both changed, emit one
  change for `rating_value` and one for `review_count` — same type, different
  `field_name`, and therefore the same `change_id`. This is a real collision:
  `make_change_id()` does not take `field_name`. So the rule is: **at most one
  `RATING_CHANGED` per observation**, with `field_name = "rating_value"` when
  the rating value moved, otherwise `"review_count"`. Record the other field
  only in the sink-side document. Do not work around the collision by inventing
  a change type or extending the identity helper.
- `AVAILABILITY_CHANGED` fires only when neither side is
  `Availability.UNKNOWN`. An `UNKNOWN -> IN_STOCK` transition is adapter
  coverage improving, not a stock event.
- `COUNTER_CHANGED` is a public-counter movement. It must never be named a
  sale, purchase, order or demand event in code, log text, index field or
  dashboard label. A negative delta is not clamped to zero: emit the change
  with the true previous/current values and mark
  `counter_reset_or_invalid = true` in the sink-side document only.
- No other condition may emit a change. Shipping price, discount, promotion and
  title movement are Phase 6+ concerns and are not change types.

### 6.6 Staleness

`OFFER_STALE` is not a per-record rule; a missing observation produces no
record. It comes from an explicit sweep over the state store:

- an offer is stale when `now - stored.observed_at > SPEED_STALE_THRESHOLD`;
- the change carries `previous_observation_id = stored.observation_id`,
  `current_observation_id = stored.observation_id`, previous value
  `stored.observed_at` and current value the sweep `now`;
- identity therefore ties to the last observation, so one offer goes stale
  **exactly once per last-seen observation**, no matter how often the sweep
  runs;
- the sweep must be bounded: a caller-supplied maximum number of offers per
  invocation, and a deterministic scan order.

`SPEED_FRESHNESS_THRESHOLD_MINUTES` is the softer dashboard threshold used for
the freshness/health document. `SPEED_STALE_THRESHOLD_MINUTES` is the harder
one that emits a change. They are separate settings on purpose.

### 6.7 Money, time and value domain

`MarketplaceChangeV1.previous_value` and `.current_value` are typed `Any` by
the frozen contract. Phase 5 fixes the domain:

- prices: `Decimal`;
- counters and review counts: `int`;
- rating: `Decimal`;
- availability: the `Availability` member value string;
- staleness: timezone-aware `datetime`.

`create_change_event()` must reject `float`, `bool`-as-int, naive datetimes and
values that `serialize_for_wire()` cannot represent. All timestamps are
timezone-aware UTC. `detected_at` comes from an injected clock, never from
`datetime.now()` inside a rule.

## 7. Work package SPD-01 — Change event factory

### Files

```text
config/marketplace_schema.py   (additive only)
config/settings.py
tests/test_change_rules.py     (factory cases)
```

### 7.1 Required API

```python
def create_change_event(
    *,
    marketplace_code: str,
    offer_id: str,
    change_type: MarketplaceChangeType,
    current_observation_id: str,
    detected_at: datetime,
    rule_version: str,
    previous_observation_id: str | None = None,
    field_name: str | None = None,
    previous_value: Any = None,
    current_value: Any = None,
) -> MarketplaceChangeV1: ...
```

Behavior:

- computes `event_id` with `make_change_id()`;
- sets `schema_version = CHANGE_SCHEMA_VERSION`;
- lowercases and validates `marketplace_code` the same way the observation
  factory does;
- enforces the Section 6.7 value domain before constructing the dataclass;
- lets `MarketplaceChangeV1.__post_init__` enforce the previous-ID and
  `field_name` requirements rather than duplicating them.

Do not add a `create_change_event` variant that takes a whole observation
object. The factory stays primitive so the pure rules own all interpretation.

### Exit criteria

- factory produces a valid frozen event for all seven types;
- invalid value types are rejected with a clear field name;
- `tests/test_marketplace_schema.py` passes unchanged.

## 8. Work package SPD-02 — Pure change rules

### Files

```text
speed_layer/change_rules.py
tests/test_change_rules.py
```

### 8.1 Required API

```python
@dataclass(frozen=True)
class OfferStateSnapshot:
    offer_id: str
    marketplace: str
    platform_listing_id: str
    observation_id: str
    observed_at: datetime
    current_price: Decimal
    list_price: Decimal | None
    availability: Availability
    rating_value: Decimal | None
    review_count: int | None
    sold_count: int | None
    raw_uri: str


class ObservationOutcome(str, Enum):
    DETECTED = "DETECTED"
    DUPLICATE = "DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class ChangeDetectionResult:
    outcome: ObservationOutcome
    changes: tuple[MarketplaceChangeV1, ...]
    next_state: OfferStateSnapshot | None
    counter_reset_or_invalid: bool
    secondary_rating_field: str | None


@dataclass(frozen=True)
class ChangeThresholds:
    large_drop_absolute: Decimal
    large_drop_percent: Decimal
    stale_after: timedelta


def state_from_observation(
    event: MarketplaceObservationV1,
) -> OfferStateSnapshot: ...


def detect_changes(
    *,
    previous: OfferStateSnapshot | None,
    event: MarketplaceObservationV1,
    detected_at: datetime,
    thresholds: ChangeThresholds,
    rule_version: str,
) -> ChangeDetectionResult: ...


def detect_stale_offers(
    *,
    states: Iterable[OfferStateSnapshot],
    now: datetime,
    thresholds: ChangeThresholds,
    rule_version: str,
    limit: int,
) -> tuple[MarketplaceChangeV1, ...]: ...
```

### 8.2 Purity constraints

- no I/O, no clock read, no randomness, no logging side effects;
- `detect_changes()` returns `next_state = None` for every non-`DETECTED`
  outcome, so a caller cannot accidentally advance state on a guard hit;
- change ordering inside `changes` is deterministic and fixed:
  `NEW_OFFER`, `PRICE_CHANGED`, `LARGE_PRICE_DROP`, `RATING_CHANGED`,
  `COUNTER_CHANGED`, `AVAILABILITY_CHANGED`;
- calling `detect_changes()` twice with identical inputs returns equal results,
  including identical `event_id` values;
- `detect_stale_offers()` sorts candidates by `(observed_at, offer_id)` and
  truncates at `limit`, so a bounded sweep is reproducible.

### Exit criteria

- all seven types are reachable by a test;
- guards return no changes and no state;
- no forbidden import appears anywhere in the module.

## 9. Work package SPD-03 — Offer state store

### Files

```text
speed_layer/offer_state.py
serving_layer/marketplace_redis.py
tests/test_offer_state.py
```

### 9.1 Required API

```python
class OfferStateStore(Protocol):
    def get_many(
        self, offer_ids: Sequence[str]
    ) -> dict[str, OfferStateSnapshot]: ...

    def put_many(self, states: Sequence[OfferStateSnapshot]) -> None: ...

    def scan_states(self, limit: int) -> tuple[OfferStateSnapshot, ...]: ...


class InMemoryOfferStateStore:  # tests and dry runs only
    ...


class RedisOfferStateStore:
    def __init__(self, client, *, namespace: str): ...
```

`get_many()` batches with one pipeline round trip, not one call per offer. A
micro-batch of 5000 observations must not become 5000 Redis round trips.

### 9.2 Redis key contract

```text
<ns>:offer:<offer_id>              HASH   latest offer state
<ns>:changes:<marketplace>         ZSET   score=detected_at epoch ms, member=change_id
<ns>:change:<change_id>            HASH   change document, TTL SPEED_CHANGE_DOC_TTL_SECONDS
<ns>:source:<marketplace>          HASH   freshness/health timestamps
<ns>:offers:index                  SET    offer_id membership for the bounded sweep
```

Fixed rules:

- every write is an overwrite (`HSET`, `ZADD`, `SADD`), never an in-place
  arithmetic update. `INCR`, `HINCRBY` and `ZINCRBY` are forbidden in this
  phase: a replay would double-count and there is no way to tell afterwards.
- the state hash stores `observed_at` and `observation_id`, so the store can
  reject a backwards write. Document plainly that this is a read-then-write
  guard, safe because one consumer owns a Kafka partition, and that it is not a
  distributed lock.
- `<ns>:changes:<marketplace>` is trimmed with `ZREMRANGEBYRANK` to
  `SPEED_RECENT_CHANGES_MAX` after each batch.
- `<ns>:offer:*` has no TTL: it is the previous-state source of truth for
  detection. Only change documents expire. Any TTL that is set must be a
  documented setting, never a literal.
- `<ns>:source:<marketplace>` holds only timestamps and last-known values
  (`last_observation_at`, `last_change_at`, `last_stale_sweep_at`,
  `freshness_threshold_minutes`). No counters.

### Exit criteria

- fake and Redis implementations satisfy the same test body;
- batched reads issue one pipeline;
- backwards writes are refused;
- no forbidden Redis command appears in the module.

## 10. Work package SPD-04 — Change producer

### Files

```text
data_ingestion/marketplace_change_producer.py
tests/test_marketplace_change_producer.py
```

### 10.1 Required API

```python
def create_change_producer(*, bootstrap_servers: str, ...): ...

def publish_change(
    producer,
    change: MarketplaceChangeV1,
    *,
    topic: str = MARKETPLACE_CHANGES.name,
    ack_timeout_seconds: float = KAFKA_PRODUCER_ACK_TIMEOUT_SECONDS,
) -> None: ...

def publish_changes(
    producer,
    changes: Sequence[MarketplaceChangeV1],
    **kwargs,
) -> None: ...
```

Rules:

- the Kafka key is `change.offer_id` encoded UTF-8, per the parent plan's topic
  table. It is not the change ID and not the listing ID: all changes for one
  offer must stay ordered in one partition.
- the value is `json.dumps(serialize_for_wire(change), ensure_ascii=False,
  sort_keys=True, separators=(",", ":")).encode("utf-8")` — the same canonical
  form Phase 4 uses. Do not introduce a second serializer.
- reject a non-`MarketplaceChangeV1` argument before touching the producer.
- wait for the broker ack with the configured timeout and let failures
  propagate. Never swallow a send failure, and never route a change to the
  observation DLQ: a change that cannot be published is an operational sink
  failure, not a bad record.
- `publish_changes()` fails fast on the first failure and reports how many
  changes were already acked, so the caller's audit row can record partial
  progress honestly.

### Exit criteria

- exact topic, key and canonical bytes;
- ack waited with configured timeout;
- failures propagate with the acked count.

## 11. Work package SPD-05 — Elasticsearch serving documents

### Files

```text
serving_layer/marketplace_es.py
tests/test_marketplace_serving.py
scripts/start_all.ps1
```

### 11.1 Required API

```python
def observation_document(event: MarketplaceObservationV1) -> dict[str, Any]: ...

def change_document(
    change: MarketplaceChangeV1,
    *,
    state: OfferStateSnapshot,
    counter_reset_or_invalid: bool = False,
    secondary_rating_field: str | None = None,
) -> dict[str, Any]: ...

def index_documents(
    client,
    index: str,
    documents: Sequence[tuple[str, dict[str, Any]]],
) -> None: ...

MARKETPLACE_OBSERVATION_MAPPING: dict[str, Any]
MARKETPLACE_CHANGE_MAPPING: dict[str, Any]
```

Rules:

- the document `_id` is `event.event_id` for observations and
  `change.event_id` for changes. Bulk indexing uses `index` action semantics
  (overwrite), never `create`, so a replay is idempotent.
- money reaches Elasticsearch twice: `scaled_float` with
  `scaling_factor: 100` for aggregation, and a `keyword` field holding the
  exact canonical Decimal string. The exact value must never exist only as a
  float.
- timestamps are `date` fields in ISO-8601 UTC.
- enrichment fields allowed in the change document and nowhere else:
  `marketplace`, `platform_listing_id`, `observed_at`, `previous_observed_at`,
  `delta_absolute`, `delta_percent`, `counter_reset_or_invalid`,
  `secondary_rating_field`, `rule_version`, `source_url`, `category_path`.
  These are serving conveniences derived from data already in the event and
  state; they must not contradict the event.
- `delta_percent` is computed with `Decimal` and quantized to 4 decimal places,
  and is `None` when the previous value is zero or missing.
- no field may be named `sale`, `sales`, `sold_units`, `orders`, `demand` or
  `revenue`. The counter field is `sold_count`, described as a public counter.
- `start_all.ps1` creates both indices with the exact mappings if absent, and
  leaves the legacy `ecommerce-*` indices untouched.

### Exit criteria

- deterministic IDs, overwrite semantics;
- exact-value keyword alongside every scaled_float;
- forbidden field names absent;
- legacy indices unchanged.

## 12. Work package SPD-06 — Streaming driver and audit

### Files

```text
speed_layer/marketplace_speed_layer.py
scripts/init_postgres.sql
tests/test_marketplace_speed_layer.py
```

### 12.1 Pure batch API

The unit-testable core takes decoded events and injected sinks:

```python
@dataclass(frozen=True)
class MicroBatchReport:
    batch_id: int
    started_at: datetime
    completed_at: datetime
    observations_in: int
    detected: int
    duplicates: int
    out_of_order: int
    conflicts: int
    decode_failures: int
    changes_by_type: dict[str, int]
    changes_published: int
    rule_version: str
    status: str          # "SUCCEEDED" | "FAILED"
    failure_stage: str | None
    failure_message: str | None


def process_micro_batch(
    *,
    batch_id: int,
    records: Sequence[Mapping[str, Any]],
    state_store: OfferStateStore,
    change_publisher: Callable[[Sequence[MarketplaceChangeV1]], None],
    es_writer: Callable[[str, Sequence[tuple[str, dict]]], None],
    redis_writer: "MarketplaceRedisWriter",
    thresholds: ChangeThresholds,
    rule_version: str,
    clock: Callable[[], datetime],
) -> MicroBatchReport: ...


def run_stale_sweep(
    *,
    state_store: OfferStateStore,
    change_publisher: Callable[[Sequence[MarketplaceChangeV1]], None],
    es_writer: Callable[[str, Sequence[tuple[str, dict]]], None],
    redis_writer: "MarketplaceRedisWriter",
    thresholds: ChangeThresholds,
    rule_version: str,
    limit: int,
    clock: Callable[[], datetime],
) -> MicroBatchReport: ...


def run_speed_layer(...) -> None: ...   # Spark entry point, pyspark imported inside
```

### 12.2 Batch algorithm

1. decode each record with `marketplace_observation_from_wire()`; a decode
   failure increments `decode_failures`, is logged with topic/partition/offset
   and is **skipped**, not published to any DLQ — Phase 4 already owns
   bad-record handling for this topic, and duplicating it here would double
   the DLQ record for one bad message;
2. group decoded events by `offer_id` and order each group by
   `(observed_at, observation_id)`, so a batch containing several observations
   of one offer walks the state forward in real order;
3. `get_many()` the previous states for all offers in the batch, once;
4. for each offer, fold `detect_changes()` across its ordered events, carrying
   `next_state` forward in memory so intra-batch transitions are detected;
5. publish all changes, then index observation and change documents, then
   `put_many()` the final state per offer, then write the Redis change/freshness
   entries — Section 6.2 ordering;
6. build the report; on any sink failure set `status = "FAILED"`, record
   `failure_stage` and re-raise after the audit row is written.

Intra-batch folding is mandatory. Reading state only once per batch and
comparing every event against that same snapshot would report one change for
several real transitions.

### 12.3 Audit DDL

Add to `scripts/init_postgres.sql`, alongside the Phase 3 audit schema:

```sql
CREATE TABLE IF NOT EXISTS audit.speed_micro_batch (
    batch_id            BIGINT       NOT NULL,
    rule_version        TEXT         NOT NULL,
    started_at          TIMESTAMPTZ  NOT NULL,
    completed_at        TIMESTAMPTZ  NOT NULL,
    observations_in     INTEGER      NOT NULL,
    detected            INTEGER      NOT NULL,
    duplicates          INTEGER      NOT NULL,
    out_of_order        INTEGER      NOT NULL,
    conflicts           INTEGER      NOT NULL,
    decode_failures     INTEGER      NOT NULL,
    changes_published   INTEGER      NOT NULL,
    changes_by_type     JSONB        NOT NULL,
    status              TEXT         NOT NULL,
    failure_stage       TEXT,
    failure_message     TEXT,
    PRIMARY KEY (rule_version, batch_id)
);
```

The primary key is `(rule_version, batch_id)` so a re-run under a new rule
version cannot collide with history. The audit writer is injectable; the
default unit tests use a fake and open no database.

### 12.4 Spark wiring

- `readStream.format("kafka")` on the observation topic only, with
  `startingOffsets="earliest"` for a new checkpoint and
  `maxOffsetsPerTrigger = SPEED_MAX_OFFSETS_PER_TRIGGER`;
- parse `value` with `from_json(col("value").cast("string"),
  MARKETPLACE_OBSERVATION_WIRE_SCHEMA)`;
- `foreachBatch` collects the micro-batch to the driver and calls
  `process_micro_batch()`. State and sinks live on the driver, so no Redis or
  Elasticsearch client is serialized to executors;
- checkpoint location is `SPEED_CHECKPOINT_DIR`;
- a raised exception must fail the query rather than being caught and logged as
  success. Silent `foreachBatch` failure is the classic way a streaming job
  reports health while losing data.

Collecting to the driver is a deliberate, documented limit for a thesis-scale
stream bounded by `SPEED_MAX_OFFSETS_PER_TRIGGER`. State the limit in the
module docstring instead of implying unbounded scalability.

### Exit criteria

- batch algorithm reproducible with fakes and a fixed clock;
- intra-batch transitions detected;
- audit row written on success and failure;
- pyspark not imported at module scope.

## 13. Work package SPD-07 — Kibana realtime draft

### Files

```text
display/kibana/setup_marketplace_kibana.py
```

Provision, idempotently:

- data views for both marketplace indices;
- recent price changes and large price drops;
- new and stale offers;
- observation rate over time;
- decode failures, duplicates, out-of-order and conflicts from the change
  index and audit-backed fields available in the documents;
- per-marketplace last successful observation time.

Rules: reuse the existing `display/kibana/setup_kibana.py` patterns, do not
modify that file, and label the counter panel as a public counter, never as
sales. Re-running the script must not create duplicate saved objects.

### Exit criteria

- script is idempotent;
- legacy setup untouched;
- no panel labels a counter as sales.

## 14. Required tests

No default test opens Kafka, Redis, Elasticsearch, Spark, PostgreSQL or the
network.

### Factory and rules

1. every one of the seven change types constructs and validates;
2. `create_change_event()` rejects float, bool-as-int and naive datetimes;
3. `NEW_OFFER` needs no previous observation ID; the other six do;
4. identical inputs produce identical `event_id`;
5. different change types on one observation produce different IDs;
6. a rule-version bump changes every ID;
7. equal Decimal values with different scale are not a price change;
8. `LARGE_PRICE_DROP` accompanies `PRICE_CHANGED`, never replaces it;
9. absolute threshold alone fires the drop;
10. percent threshold alone fires the drop;
11. a price rise never fires a drop;
12. `previous == 0` does not divide by zero;
13. `UNKNOWN -> IN_STOCK` emits no availability change;
14. `IN_STOCK -> OUT_OF_STOCK` emits one;
15. a negative counter delta emits `COUNTER_CHANGED` with true values and sets
    `counter_reset_or_invalid`;
16. both rating fields moving emits exactly one `RATING_CHANGED` with
    `rating_value` and reports `review_count` as the secondary field;
17. change ordering in the result tuple is the fixed order;
18. duplicate observation ID yields `DUPLICATE`, no changes, no next state;
19. older `observed_at` yields `OUT_OF_ORDER`;
20. equal `observed_at` with a different observation ID yields `CONFLICT`;
21. `change_rules.py` contains no pyspark/kafka/redis/elasticsearch/psycopg2
    import.

### State store

22. fake and Redis implementations pass one shared test body;
23. `get_many()` for N offers issues one pipeline;
24. a backwards `put_many()` is refused;
25. sweep scan is bounded and deterministically ordered;
26. no `INCR`/`HINCRBY`/`ZINCRBY` call is issued;
27. recent-change ZSET is trimmed to the configured maximum.

### Producer and serving

28. change publishes to the exact topic with `offer_id` as key;
29. canonical bytes are byte-identical to the Phase 4 serializer output;
30. wrong argument type is rejected before send;
31. ack failure propagates with the acked count;
32. observation and change documents use event IDs as `_id`;
33. every money field has an exact-string companion;
34. forbidden sales-like field names are absent;
35. `delta_percent` is `None` when previous is zero.

### Micro-batch

36. valid batch detects, publishes, indexes and advances state in the fixed
    order (assert call order, not just call counts);
37. intra-batch multiple observations of one offer detect each transition;
38. decode failure is counted and skipped without a DLQ publish;
39. publish failure leaves stored state unchanged;
40. re-running the identical batch after a publish failure emits identical
    change IDs;
41. Elasticsearch failure leaves stored state unchanged;
42. failure writes a `FAILED` audit row with the stage, then re-raises;
43. success writes a `SUCCEEDED` audit row whose counts reconcile:
    `observations_in == detected + duplicates + out_of_order + conflicts +
    decode_failures`;
44. stale sweep emits one `OFFER_STALE` per offer and is idempotent across two
    runs with the same last observation;
45. a fresh observation after a stale change allows a later stale change again;
46. `marketplace_speed_layer.py` imports successfully with pyspark absent.

Tests marked `integration` may use a local Kafka/Redis/Elasticsearch/Spark
environment only when explicit environment variables are present, and must skip
by default.

## 15. Verification commands

```powershell
python -m pytest `
  tests/test_change_rules.py `
  tests/test_offer_state.py `
  tests/test_marketplace_change_producer.py `
  tests/test_marketplace_serving.py `
  tests/test_marketplace_speed_layer.py `
  tests/test_marketplace_schema.py `
  tests/test_serialization.py `
  tests/test_identity.py `
  tests/test_marketplace_wire.py `
  tests/test_marketplace_silver_sink.py `
  -q

python -m pytest tests -q

python -m data_ingestion.producer `
  --source tests/fixtures/events.csv --test-mode -n 2

git diff --check
git diff --stat
```

Also run one no-service smoke that starts from two fixed Phase 1 observation
events for the same offer with a price drop between them, a fake state store,
fake publisher, fake Elasticsearch writer, fake Redis writer and a fixed clock.
Assert:

1. the first event emits exactly `NEW_OFFER`;
2. the second emits `PRICE_CHANGED` and `LARGE_PRICE_DROP`;
3. all change IDs are stable across a second identical run;
4. replaying the second event emits `DUPLICATE` with no writes;
5. no Kafka, Redis, Elasticsearch, Spark or database object is constructed.

## 16. Commit/work-package sequence

1. `feat: add marketplace change event factory` — SPD-01 only.
2. `feat: add pure marketplace change rules` — SPD-02 only.
3. `feat: add idempotent offer state store` — SPD-03 only.
4. `feat: add marketplace change producer` — SPD-04 only.
5. `feat: add deterministic marketplace serving documents` — SPD-05 only.
6. `feat: add marketplace speed layer micro-batch` — SPD-06 only.
7. `feat: add marketplace realtime kibana draft` — SPD-07 only.
8. `test: verify speed layer replay and compatibility` — test/compat fixes only.

Stop after each numbered package and show focused tests plus
`git diff --stat`. Do not implement the phase in one commit.

## 17. Definition of Done

- [ ] dependency gate recorded and green;
- [ ] `MarketplaceChangeV1` unchanged, only an additive factory;
- [ ] exactly seven change types, no new type and no new contract field;
- [ ] change identity deterministic and rule-version scoped;
- [ ] checkpoint directory versioned alongside the rule version;
- [ ] duplicate, out-of-order and conflict observations counted, not detected;
- [ ] intra-batch transitions detected by folding state forward;
- [ ] `LARGE_PRICE_DROP` additive to `PRICE_CHANGED`;
- [ ] availability changes only between two known states;
- [ ] public counters never described as sales anywhere;
- [ ] negative counter deltas surfaced, never silently clamped;
- [ ] state advance is the last write before checkpoint;
- [ ] all Redis writes are overwrites, no arithmetic commands;
- [ ] Elasticsearch keeps exact Decimal strings beside numeric fields;
- [ ] every sink failure fails the batch and is audited by stage;
- [ ] audit counts reconcile with observations consumed;
- [ ] stale changes fire once per last-seen observation;
- [ ] pure rules import no engine or client library;
- [ ] driver module imports without pyspark installed;
- [ ] legacy behavioral speed layer, ES indexer and Redis cache untouched;
- [ ] focused tests pass with no external service;
- [ ] full tests pass or environment-only failures are documented.

## 18. Mandatory rejection conditions

Reject the implementation if it:

- adds a field to `MarketplaceChangeV1` or defines a parallel change contract;
- adds a change type beyond the seven frozen members;
- uses a UUID, wall clock, price or title in a change identity;
- keys `marketplace.changes.v1` by anything other than `offer_id`;
- advances stored state before publishing and indexing;
- uses Spark stateful operators or serializes clients to executors;
- uses `INCR`, `HINCRBY` or `ZINCRBY` for any served metric;
- stores money in Elasticsearch only as a float;
- clamps a negative counter delta to zero or renames a counter as sales;
- emits `AVAILABILITY_CHANGED` from or to `UNKNOWN`;
- treats a missing observation as a change without the explicit sweep;
- catches a `foreachBatch` exception and reports the batch as successful;
- republishes Phase 4 bad records into a second DLQ;
- writes Gold marts, PostgreSQL BI tables, anomaly results or Superset assets;
- modifies crawler, Bronze, parser, retry or Silver behavior;
- claims exactly-once delivery.

## 19. Copy-ready prompts for a low-capability model

### Prompt A — change factory

```text
Implement only Section 7 of docs/PHASE_5_SPEED_LAYER_CHANGE_DETECTION_IMPLEMENTATION_PLAN.md.
Read config/marketplace_schema.py and common/identity.py fully first. Add only
the additive create_change_event() factory plus the Section 5 settings. Do not
change any existing dataclass, enum or validator. Run
tests/test_marketplace_schema.py and your new factory tests, show diff stat and
stop.
```

### Prompt B — pure rules

```text
Implement only Section 8 and tests 1-21 of the Phase 5 plan. Create
speed_layer/change_rules.py with no pyspark, kafka, redis, elasticsearch or
psycopg2 import anywhere. Follow the change table in Section 6.5 exactly,
including the single-RATING_CHANGED rule and the additive LARGE_PRICE_DROP.
Return no next state for duplicate, out-of-order and conflict outcomes. Run
focused tests and stop.
```

### Prompt C — state store

```text
Implement only Section 9 and tests 22-27 of the Phase 5 plan. Create
speed_layer/offer_state.py and serving_layer/marketplace_redis.py. Batch reads
through one pipeline, refuse backwards writes, use only overwrite commands and
trim the recent-change sorted set to the configured maximum. Write one shared
test body that runs against both the in-memory fake and a fake Redis client.
Run focused tests and stop.
```

### Prompt D — producer

```text
Implement only Section 10 and tests 28-31 of the Phase 5 plan. Create
data_ingestion/marketplace_change_producer.py. Key by offer_id, reuse the
Phase 4 canonical byte form exactly, wait for ack and propagate failures with
the acked count. Do not publish changes to any DLQ. Run focused tests and stop.
```

### Prompt E — serving documents

```text
Implement only Section 11 and tests 32-35 of the Phase 5 plan. Create
serving_layer/marketplace_es.py with deterministic _id, overwrite index
semantics, scaled_float plus exact keyword money fields and only the listed
enrichment fields. Add index creation to scripts/start_all.ps1 without touching
legacy indices. No field may be named after sales. Run focused tests and stop.
```

### Prompt F — micro-batch driver

```text
Implement Section 12 and tests 36-46 of the Phase 5 plan. Create
speed_layer/marketplace_speed_layer.py with process_micro_batch(),
run_stale_sweep() and a Spark entry point whose pyspark imports live inside
functions. Group by offer, order by observed time, fold state forward in
memory, respect the Section 6.2 sink order and write the audit row on both
success and failure before re-raising. Add the audit DDL to
scripts/init_postgres.sql. Run focused tests and stop.
```

### Prompt G — Kibana draft and final verification

```text
Implement Section 13 and the Section 15 verification of the Phase 5 plan.
Create display/kibana/setup_marketplace_kibana.py idempotently, reusing existing
patterns without editing display/kibana/setup_kibana.py. Then run the full
Phase 5 test matrix, the legacy producer smoke and the no-service change smoke,
document environment-only failures, show diff stat and stop. Do not start Gold,
PostgreSQL or Superset work.
```

## 20. Handoff to Phase 6

Phase 6 reads canonical Silver observations, not this phase's change events. It
recomputes price movement, freshness and coverage in batch from the full
history, so the Lambda architecture's batch view can correct anything the speed
layer approximated or missed while a sink was down. Phase 6 must not read Redis
or Elasticsearch as an input, and must not treat `marketplace.changes.v1` as a
source of truth.

The Phase 5 product is not "a dashboard that shows prices". It is a bounded,
deterministic, replay-safe derivation of what changed, whose every emitted event
can be traced back to two specific observation IDs and one rule version.
