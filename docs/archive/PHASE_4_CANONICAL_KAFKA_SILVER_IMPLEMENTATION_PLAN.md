# Phase 4 implementation plan — Canonical Kafka and idempotent Silver landing

> Status: ready for implementation after Phases 2 and 3 are accepted
>
> Intended implementer: a low-capability coding model working one work package
> at a time. It must follow the fixed contracts below and stop between packages.
>
> Parent plan: `Brief để xây dựng plan sơ bộ cho việc cải tiến ecommerce-lambda-architecture.md`

## 1. Objective

Implement the parent plan's Week 4 vertical slice and backlog P1-01 through
P1-04:

```text
validated MarketplaceObservationV1
-> keyed Kafka observation topic
-> strict v1 wire decode and integrity validation
-> deterministic Silver object

invalid Kafka record
-> deterministic quarantine object
-> versioned observation DLQ record
```

This phase ends at canonical Silver landing. Change detection, Redis,
Elasticsearch and Gold marts remain later phases.

## 2. Mandatory dependency gate

Before editing, record and verify:

1. Phase 1 marketplace contracts and deterministic helpers pass.
2. Accepted Phase 2 emits a complete `MarketplaceObservationV1` only after raw
   Bronze persistence.
3. Accepted Phase 3 exposes a post-acquisition success path where a publisher
   callback can be invoked and publish failures can be audited/retried.
4. Every observation contains raw URI, SHA-256, adapter version and crawl run ID.

If any dependency is missing, stop. Do not implement crawler acquisition,
scheduler or retry again inside Phase 4. Import-path adaptation is allowed only
after the reviewer identifies the accepted API.

## 3. Scope

### 3.1 In scope

- Central registry for observation, observation-DLQ and change topics.
- Kafka producer for `MarketplaceObservationV1` with correct key and ack.
- Strict wire decoder back into Phase 1 frozen dataclasses.
- Integrity recomputation for offer and observation IDs.
- Canonical Spark wire/Silver schemas for later consumers.
- Kafka consumer that lands canonical event JSON in partitioned Silver paths.
- Deterministic object paths keyed by observation ID for logical idempotency.
- Versioned DLQ envelope for decode/contract failures.
- Deterministic quarantine objects retaining source topic/partition/offset.
- Manual offset commit only after the relevant sink contract succeeds.
- Topic initialization in the existing start script.
- Offline unit/contract tests and optional service integration tests.

### 3.2 Out of scope

- Spark Structured Streaming execution and change detection.
- `marketplace.changes.v1` production.
- Elasticsearch or Redis sinks.
- Gold/temporal marts and PostgreSQL serving cache.
- Product matching or a second marketplace.
- Crawler fetching/parsing and Bronze layout changes.
- Retry/frontier redesign.
- Docker Compose service definitions or dashboards.
- Exactly-once claims across Kafka and object storage.

The implementation may claim deterministic/idempotent logical landing, not a
distributed exactly-once transaction.

## 4. Allowed file changes

Create:

```text
config/topics.py
config/marketplace_wire.py
config/marketplace_dlq.py
data_ingestion/marketplace_producer.py
data_ingestion/marketplace_silver_sink.py
tests/test_marketplace_topics.py
tests/test_marketplace_wire.py
tests/test_marketplace_producer.py
tests/test_marketplace_silver_sink.py
```

Modify only:

```text
config/settings.py
data_ingestion/schemas.py
common/dlq.py
crawler/runner.py
scripts/start_all.ps1
tests/test_ingestion_contract.py
```

`crawler/runner.py` may only wire a successful Phase 2/3 observation to the new
publisher callback. Do not alter fetching, raw persistence, parsing, leases or
retry algorithms.

## 5. Topic registry

### 5.1 Settings

Add these environment-backed settings:

```python
KAFKA_TOPIC_MARKETPLACE_OBSERVATIONS = "marketplace.observations.v1"
KAFKA_TOPIC_MARKETPLACE_OBSERVATIONS_DLQ = "marketplace.observations.v1.dlq"
KAFKA_TOPIC_MARKETPLACE_CHANGES = "marketplace.changes.v1"
KAFKA_MARKETPLACE_PARTITIONS = 3
KAFKA_PRODUCER_ACK_TIMEOUT_SECONDS = 30
KAFKA_SILVER_CONSUMER_GROUP = "marketplace-silver-v1"
```

Keep existing legacy topic settings unchanged.

### 5.2 Public registry API

In `config/topics.py` implement:

```python
@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int = 1

MARKETPLACE_OBSERVATIONS = TopicSpec(...)
MARKETPLACE_OBSERVATIONS_DLQ = TopicSpec(...)
MARKETPLACE_CHANGES = TopicSpec(...)
MARKETPLACE_TOPIC_SPECS: tuple[TopicSpec, ...] = (...)
```

Validate non-empty names, positive partitions/replication and uniqueness. Topic
names come only from settings. Do not derive the DLQ name at call time.

`scripts/start_all.ps1` must iterate/create all three registry-equivalent names
with `--if-not-exists`. Preserve legacy `ecommerce_events` creation until the
legacy pipeline is formally retired.

## 6. Strict marketplace wire decoder

Create `config/marketplace_wire.py`. It converts a decoded JSON mapping to the
existing Phase 1 dataclasses; it does not read Kafka or write storage.

### 6.1 Public API

```python
def marketplace_observation_from_wire(
    value: Mapping[str, Any],
) -> MarketplaceObservationV1: ...
```

### 6.2 Shape rules

Require exactly the Phase 1 fields for:

- MarketplaceObservationV1 top level;
- `payload`;
- MarketplaceOffer;
- OfferObservation.

Missing and unknown fields raise `WireContractError` containing the object path
and field names. Require `schema_version == marketplace-observation.v1` and
`event_type == OFFER_OBSERVED` before nested construction.

### 6.3 Type conversion

- datetime values must be ISO-8601 strings with timezone offsets; normalize UTC;
- required money/decimal values must be plain decimal strings, never JSON float;
- nullable decimals accept string or null;
- integer counts accept JSON integers but reject bool;
- enums accept only their exact string values;
- `promotion` accepts a JSON object or null;
- optional text remains null; do not turn null into empty string;
- raw URI/checksum/run/adapter lineage remains mandatory through Phase 1
  dataclass validation.

Catch conversion errors and raise `WireContractError` with field path while
preserving the original exception as `__cause__`. Never silently drop a field.

### 6.4 Integrity rules

After construction, recompute and require:

```python
offer.offer_id == make_offer_id(event.marketplace, offer.platform_listing_id)
observation.observation_id == make_observation_id(
    event.marketplace,
    offer.platform_listing_id,
    observation.observed_at,
    observation.raw_sha256,
)
event.partition_key == f"{event.marketplace.lower()}:{offer.platform_listing_id}"
event.event_id == observation.observation_id
```

Reject forged/mismatched IDs. Do not recompute seller ID because the v1 envelope
does not contain `platform_seller_id`.

## 7. Spark schemas

Append schemas to `data_ingestion/schemas.py`; keep `BEHAVIOR_EVENT_SCHEMA`
unchanged.

Expose:

```python
MARKETPLACE_OBSERVATION_WIRE_SCHEMA: StructType
MARKETPLACE_SILVER_SCHEMA: StructType
```

Wire schema mirrors the nested JSON envelope. Use:

- StringType for ISO timestamps, IDs, versions, URLs, enums and Decimal wire strings;
- LongType for counts/ranking;
- BooleanType only for actual booleans;
- MapType(StringType(), StringType(), True) for the limited promotion projection;
- nested StructType for payload/offer/observation.

Silver schema is flat and uses:

- TimestampType for occurred/produced/observed/fetched timestamps;
- DecimalType(38, 6) for monetary/rating/discount values;
- LongType for counts;
- StringType `promotion_json` preserving the canonical JSON representation;
- DateType `observed_date` derived from UTC observed time.

Every non-null requirement must match Phase 1. Add a schema contract test that
compares all field names and nested nullability explicitly. Do not change the
legacy schema or speed layer in this phase.

## 8. Observation producer

Create `data_ingestion/marketplace_producer.py`.

### 8.1 Producer creation

```python
def create_marketplace_producer(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
) -> Any: ...
```

Use `KafkaProducer` with:

- JSON UTF-8 value serialization after `serialize_for_wire()`;
- UTF-8 string key serialization;
- `acks="all"`;
- `enable_idempotence=True` when supported by the declared kafka-python-ng;
- bounded connection and delivery retries;
- no random event IDs and no generated event timestamps.

Import Kafka lazily so pure unit tests run without a broker.

### 8.2 Publish API

```python
def publish_observation(
    producer: Any,
    event: MarketplaceObservationV1,
    *,
    ack_timeout_seconds: int = KAFKA_PRODUCER_ACK_TIMEOUT_SECONDS,
) -> Any: ...
```

Rules:

- require a real MarketplaceObservationV1;
- revalidate it through serialize then strict wire decoder before sending;
- send only to `MARKETPLACE_OBSERVATIONS.name`;
- key is exactly `event.partition_key`, never offer title or crawl run ID;
- wait on returned future `.get(timeout=...)` before reporting success;
- propagate delivery failure; do not catch/log-and-continue;
- do not flush per record; caller flushes at run/batch boundary.

The thin crawler integration publishes only successful canonical outcomes.
Acquisition failures are not converted into fake observations.

## 9. Versioned marketplace DLQ

Create `config/marketplace_dlq.py` with:

```python
MARKETPLACE_DLQ_SCHEMA_VERSION = "marketplace-observation-dlq.v1"

class DlqStage(str, Enum):
    DECODE = "DECODE"
    CONTRACT_VALIDATION = "CONTRACT_VALIDATION"

@dataclass(frozen=True)
class MarketplaceObservationDlqV1:
    dlq_id: str
    schema_version: str
    failed_at: datetime
    stage: DlqStage
    source_topic: str
    source_partition: int
    source_offset: int
    source_key: str | None
    marketplace: str | None
    crawl_run_id: str | None
    raw_artifact_id: str | None
    raw_uri: str | None
    error_type: str
    error_message: str
    payload_text: str
```

Factory:

```python
def create_observation_dlq(
    *, failed_at: datetime, stage: DlqStage,
    source_topic: str, source_partition: int, source_offset: int,
    source_key: str | None, payload_text: str, error: Exception,
    marketplace: str | None = None, crawl_run_id: str | None = None,
    raw_artifact_id: str | None = None, raw_uri: str | None = None,
) -> MarketplaceObservationDlqV1: ...
```

DLQ identity is deterministic from source topic, partition, offset and stage:

```python
deterministic_id("dlq", source_topic, source_partition, source_offset, stage)
```

Validate UTC, non-negative partition/offset, required source/error/payload text,
schema version and supported raw URI when present. Truncate error message to
2000 characters. Limit payload text to 1,048,576 UTF-8 bytes without leaving an
invalid UTF-8 sequence. Payload is text, not an arbitrary Python object, so
wire serialization cannot fail unexpectedly.

Extend `common/dlq.py` with a new function while preserving the legacy API:

```python
def publish_marketplace_dlq(
    producer: Any,
    record: MarketplaceObservationDlqV1,
    *, ack_timeout_seconds: int,
) -> Any: ...
```

Send to the exact DLQ registry topic, key by `dlq_id`, wait for ack and propagate
failure. Do not use the old best-effort swallow behavior for the Silver sink.

## 10. Deterministic Silver and quarantine layout

The Phase 4 sink stores one canonical JSON envelope per observation. This is a
correctness-first landing format; later batch work may compact it to Parquet.

Valid path:

```text
silver/marketplace/offer_observations/
  marketplace=<code>/observed_date=<yyyy-mm-dd>/
  observation_id=<observation_id>.json
```

Quarantine path:

```text
silver/quarantine/offer_observations/
  observed_date=<ingest-yyyy-mm-dd>/source_topic=<topic>/
  partition=<partition>/offset=<offset>.json
```

Use URL-escaped path components and `put_bytes("silver", relative_path, data)`.
Canonical valid bytes use UTF-8 JSON with `ensure_ascii=False`, `sort_keys=True`
and compact separators. Reprocessing the same observation writes identical
bytes to the same logical object path.

Quarantine JSON contains the versioned DLQ record only. It must not contain
cookies, headers, credentials or Python repr/tracebacks.

## 11. Kafka-to-Silver sink

Create `data_ingestion/marketplace_silver_sink.py`.

### 11.1 Pure record API

Define:

```python
@dataclass(frozen=True)
class SourceRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes

@dataclass(frozen=True)
class SilverSinkResult:
    status: str  # exactly "SILVER" or "QUARANTINE"
    object_uri: str
    event_id: str | None
    dlq_id: str | None

def process_record(
    record: SourceRecord,
    *, writer: Callable[[str, str, bytes], str],
    dlq_producer: Any,
    clock: Callable[[], datetime],
) -> SilverSinkResult: ...
```

Algorithm:

1. require source topic is observation topic;
2. decode value as strict UTF-8 then JSON object;
3. decode with `marketplace_observation_from_wire`;
4. require decoded Kafka key equals event.partition_key;
5. write deterministic Silver object and return SILVER;
6. on UTF-8/JSON/wire/key error, build deterministic DLQ record;
7. write quarantine object first;
8. publish DLQ and wait for ack;
9. return QUARANTINE.

For an invalid UTF-8 value, create quarantine `payload_text` with replacement
characters only after strict decoding has failed. For a non-UTF-8 Kafka key,
store the key as `base64:<standard-base64>` in the DLQ record. These fallbacks
are evidence preservation only and must never be used for canonical decoding.

Do not catch object-store or DLQ delivery failures. The consumer must not commit
their offsets. Do not send Silver storage failures to the bad-record DLQ; they
are operational sink failures and the same valid record must be retried.

### 11.2 Consumer loop

```python
def create_consumer(...) -> Any: ...
def run_sink(...) -> None: ...
```

Use `KafkaConsumer` with:

- observation topic only;
- configured consumer group;
- `enable_auto_commit=False`;
- raw bytes for key/value;
- `auto_offset_reset="earliest"` for a new group.

Process one record at a time in this phase. Commit exactly `offset + 1` for that
topic partition only after `process_record` returns SILVER or QUARANTINE. On
writer/DLQ failure, do not commit, close cleanly and propagate a non-zero CLI
failure. Flush/close the DLQ producer and close the consumer in `finally`.

## 12. Required tests

No default test opens Kafka, MinIO, Spark cluster, PostgreSQL or network.

### Topic/schema/wire tests

1. topic names and specs are exact and unique;
2. invalid TopicSpec fails;
3. legacy behavior Spark schema remains unchanged;
4. marketplace nested schema field names/types/nullability are exact;
5. valid serialized Phase 1 event decodes to an equal object;
6. timezone values normalize to UTC;
7. JSON float for money is rejected;
8. bool counts are rejected;
9. unknown/missing fields identify their object path;
10. wrong version/type/enum is rejected;
11. forged offer ID, observation ID and partition key are rejected;
12. raw lineage omissions are rejected.

### Producer/DLQ tests

13. observation publishes to the exact topic and partition key;
14. producer waits for ack with configured timeout;
15. delivery failures propagate;
16. wrong object type is rejected before send;
17. DLQ ID is deterministic by Kafka coordinates/stage;
18. DLQ truncates oversized error/payload safely;
19. marketplace DLQ waits for ack and never calls legacy swallow behavior;
20. legacy `publish_to_dlq()` tests remain unchanged and pass.

### Silver sink tests

21. valid record writes canonical bytes to deterministic observation path;
22. duplicate valid record writes the same path and identical bytes;
23. two observation IDs write different paths;
24. mismatched Kafka key is quarantined;
25. invalid UTF-8, JSON and contract each quarantine and publish DLQ;
26. quarantine path is deterministic from topic/partition/offset;
27. quarantine write happens before DLQ publish;
28. Silver write failure propagates and does not publish bad-record DLQ;
29. quarantine write failure prevents DLQ publish;
30. DLQ ack failure propagates;
31. consumer commits offset+1 only after SILVER/QUARANTINE success;
32. consumer does not commit on any sink failure;
33. crawler integration publishes successful outcomes only;
34. replaying the same observation does not create a second logical Silver path.

Optional tests marked `integration` may use a local Kafka/MinIO/Spark environment
only when explicit environment variables are present. They must skip by default.

## 13. Verification commands

Run focused tests after each package, then:

```powershell
python -m pytest `
  tests/test_marketplace_topics.py `
  tests/test_marketplace_wire.py `
  tests/test_marketplace_producer.py `
  tests/test_marketplace_silver_sink.py `
  tests/test_ingestion_contract.py `
  tests/test_serialization.py `
  tests/test_identity.py `
  tests/test_marketplace_schema.py `
  tests/test_crawler.py `
  tests/test_crawl_worker.py `
  -q
python -m pytest tests -q
python -m data_ingestion.producer `
  --source tests/fixtures/events.csv --test-mode -n 2
git diff --check
git diff --stat
```

Also run one fake-producer/fake-writer smoke that starts from a fixed Phase 1
event, publishes, processes a synthetic SourceRecord and asserts the final
Silver JSON contains the same event/observation ID and raw URI.

## 14. Commit/work-package sequence

1. `feat: add marketplace topic registry`
   - settings, registry, topic tests and topic initialization only.
2. `feat: add strict marketplace wire decoder`
   - wire decoder and tests only.
3. `feat: add canonical marketplace kafka producer`
   - producer and thin crawler callback only.
4. `feat: add versioned marketplace observation dlq`
   - new DLQ contract/API while preserving legacy helper.
5. `feat: add deterministic kafka to silver sink`
   - sink and sink tests only.
6. `feat: add marketplace spark schemas`
   - schema additions and contract tests only.
7. `test: verify canonical kafka silver compatibility`
   - compatibility/smoke fixes only.

The model must stop after each package and show tests plus `git diff --stat`.
Do not implement the whole phase in one commit.

## 15. Definition of Done

- [ ] dependency gate is recorded;
- [ ] exact three marketplace topics are centrally registered/initialized;
- [ ] observation producer uses canonical key and waits for broker ack;
- [ ] strict decoder reconstructs and validates the Phase 1 object graph;
- [ ] offer/observation IDs and partition key are recomputed for integrity;
- [ ] canonical money remains Decimal/string without float conversion;
- [ ] every valid Silver object retains complete raw lineage;
- [ ] duplicate observation IDs resolve to the same Silver object path;
- [ ] malformed records create deterministic quarantine and DLQ records;
- [ ] operational sink failures are not mislabeled as bad data;
- [ ] offsets commit only after Silver or quarantine+DLQ success;
- [ ] legacy behavioral ingestion remains runnable;
- [ ] no speed/Gold/ES/Redis/dashboard work is introduced;
- [ ] focused tests pass without external services;
- [ ] full tests pass or environment failures are documented.

## 16. Mandatory rejection conditions

Reject the implementation if it:

- publishes before Phase 2 raw persistence;
- uses title, price or random UUID as Kafka key/event identity;
- serializes Decimal as float;
- accepts unknown schema versions silently;
- uses auto-commit;
- commits before storage/DLQ acknowledgement;
- swallows Silver write or marketplace DLQ failures;
- stores invalid records without source topic/partition/offset;
- appends duplicate logical objects using random filenames;
- changes the legacy behavioral schema;
- starts change detection, Spark streaming sinks or downstream serving early.

## 17. Copy-ready prompts for a low-capability model

### Prompt A — topics

```text
Implement only Sections 5 and the topic tests from
docs/PHASE_4_CANONICAL_KAFKA_SILVER_IMPLEMENTATION_PLAN.md. Read the full plan
and config/settings.py first. Create config/topics.py, add only the listed
settings and update only Kafka topic initialization in scripts/start_all.ps1.
Keep legacy topics. Run focused tests, show diff stat and stop.
```

### Prompt B — wire decoder

```text
Implement only Section 6 and tests 5–12 of the Phase 4 plan. Read all Phase 1
marketplace dataclasses and identity helpers first. Create only
config/marketplace_wire.py and tests/test_marketplace_wire.py. Be strict about
shape, Decimal strings, UTC, enums, lineage and recomputed IDs. Run focused
tests and stop. Do not import Kafka/Spark or edit crawler code.
```

### Prompt C — producer

```text
Implement only Section 8 of the Phase 4 plan. Create
data_ingestion/marketplace_producer.py and its fake-producer tests. Add only the
thin accepted callback in crawler/runner.py. Publish successful Phase 1 events
with exact key/topic and wait for ack. Run focused and crawler compatibility
tests, then stop. Do not add consumer, DLQ, Silver or Spark code.
```

### Prompt D — DLQ

```text
Implement only Section 9 of the Phase 4 plan. Create the versioned DLQ contract
and tests; extend common/dlq.py without changing the existing publish_to_dlq
behavior or tests. Use caller-supplied fixed timestamps and deterministic Kafka
coordinate IDs. Run focused plus legacy DLQ tests and stop.
```

### Prompt E — Silver sink

```text
Implement Sections 10–11 and Silver sink tests from the Phase 4 plan. Use raw
Kafka bytes, strict decoder, deterministic object paths, quarantine-before-DLQ
and manual commit after all required acknowledgements. Inject writer/producer/
clock in unit tests. Do not start Spark streaming or downstream serving. Run
focused tests, show diff stat and stop.
```

### Prompt F — Spark schemas and final verification

```text
Implement only Section 7 and final compatibility verification from the Phase 4
plan. Append marketplace schemas without changing BEHAVIOR_EVENT_SCHEMA. Add
exact schema contract tests, run the full Phase 4 matrix and legacy producer
smoke, document environment-only failures, show diff stat and stop. Do not
implement change detection, Gold, Elasticsearch, Redis or dashboards.
```

## 18. Handoff to Phase 5

Phase 5 may consume `marketplace.observations.v1` with the fixed Spark wire
schema, compare previous/current offer state and emit bounded
`MarketplaceChangeV1` events. It must treat Silver/raw observations as history
and must not replace them with derived changes.
