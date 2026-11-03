# Phase 1 implementation plan — Marketplace foundation

> Status: ready for implementation
>
> Intended implementer: a coding model that should follow explicit, small tasks
> and should not make architecture or scope decisions on its own.
>
> Parent plan: `Brief để xây dựng plan sơ bộ cho việc cải tiến ecommerce-lambda-architecture.md`

## 1. Objective

Build the shared foundation required by the crawler-first marketplace pipeline
without replacing or breaking the existing behavioral pipeline.

This phase implements only:

1. A generic JSON-wire serializer.
2. Deterministic identity helpers.
3. Versioned marketplace domain contracts and validation.
4. Compatibility wiring for the existing `to_wire()` API.
5. Unit tests for all of the above.

At the end of this phase, the repository still runs the legacy Kaggle
`view/cart/purchase` pipeline. No Kafka topic, Spark job, database table,
crawler orchestration, or dashboard is migrated yet.

## 2. Why this phase comes first

The current crawler cannot publish a normalized snapshot because
`config.schema.to_wire()` serializes `event_time` only. A price snapshot contains
`snapshot_time`, so `json.dumps(to_wire(snapshot))` raises:

```text
TypeError: Object of type datetime is not JSON serializable
```

The future crawler, Kafka, Silver, speed, batch, Elasticsearch, and Redis paths
also need to agree on deterministic IDs and one versioned contract. Those
primitives must be fixed before any downstream refactor starts.

## 3. Scope boundaries

### 3.1 In scope

- `datetime`, `Decimal`, `Enum`, dataclass, mapping, list and tuple wire
  serialization.
- Stable SHA-256 identifiers for raw artifacts, offers, observations and
  derived changes.
- Marketplace domain dataclasses and enums.
- Explicit validation of required fields, UTC timestamps, prices, counts,
  checksums and lineage.
- Observation and change event envelopes.
- Backward compatibility for behavioral event serialization.
- Fast unit tests without network, Kafka, Spark, Docker, MinIO or PostgreSQL.

### 3.2 Out of scope

Do not implement or modify any of the following in this phase:

- Tiki HTTP fetching or pagination.
- `SiteCrawler` fetch/parse separation.
- Bronze storage paths or MinIO writes.
- Kafka topic creation or publishing behavior.
- Spark schemas or streaming jobs.
- PostgreSQL DDL.
- Elasticsearch or Redis writers.
- Batch marts, anomaly logic, dashboards or ML.
- A second marketplace.
- Product entity matching.
- Moving legacy code into a `legacy/` directory.
- New third-party dependencies such as Pydantic.

## 4. Safety and compatibility rules

The implementer must follow all of these rules:

1. Keep `normalize_event()`, `validate_event()`, `CANONICAL_FIELDS`,
   `EVENT_TYPES`, and the legacy behavioral contract working.
2. Keep the public function name `config.schema.to_wire()` working because
   both `data_ingestion.producer` and `crawler.runner` import it.
3. Do not rename existing files or functions.
4. Do not modify `speed_layer/`, `batch_layer/`, `serving_layer/`, `display/`,
   `docker-compose.yml`, SQL files, or PowerShell scripts.
5. Do not perform live network calls in tests.
6. Do not use random UUIDs for entity or event identities covered by this
   phase. `crawl_run_id` is supplied by the future orchestration layer and may
   be a UUID because it identifies one execution, not a stable business entity.
7. Do not silently convert unsupported objects with `str(value)` in the wire
   serializer. Raise `TypeError` instead.
8. Do not use a float as the canonical representation of money. Use
   `Decimal` in memory and a plain decimal string on the JSON wire.
9. Do not infer missing availability as in stock. `UNKNOWN` is the default.
10. Do not generate timestamps inside serialization or identity functions.
    The caller must provide the observation/fetch timestamps.

## 5. Target file changes

Create these files:

```text
common/serialization.py
common/identity.py
config/marketplace_schema.py
tests/test_serialization.py
tests/test_identity.py
tests/test_marketplace_schema.py
```

Modify only these existing files:

```text
config/schema.py
tests/test_schema.py
```

No other file should change during this phase.

## 6. Fixed contract decisions

These decisions are already made. The implementer must not choose alternatives.

### 6.1 Time

- New marketplace dataclasses require timezone-aware `datetime` values.
- Marketplace timestamps are normalized to UTC for identity generation and
  wire serialization.
- Aware timestamps serialize with an explicit `+00:00` offset.
- Legacy naive behavioral datetimes remain serializable for backward
  compatibility; do not add a new validation restriction to legacy events.
- Identity timestamps use microsecond precision.

Example:

```text
2026-09-04T08:30:00.123456+00:00
```

### 6.2 Money and numeric values

- Marketplace prices use `Decimal` in memory.
- `Decimal("16990000.00")` serializes to the JSON string
  `"16990000.00"`.
- The serializer must preserve the scale supplied by the dataclass value.
- Numeric counts use `int`.
- All price/count values must be non-negative unless a future derived contract
  explicitly allows negative values. This phase has no such exception.

### 6.3 Missing values

- Missing optional values remain `None` and become JSON `null`.
- Availability defaults to `Availability.UNKNOWN`.
- Seller official status defaults to `OfficialStatus.UNKNOWN`.
- Offer active status defaults to `OfferActiveStatus.UNKNOWN`.
- Do not turn an empty or absent source field into zero, `False`, or in-stock.

### 6.4 IDs

- IDs are lowercase SHA-256 hex digests with a readable prefix.
- The separator between identity parts is ASCII unit separator `\x1f`.
- All input parts are normalized before hashing.
- Full 64-character hashes are retained; do not truncate them.

Output shape:

```text
<prefix>_<64 lowercase hex characters>
```

Examples of prefixes:

```text
raw_<hash>
seller_<hash>
offer_<hash>
obs_<hash>
change_<hash>
```

### 6.5 Schema versions

Use exactly these constants:

```python
OBSERVATION_SCHEMA_VERSION = "marketplace-observation.v1"
CHANGE_SCHEMA_VERSION = "marketplace-change.v1"
OBSERVATION_EVENT_TYPE = "OFFER_OBSERVED"
```

Do not make version strings configurable through environment variables.

## 7. Work package FND-00 — Baseline

### Purpose

Establish that test failures after this phase are caused by the change, not by
an unknown starting state.

### Steps

1. Use Python 3.12.
2. Ensure `pytest` and the already-declared test dependencies are installed.
3. Run:

   ```powershell
   python -m pytest tests -q
   ```

4. Record the result in the PR description.
5. Do not fix unrelated legacy failures in this work package. List them as
   pre-existing failures.

### Exit criteria

- The exact Python version and baseline test result are recorded.
- No repository file is changed by this work package.

## 8. Work package FND-01 — Generic wire serializer

### Files

- Create `common/serialization.py`.
- Modify `config/schema.py`.
- Create `tests/test_serialization.py`.
- Modify `tests/test_schema.py`.

### 8.1 Required public API

Implement this exact public function:

```python
def serialize_for_wire(value: Any) -> Any:
    """Return a JSON-compatible representation of value.

    Raise TypeError for unsupported values or mappings with non-string keys.
    """
```

Do not add a JSON string encoder. This function returns Python primitives; the
caller still invokes `json.dumps()`.

### 8.2 Required type behavior

Apply rules in this order:

1. `None`, `str`, `int`, `float`, `bool`: return unchanged.
2. `datetime`:
   - if timezone-aware, convert to UTC and return `isoformat()`;
   - if naive, return `isoformat()` unchanged for legacy compatibility.
3. `Decimal`: return `format(value, "f")`.
4. `Enum`: recursively serialize `value.value`.
5. Dataclass instance: iterate `dataclasses.fields(value)` and recursively
   serialize every field into a dictionary.
6. `Mapping`: require every key to be `str`; recursively serialize values.
7. `list` or `tuple`: recursively serialize elements into a list.
8. Any other type: raise `TypeError` with the unsupported type name.

Do not support `set`, arbitrary iterators, bytes, Path, numpy scalars, pandas
objects, or Spark Rows in this phase.

### 8.3 Compatibility change in `config/schema.py`

Import `serialize_for_wire` and replace the current field-name-specific
implementation with:

```python
def to_wire(event: dict[str, Any]) -> dict[str, Any]:
    """Serialize a supported event mapping for JSON transport."""
    wire = serialize_for_wire(event)
    if not isinstance(wire, dict):
        raise TypeError("event must serialize to a dictionary")
    return wire
```

Do not change either normalizer in this work package.

### 8.4 Required tests

Add these exact test behaviors:

1. `test_serialize_for_wire_handles_nested_supported_values`
   - nested dataclass;
   - timezone-aware datetime;
   - Decimal;
   - Enum;
   - tuple nested in a dictionary;
   - `None`.
2. `test_serialize_for_wire_preserves_decimal_scale`
   - `Decimal("10.50")` becomes `"10.50"`.
3. `test_serialize_for_wire_converts_aware_datetime_to_utc`
   - two-hour offset becomes the equivalent `+00:00` time.
4. `test_serialize_for_wire_keeps_legacy_naive_datetime_serializable`.
5. `test_serialize_for_wire_rejects_unknown_type`.
6. `test_serialize_for_wire_rejects_non_string_mapping_key`.
7. Update `tests/test_schema.py` with
   `test_to_wire_serializes_price_snapshot_time_to_json`:

   ```python
   snapshot = normalize_price_snapshot({
       "site": "tiki",
       "product_id": "p1",
       "price": 100,
   })
   payload = json.dumps(to_wire(snapshot))
   assert "snapshot_time" in payload
   ```
8. Keep the existing behavioral `event_time` serialization test unchanged.

### Exit criteria

- `json.dumps(to_wire(snapshot))` succeeds.
- Legacy event serialization still succeeds.
- Unsupported types fail loudly.
- All serializer tests pass.

## 9. Work package FND-02 — Deterministic identity

### Files

- Create `common/identity.py`.
- Create `tests/test_identity.py`.

### 9.1 Required public API

Implement these exact functions:

```python
def deterministic_id(prefix: str, *parts: Any) -> str: ...

def make_raw_artifact_id(
    marketplace_code: str,
    request_url: str,
    fetched_at: datetime,
    body_sha256: str,
) -> str: ...

def make_offer_id(
    marketplace_code: str,
    platform_listing_id: str,
) -> str: ...

def make_seller_id(
    marketplace_code: str,
    platform_seller_id: str,
) -> str: ...

def make_observation_id(
    marketplace_code: str,
    platform_listing_id: str,
    observed_at: datetime,
    raw_sha256: str,
) -> str: ...

def make_change_id(
    offer_id: str,
    current_observation_id: str,
    change_type: str,
    rule_version: str,
) -> str: ...
```

### 9.2 Normalization algorithm

Use one private helper `_normalize_identity_part(value)`:

- `None` becomes an empty string.
- `Enum` uses its `.value`, then normalizes recursively.
- A datetime must be timezone-aware. Convert to UTC and call
  `isoformat(timespec="microseconds")`. Raise `ValueError` for naive datetime.
- `Decimal` becomes `format(value.normalize(), "f")`; normalize negative zero
  to `"0"`.
- `str` is stripped of surrounding whitespace.
- `int` and `bool` use `str(value)`.
- Any unsupported type raises `TypeError`.

The generic `deterministic_id()` algorithm is:

```python
normalized = [prefix.strip().lower(), *normalized_parts]
payload = "\x1f".join(normalized).encode("utf-8")
digest = hashlib.sha256(payload).hexdigest()
return f"{prefix.strip().lower()}_{digest}"
```

Validation:

- `prefix` must be non-empty and contain only lowercase ASCII letters, digits,
  and underscores after normalization.
- At least one identity part is required.
- Convenience helpers must reject empty required string fields.
- Hash arguments such as `body_sha256` and `raw_sha256` must be exactly 64
  hexadecimal characters and are normalized to lowercase.

Convenience helper composition:

```text
raw artifact = raw(marketplace_code, request_url, fetched_at, body_sha256)
seller       = seller(marketplace_code, platform_seller_id)
offer        = offer(marketplace_code, platform_listing_id)
observation  = obs(marketplace_code, platform_listing_id, observed_at, raw_sha256)
change       = change(offer_id, current_observation_id, change_type, rule_version)
```

Do not include product title, price, seller name, category or parser version in
`offer_id`; those values may change over time.

### 9.3 Required tests

1. Same inputs produce the same ID.
2. Any changed input produces a different ID.
3. Equivalent timestamps with different UTC offsets produce the same ID.
4. Naive datetime raises `ValueError`.
5. Invalid prefix raises `ValueError`.
6. Missing required field in each convenience helper raises `ValueError`.
7. Invalid SHA-256 text raises `ValueError`.
8. Every convenience ID has the correct prefix plus 64 lowercase hex chars.
9. `Decimal("10.0")` and `Decimal("10.00")` normalize identically in the
   generic helper.

### Exit criteria

- IDs are reproducible across separate calls.
- Timezone-equivalent inputs hash identically.
- No random ID generation exists in the new helper.
- All identity tests pass.

## 10. Work package FND-03 — Marketplace domain contracts

### Files

- Create `config/marketplace_schema.py`.
- Create `tests/test_marketplace_schema.py`.

### 10.1 Implementation constraints

- Use the standard library only: `dataclasses`, `datetime`, `decimal`, `enum`,
  `typing`, `urllib.parse` and `re` if needed.
- Use `@dataclass(frozen=True)` for domain records.
- Do not use Pydantic.
- Keep validation in `__post_init__` and small private helper functions.
- Do not write source-specific field names or Tiki-specific logic here.
- Import deterministic ID helpers from `common.identity`.
- Domain instances must be directly serializable through
  `common.serialization.serialize_for_wire()`.

### 10.2 Required enums

Implement string enums with exactly these values:

```python
class Availability(str, Enum):
    IN_STOCK = "IN_STOCK"
    OUT_OF_STOCK = "OUT_OF_STOCK"
    UNKNOWN = "UNKNOWN"

class OfficialStatus(str, Enum):
    OFFICIAL = "OFFICIAL"
    NOT_OFFICIAL = "NOT_OFFICIAL"
    UNKNOWN = "UNKNOWN"

class OfferActiveStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    UNKNOWN = "UNKNOWN"

class CrawlRunStatus(str, Enum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"

class ResourceType(str, Enum):
    LISTING_PAGE = "LISTING_PAGE"
    PRODUCT_DETAIL = "PRODUCT_DETAIL"

class MarketplaceChangeType(str, Enum):
    NEW_OFFER = "NEW_OFFER"
    PRICE_CHANGED = "PRICE_CHANGED"
    LARGE_PRICE_DROP = "LARGE_PRICE_DROP"
    RATING_CHANGED = "RATING_CHANGED"
    COUNTER_CHANGED = "COUNTER_CHANGED"
    AVAILABILITY_CHANGED = "AVAILABILITY_CHANGED"
    OFFER_STALE = "OFFER_STALE"
```

Do not add `SALE`, `PURCHASE`, `DEMAND`, `FRAUD`, or `SCAM` event types.

### 10.3 Validation helpers

Implement private helpers with clear error messages containing the field name:

```python
_require_text(value: str, field_name: str) -> str
_require_aware_datetime(value: datetime, field_name: str) -> datetime
_require_non_negative_decimal(value: Decimal | None, field_name: str) -> None
_require_non_negative_int(value: int | None, field_name: str) -> None
_require_sha256(value: str, field_name: str) -> str
_require_http_url(value: str, field_name: str) -> str
_require_currency(value: str) -> str
```

Rules:

- Text values are stripped and must not be empty when required.
- Datetimes must be timezone-aware; normalize them to UTC inside frozen
  dataclasses using `object.__setattr__`.
- SHA-256 is 64 hexadecimal characters, stored lowercase.
- URLs must use `http` or `https` and have a hostname.
- Currency is exactly three ASCII letters and is stored uppercase.
- `bool` must not be accepted as an integer count.

### 10.4 Required dataclasses

Implement the following fields exactly. Optional fields may default to `None`.

#### `Marketplace`

```python
marketplace_id: str
code: str
name: str
base_url: str
default_currency: str
locale: str
semantics_version: str
active: bool = True
```

Validation:

- All text fields required.
- `code` stored lowercase.
- `base_url` is a valid HTTP(S) URL.
- Currency stored uppercase.
- `active` must be a real boolean.

#### `CrawlRun`

```python
crawl_run_id: str
marketplace_id: str
started_at: datetime
completed_at: datetime | None
status: CrawlRunStatus
requested: int = 0
succeeded: int = 0
failed: int = 0
raw_bytes: int = 0
parsed: int = 0
rejected: int = 0
adapter_version: str = ""
error_summary: dict[str, Any] | None = None
```

Validation:

- IDs, marketplace ID and adapter version required.
- All counters non-negative and cannot be bool.
- `completed_at`, when present, must not be before `started_at`.
- `RUNNING` requires `completed_at is None`.
- Any terminal status requires `completed_at`.
- `succeeded + failed` must not exceed `requested`.
- Do not compare `parsed + rejected` with successful request count. One
  successful listing-page request can legitimately produce many parsed or
  rejected offer records.

#### `RawArtifact`

```python
raw_artifact_id: str
crawl_run_id: str
marketplace_id: str
request_url: str
resource_type: ResourceType
fetched_at: datetime
http_status: int | None
content_type: str | None
body_sha256: str
raw_uri: str
adapter_version: str
raw_bytes: int
```

Validation:

- All IDs, URLs, checksum, URI and adapter version required.
- `request_url` is HTTP(S).
- `raw_uri` is required but may use `s3a`, `s3`, or `file` scheme.
- HTTP status, when present, must be from 100 through 599.
- `raw_bytes` non-negative.
- `raw_artifact_id` must equal `make_raw_artifact_id(...)` using marketplace
  **code**, not marketplace ID. Because this dataclass only has marketplace ID,
  do not recompute the ID in `__post_init__`; verify ID composition in the
  factory described below.

#### `Seller`

```python
seller_id: str
marketplace_id: str
platform_seller_id: str
first_seen_at: datetime
last_seen_at: datetime
seller_name: str | None = None
seller_url: str | None = None
official_status: OfficialStatus = OfficialStatus.UNKNOWN
```

Validation:

- IDs required.
- Optional name becomes `None` if blank.
- Optional URL, when present, must be HTTP(S).
- `last_seen_at >= first_seen_at`.

#### `MarketplaceOffer`

```python
offer_id: str
marketplace_id: str
platform_listing_id: str
seller_id: str | None
product_title: str
brand: str | None
category_path: str | None
source_url: str
currency: str
first_seen_at: datetime
last_seen_at: datetime
active_status: OfferActiveStatus = OfferActiveStatus.UNKNOWN
```

Validation:

- Offer, marketplace, listing ID and title required.
- Optional text becomes `None` when blank.
- Source URL valid.
- Currency uppercase.
- `last_seen_at >= first_seen_at`.

#### `OfferObservation`

```python
observation_id: str
offer_id: str
observed_at: datetime
fetched_at: datetime
current_price: Decimal
list_price: Decimal | None = None
shipping_price: Decimal | None = None
discount_amount: Decimal | None = None
discount_percent: Decimal | None = None
rating_value: Decimal | None = None
rating_scale: Decimal | None = None
rating_count: int | None = None
review_count: int | None = None
sold_count: int | None = None
availability: Availability = Availability.UNKNOWN
promotion: dict[str, Any] | None = None
ranking_position: int | None = None
raw_uri: str = ""
raw_sha256: str = ""
adapter_version: str = ""
crawl_run_id: str = ""
```

Validation:

- Observation ID, offer ID, raw URI, checksum, adapter version and run ID
  required.
- Both timestamps timezone-aware and normalized to UTC.
- All prices non-negative.
- `discount_percent` is from 0 through 100 when present.
- Counts non-negative and cannot be bool.
- Ranking position must be greater than zero when present.
- If `rating_value` exists, `rating_scale` is required and greater than zero.
- `rating_value` cannot exceed `rating_scale`.
- Do not require `observed_at == fetched_at`; they describe different moments.

#### `ObservationPayload`

```python
offer: MarketplaceOffer
observation: OfferObservation
```

Validation:

- `observation.offer_id == offer.offer_id`.

#### `MarketplaceObservationV1`

```python
event_id: str
schema_version: str
event_type: str
occurred_at: datetime
produced_at: datetime
marketplace: str
partition_key: str
crawl_run_id: str
raw_uri: str
payload: ObservationPayload
```

Validation:

- Required text is non-empty.
- `schema_version == OBSERVATION_SCHEMA_VERSION`.
- `event_type == OBSERVATION_EVENT_TYPE`.
- `event_id == payload.observation.observation_id`.
- `occurred_at == payload.observation.observed_at` after UTC normalization.
- `crawl_run_id` and `raw_uri` equal the values in the observation.
- `produced_at >= occurred_at` is **not** required because source timestamps
  can have clock skew; a later data-quality phase handles tolerance.

#### `MarketplaceChangeV1`

```python
event_id: str
schema_version: str
change_type: MarketplaceChangeType
detected_at: datetime
marketplace: str
offer_id: str
previous_observation_id: str | None
current_observation_id: str
field_name: str | None
previous_value: Any
current_value: Any
rule_version: str
```

Validation:

- Required IDs, marketplace and rule version are non-empty.
- `schema_version == CHANGE_SCHEMA_VERSION`.
- `detected_at` timezone-aware.
- `NEW_OFFER` allows `previous_observation_id is None`.
- Every other type requires `previous_observation_id`.
- `field_name` may be `None` only for `NEW_OFFER` and `OFFER_STALE`.
- Do not implement change detection rules here; this is only the contract.

### 10.5 Required factories

Factories centralize ID creation so callers do not choose their own algorithms.

Implement:

```python
def create_raw_artifact(
    *,
    marketplace_code: str,
    crawl_run_id: str,
    marketplace_id: str,
    request_url: str,
    resource_type: ResourceType,
    fetched_at: datetime,
    http_status: int | None,
    content_type: str | None,
    body_sha256: str,
    raw_uri: str,
    adapter_version: str,
    raw_bytes: int,
) -> RawArtifact: ...

def create_marketplace_offer(
    *,
    marketplace_code: str,
    marketplace_id: str,
    platform_listing_id: str,
    seller_id: str | None,
    product_title: str,
    brand: str | None,
    category_path: str | None,
    source_url: str,
    currency: str,
    first_seen_at: datetime,
    last_seen_at: datetime,
    active_status: OfferActiveStatus = OfferActiveStatus.UNKNOWN,
) -> MarketplaceOffer: ...

def create_offer_observation(
    *,
    marketplace_code: str,
    platform_listing_id: str,
    offer_id: str,
    observed_at: datetime,
    fetched_at: datetime,
    current_price: Decimal,
    raw_uri: str,
    raw_sha256: str,
    adapter_version: str,
    crawl_run_id: str,
    **optional_fields: Any,
) -> OfferObservation: ...

def create_observation_event(
    *,
    marketplace_code: str,
    offer: MarketplaceOffer,
    observation: OfferObservation,
    platform_listing_id: str,
    produced_at: datetime,
) -> MarketplaceObservationV1: ...
```

Factory rules:

- Generate IDs only through `common.identity` convenience helpers.
- Observation event ID equals observation ID.
- Partition key is exactly
  `f"{marketplace_code.lower()}:{platform_listing_id}"`.
- Do not call `datetime.now()` inside factories. The caller supplies all times.
- `create_offer_observation()` may accept only names that are real
  `OfferObservation` optional fields; reject unknown keys with `TypeError`.

### 10.6 Required tests

At minimum add these tests:

1. Valid Marketplace normalizes code and currency.
2. Marketplace rejects empty code and invalid URL.
3. CrawlRun enforces status/completion rules.
4. CrawlRun rejects inconsistent counters.
5. RawArtifact factory creates the expected deterministic ID.
6. RawArtifact rejects invalid HTTP status, checksum and raw URI.
7. Seller IDs created by `make_seller_id()` are deterministic.
8. Seller normalizes timestamps to UTC and validates their ordering.
9. Offer factory creates the expected deterministic ID.
10. Offer keeps missing optional brand/category/seller as `None`.
11. OfferObservation factory creates the expected deterministic ID.
12. OfferObservation defaults availability to `UNKNOWN`.
13. OfferObservation rejects negative prices and counts.
14. OfferObservation rejects bool values for counts.
15. OfferObservation validates rating value against rating scale.
16. Observation payload rejects mismatched offer IDs.
17. Observation event enforces version, type and lineage fields.
18. Observation event serializes through `serialize_for_wire()` and then
    `json.dumps()` without a custom encoder.
19. Change contract accepts `NEW_OFFER` without a previous observation.
20. Change contract rejects a price change without a previous observation.
21. Every marketplace dataclass rejects naive timestamps.

Use fixed datetimes and fixed checksums. Do not use `datetime.now()` in tests.

Recommended fixed values:

```python
FETCHED_AT = datetime(2026, 9, 4, 8, 30, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 9, 4, 8, 30, 1, tzinfo=timezone.utc)
RAW_SHA256 = "a" * 64
```

### Exit criteria

- All required dataclasses and factories exist.
- Missing values are not fabricated.
- IDs come only from deterministic helpers.
- Every normalized observation has raw lineage.
- A complete observation envelope can be serialized by standard
  `json.dumps()` after `serialize_for_wire()`.
- All domain tests pass.

## 11. Work package FND-04 — Legacy compatibility check

### Purpose

Prove that adding the marketplace foundation did not break the behavioral
pipeline before the new pipeline has a replacement.

### Steps

1. Run focused tests:

   ```powershell
   python -m pytest `
     tests/test_schema.py `
     tests/test_ingestion_contract.py `
     tests/test_data_ingestion.py `
     tests/test_crawler.py `
     tests/test_serialization.py `
     tests/test_identity.py `
     tests/test_marketplace_schema.py `
     -q
   ```

2. Run the whole test suite:

   ```powershell
   python -m pytest tests -q
   ```

3. Run the producer's no-Kafka mode:

   ```powershell
   python -m data_ingestion.producer `
     --source tests/fixtures/events.csv `
     --test-mode `
     -n 2
   ```

4. Run a direct marketplace serialization smoke command:

   ```powershell
   @'
   import json
   from datetime import datetime, timezone
   from decimal import Decimal
   from config.marketplace_schema import (
       create_marketplace_offer,
       create_offer_observation,
       create_observation_event,
   )
   from common.serialization import serialize_for_wire

   observed_at = datetime(2026, 9, 4, 8, 30, tzinfo=timezone.utc)
   offer = create_marketplace_offer(
       marketplace_code="tiki",
       marketplace_id="marketplace-tiki",
       platform_listing_id="p1",
       seller_id=None,
       product_title="Fixture product",
       brand=None,
       category_path="1846",
       source_url="https://tiki.vn/p1.html",
       currency="VND",
       first_seen_at=observed_at,
       last_seen_at=observed_at,
   )
   observation = create_offer_observation(
       marketplace_code="tiki",
       platform_listing_id="p1",
       offer_id=offer.offer_id,
       observed_at=observed_at,
       fetched_at=observed_at,
       current_price=Decimal("100.00"),
       raw_uri="file:///tmp/raw.json",
       raw_sha256="a" * 64,
       adapter_version="test-v1",
       crawl_run_id="run-1",
   )
   event = create_observation_event(
       marketplace_code="tiki",
       offer=offer,
       observation=observation,
       platform_listing_id="p1",
       produced_at=observed_at,
   )
   print(json.dumps(serialize_for_wire(event), ensure_ascii=False))
   '@ | python -
   ```

### Exit criteria

- Existing behavioral unit tests still pass.
- Existing producer test mode still prints JSON.
- Marketplace smoke command prints valid nested JSON.
- No service or network is required.

## 12. Required test matrix

| Concern | Required proof |
|---|---|
| Legacy behavioral wire format | `event_time` remains JSON serializable |
| Current crawler snapshot | `snapshot_time` becomes JSON serializable |
| Money precision | Decimal scale is preserved as a string |
| Nested contracts | Dataclass/enum/list/dict recursion works |
| Fail-fast behavior | Unknown types and bad mapping keys raise |
| Replay identity | Same source identity produces the same ID |
| Temporal identity | Equivalent timezone instants produce the same ID |
| Offer stability | Title/price changes do not change `offer_id` |
| Observation lineage | Observation requires raw URI and SHA-256 |
| Missing semantics | Availability defaults to `UNKNOWN` |
| Contract versioning | Wrong schema version is rejected |
| Envelope consistency | Event, payload, run and raw fields agree |

## 13. PR and commit sequence

Do not combine the whole phase into one large unreviewable commit.

Recommended sequence:

1. `test: record marketplace foundation baseline`
   - no code changes; PR description only.
2. `feat: add generic wire serialization`
   - FND-01 files only.
3. `feat: add deterministic marketplace identities`
   - FND-02 files only.
4. `feat: add marketplace domain contracts`
   - FND-03 files only.
5. `test: verify legacy and marketplace contract compatibility`
   - test fixes required by FND-04 only.

If the workflow requires a single PR, retain the same ordering as separate
commits.

## 14. Instructions for a low-capability coding model

For each work package, give the coding model only that package plus the global
scope/safety rules. Do not ask it to implement the entire phase in one prompt.

The coding model must follow this loop:

1. Read every file listed in the work package.
2. State which exact files it will create or modify.
3. Implement only the requested public APIs.
4. Add the named tests before moving to another package.
5. Run focused tests.
6. Run all currently runnable tests.
7. Show `git diff --stat` and summarize any failure.
8. Stop. Do not start the next package automatically.

The reviewer must reject the change if the model:

- modifies unrelated layers;
- adds dependencies;
- invents source fields;
- uses UUID/randomness for marketplace IDs;
- serializes Decimal to float;
- defaults availability to in stock;
- catches validation errors and continues silently;
- changes existing behavioral semantics;
- performs network calls in unit tests;
- begins Kafka, Spark, SQL or crawler refactoring early.

## 15. Copy-ready task prompts

### Prompt A — Serializer

```text
Implement work package FND-01 from
docs/PHASE_1_MARKETPLACE_FOUNDATION_IMPLEMENTATION_PLAN.md.

Read the full plan, common/__init__.py, config/schema.py and tests/test_schema.py
before editing. Create only common/serialization.py and
tests/test_serialization.py; modify only config/schema.py and
tests/test_schema.py. Keep every existing behavioral API compatible. Add every
test required by FND-01, run the focused tests and then stop. Do not implement
identity or marketplace dataclasses yet.
```

### Prompt B — Identity

```text
Implement work package FND-02 from
docs/PHASE_1_MARKETPLACE_FOUNDATION_IMPLEMENTATION_PLAN.md.

Read the full plan and the completed serializer changes first. Create only
common/identity.py and tests/test_identity.py. Implement the exact public
functions, normalization algorithm and validation rules from FND-02. Add every
required test, run the focused tests and then stop. Do not modify crawler,
Kafka, Spark, SQL, serving or dashboard files.
```

### Prompt C — Domain contracts

```text
Implement work package FND-03 from
docs/PHASE_1_MARKETPLACE_FOUNDATION_IMPLEMENTATION_PLAN.md.

Read the full plan plus common/serialization.py and common/identity.py before
editing. Create only config/marketplace_schema.py and
tests/test_marketplace_schema.py. Implement exactly the enums, frozen
dataclasses, validation helpers and factories specified by FND-03. Use only the
standard library and the completed common helpers. Add every required test, run
the focused tests and then stop. Do not integrate the contract into crawler or
Kafka yet.
```

### Prompt D — Compatibility verification

```text
Execute work package FND-04 from
docs/PHASE_1_MARKETPLACE_FOUNDATION_IMPLEMENTATION_PLAN.md.

Do not add features. Run the focused suite, full suite, legacy producer test
mode and marketplace serialization smoke command. Fix only regressions caused
by FND-01 through FND-03 and only inside the files allowed by the Phase 1 plan.
Report pre-existing/environment failures separately. Show git diff --stat and
stop.
```

## 16. Phase Definition of Done

Phase 1 is complete only when every item below is true:

- [ ] Baseline environment/result recorded.
- [ ] `serialize_for_wire()` supports every specified type recursively.
- [ ] `config.schema.to_wire()` delegates to the generic serializer.
- [ ] Legacy behavioral events still serialize.
- [ ] Price snapshots containing `snapshot_time` serialize.
- [ ] Unsupported wire values fail loudly.
- [ ] All five deterministic convenience ID helpers exist.
- [ ] IDs use normalized full SHA-256 and contain no randomness.
- [ ] Marketplace enums, dataclasses and factories exist.
- [ ] Marketplace timestamps require timezone awareness.
- [ ] Money uses Decimal in memory and decimal strings on the wire.
- [ ] Missing availability remains `UNKNOWN`.
- [ ] Observation raw URI/checksum/run/adapter lineage is mandatory.
- [ ] Observation and change schema versions are enforced.
- [ ] Required new unit tests pass.
- [ ] Existing tests pass or pre-existing environment failures are documented.
- [ ] No out-of-scope layer changed.

## 17. Handoff to Phase 2

After this phase is accepted, Phase 2 may implement the acquisition boundary:

```text
FetchResult
→ persist raw body and metadata sidecar
→ RawArtifact
→ parse marketplace-specific response
→ MarketplaceOffer + OfferObservation
→ MarketplaceObservationV1
```

Phase 2 must consume the serializer, identity helpers and contracts from this
phase. It must not introduce a second competing ID algorithm or event schema.
