# Phase 2 implementation plan — Raw-first marketplace acquisition

> Status: ready for implementation after Phase 1 acceptance
>
> Intended implementer: a low-capability coding model working one work package
> at a time. It must follow the fixed APIs and stop after every package.
>
> Parent plan: `Brief để xây dựng plan sơ bộ cho việc cải tiến ecommerce-lambda-architecture.md`

## 1. Objective

Implement the parent plan's Week 2 vertical slice and backlog P0-03, P0-04,
P0-05, P0-07 and P0-11. It consumes the P0-02 serializer and P0-06 identities
accepted in Phase 1:

```text
one bounded listing-page request
-> exact request-URL robots check
-> HTTP response as immutable bytes
-> raw body persisted to Bronze
-> metadata sidecar persisted to Bronze
-> RawArtifact with checksum and URI
-> parse the already-persisted bytes
-> MarketplaceOffer + OfferObservation
-> MarketplaceObservationV1
-> Phase2AcquisitionReport
```

At the end of this phase, one source adapter can acquire Tiki listing pages and
produce canonical, versioned observation envelopes with complete raw lineage.
The same saved body can be parsed again without another network request.

This phase does **not** publish to Kafka. Phase 3 adds persistent scheduling,
retry and audit. Phase 4 publishes accepted observations and lands Silver.

## 2. Why Phase 2 is a separate boundary

The current transitional crawler has useful pagination, source-field mapping,
robots and object-store code, but its accepted path is not raw-first:

1. `TikiCrawler.fetch_listing()` calls `response.json()` before storage.
2. `SiteCrawler.crawl()` parses and validates individual products before the
   runner writes anything to Bronze.
3. `crawler/runner.py` stores one reconstructed product dictionary, not the
   original page response bytes.
4. Raw objects have no deterministic artifact ID, checksum, metadata sidecar,
   adapter version or crawl-run lineage.
5. The runner publishes the old mutable price-snapshot dictionary rather than
   a Phase 1 `MarketplaceObservationV1`.
6. Missing availability currently becomes in-stock in some paths.

Those paths are migration input, not the Phase 2 target. A parser failure must
never be able to erase the evidence needed to diagnose and reparse that source
response.

## 3. Mandatory dependency gate

Before editing, record and verify all of the following:

1. `common/serialization.py` exists and its tests pass.
2. `common/identity.py` exists and its tests pass.
3. `config/marketplace_schema.py` exists and its tests pass.
4. Phase 1 exposes `ResourceType`, `RawArtifact`, `MarketplaceOffer`,
   `OfferObservation`, `MarketplaceObservationV1` and the four factories used
   below.
5. `common.object_store.put_bytes(zone, relative_path, data)` writes exact bytes
   and returns a supported `file://`, `s3://` or `s3a://` URI.
6. The existing behavioral event tests are either green or their pre-existing
   failures are recorded.

If any Phase 1 API is absent or has a different accepted name, stop and report
the exact mismatch. Do not recreate serializer, identity or marketplace-domain
contracts inside Phase 2. The reviewer must first identify the accepted Phase
1 commit/API.

## 4. Scope

### 4.1 In scope

- Frozen contracts for one listing-page acquisition request and result.
- HTTP fetch result containing original response bytes and safe metadata.
- Exact request URL generation and robots checking against that URL.
- Raw body SHA-256 and deterministic Bronze body/sidecar paths.
- Body-before-sidecar-before-parse ordering.
- A pure, replayable Tiki listing-page parser.
- Mapping Tiki fields into Phase 1 canonical offer/observation/event contracts.
- Bounded category pagination with empty-page, reported-last-page, repeated-page
  and configured-maximum termination.
- Per-record rejection without losing valid siblings from the same page.
- One-shot CLI collection for the primary source.
- Offline fixtures, contract tests and failure-ordering tests.
- A stable Phase 2 report consumed by Phase 3 and Phase 4.

### 4.2 Out of scope

- Kafka producers, topic creation, observation DLQ or Silver landing.
- PostgreSQL frontier, leases, persistent retries, crawl-run SQL or circuit
  breaker.
- Spark schemas, streaming, batch, Elasticsearch, Redis or dashboards.
- A second marketplace adapter.
- Product-detail requests or one-request-per-product enrichment.
- Seller detail enrichment, product matching or cross-market comparison.
- Adaptive cadence, anti-bot bypass, proxy rotation or CAPTCHA handling.
- Raw replay CLI that reads arbitrary historical object-store URIs. Phase 2
  only proves that the parser can replay supplied saved bytes.
- Deleting the legacy behavioral ingestion pipeline.

## 5. Allowed file changes

Create:

```text
crawler/contracts.py
crawler/raw_store.py
crawler/acquisition.py
tests/test_crawler_contracts.py
tests/test_raw_store.py
tests/test_marketplace_acquisition.py
tests/test_tiki_contract.py
```

Modify only:

```text
config/settings.py
crawler/base.py
crawler/sites/tiki.py
crawler/runner.py
tests/test_crawler.py
tests/test_object_store.py
tests/fixtures/tiki_listing_sample.json
```

The fixture may change only to match a captured, sanitized source response.
Do not change `config/schema.py`, `common/identity.py`,
`config/marketplace_schema.py`, downstream layers, SQL, Compose or dashboards.

The existing `normalize_price_snapshot()` contract may remain for compatibility,
but the accepted Phase 2 path must not import or call it.

## 6. Fixed architectural decisions

These decisions are already made. The implementer must not choose alternatives.

### 6.1 Acquisition unit

One Phase 2 acquisition call handles exactly one HTTP listing page. It creates
at most one `RawArtifact` and may create zero or many canonical observations.

The generic source target and page are separate in memory. For Phase 3 storage
inside `CrawlTask.target`, encode them with the helper specified in Section 7.5.
Do not hide a category-wide unbounded crawl inside one Phase 3 task.

### 6.2 Time

- Every new timestamp is timezone-aware and normalized to UTC.
- `fetched_at` is captured after the HTTP response bytes are available.
- Tiki does not provide a reliable row-level observation timestamp in the
  listing response, so `observed_at = fetched_at`.
- `produced_at` is supplied by the acquisition clock after raw persistence and
  immediately before/while canonical parsing.
- No parser, serializer, factory or test calls `datetime.now()` internally.
- Production entry points inject a UTC clock whose default calls
  `datetime.now(timezone.utc)`.

### 6.3 Raw fidelity

- Use `response.content`; never reconstruct raw bytes with `response.json()` or
  `json.dumps(response.json())`.
- Store the body unchanged, including whitespace and source key ordering.
- Compute SHA-256 over exactly those stored bytes.
- A response body is stored even for HTTP 4xx/5xx before the HTTP failure is
  reported.
- Network failures with no HTTP response have no fabricated raw artifact.
- Do not store request/response cookies, authorization headers, tokens or full
  arbitrary header maps.

### 6.4 Raw-first ordering

The only accepted ordering is:

```text
fetch bytes
-> write body
-> construct RawArtifact from returned body URI
-> write metadata sidecar
-> inspect HTTP status
-> decode/parse body
-> construct canonical observations
```

If the body write fails, do not parse. If the sidecar write fails, do not parse.
If parsing fails, retain the successfully written body and sidecar.

### 6.5 Money and missing values

- Convert source numeric values to `Decimal` using `Decimal(str(value))`.
- Reject booleans as numeric values.
- Never convert marketplace money through `float`.
- Missing optional values stay `None`.
- Missing availability is `Availability.UNKNOWN`, never in-stock.
- Missing seller data does not create a synthetic seller name.
- Do not interpret public sold counters as transactions.

### 6.6 Publication

Phase 2 returns canonical event objects in memory. It does not create a Kafka
producer, send an event, create a topic or publish a DLQ record. This keeps the
raw-first correctness boundary testable without services.

## 7. Work package ACQ-01 — Acquisition contracts

### Files

- Create `crawler/contracts.py`.
- Create `tests/test_crawler_contracts.py`.

### 7.1 Required enums

```python
class AcquisitionStatus(str, Enum):
    SUCCEEDED = "SUCCEEDED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"

class AcquisitionStage(str, Enum):
    ROBOTS = "ROBOTS"
    FETCH = "FETCH"
    HTTP = "HTTP"
    STORAGE = "STORAGE"
    PARSE = "PARSE"
    VALIDATION = "VALIDATION"
```

`PARTIAL` means at least one canonical observation and at least one rejected
source record from a response whose raw body and sidecar were stored. It is not
an operational retry signal.

### 7.2 Required exception types

Expose these exception classes:

```python
class Phase2AcquisitionError(Exception): ...
class RobotsDeniedError(Phase2AcquisitionError): ...
class FetchTransportError(Phase2AcquisitionError): ...
class HttpResponseError(Phase2AcquisitionError): ...
class RawPersistenceError(Phase2AcquisitionError): ...
class ListingPageParseError(Phase2AcquisitionError): ...
class CanonicalRecordError(Phase2AcquisitionError): ...
```

`HttpResponseError` stores `status_code: int` and `retry_after: str | None`.
`RawPersistenceError` stores `write_stage`, which is exactly `BODY` or
`METADATA`, and may store the already-created `RawArtifact` after a metadata
failure. Preserve caught exceptions as `__cause__`; do not put stack traces in
error messages.

### 7.3 ListingPageRequest

```python
@dataclass(frozen=True)
class ListingPageRequest:
    marketplace_code: str
    marketplace_id: str
    target: str
    page: int
    resource_type: ResourceType = ResourceType.LISTING_PAGE
```

Rules:

- required text is stripped and non-empty;
- marketplace code is stored lowercase;
- page is an integer greater than zero and cannot be bool;
- this phase accepts only `ResourceType.LISTING_PAGE`.

### 7.4 FetchResult

```python
@dataclass(frozen=True)
class FetchResult:
    request_url: str
    fetched_at: datetime
    http_status: int
    content_type: str | None
    body: bytes
    retry_after: str | None = None
    elapsed_ms: int | None = None
```

Rules:

- request URL is valid HTTP(S);
- fetched time is aware and normalized to UTC;
- status is an integer from 100 through 599, not bool;
- body must be `bytes`, including when it is empty;
- optional text is stripped and blank becomes `None`;
- elapsed milliseconds, when present, is non-negative and not bool.

Do not add parsed JSON, arbitrary response headers or a `requests.Response` to
this contract.

### 7.5 Phase 3 task-target helpers

Implement exactly:

```python
def encode_listing_page_task_target(target: str, page: int) -> str: ...
def decode_listing_page_task_target(value: str) -> tuple[str, int]: ...
```

The encoded value is compact canonical JSON with exactly these keys:

```json
{"page":1,"target":"1846"}
```

Use UTF-8 text, `ensure_ascii=False`, `sort_keys=True` and compact separators.
Decoding rejects missing/unknown keys, blank targets, bool pages and pages below
one. This exact string is what Phase 3 stores and hashes as `CrawlTask.target`.

### 7.6 Parse/result contracts

```python
@dataclass(frozen=True)
class RecordRejection:
    record_index: int
    stage: AcquisitionStage
    error: Exception
    platform_listing_id: str | None = None

@dataclass(frozen=True)
class ParsedListingPage:
    observations: tuple[MarketplaceObservationV1, ...]
    rejections: tuple[RecordRejection, ...]
    source_record_count: int
    duplicate_count: int
    last_page: int | None

@dataclass(frozen=True)
class AcquisitionFailure:
    stage: AcquisitionStage
    error: Exception

@dataclass(frozen=True)
class Phase2AcquisitionReport:
    status: AcquisitionStatus
    marketplace_code: str
    marketplace_id: str
    target: str
    page: int
    resource_type: ResourceType
    crawl_run_id: str
    request_url: str
    started_at: datetime
    completed_at: datetime
    http_status: int | None
    retry_after: str | None
    raw_artifact: RawArtifact | None
    raw_metadata_uri: str | None
    observations: tuple[MarketplaceObservationV1, ...]
    rejections: tuple[RecordRejection, ...]
    source_record_count: int
    duplicate_count: int
    last_page: int | None
    failure: AcquisitionFailure | None
```

`RecordRejection` and `AcquisitionFailure` expose read-only `error_type` and
`error_message` properties. `error_type` is the exception class name.
`error_message` is `str(error)` capped at 2000 characters. These properties,
not repr/traceback, are what later audit code may persist.

Expose read-only properties:

```python
canonical_observation_count: int
rejected_count: int
raw_bytes: int
page_exhausted: bool
is_task_success: bool
```

Property rules:

- `canonical_observation_count == len(observations)`;
- `rejected_count == len(rejections)`;
- raw bytes are zero without an artifact;
- page is exhausted when source row count is zero or `page >= last_page`;
- task success is true for `SUCCEEDED` and `PARTIAL` only.

Validation rules:

- all timestamps are aware UTC and completion is not before start;
- counts are non-negative integers and not bool;
- `source_record_count` reconciles with accepted, rejected and duplicate rows;
- `SUCCEEDED` and `PARTIAL` require raw artifact + metadata URI and no failure;
- `SUCCEEDED` requires zero rejections;
- `PARTIAL` requires both at least one observation and one rejection;
- `FAILED` requires a failure and contains no accepted observations;
- no observation may exist without a complete raw artifact + sidecar;
- every observation must match report marketplace, crawl run, raw URI and raw
  checksum;
- error strings exposed through helper properties are capped at 2000 chars.

### 7.7 Adapter protocol

Expose the structural protocol consumed by orchestration:

```python
class ListingPageAdapter(Protocol):
    site_name: str
    marketplace_id: str
    adapter_version: str

    def request_url(self, target: str, page: int) -> str: ...
    def allowed(self, request_url: str) -> bool: ...
    def throttle(self) -> None: ...
    def fetch_listing_page(self, request: ListingPageRequest) -> FetchResult: ...
    def parse_listing_page(
        self,
        *,
        request: ListingPageRequest,
        fetch_result: FetchResult,
        raw_artifact: RawArtifact,
        crawl_run_id: str,
        produced_at: datetime,
    ) -> ParsedListingPage: ...
```

The protocol is for typing and fake adapters. Do not put shared HTTP, storage
or parsing implementation in it.

### 7.8 Required tests

1. Every dataclass rejects naive times.
2. FetchResult preserves byte-for-byte bodies, including invalid UTF-8.
3. FetchResult rejects bool status/elapsed values.
4. Task-target encoding is deterministic and round-trips Unicode.
5. Task-target decoding rejects unknown/missing keys and invalid pages.
6. Parsed page counts must reconcile.
7. PARTIAL requires accepted and rejected records.
8. Successful reports require both body artifact and sidecar.
9. Failed reports require a failure and may have no artifact for transport
   failure.
10. Report lineage mismatch is rejected.
11. Report properties return exact counts/status.

### Exit criteria

- All contracts validate their own invariants.
- Phase 3 can identify target/page/resource, failure stage, status and counts
  without inspecting adapter-specific data.
- No network or storage import is needed to test the contracts.

## 8. Work package ACQ-02 — Raw Bronze persistence

### Files

- Create `crawler/raw_store.py`.
- Create `tests/test_raw_store.py`.
- Modify `tests/test_object_store.py` only if an exact-byte regression test is
  missing.

### 8.1 Public API

```python
RAW_METADATA_SCHEMA_VERSION = "marketplace-raw-artifact-metadata.v1"

@dataclass(frozen=True)
class PersistedRawArtifact:
    artifact: RawArtifact
    metadata_uri: str

def persist_fetch_result(
    *,
    request: ListingPageRequest,
    fetch_result: FetchResult,
    crawl_run_id: str,
    adapter_version: str,
    writer: Callable[[str, str, bytes], str] = put_bytes,
) -> PersistedRawArtifact: ...
```

`writer` uses the existing `(zone, relative_path, data) -> URI` contract. Do
not instantiate MinIO directly in this module.

### 8.2 Deterministic path layout

Compute `body_sha256` over `fetch_result.body`, then create the raw artifact ID
with the Phase 1 identity helper. Use:

```text
bronze/marketplace/raw/
  marketplace=<escaped-code>/
  observed_date=<yyyy-mm-dd>/
  hour=<HH>/
  crawl_run_id=<escaped-run-id>/
  raw_artifact_id=<raw-id>/body.bin

bronze/marketplace/raw/
  marketplace=<escaped-code>/
  observed_date=<yyyy-mm-dd>/
  hour=<HH>/
  crawl_run_id=<escaped-run-id>/
  raw_artifact_id=<raw-id>/metadata.json
```

Paths passed to `put_bytes("bronze", ...)` start at `marketplace/raw/...`; the
bucket/zone name is not repeated in the relative path. Escape dynamic path
components with `urllib.parse.quote(value, safe="-_.~")`. Do not use title,
category text, current time, random UUID or platform listing ID in filenames.

### 8.3 Write algorithm

1. Validate request, crawl run and adapter version before writing.
2. Compute checksum and deterministic raw artifact ID.
3. Write `body.bin` with the exact source bytes.
4. Use the returned body URI to call Phase 1 `create_raw_artifact()`.
5. Serialize the metadata object with `serialize_for_wire()`.
6. Encode metadata with UTF-8 JSON using `ensure_ascii=False`,
   `sort_keys=True` and compact separators.
7. Write `metadata.json`.
8. Return both the Phase 1 artifact and metadata URI.

The metadata JSON shape is exactly:

```text
schema_version
raw_artifact
request.target
request.page
response.retry_after
response.elapsed_ms
```

`raw_artifact` is the serialized Phase 1 object. Do not add body text, cookies,
headers, credentials, Python repr or traceback.

If the body write fails, raise `RawPersistenceError(write_stage="BODY")` and
do not attempt metadata. If metadata fails, raise
`RawPersistenceError(write_stage="METADATA")` carrying the already-created
artifact. Preserve the original writer exception as the cause.

### 8.4 Required tests

1. Body bytes written are exactly the FetchResult bytes.
2. Body write occurs before metadata write.
3. Body checksum equals SHA-256 of the written bytes.
4. Raw artifact ID equals `make_raw_artifact_id()`.
5. Body and metadata paths match the exact partition layout.
6. UTC date/hour, not local timezone, controls the path.
7. Dynamic path values are escaped.
8. Metadata is deterministic canonical UTF-8 JSON.
9. Metadata contains lineage and only the safe fixed fields.
10. A duplicate call with identical inputs writes identical paths and bytes.
11. Body writer failure prevents metadata and raises BODY failure.
12. Metadata writer failure retains artifact/body URI in the raised error.
13. The function never decodes the body.

### Exit criteria

- Any HTTP response can be preserved even when it is invalid JSON or invalid
  UTF-8.
- A successful return proves both body and sidecar were acknowledged.
- RawArtifact checksum, byte count, URI, adapter version and crawl-run lineage
  are complete.

## 9. Work package ACQ-03 — Fetch/parse adapter boundary

### Files

- Modify `crawler/base.py`.
- Modify `tests/test_crawler.py`.

### 9.1 Preserve and extend SiteCrawler

Keep the public class name `SiteCrawler`. Add these required attributes:

```python
site_name: str
marketplace_id: str
adapter_version: str
base_url: str
```

Expose public wrappers used by the orchestrator:

```python
def allowed(self, request_url: str) -> bool: ...
def throttle(self) -> None: ...
```

They may delegate to existing protected helpers. Robots must be evaluated
against the exact request URL returned by the adapter, including the actual
path and query.

Add these abstract methods:

```python
def request_url(self, target: str, page: int) -> str: ...

def fetch_listing_page(
    self, request: ListingPageRequest
) -> FetchResult: ...

def parse_listing_page(
    self,
    *,
    request: ListingPageRequest,
    fetch_result: FetchResult,
    raw_artifact: RawArtifact,
    crawl_run_id: str,
    produced_at: datetime,
) -> ParsedListingPage: ...
```

`fetch_listing_page()` performs HTTP only. It must not parse JSON, call
`raise_for_status()`, create canonical objects, write storage or publish.

`parse_listing_page()` performs no network, sleep, storage or publication. It
must parse `fetch_result.body`, which is the same byte sequence already saved
to `raw_artifact.raw_uri` by the orchestrator.

The old `fetch_listing()`, `parse_product()` and `crawl()` helpers may remain
temporarily for compatibility, but:

- the accepted Phase 2 runner must not call them;
- they must be marked legacy in docstrings;
- no new test may use them to prove raw-first correctness;
- they must not be modified to publish canonical events.

### 9.2 Testability

Allow tests to inject or monkeypatch:

- HTTP GET callable;
- UTC clock;
- sleeper;
- jitter source;
- robots result.

Defaults may use `requests.get`, `datetime.now(timezone.utc)`, `time.sleep` and
`random.uniform`. No module import or constructor used in a unit test may make
an unavoidable live request.

### 9.3 Required tests

1. Robots receives the exact adapter request URL.
2. Disallowed URL prevents fetch.
3. Fetch method can return invalid JSON bytes without parsing them.
4. Parse method can run twice on the same supplied bytes without HTTP.
5. Throttle dependencies can be fixed to zero in tests.
6. Existing robots behaviors remain covered.
7. Existing legacy behavioral schema tests remain untouched.

### Exit criteria

- HTTP acquisition and parsing are mechanically separable.
- A caller can guarantee storage between fetch and parse.
- Unit tests can exercise each side without external services.

## 10. Work package ACQ-04 — Tiki raw-first adapter

### Files

- Modify `crawler/sites/tiki.py`.
- Create `tests/test_tiki_contract.py`.
- Modify the sanitized Tiki fixture only when necessary.

### 10.1 Fixed adapter constants

Use exact values:

```python
site_name = "tiki"
marketplace_id = "marketplace-tiki"
adapter_version = "tiki-listing-v1"
base_url = "https://tiki.vn"
default_currency = "VND"
rating_scale = Decimal("5")
```

Do not make identity/schema/adapter version strings environment configurable.

### 10.2 Exact request URL

`request_url(target, page)` returns the listing endpoint with encoded query:

```text
https://tiki.vn/api/personalish/v1/blocks/listings
  ?category=<target>&page=<page>&limit=40
```

Construct it with `urllib.parse.urlencode`; do not concatenate unescaped target
text. `fetch_listing_page()` must request that exact returned URL and supply
only the configured User-Agent and timeout. Follow normal HTTP redirects and
record the final `response.url` in FetchResult.

Capture:

- final URL;
- response status;
- raw `response.content`;
- Content-Type;
- Retry-After;
- non-negative elapsed milliseconds when available;
- injected UTC fetched time.

Do not call `response.json()` or `raise_for_status()`.

### 10.3 Top-level page parsing

Decode bytes as strict UTF-8, then JSON. Require:

- top level is an object;
- `data` exists and is a list;
- every non-empty row is an object;
- `paging`, when present, is an object;
- `paging.last_page`, when present, is a positive integer and not bool.

Invalid UTF-8, JSON or page shape raises `ListingPageParseError` and produces no
canonical observation. An empty `data` list is a valid exhausted page.

### 10.4 Per-row mapping

For every row, create Phase 1 objects through factories only. Never construct
IDs manually.

| Canonical field | Tiki source/rule |
|---|---|
| platform listing ID | required `id`, converted to stripped string |
| product title | required non-blank `name` |
| seller ID | `make_seller_id("tiki", seller_id)` when source seller ID exists; otherwise null |
| brand | non-blank `brand_name`, otherwise null |
| category path | non-blank `primary_category_path`, otherwise request target |
| source URL | `https://tiki.vn/<url_key>.html`; required valid `url_key` |
| currency | `VND` |
| current price | required `price` converted directly to Decimal |
| list price | positive `list_price`; otherwise positive `original_price`; otherwise null |
| discount amount | optional `discount` as Decimal |
| discount percent | optional `discount_rate` as Decimal |
| rating value | optional `rating_average` as Decimal |
| rating scale | Decimal 5 only when rating value exists |
| rating count | null; do not equate it with review count |
| review count | optional `review_count` as int |
| sold count | optional `quantity_sold.value` as int |
| availability | Section 10.5 |
| ranking position | `(request.page - 1) * 40 + one-based row index` |
| promotion | null in Phase 2 |
| first/last seen | observed time for this immutable event |
| active status | `UNKNOWN` |
| observed/fetched time | `fetch_result.fetched_at` |
| raw URI/SHA | from RawArtifact |
| adapter/run lineage | fixed adapter version and supplied crawl run ID |
| produced time | supplied `produced_at` |

If an optional source numeric field is present but malformed, reject the row;
do not silently convert it to null. Reject bool as a price/count. Use the Phase
1 factory validation as the final authority.

### 10.5 Availability semantics

Use only explicit source evidence:

- `availability` equal to `0`/`false`, or `shippable` equal to `false` ->
  `OUT_OF_STOCK`;
- `availability` equal to `1`/`true` and `shippable` is not false ->
  `IN_STOCK`;
- field absent, null or unrecognized -> `UNKNOWN`.

Do not call `bool(missing_value)` and do not default missing values to one/true.

### 10.6 Per-record isolation and deduplication

- A row conversion/factory failure creates one `RecordRejection` at stage
  `VALIDATION` and does not stop valid sibling rows.
- Include source row index and a source listing ID only when safely available.
- Error messages are bounded and contain no full raw row.
- Within one page, keep the first event for a repeated platform listing ID and
  increment `duplicate_count` for later occurrences.
- Preserve source order for accepted events.
- Reconcile accepted + rejected + duplicate counts with source row count.

### 10.7 Required tests

1. Request URL includes exact category, page and limit.
2. Fetch returns source bytes unchanged and never invokes JSON parsing.
3. 404/429/500 responses still return FetchResult with their bodies.
4. Retry-After and content type are captured without full headers.
5. Fixture produces deterministic Phase 1 offer/observation/event IDs.
6. Every event contains raw URI, checksum, adapter version and crawl run ID.
7. Money is Decimal in memory and a string after wire serialization.
8. Seller ID is deterministic and seller name is not fabricated.
9. Missing brand/category/seller fields follow the fixed null/fallback rules.
10. Missing availability is UNKNOWN.
11. Explicit available/unavailable values map correctly.
12. Sold count remains a public counter only.
13. Missing ID/title/price/URL rejects only that row.
14. Malformed optional numeric values reject the row.
15. Empty data returns zero observations and no rejection.
16. Invalid UTF-8/JSON/top-level shape fails the whole page as PARSE.
17. Duplicate listing IDs are suppressed and counted.
18. Reparse with identical body, artifact and times returns equal events.

### Exit criteria

- The adapter has no storage or Kafka dependency.
- It emits only Phase 1 canonical contracts.
- No source-specific field escapes into the downstream envelope.
- Missing values are not invented.

## 11. Work package ACQ-05 — Raw-first orchestration

### Files

- Create `crawler/acquisition.py`.
- Create `tests/test_marketplace_acquisition.py`.

### 11.1 Public API

```python
def acquire_listing_page(
    adapter: ListingPageAdapter,
    request: ListingPageRequest,
    *,
    crawl_run_id: str,
    writer: Callable[[str, str, bytes], str] = put_bytes,
    clock: Callable[[], datetime],
) -> Phase2AcquisitionReport: ...
```

The clock is required at this pure orchestration layer; the production runner
passes its UTC default explicitly. Tests always pass a fixed/sequence clock.

Before side effects, require request marketplace code and ID to equal the
adapter attributes. A mismatch is a caller programming error and raises
`ValueError`; do not return it as a source failure.

### 11.2 Exact algorithm

1. Record `started_at` from the injected clock.
2. Build the exact request URL.
3. Check robots against that exact URL.
4. If denied, return FAILED/ROBOTS with `RobotsDeniedError`; do not throttle,
   fetch, write or parse.
5. Throttle once.
6. Call `fetch_listing_page()` once.
7. On transport exception, return FAILED/FETCH with no raw artifact.
8. Require FetchResult URL to be the requested URL or a valid final redirect
   URL; do not replace it with the base URL.
9. Call `persist_fetch_result()` once.
10. On body/metadata failure, return FAILED/STORAGE and do not parse. Preserve
    an artifact in the report only when metadata failure created one.
11. If status is outside 200–299, return FAILED/HTTP with the persisted raw
    artifact and metadata; do not parse.
12. Capture a supplied produced time from the clock and call the adapter's pure
    parser using the persisted artifact.
13. On page decode/shape error, return FAILED/PARSE while retaining lineage.
14. If at least one event and at least one rejection exist, return PARTIAL.
15. If source rows exist but all non-duplicate rows are rejected, return
    FAILED/VALIDATION with all row rejections and retained lineage.
16. Otherwise return SUCCEEDED, including a valid empty page.
17. Record `completed_at` from the clock and construct a fully validated report.

Catch `Exception` only around the boundary currently being executed. Never
catch `BaseException`; `KeyboardInterrupt` and `SystemExit` propagate. Do not
turn a programmer invariant error raised while constructing the final report
into a fetch/parse failure.

### 11.3 Ordering and failure tests

Use fake adapter/writer calls recorded in a list. Required proofs:

1. Success order is URL -> robots -> throttle -> fetch -> body write -> metadata
   write -> parse.
2. Parser observes the exact RawArtifact returned after body persistence.
3. Robots denial has no fetch/write/parse call.
4. Network failure has no write/parse call.
5. Body failure has no metadata/parse call.
6. Metadata failure has no parse call and reports retained body artifact.
7. 429 and 500 bodies are stored before HTTP failure is returned.
8. HTTP failure captures status and Retry-After.
9. Parse failure retains raw artifact and metadata URI.
10. Mixed valid/invalid rows return PARTIAL and are task-successful.
11. All-invalid rows return terminal VALIDATION failure.
12. Empty page is successful and exhausted.
13. Canonical event lineage mismatch fails loudly.
14. No success path imports or calls Kafka/DLQ.

### Exit criteria

- There is one auditable function where raw-first ordering is enforced.
- Expected operational/source/data failures become structured reports.
- Programmer errors are not swallowed.
- Phase 3 can invoke this function exactly once per leased listing-page task.

## 12. Work package ACQ-06 — Bounded one-shot runner

### Files

- Modify `config/settings.py`.
- Modify `crawler/runner.py`.
- Modify runner-related tests in `tests/test_crawler.py`.

### 12.1 Settings

Keep existing legacy settings and add:

```python
CRAWL_HTTP_TIMEOUT_SECONDS = 10
```

It is a positive number used by the Tiki HTTP call. Existing request delay,
jitter, maximum page and Tiki category settings remain. Do not add scheduler,
retry or Kafka marketplace settings in this phase.

### 12.2 Stable Phase 3 entry point

Expose in `crawler/runner.py`:

```python
def execute_listing_page(
    *,
    site: str,
    task_target: str,
    crawl_run_id: str,
    writer: Callable[[str, str, bytes], str] = put_bytes,
    clock: Callable[[], datetime],
) -> Phase2AcquisitionReport: ...
```

It must:

1. resolve the registered adapter;
2. decode target/page with the fixed helper;
3. create `ListingPageRequest` from adapter identity;
4. invoke `acquire_listing_page()` once;
5. return the report unchanged.

This is the thin Phase 2 executor Phase 3 wraps. It contains no SQL, retry,
sleep loop or Kafka logic.

### 12.3 Bounded local collection

The CLI may call page acquisition repeatedly for inspection/initial collection.
For each configured site and target:

1. generate one caller-owned crawl-run UUID for the one-shot run;
2. request pages from one through configured maximum;
3. stop the current target after a terminal failed page;
4. stop after a successful empty page;
5. stop when current page reaches reported `last_page`;
6. stop when the current non-empty offer-ID set is a subset of already-seen
   offer IDs for that target (endpoint ignored page);
7. continue with the next target when one target fails;
8. report pages, raw bytes, accepted, rejected and duplicate counts.

Random UUID is allowed only for `crawl_run_id`. Never use it for raw, offer,
observation or event IDs.

### 12.4 Transitional behavior to remove from accepted path

The Phase 2 runner must not:

- import `normalize_price_snapshot` or `PRICE_SNAPSHOT_FIELDS`;
- create a Kafka producer;
- send `ecommerce_price_snapshots`;
- call legacy best-effort `publish_to_dlq()`;
- write per-product dictionaries under `crawl_raw/`;
- parse a response before `persist_fetch_result()` succeeds.

Do not delete the legacy behavioral producer or behavioral topic. The old
crawler snapshot helpers can remain outside the accepted entry point until a
later cleanup, but `main()` must execute the raw-first path.

A live `--dry-run` flag must not bypass Bronze and still claim canonical
success. Either deprecate it with a clear error or redefine it as “no downstream
publish” while still persisting raw; document the chosen compatibility behavior
in `--help`. Since Phase 2 has no downstream publisher, the recommended choice
is to retain the flag as a deprecated no-op warning while always writing raw.

Optional inspection output must be generated only from already accepted Phase
1 events after raw persistence. JSONL is preferred. Do not make a parallel CSV
schema part of Phase 2 correctness.

### 12.5 Required tests

1. Site registry creates an adapter with fixed identity/version.
2. `execute_listing_page()` decodes one target and invokes acquisition once.
3. Unknown site and malformed task target fail before network/storage.
4. Bounded runner stops at empty page.
5. Bounded runner stops at reported last page.
6. Bounded runner stops at configured maximum.
7. Bounded runner stops repeated-page loops.
8. One target failure does not stop another target.
9. Summary counts reconcile with reports.
10. Main path has no Kafka producer/send/DLQ call.
11. Dry-run cannot produce canonical success without raw persistence.

### Exit criteria

- The initial source can begin raw-first collection before Phase 3 exists.
- The Phase 3 one-page executor boundary is stable.
- Pagination is bounded and source failures are isolated by target.

## 13. Cross-phase handoff contract

### 13.1 Handoff to Phase 3

Phase 3 imports `Phase2AcquisitionReport` and maps it without source-specific
inspection:

| Phase 2 report | Phase 3 use |
|---|---|
| marketplace/target/page/resource | attempt/task identity and audit context |
| raw_artifact ID/URI/raw_bytes | request-attempt audit |
| canonical observation count | parsed count |
| rejection count | rejected count |
| HTTP status/Retry-After | retry decision |
| ROBOTS | ROBOTS_DENIED |
| FETCH | classify underlying transport error |
| HTTP | RATE_LIMITED, CLIENT_ERROR or SERVER_ERROR from status |
| STORAGE | STORAGE_ERROR |
| PARSE | PARSE_ERROR |
| VALIDATION | VALIDATION_ERROR |

`SUCCEEDED` and `PARTIAL` complete the acquisition task. PARTIAL contributes
rejected-count audit but is not retried automatically. A FAILED report is
classified by Phase 3. Phase 3 must not move raw persistence or parsing into
the worker.

### 13.2 Handoff to Phase 4

Phase 4 may publish only `report.observations` from a report whose
`is_task_success` is true. Each event already contains:

- deterministic event/observation/offer IDs;
- canonical partition key;
- raw body URI and SHA-256;
- adapter version;
- crawl run ID;
- timezone-aware timestamps;
- Decimal values that serialize as strings.

Acquisition failures and rejected source rows are not fake observations. Phase
4's Kafka bad-record DLQ concerns records that reached Kafka/Silver decoding;
it does not replace Phase 2 raw evidence or Phase 3 acquisition audit.

## 14. Required end-to-end Phase 2 test matrix

| Concern | Required proof |
|---|---|
| Raw-first | writer body + sidecar complete before parser call |
| Raw fidelity | exact response bytes and checksum survive |
| HTTP evidence | 4xx/5xx body saved before status handling |
| Transport failure | no RawArtifact is fabricated |
| Replay | same saved bytes/artifact/times produce equal canonical events |
| Identity | IDs come only from Phase 1 deterministic factories |
| Lineage | every event links to body URI/hash/run/adapter |
| Semantics | Decimal money, UNKNOWN missing availability, no fake seller |
| Isolation | bad row does not discard valid sibling rows |
| Pagination | empty/last/max/repeat conditions terminate |
| Service isolation | default unit tests use no network, MinIO, Kafka or DB |
| Compatibility | behavioral event pipeline remains runnable |

## 15. Verification commands

Run focused tests after each package. At phase completion run:

```powershell
python -m pytest `
  tests/test_serialization.py `
  tests/test_identity.py `
  tests/test_marketplace_schema.py `
  tests/test_crawler_contracts.py `
  tests/test_raw_store.py `
  tests/test_tiki_contract.py `
  tests/test_marketplace_acquisition.py `
  tests/test_crawler.py `
  tests/test_object_store.py `
  tests/test_schema.py `
  -q

python -m pytest tests -q

python -m data_ingestion.producer `
  --source tests/fixtures/events.csv `
  --test-mode `
  -n 2

git diff --check
git diff --stat
```

Also run one no-service smoke using the Tiki fixture as FetchResult body, a
fake writer and fixed clock. Assert:

1. two objects are written in body-then-metadata order;
2. one valid canonical event is produced;
3. its raw URI/checksum match the body object;
4. `json.dumps(serialize_for_wire(event))` succeeds;
5. no Kafka/network/database object is constructed.

Optional live smoke is allowed only after unit acceptance and only with a
conservative one-page target. Record timestamp, URL, HTTP status, latency,
bytes, artifact URI, parsed/rejected count and source constraints. A live smoke
failure is not fixed by adding anti-bot behavior.

## 16. Commit/work-package sequence

1. `feat: add phase two acquisition contracts`
   - ACQ-01 files only.
2. `feat: persist immutable raw marketplace artifacts`
   - ACQ-02 files only.
3. `refactor: separate marketplace fetch and parse boundaries`
   - ACQ-03 files only.
4. `feat: map tiki pages to canonical observations`
   - ACQ-04 files only.
5. `feat: enforce raw-first page acquisition`
   - ACQ-05 files only.
6. `feat: add bounded raw-first crawler runner`
   - ACQ-06 files only.
7. `test: verify phase two replay and compatibility`
   - test/compatibility fixes inside allowed files only.

The model must stop after each numbered package. A reviewer decides whether to
continue.

## 17. Instructions for a low-capability coding model

For every work package, the coding model must follow this loop:

1. Read this entire plan and every file named by the current package.
2. State the exact files it will create/modify.
3. Confirm the Phase 1 dependency names used by that package.
4. Add the named tests with fixed bytes and UTC times.
5. Implement only the package's public API.
6. Run focused tests.
7. Run all currently runnable Phase 1 + completed Phase 2 tests.
8. Show `git diff --check` and `git diff --stat`.
9. Report pre-existing/environment failures separately.
10. Stop; do not begin the next package automatically.

The reviewer must reject the change if the model:

- parses JSON before the body writer acknowledges;
- stores reconstructed JSON instead of `response.content`;
- creates raw paths with random/time-of-write filenames;
- writes one raw object per parsed product rather than per response;
- omits metadata sidecar, checksum, raw URI, run ID or adapter version;
- publishes Kafka/DLQ or adds SQL/Spark/downstream work;
- creates a second serializer, ID algorithm or event schema;
- converts money through float;
- invents in-stock, seller, rating, sales or transaction semantics;
- catches programmer errors and continues silently;
- performs live calls in default tests;
- starts Phase 3 or Phase 4 code early;
- modifies files outside the package allowlist.

## 18. Copy-ready prompts for a low-capability model

### Prompt A — contracts

```text
Implement only ACQ-01 from
docs/PHASE_2_RAW_FIRST_ACQUISITION_IMPLEMENTATION_PLAN.md.

Read the full Phase 2 plan and the accepted Phase 1 marketplace contracts
before editing. Create only crawler/contracts.py and
tests/test_crawler_contracts.py. Implement the exact enums, exceptions,
dataclasses, validation and task-target helpers. Use fixed UTC times and no
network/storage imports in tests. Run focused tests, show diff stat and stop.
Do not implement raw storage, adapters, runner, Kafka or SQL.
```

### Prompt B — raw store

```text
Implement only ACQ-02 from the Phase 2 plan. Read completed
crawler/contracts.py, Phase 1 identity/schema factories and
common/object_store.py first. Create crawler/raw_store.py and
tests/test_raw_store.py. Write exact response bytes before deterministic
metadata, use the fixed Bronze layout and inject a fake writer in every test.
Run focused plus ACQ-01 tests, show diff stat and stop. Do not parse JSON or
modify crawler/Kafka/SQL code.
```

### Prompt C — adapter boundary

```text
Implement only ACQ-03 from the Phase 2 plan. Read crawler/base.py and its
existing tests fully. Preserve SiteCrawler and robots behavior, then add the
exact HTTP-only fetch and pure parse interfaces. Update only crawler/base.py
and tests/test_crawler.py. Prove the boundaries with offline fakes. Run focused
tests and stop. Do not implement Tiki mapping, storage orchestration or Kafka.
```

### Prompt D — Tiki contract

```text
Implement only ACQ-04 from the Phase 2 plan. Read the full Tiki adapter,
sanitized fixture, Phase 1 contracts and completed crawler contracts first.
Modify crawler/sites/tiki.py and create tests/test_tiki_contract.py. Fetch raw
bytes without decoding; implement the exact field/Decimal/null/availability
rules and construct events only through Phase 1 factories. Run focused tests,
show diff stat and stop. Do not write storage, Kafka, retries or SQL.
```

### Prompt E — raw-first orchestrator

```text
Implement only ACQ-05 from the Phase 2 plan. Create crawler/acquisition.py and
tests/test_marketplace_acquisition.py using the completed fakeable adapter and
raw store. Enforce exact robots/fetch/body/metadata/status/parse ordering and
return Phase2AcquisitionReport for expected failures. Add every ordering and
failure test from ACQ-05. Run focused plus completed Phase 2 tests and stop. Do
not modify runner, Kafka, scheduler or SQL.
```

### Prompt F — runner and final verification

```text
Implement only ACQ-06 and final Phase 2 verification. Read crawler/runner.py
and all completed Phase 2 APIs first. Add the exact one-page Phase 3 entry
point, switch main to bounded raw-first collection and remove Kafka/legacy
price-snapshot use from the accepted runner path. Preserve the behavioral
pipeline. Run the full Phase 2 matrix, producer compatibility smoke,
git diff --check and diff stat, then stop. Do not implement Phase 3 retries/SQL
or Phase 4 Kafka/Silver work.
```

## 19. Phase Definition of Done

- [ ] Phase 1 dependency gate is recorded and green.
- [ ] One acquisition call represents exactly one listing-page response.
- [ ] Robots checks the exact request URL.
- [ ] Fetch returns original bytes without JSON decoding.
- [ ] Any HTTP response body is written before status inspection/parsing.
- [ ] Body and metadata sidecar paths are deterministic.
- [ ] RawArtifact ID/checksum/URI/bytes/run/adapter lineage are complete.
- [ ] Metadata contains no secrets or arbitrary headers.
- [ ] Parser is pure and replayable from supplied saved bytes.
- [ ] Tiki output uses Phase 1 factories and canonical envelopes only.
- [ ] Money is Decimal; missing values remain null/UNKNOWN.
- [ ] Per-row failures preserve valid sibling observations.
- [ ] Pagination terminates on empty, last-page, repeat or max bound.
- [ ] Phase2AcquisitionReport exposes every field Phase 3 audit/retry needs.
- [ ] Successful reports expose complete observations Phase 4 can publish.
- [ ] The accepted runner performs no Kafka/DLQ/SQL/downstream work.
- [ ] Default tests open no network, Kafka, MinIO or PostgreSQL.
- [ ] Existing behavioral tests/producer remain green or pre-existing failures
      are documented.
- [ ] No out-of-scope file/layer changed.

## 20. Final handoff

Once this phase is accepted, Phase 3 can turn
`execute_listing_page(task_target, crawl_run_id)` into a restart-safe scheduled
worker without changing fetch, raw storage or parser behavior. Phase 4 can
publish only the report's accepted `MarketplaceObservationV1` objects and trust
that each one was created after durable raw body + sidecar persistence.

That boundary is the Phase 2 product: not “a crawler that returns products”,
but a deterministic and replayable acquisition transaction with explicit raw
evidence, canonical output and machine-auditable failure semantics.
