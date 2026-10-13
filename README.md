# Marketplace Price Intelligence — Lambda Architecture

A data platform that tracks **public catalog and price state on a Vietnamese
marketplace (Tiki)**, reports changes within seconds of a crawl, and builds a
versioned, quality-gated price history for BI.

> Status: Phases 1–8 are merged into `develop`. Phase 9 (benchmarks, 30-day
> live collection, demo) is in progress on `phase-9-*` branches. Session log:
> [`docs/PROGRESS.md`](docs/PROGRESS.md).

---

## 1. The end-to-end problem

The platform answers three questions about public listings:

1. **How does an offer's advertised price move over time?** This needs a
   complete, correct daily history, plus anomaly verdicts against the offer's
   own past.
2. **How complete and how fresh is the collection?** This needs the coverage
   of each source and the reliability of each crawl, reconciled against what
   the crawler says it published.
3. **When a listing changes, how soon do we know?** This needs a change event
   within seconds of the crawl that saw it.

No single path does all three well. Questions 1 and 2 need **completeness and
reproducibility**: every observation, late ones included, cut at a fixed
instant and checked before anyone sees it. Question 3 needs **low latency**:
react to each observation as it arrives, with only the state of that offer.
That split is the Lambda architecture.

The constraints that shaped everything:

- **Only public state is observable.** The platform sees price, list price,
  availability, rating and the counters a page displays. It sees no orders,
  carts or users. A change in a displayed "sold" counter is a number changing,
  not a sale, and no table calls it demand.
- **The source is undocumented and unreliable.** The Tiki listing endpoint
  behaves like a recommender: ordering changes between requests, pages overlap,
  and `total` is not trustworthy (about 2,000 products per category at most).
  A full 9-category crawl gave 14,117 unique products in 17 m 22 s with 0
  failures, throttled to 2 s plus jitter per request.
- **Raw evidence must survive anything downstream.** If Kafka, MinIO or a
  parser fails, what was fetched must still be recoverable and re-parseable.

## 2. Architecture

```text
                     Tiki listing API  (ops/stub_source.py offline)
                                   │  frontier lease · robots.txt · rate limit · circuit breaker
                     ┌─────────────┴─────────────┐
 ACQUISITION         │   crawler/service.py      │
                     └─────────────┬─────────────┘
                                   │ 1. raw body + sidecar (sha256)     ← written FIRST
                     Bronze  s3a://…/bronze   append-only
                                   │ 2. canonical observation, acks=all
                     Kafka  marketplace.observations.v1  (3 partitions, key = marketplace+listing)
                  ┌────────────────┴──────────────────────────────┐
 SPEED            │                                               │            BATCH
 Spark Structured Streaming                         marketplace_silver_service.py
 trigger 30 s · state per offer_id                  Kafka → Silver, 1 object / observation
 observation → 7 change types                       (undecodable / invalid → .dlq)
   ├→ Kafka  marketplace.changes.v1                               │
   ├→ Elasticsearch  changes · current offers       marketplace_scheduler.py (daily, lag 30 min)
   └→ Redis  rt:offer · rt:changes · rt:source        → Silver ─(as_of)→ Gold run (10 datasets)
                  │                                    → 17 quality rules (13 mandatory)
       ops/es_projector.py                             → publish PostgreSQL cache (one transaction)
       (audit, DLQ → ES)                               → promote gold/current.json (compare-and-swap)
                  │                                               │
 SERVING       Kibana  (seconds old,                    Superset  (one validated version,
               operational)                             the last good as_of)
```

Two rules hold across the whole picture:

- **Bronze before Kafka, Kafka before anything else.** A crawl that fetched and
  parsed but could not publish is audited `PUBLISH_ERROR`, and its raw body is
  already in Bronze. `crawler/reparse.py` rebuilds observations from Bronze
  after verifying the checksum.
- **The two paths share nothing but the topic and the frozen contracts.** The
  speed path cannot corrupt Gold, and a refused batch cannot touch realtime
  state.

## 3. Layers and their configuration

Every value below is an environment variable read by `config/settings.py`,
shown with its default. `validate_marketplace_settings()` refuses
combinations that cannot work.

### 3.1 Acquisition: `crawler/`

A **frontier** in PostgreSQL holds one task per (marketplace, resource, target,
scheduled time). Workers lease tasks, crawl them, write Bronze, publish to
Kafka, and record every attempt with its `error_kind`.

| Setting | Default | Why |
|---|---|---|
| `CRAWL_ACTIVE / NORMAL / COLD_CADENCE_MINUTES` | 60 / 240 / 720 | recrawl tiers; the ACTIVE cadence bounds how fresh anything downstream can be |
| `CRAWL_REQUEST_DELAY_SECONDS` + `CRAWL_JITTER_SECONDS` | 2.0 + 1.0 | politeness; Tiki sets no `Crawl-delay` |
| `CRAWL_LEASE_SECONDS` | 300 | a dead worker's tasks return after the lease; a late completion raises `LeaseLostError` |
| `CRAWL_MAX_ATTEMPTS`, `CRAWL_RETRY_BASE/MAX_SECONDS` | 5, 30 / 1800 | exponential backoff with 20 % jitter |
| `CRAWL_CIRCUIT_FAILURE_THRESHOLD`, `CRAWL_CIRCUIT_OPEN_SECONDS` | 5, 900 | after N consecutive failures the source is left alone |
| `CRAWL_MAX_PAGES` | 50 | pages per category; the endpoint's `last_page` cuts earlier |

A page shape that drifts becomes `PARSE_ERROR`: terminal, raw body kept, and
the circuit does not open, because drift is not an outage.

### 3.2 Ingestion: Kafka and Silver

| Topic | Key | Content |
|---|---|---|
| `marketplace.observations.v1` | `(marketplace, platform_listing_id)` | one observation per sighting of an offer |
| `marketplace.observations.v1.dlq` | | only records that failed `DECODE` or `CONTRACT_VALIDATION` |
| `marketplace.changes.v1` | `offer_id` | change events from the speed layer |

Producers use `acks=all` and `max_in_flight_requests_per_connection=1`, so the
order of one listing's observations is kept across retries. `kafka-python-ng`
has no idempotent producer, so delivery is **at-least-once**, made safe by
**deterministic identity**: `observation_id` is a hash of marketplace, listing,
`observed_at` and raw hash; `offer_id` of marketplace and listing; `event_id`
of offer, observation, change type and rule version. A redelivery overwrites
itself.

The Silver sink writes **one object per observation**, at a path computed
from the observation, and commits the offset only after the write. A crash
between the two re-delivers the record, which rewrites the same bytes. A
storage outage stops the sink and grows lag; it never sends a valid record to
the DLQ.

### 3.3 Speed layer: `speed_layer/`

A Spark Structured Streaming query reads the observations topic, decodes and
validates each record, groups by `offer_id`, and runs
`applyInPandasWithState`. The state is the offer's last applied observation.

For each observation, in event-time order within the trigger:

| Disposition | When | Effect |
|---|---|---|
| `APPLIED` | newer than the state | new state; zero or more change events |
| `DUPLICATE` | same `observation_id` as the state | nothing |
| `LATE` | older than the state | nothing in speed; **batch still sees it** in Silver |

Change rules (`speed-rules.v1`) produce exactly seven types: `NEW_OFFER`,
`PRICE_CHANGED`, `LARGE_PRICE_DROP` (a drop of at least 100,000 VND or 20 %),
`RATING_CHANGED`, `COUNTER_CHANGED`, `AVAILABILITY_CHANGED`, and
`OFFER_STALE`. `OFFER_STALE` is raised by a processing-time timeout when no
observation has arrived for 6 hours. It means "not seen", not "delisted".

| Setting | Default | Why |
|---|---|---|
| `MARKETPLACE_STREAM_TRIGGER` | 30 seconds | the micro-batch interval, and the largest part of speed latency |
| `MARKETPLACE_STALE_AFTER_SECONDS` | 21600 | the timeout behind `OFFER_STALE` |
| `MARKETPLACE_LARGE_DROP_ABSOLUTE / RELATIVE` | 100000 / 0.20 | the `LARGE_PRICE_DROP` threshold |
| `MARKETPLACE_SPEED_SHUFFLE_PARTITIONS` | 4 | state partitions; small on purpose for one machine |
| `MARKETPLACE_STREAM_CHECKPOINT_VERSION` | v1 | a new version replays from `earliest` to the same document set |
| `REDIS_MARKETPLACE_OFFER_TTL_SECONDS`, `…_RECENT_CHANGES_MAX` | 86400, 5000 | realtime state is bounded |

Each micro-batch writes Kafka, then Elasticsearch, then Redis, and is audited
in `audit.marketplace_speed_batch`, keyed by `(query_name, query_id,
batch_id)`. A batch that fails any sink is audited `FAILED` and retried as
the same batch. Because every document `_id` and sorted-set member is
deterministic, the retry creates no duplicates.

### 3.4 Batch layer: `batch_layer/`

`marketplace_scheduler.py` is a plain loop (not Airflow), so a crash leaves
only an audit row to resume. Once per interval it cuts a window and runs
`marketplace_warehouse.py`:

```text
Silver (observed_at ≤ as_of) + audit (crawl runs finished ≤ as_of)
   → 10 Gold datasets in gold/marketplace/runs/run_id=<run>/
   → 17 quality rules        (any mandatory failure → QUALITY_FAILED, nothing published)
   → staging tables → one transaction: TRUNCATE + refill cache.*
   → current.json ← run manifest   (compare-and-swap; only after the cache is published)
```

| Setting | Default | Why |
|---|---|---|
| `MARKETPLACE_BATCH_INTERVAL_SECONDS` | 86400 | one window a day; windows are aligned to the Unix epoch in UTC |
| `MARKETPLACE_BATCH_AS_OF_LAG_SECONDS` | 1800 | a window is cut 30 min after it closes, so crawl runs settle first |
| `MARKETPLACE_QUALITY_RECONCILIATION_SETTLE / LOOKBACK_SECONDS` | 900 / 172800 | the lookback must exceed interval + settle, or a crawl run could age out unreconciled (enforced) |
| `MARKETPLACE_FRESHNESS_SECONDS` | 21600 | `FRESH` / `STALE` at the run's `as_of` |
| `MARKETPLACE_ANOMALY_WINDOW_DAYS`, `…_MIN_SAMPLES` | 14, 7 | the history an anomaly verdict needs |
| `MARKETPLACE_ANOMALY_MAD_THRESHOLD`, `…_IQR_MULTIPLIER` | 3.5, 1.5 | robust outlier fences |
| `MARKETPLACE_COUNTER_MAX_GAP_SECONDS` | 86400 | a counter transition over a longer gap is not trusted |
| `MARKETPLACE_BATCH_SHUFFLE_PARTITIONS` | 8 | |
| `MARKETPLACE_BATCH_LOCK_KEY` | 820801 | PostgreSQL advisory lock: one batch at a time; a second exits 75 and writes nothing |

The scheduler never backfills. An old window is an operator command
(`mp batch -AsOf … -AllowBackfill`), and a manifest older than the live
pointer is refused unless backfill is explicit.

### 3.5 Serving

| Store | Role | Holds |
|---|---|---|
| MinIO Gold | **authoritative** | every run, its manifest, and `current.json` (the last good version) |
| PostgreSQL `cache` | BI serving | exactly the version `current.json` names: 10 marts |
| PostgreSQL `audit` | operational record | frontier, crawl runs and attempts, batch runs, quality results, speed batches |
| Elasticsearch | recent operational and realtime state | changes, current offers, plus source health, crawl attempts, speed batches and DLQ (from `ops/es_projector.py`) |
| Redis | latest realtime state | `rt:offer:<id>`, `rt:changes:recent`, `rt:source:<m>:last_observation` |

Superset reads only `cache` (validated, as of the last good run). Kibana reads
only Elasticsearch (seconds old, not validated). The dashboards say which is
which.

## 4. Data model

### 4.1 Medallion zones

| Zone | Grain | Write policy |
|---|---|---|
| Bronze `raw/marketplace=/observed_date=/hour=/crawl_run_id=/raw_artifact_id=` | one HTTP response + metadata sidecar | append-only, never rewritten |
| Silver `offer_observations/marketplace=/observed_date=/observation_id=.json` | one observation | idempotent by path |
| Gold `runs/run_id=<run>/<dataset>/` | one directory per run | never overwritten; the pointer chooses which is served |

### 4.2 Gold and the cache: a star around the offer

The marts are a **dimensional model centred on the offer**. Two "current"
tables act as dimensions, and the daily marts are facts at an explicit grain
that join to them on natural keys.

```text
                         ┌──────────────────────────┐
                         │   seller_current  (dim)  │
                         │ PK (seller_id,marketplace)│
                         └────────────▲─────────────┘
                                      │ seller_id
 ┌─────────────────────────┐   ┌──────┴───────────────────┐   ┌──────────────────────────┐
 │ offer_price_history_daily│   │    offer_current  (dim)  │   │   offer_change_daily      │
 │ (marketplace, offer_id, ├──►│ PK offer_id               │◄──┤ (marketplace, offer_id,   │
 │  observed_date, currency)│   │ title, brand,             │   │  observed_date, currency) │
 └─────────────────────────┘   │ category_path, seller_id, │   └──────────────────────────┘
 ┌─────────────────────────┐   │ latest price/rating/      │   ┌──────────────────────────┐
 │ price_anomaly_daily      ├──►│ counters, raw lineage     │◄──┤ counter_delta_daily       │
 │ (… , currency)           │   └──────▲───────────────────┘   │ (… , counter_name)        │
 └─────────────────────────┘          │                        └──────────────────────────┘
                              ┌───────┴──────────┐
                              │ offer_freshness   │  PK offer_id, judged at the run's as_of
                              └──────────────────┘

 Aggregate facts, no offer key:
   category_price_daily      (marketplace, category_path, observed_date, currency)
   source_coverage_daily     (marketplace, observed_date)
   crawl_reliability_daily   (marketplace, request_date)
```

| Table | Type | Grain | Measures |
|---|---|---|---|
| `offer_current` | dimension (current state) | `offer_id` | title, brand, category path, seller; latest price, list price, rating, counters, availability; Bronze lineage |
| `seller_current` | dimension | `(seller_id, marketplace)` | first/last seen, observed offer count |
| `offer_price_history_daily` | periodic snapshot fact | offer × day × currency | first/last/min/max/avg price, observation and distinct-price counts |
| `offer_change_daily` | transaction summary fact | offer × day × currency | price changes, drops, increases, signed and absolute change, rating/counter/availability changes |
| `counter_delta_daily` | fact | offer × day × counter | raw and valid delta, invalid transitions with reasons, velocity proxy |
| `price_anomaly_daily` | fact | offer × day × currency | baseline median/MAD/IQR, fences, robust score, status |
| `offer_freshness` | snapshot at `as_of` | `offer_id` | age, `FRESH` / `STALE` / `FUTURE` |
| `category_price_daily` | aggregate fact | category × day × currency | min, p25, median, p75, max, avg |
| `source_coverage_daily` | aggregate fact | marketplace × day | eligible / observed / missing offers, coverage and rejection rates |
| `crawl_reliability_daily` | aggregate fact | marketplace × request day | success rate, latency avg/p95, raw bytes, five error-kind counts |

Design choices, and why:

- **Natural, deterministic keys instead of surrogate keys.** `offer_id` is a
  hash of marketplace and listing ID, so Gold built from a replay, a restore
  or a rebuild joins exactly as the original did. Surrogate keys would differ
  per run.
- **`currency` is part of every price grain.** An offer that switches
  currency mid-day yields one row per currency, not an average across
  currencies.
- **No `dim_date`.** `observed_date` is a degenerate date column. Calendars
  carry no business attributes here (no fiscal periods, no store hours).
- **Dimensions hold current state (SCD type 1); history lives in the facts and
  Silver.** Price history is the fact itself. Attribute history (a renamed
  title, a moved category) stays recoverable from Silver, so a type-2
  dimension would duplicate what the lake already keeps.
- **The category hierarchy is kept whole.** Tiki categories are a ragged tree
  4–8 levels deep (964 distinct paths in one crawl), so `category_path` is
  stored as the full path rather than fixed `level_1…level_n` columns.
- **Verdicts carry their rules.** Freshness, counter and anomaly rows store
  their rule version and parameters, so a stored verdict can still be checked
  after the configuration changes.

Grading rules that are easy to misread:

- a **negative counter delta is never clamped to zero**: it is kept and
  flagged `counter_reset_or_invalid`;
- a **price anomaly compares an offer only with its own 14-day history**, and
  `INSUFFICIENT_HISTORY` / `INSUFFICIENT_DISPERSION` mean "no verdict", not
  "normal";
- `rating = 0` with `review_count = 0` (about 47 % of products) means "no
  reviews yet", so an average rating must leave those rows out.

The full column lists, the contracts and the audit tables are in
[`docs/DATA_MODEL.md`](docs/DATA_MODEL.md).

## 5. Speed latency and batch strength

### 5.1 Where speed latency comes from

From a change on the site to a change event in Kibana:

| Hop | Typical cost | What controls it |
|---|---|---|
| change on site → next crawl | up to the cadence (60 min ACTIVE) | `CRAWL_*_CADENCE_MINUTES`; **the dominant term** |
| crawl attempt | ~2.9 s p50, ~3.7 s p95 (live Tiki) | the source and the throttle |
| parse → Kafka ack | ~30 ms p50 | `acks=all` on one broker |
| wait for the trigger | 0–30 s | `MARKETPLACE_STREAM_TRIGGER` |
| micro-batch | ~1.1 s fixed + per-record cost | Spark planning and state, then Kafka / ES / Redis writes |

Measured in Phase 9 (live collection and benchmarks; see PROGRESS §26–31):
speed latency from produce to all sinks written is **8–24 s p50** at a 30 s
trigger, so the trigger, not processing, sets it. A micro-batch costs about
1.1 s however small it is, which is why the trigger is not lowered blindly:
at a 2 s trigger that fixed cost leaves room for only tens of observations
per second.

How the design keeps latency low without giving up correctness:

- **State per offer, not per window.** A change needs only the offer's last
  observation, so there is no join, no lookback and no wait for a window to
  close.
- **Ordering comes from the key, not from waiting.** All observations of one
  listing share a partition and are sorted by event time within a trigger.
  An out-of-order observation is marked `LATE` and left to batch, rather than
  holding the stream open with a watermark.
- **Bounded sinks.** Elasticsearch takes one bulk request per micro-batch,
  Redis one pipeline. On `phase-9-*` branches, Kafka sends are also batched,
  the source watermark is read once per batch, and sink clients are reused
  across batches. That raised saturation from ~180 to ~440 records/s per
  micro-batch.
- **Idempotent retries.** A failed micro-batch is retried whole, and
  deterministic IDs make the retry invisible downstream.

### 5.2 What batch can do that speed cannot

| | Speed | Batch |
|---|---|---|
| Input | each observation once, in arrival order | **every** Silver row observed ≤ `as_of`, late ones included |
| Context | one offer's last state | full history: 14-day baselines, daily distributions, cross-offer aggregates |
| Checked against | its own contract | the crawl audit (Silver must reconcile with what the crawler says it published) and 17 rules |
| Replayable | no (arrival-dependent) | yes: a run is a function of `as_of`, Silver and the audit, and its manifest reads no wall clock. A rebuild can see more rows only if they landed in Silver after the first run read it |
| Versioned | latest state only | every run kept in Gold; the pointer moves only to a run that passed |
| Failure visible as | a `FAILED` micro-batch, retried | `QUALITY_FAILED` with all 17 results stored; the previous version keeps serving |

The quality gate checks Silver (raw lineage complete, unique
`observation_id`, non-negative prices, valid currency, no observation from
the future), the audit (Silver reconciles with crawl attempts), and Gold
(daily row counts and price aggregates reconcile with Silver, one current row
per offer, freshness judged at `as_of`). Counter-transition and anomaly-coverage
rates are advisory.

Measured on replayed data (Phase 9, PROGRESS §27): a batch over 200,000 Silver
rows takes about 6 minutes on one machine (~530 rows/s), and the cost is
stable to within 2 % between runs. The current bottleneck of the batch path
is upstream. The Silver sink writes one object per observation and drains
80–116 records/s. That is ample for the current collection rate (a few
hundred observations an hour) but is the first thing to change at a larger
scale.

## 6. Failure behaviour

Every row below was established by a drill (`mp drill d1 … d11`) against the
running stack.

| Lost | Result | After recovery |
|---|---|---|
| the source | attempts audited by `error_kind`; circuit opens | tasks resume when it closes |
| Kafka | `PUBLISH_ERROR`; raw body in Bronze | the window reconciles |
| MinIO | Silver sink stops committing; lag grows; no DLQ | lag returns to 0, Silver matches the audit |
| Elasticsearch / Redis | micro-batch `FAILED`, retried | no duplicate IDs or members |
| speed killed | checkpoint holds offsets | no change lost or duplicated |
| a crawl worker | leases expire | another worker takes the tasks |
| page shape | `PARSE_ERROR`, raw kept, circuit unchanged | reparse after the adapter is fixed |
| Silver vs audit | `QUALITY_FAILED`; cache and pointer unchanged | resume the run |
| cache publish | run `FAILED`; previous version intact | resume; pointer and cache move together |
| a second batch | exits 75, no audit row | — |
| the whole stack | `mp backup` / `mp restore` into a new project | pointer and cache agree; reparse sample identical |

## 7. Technology

Kafka 4.2 (KRaft) · Spark 4.0 / PySpark 4.0.4 · MinIO (S3A) · PostgreSQL 18 ·
Elasticsearch and Kibana 8.18 · Redis 8.6 · Superset 4.1 · Python 3.12 ·
Docker Compose with profiles. Two pinned images:
`ecommerce/marketplace-python` and `ecommerce/spark-marketplace:4.0.1`.

## 8. Running it

```powershell
copy .env.example .env
.\scripts\mp.ps1 up -With crawl,ingest,speed,batch,serve
.\scripts\mp.ps1 smoke        # the full slice against the offline stub; never contacts Tiki
.\scripts\mp.ps1 validate     # 11 read-only checks over the running stack
```

Superset is at http://localhost:8088, Kibana at http://localhost:5601, and
the MinIO console at http://localhost:9001. Tests: `python -m pytest`
(1,006 offline tests; the drills need the stack: `-m drill`).

| Document | For |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | the design in depth |
| [DATA_MODEL.md](docs/DATA_MODEL.md) | every contract, table, index and key |
| [RUNBOOK.md](docs/RUNBOOK.md) | operating the stack: seeding, backfill, drills, backup |
| [PHASE_INDEX.md](docs/PHASE_INDEX.md) | phases, frozen contracts, branching model |

The repository also keeps its original Kaggle clickstream demo (a classic star
schema over views, carts and purchases) under the `legacy` profile. It is
independent of everything above; see [ARCHITECTURE.md §11](docs/ARCHITECTURE.md).
