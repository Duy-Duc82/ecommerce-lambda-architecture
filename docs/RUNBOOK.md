# Runbook — marketplace stack

> Phase 8, plan section 13. Written for an operator who has the stack and a
> problem, not for a reader learning the system — that is
> [ARCHITECTURE.md](ARCHITECTURE.md), and the tables are in
> [DATA_MODEL.md](DATA_MODEL.md).
>
> Every procedure here was run on a real stack; where a number is quoted, the
> run that produced it is named.

## Starting the stack

```powershell
copy .env.example .env                       # once; edit host ports if taken
docker compose up -d                         # core only
docker compose --profile ingest --profile speed --profile batch up -d
docker compose --profile crawl up -d         # needs a seeded frontier
docker compose --profile serve up -d         # Kibana, Superset
.\scripts\start_all.ps1                      # the legacy Kaggle demo
```

| Profile | Services |
|---|---|
| *(none)* | kafka, kafka-init, minio, minio-init, postgres-dw, redis, elasticsearch |
| `crawl` | crawl-worker |
| `ingest` | silver-sink |
| `speed` | speed (checkpoints in volume `speed_checkpoints`) |
| `batch` | batch-scheduler; `batch-once` for operator commands |
| `serve` | kibana, superset, superset-init, kibana-marketplace-setup |
| `legacy` | spark, spark-worker, kibana, kibana-setup |
| `jobs` | warehouse-job (legacy; `scripts/run_warehouse.ps1`) |
| `ops` | ops (a `run --rm` tool container; `backup`, `restore`, `validate`), es-projector (a loop service) |
| `smoke` | stub-source, the offline Tiki stub |

Operator batch run, for example a resume or a backfill:

```powershell
docker compose --profile batch run --rm batch-once `
  python3 -m batch_layer.marketplace_warehouse --run-id mp-20261002T0000Z `
  --as-of 2026-10-02T00:00:00Z --resume
```

Exit code 75 with `{"status": "ALREADY_RUNNING"}` means another batch holds
the advisory lock; nothing was started.

`docker-compose.override.yml` is no longer needed: host ports come from the
`*_HOST_PORT` variables in `.env`. Delete an old override file, or it keeps
applying its own `!override` ports and images.

## Four stacks on one machine (Phase 9 plan section 5)

Live collection runs for 30 days and must never stop for a test. Every other
use of the stack therefore runs in a Compose project of its own, beside it:

| Stack | Env file | Project | Container names | What may run in it |
|---|---|---|---|---|
| live | `env/live.env` | `mp-live` | unprefixed (`kafka`, `speed`, ...) | live collection only: `up`, `status`, `validate`, `batch`, `backup` |
| bench | `env/bench.env` | `mp-bench` | `bench-kafka`, ... | `smoke`, `drill`, benchmarks |
| demo | `env/demo.env` | `mp-demo` | `demo-kafka`, ... | the offline demo |
| *(none)* | `.env` only | the directory name | unprefixed | nothing while `mp-live` is up: its names are taken |

```powershell
.\scripts\mp.ps1 -EnvFile env/live.env status
.\scripts\mp.ps1 -EnvFile env/live.env validate -Json data/ops/live/validate-2026-10-05.json
.\scripts\mp.ps1 -EnvFile env/bench.env smoke
.\scripts\mp.ps1 -EnvFile env/bench.env drill d3
```

`-EnvFile` sets the file's variables for that one call and puts the caller's
environment back afterwards. Each isolated file sets its own project name,
`MP_CONTAINER_PREFIX`, every published port, and the host-side client
addresses that drills use. Without those addresses, a drill would reach the
live stack's PostgreSQL on 5433.

**`smoke`, `drill` and `down -Volumes` refuse the live stack, with exit
code 2.** Two checks decide it: the project this call targets, and the
`com.docker.compose.project` label of the `kafka` container the call would
reach. So the checks still hold when `-EnvFile` is forgotten. `ops.drills`
repeats the label check before the baseline, the drill and its restore.
Never point an isolated stack at the live site: its stub categories `9001`–`9003`
do not exist on Tiki.

## `mp` — one command (plan section 9)

```powershell
.\scripts\mp.ps1 up -With crawl,ingest,speed,batch   # core plus the listed profiles
.\scripts\mp.ps1 status                              # containers, then the last run of each component
.\scripts\mp.ps1 migrate                             # re-apply scripts/init_postgres.sql (idempotent)
.\scripts\mp.ps1 seed --category 1846 --pages 2      # crawler.seed_frontier
.\scripts\mp.ps1 batch -AsOf 2026-10-02T00:00:00Z    # one-shot operator batch
.\scripts\mp.ps1 validate -Json validate.json        # plan 9.2, exit 1 if any check fails
.\scripts\mp.ps1 smoke                               # plan 9.1
.\scripts\mp.ps1 down [-Volumes]                     # -Volumes asks before deleting data
```

Docker itself (up, down, the Spark batch) is driven from mp.ps1 on the host.
Everything that reads or writes the stack runs as `python -m ops ...` in the
`ops` container. mp.ps1 always passes `-f docker-compose.yml`, so an old
override file never applies to it.

### Seeding the frontier

Nothing is crawled until the frontier has tasks.

```powershell
.\scripts\mp.ps1 migrate                              # idempotent; safe to re-run
.\scripts\mp.ps1 seed --category 1846 --pages 2       # real Tiki categories
```

`seed_frontier` is idempotent: seeding the same targets for the same schedule
enqueues nothing the second time, because `(marketplace_code, resource_type,
target, scheduled_for)` is unique.

**The smoke seeds its own universe** — categories `9001`–`9003`, which do not
exist on Tiki — and *parks* them when it ends. Never leave them `READY`: a
later `mp up` with the real `TIKI_LISTING_URL` would send the crawler at the
live site looking for them. If a smoke was interrupted, park them by hand:

```powershell
docker compose --profile ops run --rm --no-deps ops python -m ops park-smoke
```

### Daily operational check (Brief section 20)

One command answers all of it:

```powershell
.\scripts\mp.ps1 validate -Json validate.json      # exit 1 if any check fails
.\scripts\mp.ps1 status                            # the last run of each component
```

Every check is derived from data the stack recorded itself, never from a wall
clock — windows are anchored on the newest crawl attempt, the pointer's
`as_of`, or the newest offer state.

| What Brief section 20 asks | The check |
|---|---|
| is the raw evidence still there | `bronze_present` — every recent attempt's `raw_uri` resolves |
| did anything get stuck between Kafka and Silver | `kafka_to_silver_lag` |
| does Silver agree with what the crawler says it published | `silver_reconciles_with_audit` |
| is anything valid being quarantined | `dlq_only_bad_records` — only `DECODE` and `CONTRACT_VALIDATION` |
| did the last batch end cleanly | `batch_latest_terminal` |
| is the quality evidence complete | `quality_results_complete` — one row per rule |
| is the serving version coherent | `pointer_matches_cache`, `pointer_gold_exists` |
| is the realtime path alive and not duplicating | `es_changes_unique`, `redis_offer_state_present`, `speed_last_batch_succeeded` |

A failing check prints what it observed next to what it expected. Start there,
not in the logs.

### Backfill

The scheduler never backfills. It skips a `SUCCEEDED` window, starts an absent
one, resumes any other status, and never passes `--allow-backfill` — so an old
window is always a deliberate operator action:

```powershell
.\scripts\mp.ps1 batch -AsOf 2026-09-28T00:00:00Z -AllowBackfill
```

Without `-AllowBackfill`, promoting a manifest older than the live pointer is
refused with `BACKFILL_REFUSED`: the run still writes its Gold and its
manifest, it simply does not become the serving version. That is usually what
you want — a backfill is for filling a hole in history, not for rolling the
pointer backwards.

`-QualityOnly` runs the gate without publishing, which is how a restored stack
is checked.

### Two constraints that bite

**`--skip-postgres` can advance the pointer past the cache.** It skips the
publish but not the promotion, so the pointer ends up naming a run whose rows
never reached `cache`, and `validate`'s `pointer_matches_cache` fails. Use it
only to inspect Gold, never on a stack anyone is serving from
(`PROGRESS.md` section 10.6). The offline smoke that Phase 6 section 16 and
Phase 7 section 18 once described around this flag does not exist: the crawl
audit is read over JDBC unconditionally, so `mp smoke` replaced it.

**The reconciliation lookback must exceed the batch interval plus the settle
delay.** A crawl run is reconciled against Silver only once it has been
finished for `MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS`, and only
while it is inside `MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS`. If
the lookback is too short, a run can settle and age out between two scheduled
batches without ever being reconciled — the gate would then pass runs it had
never looked at. `validate_marketplace_settings()` refuses such a
configuration at import, so this fails at startup rather than silently.

### Smoke

`mp smoke` never contacts Tiki. It starts `stub-source` and points the crawler
at it. Then it:

1. seeds its own universe: categories `9001`–`9003` × 2 pages, ACTIVE tier,
   recrawled every minute;
2. waits until every smoke target has been crawled successfully twice, Silver
   holds exactly what those attempts parsed, and a speed micro-batch has
   emitted changes;
3. stops the crawler and waits for Silver lag 0;
4. runs one batch with `as_of` at the first minute boundary one settle delay
   (60 s in the smoke) after the last crawl run finished;
5. runs `validate`, whose report lands in `data/ops/smoke-validate.json`.

However it ends, the smoke stops the crawler, parks its tasks (`DISABLED`, so a
later `mp up` against the real Tiki never crawls the made-up categories) and
restores the environment variables it set. The next smoke re-enables them.

The plan asks for a **fresh stack**. To get one without touching the
development data, run the smoke in its own Compose project and remove that
project afterwards:

```powershell
docker compose -f docker-compose.yml --profile "*" down        # keeps the dev volumes
$env:COMPOSE_PROJECT_NAME = "mp-smoke"; .\scripts\mp.ps1 smoke
docker compose -f docker-compose.yml --profile "*" down --volumes; Remove-Item Env:COMPOSE_PROJECT_NAME
docker compose -f docker-compose.yml up -d                      # the dev stack again
```

On a stack that also holds host-run crawls with a `local` lake,
`bronze_present` fails: those attempts recorded `file:///D:/...` raw URIs that
no container can read (`PROGRESS.md` §16.4).

### Drills D1-D11 (plan section 10)

A drill injects one fault into the **running** stack, asserts what the system
does while faulted, removes the fault and checks the system came back. It runs
on the host, not in the `ops` container, because it drives Docker:

```powershell
$env:COMPOSE_PROJECT_NAME = "mp-smoke"
.\scripts\mp.ps1 smoke            # a drill starts from a passing validate
.\scripts\mp.ps1 drill d1         # then d2 ... d11, one at a time
.\scripts\mp.ps1 drill all        # or all eleven in order
```

`mp drill` runs `.venv\Scripts\python.exe -m ops.drills` with
`DATA_LAKE_PROFILE=minio`, so the host `.env` must publish the stack's ports.
Each drill writes `data/ops/drills/<name>.json`: every step with its
timestamp, the observations, and `passed`.

| Drill | Inject | Recovery |
|---|---|---|
| D1 | stub mode `429`, `500`, `timeout` | stub mode `ok` |
| D2 | `compose stop kafka` mid-crawl | `compose up -d kafka` |
| D3 | `compose stop minio` under the Silver sink | `compose up -d minio` |
| D4 | `compose stop elasticsearch`, then `redis` | `compose up -d <service>` |
| D5 | SIGKILL the speed container inside a micro-batch; then delete this project's `speed_checkpoints` volume | the restart policy brings `speed` back; the second variant replays from `earliest` |
| D6 | SIGKILL crawl-worker A while it holds leases | start worker B under a second `CRAWL_SERVICE_WORKER_ID` |
| D7 | stub mode `drift` | stub mode `ok` |
| D8 | one planted row in `audit.crawl_request_attempt` whose `parsed_count` Silver denies | delete that row by the `attempt_id` the insert returned, then resume the same run |
| D9 | `CHECK` constraint `drill_d9_reject_one_cache_row` on `cache.marketplace_offer_current`, `NOT VALID` | `DROP CONSTRAINT`, then resume the same run |
| D10 | two `batch-once` containers started at once under different run ids | none needed; the refused one did nothing |
| D11 | `ops backup`, then the whole stack **down** and a restore into `<project>-restore` | the drill removes the restored project with its volumes; the harness then brings the real stack back |

**D10 publishes under a run id ending `-d10`.** The two racing batches carry
different run ids on purpose: with one id, "the refused run wrote no audit
row" cannot be told from the winner's row. Whichever takes the lock publishes,
so the pointer may afterwards name `mp-<stamp>Z-d10`. Nothing parses a run id,
so this is cosmetic; the next scheduled batch moves the pointer on again.

**D5 takes about twenty minutes**, and most of that is deliberate. A
micro-batch's audit row is open only while the sinks are written — 10 ms for
an empty batch, 28-90 ms for one crawl cycle — and a `docker kill` needs
~220 ms to land, so the drill stops the query and lets the crawler build a
Kafka backlog first. The batch that drains it is long enough to be killed
inside, and the proof that the kill landed there is the audit row left
`RUNNING` with no `completed_at`. A terminal status there fails the drill
rather than passing it quietly.

Rules the drill code keeps, and so must anyone adding one:

- **Inject only through Compose, the stub's mode endpoint, or one documented
  SQL statement whose reverse is in the same drill.** Never edit a data file
  by hand, and never leave a SQL mutation behind: it is undone in the same
  drill, pass or fail. Only two drills mutate anything, and each undoes its
  own in a `finally`, so a drill that fails mid-way still cleans up:

  | Drill | Mutation | Reverse |
  |---|---|---|
  | D8 | one `INSERT` into `audit.crawl_request_attempt`, carrying the marker `drill-d8 planted mismatch` and returning its `attempt_id` | `DELETE ... WHERE attempt_id = <that id>` — never a `WHERE` that could match a real row |
  | D9 | `ALTER TABLE cache.marketplace_offer_current ADD CONSTRAINT drill_d9_reject_one_cache_row ... NOT VALID` | `DROP CONSTRAINT IF EXISTS` the same name |

  A killed process has no `finally`, so the **baseline refuses to start** when
  it finds either leftover and names what to remove. To clear one by hand:

  ```powershell
  docker compose -f docker-compose.yml exec postgres-dw psql -U admin -d data_warehouse -c `
    "DELETE FROM audit.crawl_request_attempt WHERE error_message LIKE 'drill-d8 planted mismatch%';"
  docker compose -f docker-compose.yml exec postgres-dw psql -U admin -d data_warehouse -c `
    "ALTER TABLE cache.marketplace_offer_current DROP CONSTRAINT IF EXISTS drill_d9_reject_one_cache_row;"
  ```
- **Everything removed is scoped to the current Compose project.** D5 filters
  volumes by `label=com.docker.compose.project=<project>`; a bare
  `--filter name=speed_checkpoints` would also match another project's stack
  on the same machine.
- **A drill that cannot reach its baseline fails.** It never "skips green".
- D6 clears the container's restart policy (`docker update --restart no`)
  before killing worker A, because the five long-running services carry
  `restart: unless-stopped` and Docker would otherwise bring A straight back.

**When a drill fails.** However it ends, `run()` restores the stack: it starts
every service it stopped, puts the stub back in `ok`, parks the smoke tasks,
waits for the Silver sink to catch up and then waits for `validate` to pass.
A drill is only recorded as passed if that restore also passed, so `drill all`
cannot cascade. If the restore itself fails, the record says
`restore_failed` and the stack needs a look before the next drill:

```powershell
.\scripts\mp.ps1 status
.\scripts\mp.ps1 validate -Json validate.json   # which check is red
docker compose -f docker-compose.yml --profile "*" up -d
```

A drill that reveals a production bug gets a failing unit test and a fix, in
two commits, before it is marked passing.

### Kafka lost its topics

Kafka keeps its log on the `kafka_data` volume, so recreating the container
keeps every topic. If the volume itself is lost, the speed query refuses to
start (`Some data may have been lost`), because its checkpoint holds offsets
the new topics never had. The changes it projected are rebuildable, so:

```powershell
docker compose -f docker-compose.yml --profile speed rm -sf speed
# Compose prefixes the volume with the project name, which is the directory
# unless COMPOSE_PROJECT_NAME says otherwise -- so ask Docker rather than
# guessing, or another project's checkpoints are one typo away.
docker volume ls -q --filter "label=com.docker.compose.project=$(docker inspect kafka -f '{{index .Config.Labels \"com.docker.compose.project\"}}')" --filter name=speed_checkpoints
docker volume rm <the volume that printed>
.\scripts\mp.ps1 up -With speed
```

## Kibana — index templates, dashboards and the projector (plan section 11)

### What `serve` installs

`kibana-marketplace-setup` runs once when the `serve` profile comes up. It is
idempotent, so re-running it is the fix for most Kibana problems:

```powershell
docker compose --profile serve up -d kibana
docker compose --profile serve run --rm --no-deps kibana-marketplace-setup
# on the host instead of in a container:
$env:LOCAL = "true"; python -m display.kibana.setup_marketplace_kibana
```

It installs six composable index templates, creates the four projector
indices if they are absent (a Lens panel over a missing index is an error,
not an empty chart), deletes the Phase 5 dashboard shell, and imports
`display/kibana/saved_objects/marketplace_dashboards.ndjson` with
`overwrite=true`.

That `.ndjson` is **generated**, not hand-edited. Change
`display/kibana/marketplace_dashboards.py` and regenerate:

```powershell
python -m display.kibana.marketplace_dashboards
```

`tests/test_kibana_marketplace.py` fails if the committed file and the
builder disagree.

### The projector

```powershell
docker compose --profile ops up -d es-projector     # the loop service
docker compose --profile ops run --rm --no-deps es-projector `
  python -m ops.es_projector --once                 # one pass, then exit
docker compose --profile ops run --rm --no-deps es-projector `
  python -m ops.es_projector --once --rebuild       # re-index everything
```

| Index | Source | `_id` |
|---|---|---|
| `marketplace-source-health-v1` | `audit.crawl_source_state` + Redis `rt:source:<mkt>:last_observation` | `marketplace_code` |
| `marketplace-crawl-attempts-v1` | `audit.crawl_request_attempt` joined to the frontier | `attempt_id` |
| `marketplace-speed-batches-v1` | `audit.marketplace_speed_batch` | `query_name:query_id:batch_id` |
| `marketplace-dlq-v1` | the Kafka DLQ topic, group `marketplace-dlq-projector-v1` | `dlq_id` |

Every pass re-reads the last `MARKETPLACE_OPS_PROJECT_OVERLAP_SECONDS`
(900 by default) of settled rows. A row updated after it was first projected,
and a pass that died halfway, are both repaired by the next pass, because
every `_id` is derived from the row's own key. The watermark lives in the
process only: a restart resumes from `now - overlap`.

`--rebuild` drops the window for one pass and rewinds the DLQ consumer to the
beginning. Use it after a long outage, or after deleting an index.

A projection failure is logged and the loop carries on — an outage costs
freshness, never a document. The container heartbeat beats once per pass,
failed pass included, so a *hung* projector goes unhealthy while one waiting
out an Elasticsearch restart does not.

The projector is read-only towards the pipeline: no lock, no PostgreSQL
write, and the only offsets it advances are its own, committed **after** the
records are indexed.

### What the panels actually measure

- **"Speed micro-batch duration"** is `completed_at - started_at` for one
  Spark micro-batch. It is **not** observation-to-change latency: the change
  contract carries no processed time and Phase 8 does not add one. End-to-end
  latency is a Phase 9 measurement (P2-01).
- **"Fetch latency"** is `latency_ms` of one crawl request — the fetch alone.
- **Freshness** on the source-health dashboard is `now - last observation`
  measured when the projector last ran, so it is at most one interval stale.
  Gold-level freshness and coverage stay in Superset
  (`cache.marketplace_offer_freshness`,
  `cache.marketplace_source_coverage_daily`).
- A **public counter change** is a change in a number the marketplace
  displays. It is not a sale and not a demand signal.

### Recreating `marketplace-changes-v1` and `marketplace-offers-current-v1`

An index template applies only to an index created after it. Both of these
were mapped dynamically before Phase 8, which made `change_type` and
`availability` `text` fields — so every terms aggregation over them failed
with *"Fielddata is disabled"*, and the two dashboard panels built on them
were empty. Recreating them is a deliberate, one-off replay:

```powershell
docker compose --profile speed stop speed
docker exec elasticsearch curl -s -X DELETE `
  'http://localhost:9200/marketplace-changes-v1,marketplace-offers-current-v1'
# .env: MARKETPLACE_STREAM_CHECKPOINT_VERSION=v2  (any new value)
docker compose --profile speed up -d speed
```

A new checkpoint version starts the query from `earliest`, and the
deterministic `_id`s rebuild the same documents. Redis is rebuilt by the same
replay. This is drill D5's replay variant, run once on purpose.

It works only because a speed batch is keyed by
`(query_name, query_id, batch_id)` — batch IDs restart at 0 under a new
checkpoint, and the old `(query_name, batch_id)` key made the audit skip every
replayed batch as already `SUCCEEDED` (fixed in WP2).

**Run for real on 2026-10-04.** Before: 1001 changes, 12 offers, `change_type`
mapped as `text`. After the replay: 1001 changes, 12 offers — the same
documents, not more and not fewer — with `change_type` and `availability` as
`keyword`, and `current_value` a keyword carrying a `numeric` sub-field
(`scaled_float`, `ignore_malformed`) so the price charts aggregate and the
`NEW_OFFER` JSON-text value is skipped rather than rejected.

### When Kibana shows "no data"

1. Is the index there at all?
   `docker exec elasticsearch curl -s 'http://localhost:9200/_cat/indices/marketplace-*?v'`
2. Is the projector running and recent?
   `docker logs es-projector --tail 5` — one `{"event": "projected", ...}` per
   interval.
3. Each dashboard has its own stored time range (24 hours for realtime, 7 days
   for source health). A stack idle for longer shows nothing, correctly.
4. A terms aggregation failing with *"Fielddata is disabled"* means the index
   predates its template. Recreate it, above.
## Backup and restore (plan section 12)

### What a backup holds, and what it does not

| Store | In the backup | Why |
|---|---|---|
| Bronze, all of it | yes | raw truth; nothing can recreate it |
| Silver, all of it | yes | recreatable only by replaying a Kafka topic whose retention does not keep it |
| `current.json` and the Gold manifests | yes | the single definition of the serving version |
| the Gold run `current.json` names | yes | a pointer that does not resolve after a restore is not a pointer |
| other Gold runs | no | rebuildable from Silver |
| PostgreSQL `audit` and `cache` | yes, `pg_dump -Fc` per schema | run history, quality evidence, crawl audit, frontier |
| Kafka, Elasticsearch, Redis, speed checkpoints | **no** | derived or transient; they refill from new crawls |
| adapter versions and fixtures | already in git | Brief section 20 |

### Taking one

```powershell
.\scripts\mp.ps1 backup
# or, inside the ops container:
docker compose --profile ops run --rm --no-deps ops python -m ops backup
docker compose --profile ops run --rm --no-deps ops python -m ops backups   # list
```

It lands in `data/ops/backups/bk-<UTC timestamp>/`, which both Compose
projects can see because `./data/ops` is bind-mounted at `/reports`.

The backup holds the **batch advisory lock** for its whole duration. Not to
freeze Bronze and Silver — they are append-only, so a copy taken seconds early
is older, never torn — but so no batch can promote a new pointer between
reading `current.json` and dumping the cache that must agree with it. That is
also why `pg_dump` runs inside the ops container rather than from the host: a
session lock belongs to the session that took it.

`pg_dump` and `pg_restore` are **pinned to major 18** in
`docker/marketplace-python/Dockerfile`, matching the pinned `postgres:18.3`.
pg_dump refuses a server newer than itself, and Debian's own client is 17.
Change the two pins together.

**A backup is refused, before anything is copied, when the pointer's `run_id`
and `audit.marketplace_cache_version.run_id` differ.** A backup of the two
disagreeing restores into a state `validate` rejects, so it is not worth the
bytes. Both run IDs go into `backup-manifest.json`, with every copied object's
path, size and SHA-256, the per-zone counts, and the tool versions.

### Restoring

Restore into a **separate Compose project with fresh volumes**. Never over the
running stack: `mp restore` refuses the current project by name, and refuses
to start at all while any project is up, because the fixed `container_name`s
would collide.

```powershell
.\scripts\mp.ps1 down                       # the fixed container names are global
.\scripts\mp.ps1 restore -BackupId bk-20261003T192610Z -Project mp-restore
```

What it does, in this order:

1. brings up the core of `-Project` and migrates it;
2. **verifies every SHA-256 before writing anything.** One changed byte, one
   missing file, and nothing at all is written — a half-restored stack is
   worse than an untouched one, because it looks restored;
3. `pg_restore`s `audit` then `cache`;
4. writes every lake object, with **`current.json` last**. A pointer that
   arrives before the data it names is a window in which the stack is
   confidently serving nothing;
5. runs three checks: the restored pointer equals the restored cache version;
   every dataset the pointer names exists with its manifest row count;
   `crawler.reparse` on a deterministic sample of restored raw artifacts
   returns `IDENTICAL`;
6. runs a **quality-only batch** at the pointer's `as_of` under a new run ID,
   over the restored Silver and audit. Its row counts are reported next to
   the pointer's but **not asserted equal**: the `as_of` cut excludes
   observations made after the pointer's window, yet a row observed inside it
   that landed late in Silver legitimately changes a rebuild (`PHASE_7`
   section 13). An exact-count check would fail on correct data;
7. runs `ops validate --restored`.

Then remove it:

```powershell
docker compose -f docker-compose.yml -p mp-restore --profile * down --volumes
```

### Why `validate --restored` exists

Three `validate` checks must fail on a freshly restored stack, and only these
three:

| Check | Why it fails |
|---|---|
| `es_changes_unique` | Elasticsearch is not backed up; nothing has streamed yet |
| `redis_offer_state_present` | Redis is not backed up, same reason |
| `kafka_to_silver_lag` | the broker is new, so the Silver consumer group has never existed; asking for its offsets raises `GroupCoordinatorNotAvailableError` |

`--restored` tolerates exactly those and fails on a fourth. Plain `validate`
on a restored stack reports `passed: false`, correctly — the stack really is
missing its derived stores until it crawls again.

### Which raw artifacts the reparse check samples

Only attempts with `error_kind IS NULL AND parsed_count > 0`.

An attempt that parsed nothing into the pipeline has a raw body in Bronze and
no Silver row **by design** — drill D2 makes exactly that, with Kafka down —
so reparsing it reports `NEW_OBSERVATIONS`, correctly. And `parsed_count` is
the *acknowledged* count, so a partial publish also leaves observations Silver
will never have; the audit cannot tell a partial publish from a complete one
by count alone, but a partial one is always `FAILED` with an `error_kind`.
Sampling either would fail a perfectly faithful restore — which is how this
was found, on 2026-10-04.

## Image pins (plan section 5.3)

Checked 2026-10-02 with `docker manifest inspect <image>`:

| Image | Pin | Pullable? |
|---|---|---|
| `minio/minio` | `@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e` (RELEASE.2025-09-07) | **No.** Every Docker Hub and quay.io tag, `latest` included, answers `denied: requested access to the resource is denied` |
| `minio/mc` | `@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727` | **No**, as above |
| `elasticsearch` | `8.18.1` | yes |
| `kibana` | `8.18.1` | yes |
| `python` | `3.12.11-slim` | yes |
| `apache/spark` | `4.0.1` | yes |

MinIO stopped publishing pullable images, so both MinIO images are pinned by
the digest of the copies cached on the development machine, with
`pull_policy: missing`. A machine without them must load them from another
machine:

```powershell
docker save minio/minio@sha256:14cea4... minio/mc@sha256:a7fe34... -o minio-images.tar   # on a machine that has them
docker load -i minio-images.tar
```

Elasticsearch and Kibana are on 8.18.1 rather than 8.18.0 because the 8.18.0
image on the development machine was pulled while the disk was full and
stayed corrupt across re-pulls (`PROGRESS.md` §13.4).

The jar versions in `docker/spark-marketplace/Dockerfile` and their sources
are listed in that file.
