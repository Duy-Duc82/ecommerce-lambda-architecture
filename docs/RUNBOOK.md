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
| `serve` | kibana, superset, superset-init |
| `legacy` | spark, spark-worker, kibana, kibana-setup |
| `jobs` | warehouse-job (legacy; `scripts/run_warehouse.ps1`) |
| `ops` | ops (a `run --rm` tool container) |
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

### Drills D1-D6 (plan section 10)

A drill injects one fault into the **running** stack, asserts what the system
does while faulted, removes the fault and checks the system came back. It runs
on the host, not in the `ops` container, because it drives Docker:

```powershell
$env:COMPOSE_PROJECT_NAME = "mp-smoke"
.\scripts\mp.ps1 smoke            # a drill starts from a passing validate
.\scripts\mp.ps1 drill d1         # then d2 ... d6, one at a time
.\scripts\mp.ps1 drill all        # or all six in order
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
  drill, pass or fail.
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
