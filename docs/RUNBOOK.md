# Runbook — marketplace stack

> Phase 8. This file grows with each work package; WP10 completes it
> (plan section 13). Until then it holds what a WP had to record.

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

### Kafka lost its topics

Kafka keeps its log on the `kafka_data` volume, so recreating the container
keeps every topic. If the volume itself is lost, the speed query refuses to
start (`Some data may have been lost`), because its checkpoint holds offsets
the new topics never had. The changes it projected are rebuildable, so:

```powershell
docker compose -f docker-compose.yml --profile speed rm -sf speed
docker volume rm ecommerce-lambda-architecture_speed_checkpoints
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
