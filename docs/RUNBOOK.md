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
