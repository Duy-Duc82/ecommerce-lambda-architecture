# Marketplace Price Intelligence: pipeline ELT đa nguồn

Nền tảng dữ liệu theo dõi **trạng thái công khai của catalog và giá** trên sàn thương mại điện tử Việt Nam (Tiki, Lazada). Một lần crawl nuôi hai đường: **batch** (Spark, Airflow, chạy lại được, có phiên bản, qua cổng chất lượng) và **speed** (Spark Structured Streaming, sự kiện thay đổi giá trong vài giây).

Hệ thống chỉ thấy trạng thái công khai mà trang hiển thị: giá, giá niêm yết, tình trạng hàng, đánh giá và các bộ đếm. Không có đơn hàng, giỏ hàng hay người dùng; bộ đếm "đã bán" đổi giá trị không phải là một giao dịch.

Nhật ký công việc: [`docs/PROGRESS.md`](docs/PROGRESS.md). Kết quả refactor kiến trúc: [`docs/REFACTOR_REPORT.md`](docs/REFACTOR_REPORT.md).

## Luồng dữ liệu

```text
Tiki, Lazada (trang công khai, robots.txt, ≥ 2 s/request, một tab trình duyệt)
   -> ingestion: Fetcher -> Extractor           giữ mọi trường của dòng nguồn (ELT)
   -> Bronze/raw (phản hồi nguyên vẹn) + Bronze/records (JSONL.gz theo trang)
   -> Kafka marketplace.records.v1
        |-- speed: normalizer -> trạng thái theo offer -> change event -> Elasticsearch, Redis
        '-- batch (Airflow @hourly): silver -> dimensions -> marts -> quality -> publish -> promote
                                     Silver v2 Parquet   Gold theo run   PostgreSQL cache.*   Superset
```

Sơ đồ đầy đủ, bảng module, pattern và luật phụ thuộc nằm ở [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). Quyết định kiến trúc nằm ở [`docs/adr/`](docs/adr/).

## Cấu trúc mã

```text
src/ecomdata/
  domain/          thuần, không I/O: registry nguồn, record envelope, luật thay đổi, ordering
  settings/        cấu hình theo concern, đọc ở composition root
  infra/           adapter: Spark, Postgres, Kafka, object store, HTTP, trình duyệt
  ingestion/       thu thập: fetcher, extractor và discovery theo site, frontier, Bronze
  processing/      normalize, batch theo stage, speed
  orchestration/   DAG Airflow (marketplace_batch, marketplace_maintenance, source_discovery)
  serving/         projector Elasticsearch, Kibana, Superset
  ops/             CLI vận hành: smoke, validate, backup, bench, drill, evaluate
compose/  docker/  env/  scripts/  observability/  services/  tests/
legacy/kaggle/     pipeline Kaggle cũ, độc lập
```

`ingestion` và `processing` không import lẫn nhau; `domain` không import thư viện hạ tầng. `import-linter` (4 contract) và `tests/architecture/` kiểm các luật này.

## Chạy

Cần Docker Desktop, PowerShell và Python 3.12.

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[dev,spark]"

# Một stack cô lập (project mp-refactor, prefix rf-), nguồn thật:
.\scripts\mp.ps1 up -EnvFile env/refactor.env -With ops,crawl,crawl-browser
.\scripts\mp.ps1 migrate -EnvFile env/refactor.env
.\scripts\mp.ps1 seed -EnvFile env/refactor.env --source tiki --from-discovery --limit 25 --pages 1 --tier ACTIVE
.\scripts\mp.ps1 up -EnvFile env/refactor.env -With speed,orchestration,serve

# Smoke offline (stub, không chạm site thật). Chưa chạy lại sau refactor, xem REFACTOR_REPORT:
.\scripts\mp.ps1 smoke -EnvFile env/bench.env
```

Cấu hình ghép lớp: `env/base.env`, rồi `env/ports.<stack>.env`, rồi `env/<stack>.env`. Mỗi stack có project, prefix container, volume và cổng riêng (`tests/unit/ops/test_compose_isolation.py`). Airflow ở `http://localhost:<AIRFLOW_HOST_PORT>`, Superset ở `<SUPERSET_HOST_PORT>`; các cổng nằm trong `env/ports.<stack>.env`.

Chạy một stage của batch bằng tay:

```powershell
python -m ecomdata.processing.batch run --run-id <id> --as-of <ISO> --stage silver|dimensions|marts|quality|publish|promote   # hoặc --all
```

## Kiểm thử

```powershell
.\.venv\Scripts\python -m pytest -q -p no:cacheprovider      # toàn bộ, trừ drill và integration (khoảng 25 phút)
.\.venv\Scripts\python -m ruff check src tests services
.\.venv\Scripts\lint-imports.exe
```

Test Spark trên Windows truyền dữ liệu trong bộ nhớ (không có `winutils`); Parquet và MinIO chỉ chạy được trong Docker. Test `integration` chạm mạng (Lazada thật) và mặc định bị bỏ chọn; drill cần stack chạy (`-m drill`).

## Công nghệ

Kafka 4.2 (KRaft), Spark 4.0 (Python 3.12), MinIO (S3A), PostgreSQL 18, Elasticsearch và Kibana 8.18, Redis, Superset 4.1, Airflow 3.3, Playwright 1.55 (Lazada). Image: `ecommerce/marketplace-python:2`, `ecommerce/spark-marketplace:4.0.1-r3`, `ecommerce/crawler-browser:1`, `ecommerce/airflow:3.3.2-r1`.

## Tài liệu

| Tài liệu | Nội dung |
|---|---|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | thiết kế chi tiết, luật phụ thuộc, cách thêm một nguồn |
| [`docs/adr/`](docs/adr/) | các quyết định kiến trúc |
| [`docs/DATA_MODEL.md`](docs/DATA_MODEL.md) | hợp đồng dữ liệu, bảng, khoá |
| [`docs/RUNBOOK.md`](docs/RUNBOOK.md) | vận hành: seed, backfill, drill, backup |
| [`docs/DEMO_RUNBOOK.md`](docs/DEMO_RUNBOOK.md) | chạy và trình diễn stack dài hạn |
| [`docs/evidence/refactor-20261025/`](docs/evidence/refactor-20261025/) | bằng chứng chạy thật trên Tiki và Lazada |
| [`docs/archive/`](docs/archive/) | plan các phase cũ |

Shopee không được crawl; hướng hợp lệ duy nhất là Affiliate Open API (ADR-0006).
