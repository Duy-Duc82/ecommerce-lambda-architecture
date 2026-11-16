# 1. Tổng quan hệ thống

> Đúng với: commit `9c73660` · Cập nhật: 2026-11-16 · Nguồn chân lý: `src/ecomdata/`, `compose/docker-compose.yml`, `databricks.yml`, `databricks/resources/jobs.yml`

## 1.1 Bài toán và phạm vi

Hệ thống quan sát **trạng thái công khai của catalog và giá** trên sàn thương mại điện tử Việt Nam (Tiki là nguồn chính, Lazada là nguồn minh hoạ đa nguồn). Dữ liệu nó có là những gì sàn hiển thị cho một khách vãng lai trên trang danh mục: tiêu đề, giá, giá niêm yết, mức giảm, tình trạng còn hàng, điểm đánh giá, các bộ đếm công khai (đã bán, số đánh giá) và vị trí xếp hạng.

Hệ thống **không** có đơn hàng, thanh toán, giỏ hàng hay người dùng, và không mô hình nào giả lập chúng. Bộ đếm `sold_count` tăng không có nghĩa là một đơn hàng: mọi bảng chạm tới counter đều giữ cả delta thô lẫn phán quyết hợp lệ.

Ba câu hỏi nghiệp vụ hệ thống trả lời:

1. Giá của một offer thay đổi thế nào theo thời gian? Thay đổi nào bất thường so với lịch sử của chính nó? (mart `offer_price_history_daily`, `price_anomaly_daily`)
2. Mức giảm giá sàn hiển thị có đáng tin không, so với giá trước đó của chính offer? (mart `discount_credibility_daily`, ADR-0009)
3. Khi một listing thay đổi (giá, tồn kho, đánh giá), bao lâu thì thấy được? Độ phủ và độ tươi của mỗi nguồn ra sao? (speed layer, mart `source_coverage_daily`, `offer_freshness`)

## 1.2 Kiến trúc tổng thể

Hệ thống theo kiến trúc **Lambda**, thu thập theo kiểu **ELT** (ADR-0001). Crawler lưu nguyên vẹn phản hồi và từng dòng nguồn vào Bronze; mọi chuẩn hoá làm sau, trong Spark. Từ cùng một Bronze có hai đường:

- **Batch** (đúng, chạy lại được, có phiên bản): mỗi giờ dựng Silver, star `dw`, 12 mart, qua cổng chất lượng rồi publish vào PostgreSQL.
- **Speed** (nhanh): Kafka → Spark Structured Streaming → phát hiện 7 loại thay đổi theo offer → Kafka, Elasticsearch, Redis.

**Hình 1.1 — Toàn cảnh kiến trúc Lambda**

> Loại hình: flowchart trái → phải. Ý chính người xem phải thấy: Bronze được ghi trước và là điểm rẽ nhánh; speed đọc bản sao trên Kafka, batch đọc lại Bronze; chỉ run qua cổng chất lượng mới tới PostgreSQL và Superset.

Khung (nhóm):

| Khung | Màu tầng | Chứa node |
|---|---|---|
| Nguồn ngoài | nguồn | 1, 2 |
| Thu thập | thu thập | 3, 4 |
| Bronze (MinIO/S3) — nguồn sự thật | lake | 5, 6, 7 |
| Speed layer | speed (topic tô màu Kafka) | 8–12 |
| Batch layer (mỗi giờ) | batch (kho tô màu lake) | 13–17 |
| Phục vụ | phục vụ | 18–22 |
| (đứng riêng) | điều phối, giám sát | 23, 24 |

Node:

| # | Nhãn trên hình | Hình dạng | Tầng/màu | Ghi chú |
|---|---|---|---|---|
| 1 | Tiki — HTTP, `__NEXT_DATA__` | chữ nhật | nguồn | nguồn chính |
| 2 | Lazada — Playwright, XHR JSON | chữ nhật | nguồn | nguồn minh hoạ |
| 3 | PostgreSQL `audit.crawl_frontier` | trụ | thu thập | hàng đợi việc crawl |
| 4 | crawl-worker (Fetcher → Extractor) | bo góc | thu thập | |
| 5 | raw — `part-N.bin.gz` + `metadata.json` | trụ | lake | |
| 6 | records — JSONL.gz, `marketplace-record.v1` | trụ | lake | điểm rẽ nhánh batch/speed |
| 7 | crawl_audit — JSONL.gz | trụ | lake | |
| 8 | `marketplace.records.v1` | lục giác | Kafka | |
| 9 | normalizer (foreachBatch) | bo góc | speed | |
| 10 | `marketplace.observations.v1` | lục giác | Kafka | |
| 11 | stateful query — 7 loại thay đổi | bo góc | speed | |
| 12 | `marketplace.changes.v1` | lục giác | Kafka | |
| 13 | Silver (Delta) | trụ | lake | |
| 14 | Gold `dw` — star: 8 dim/bridge, 6 fact | trụ | lake | |
| 15 | Gold runs — 12 mart theo run | trụ | lake | |
| 16 | quality gate — 22 luật | thoi | batch | |
| 17 | `current.json` | trụ | lake | pointer tới run đang phục vụ |
| 18 | PostgreSQL `cache.*` | trụ | phục vụ | |
| 19 | Elasticsearch | trụ | phục vụ | |
| 20 | Redis `rt:*` | trụ | phục vụ | |
| 21 | Superset | chữ nhật | phục vụ | |
| 22 | Kibana | chữ nhật | phục vụ | |
| 23 | Airflow — 4 DAG | chữ nhật | điều phối | |
| 24 | Alloy · Prometheus · Loki · Grafana | chữ nhật | giám sát | |

Cạnh:

| Từ | Tới | Nhãn trên cạnh | Nét |
|---|---|---|---|
| 1 Tiki | 4 crawl-worker | | liền |
| 2 Lazada | 4 crawl-worker | | liền |
| 3 frontier | 4 crawl-worker | lease | đứt |
| 4 crawl-worker | 5 raw | | đậm |
| 4 crawl-worker | 6 records | | đậm |
| 4 crawl-worker | 7 crawl_audit | | liền |
| 6 records | 8 `records.v1` | | đậm |
| 8 `records.v1` | 9 normalizer | | liền |
| 9 normalizer | 10 `observations.v1` | | liền |
| 10 `observations.v1` | 11 stateful query | | liền |
| 11 stateful query | 12 `changes.v1` | | liền |
| 11 stateful query | 19 Elasticsearch | | liền |
| 11 stateful query | 20 Redis | | liền |
| 6 records | 13 Silver | | đậm |
| 13 Silver | 14 `dw` | | đậm |
| 7 crawl_audit | 14 `dw` | | liền |
| 14 `dw` | 15 Gold runs | | đậm |
| 15 Gold runs | 16 quality gate | | đậm |
| 16 quality gate | 18 PostgreSQL `cache.*` | PASS | đậm |
| 16 quality gate | 17 `current.json` | promote | liền |
| 18 PostgreSQL `cache.*` | 21 Superset | | liền |
| 19 Elasticsearch | 22 Kibana | | liền |
| 23 Airflow | 13 Silver (hoặc cả khung Batch) | lập lịch | đứt |
| 23 Airflow | 3 frontier | seed SWEEP | đứt |
| 24 Grafana | 18 PostgreSQL | đọc audit, metrics | đứt |

*Hình 1.1. Toàn cảnh. Bronze được ghi trước và là nguồn sự thật. Kafka chỉ là bản sao nhanh của Bronze/records. Batch đọc lại Bronze theo cửa sổ ngày; PostgreSQL chỉ giữ bản cache của run đã được promote.*

## 1.3 Các tầng dữ liệu

| Tầng | Nơi lưu | Định dạng | Ai ghi | Chính sách ghi | Vai trò |
|---|---|---|---|---|---|
| Frontier, audit vận hành | PostgreSQL schema `audit` | bảng | crawl-worker, batch, speed | upsert, transaction | hàng đợi việc crawl, trạng thái run, kết quả quality, số đo vận hành (Grafana đọc) |
| **Bronze/raw** | bucket `ecommerce-bronze`, `marketplace/raw/…` | gzip nhị phân + `metadata.json` | crawl-worker | append-only, không ghi đè | phản hồi HTTP nguyên vẹn, có SHA-256 |
| **Bronze/records** | `marketplace/records/source=…/observed_date=…/hour=…/` | JSONL.gz | crawl-worker | idempotent theo đường dẫn | từng dòng nguồn với toàn bộ trường (trừ PII khai báo) |
| **Bronze/crawl_audit** | `marketplace/crawl_audit/…` | JSONL.gz | crawl-worker | một file mỗi crawl run | bản lưu của audit crawl (ADR-0008) |
| Kafka | 5 topic, 3 partition | JSON canonical | crawler, speed | at-least-once | đường nhanh; không phải bản lưu |
| **Silver** | `ecommerce-silver`, `marketplace/<bảng>/` | Delta, partition `source_code, observed_date` | stage `silver` | thay nguyên partition ngày | observation chuẩn hoá, bảng rộng theo nguồn, page context, quarantine |
| **Gold `dw`** (star) | `ecommerce-gold`, `marketplace/dw/<bảng>/` | Delta (bắt buộc) | stage `dimensions`, `facts` | `MERGE` theo khoá, giữ mọi version | mô hình chiều dùng chung cho mọi run |
| **Gold runs** | `marketplace/runs/run_id=<run>/<mart>/` | Delta (mặc định) | stage `marts` | một thư mục mỗi run, không ghi đè run khác | 12 mart của một run |
| Manifest, pointer | `marketplace/manifests/run_id=<run>/manifest.json`, `marketplace/current.json` | JSON canonical | `quality`, `promote` | pointer thay nguyên file, compare-and-swap | run nào đang được phục vụ |
| **Cache phục vụ** | PostgreSQL `cache.marketplace_*` (12 bảng) | bảng | stage `publish` | TRUNCATE + INSERT trong một transaction | Superset đọc |
| Speed serving | ES `marketplace-*-v1`, Redis `rt:*` | document, hash/zset | speed, es-projector | upsert theo id | Kibana, tra cứu thời gian thực |

Nguyên tắc lưu trữ (ADR-0008, memory dự án 2026-10-26): **lake là bản lưu, PostgreSQL chỉ là cache**. Xoá sạch `cache.*` thì dựng lại được từ Gold; xoá Gold thì dựng lại được từ Bronze.

## 1.4 Tầng module trong code

Code nằm trong một package `ecomdata` (`src/ecomdata/`), chia theo Ports & Adapters (ADR-0003). `import-linter` kiểm luật import trong CI.

**Hình 1.2 — Tầng module trong code**

> Loại hình: sơ đồ khối xếp chồng trên → dưới (5 tầng). Ý chính người xem phải thấy: tầng trên chỉ import tầng dưới; `ingestion` và `processing` nằm cùng tầng nhưng không import nhau.

Node (mỗi tầng là một khối ngang, tầng 1 trên cùng):

| # | Nhãn trên hình | Hình dạng | Tầng/màu | Ghi chú |
|---|---|---|---|---|
| 1 | Tầng 1: điều phối, vận hành, trình bày — 3 ô con: `orchestration` (DAG Airflow, `tasks.py`), `ops` (CLI vận hành, backup, drill, bench), `serving` (es_projector, Kibana, Superset) | khung chứa 3 chữ nhật | điều phối | |
| 2 | Tầng 2: ứng dụng (không import lẫn nhau) — 2 ô con: `ingestion` (fetcher, extractor, pipeline, frontier, worker), `processing` (normalize · batch · speed) | khung chứa 2 chữ nhật | thu thập | vẽ vạch ngăn giữa 2 ô để thể hiện "không import nhau" |
| 3 | Tầng 3: `infra` — Spark, Postgres, Kafka, object store, HTTP, Playwright | chữ nhật | batch | |
| 4 | Tầng 4: `settings` — dataclass frozen, `load_settings()` | chữ nhật | batch | |
| 5 | Tầng 5: `domain` (thuần, không I/O) — sources, records, observations, change_rules, quality_rules, discount_rules, datasets, batch_run | chữ nhật | lake | |

Cạnh:

| Từ | Tới | Nhãn trên cạnh | Nét |
|---|---|---|---|
| Tầng 1 | Tầng 2 | import | liền |
| Tầng 2 | Tầng 3 | import | liền |
| Tầng 3 | Tầng 4 | import | liền |
| Tầng 4 | Tầng 5 | import | liền |

*Hình 1.2. Tầng trên chỉ import tầng dưới. `ingestion` và `processing` không biết nhau: chúng chỉ gặp nhau qua hợp đồng dữ liệu trong `domain` (envelope `marketplace-record.v1`) và qua Bronze.*

Hai điểm khiến batch và speed cho cùng một kết quả:

- **Một normalizer theo nguồn** (`processing/normalize/sources/{tiki,lazada}.py` + `common.py`) dùng cho cả stage `silver` lẫn query normalizer của speed.
- **Một registry**: `domain/sources.py` (mã nguồn → `SourceSpec`), `domain/datasets.py` (bảng Gold, khoá, khoá ngoại), `domain/quality_rules.py`, `domain/change_rules.py`, `domain/discount_rules.py`.

## 1.5 Công nghệ

| Mảng | Công nghệ | Phiên bản (pin trong `constraints.txt`, image) |
|---|---|---|
| Ngôn ngữ | Python | 3.12 |
| Xử lý | Apache Spark (PySpark), Structured Streaming, Delta Lake | Spark 4.0.1 (image `ecommerce/spark-marketplace:4.0.1-r4`) |
| Thu thập | `requests` (Tiki), Playwright Chromium (Lazada) | image `ecommerce/crawler-browser:1` (Playwright v1.55.0) |
| Message bus | Apache Kafka, KRaft | 4.2.0 |
| Object store | MinIO (local), S3 (Databricks) | |
| Cơ sở dữ liệu | PostgreSQL | 18.3 |
| Speed serving | Elasticsearch, Kibana; Redis | 8.18.1; 8.6.2 |
| BI | Apache Superset | 4.1.1 |
| Điều phối | Apache Airflow (LocalExecutor, DockerOperator) | 3.3.2 |
| Giám sát | Grafana Alloy, Prometheus, Loki, Grafana | 1.10.0, 3.2.1, 3.4.2, 11.5.2 |
| ML | scikit-learn Isolation Forest (chỉ xếp hạng) | |
| Cloud | Databricks Asset Bundle, DBR 17.3 (Spark 4.0) | |

## 1.6 Triển khai

### Local: Docker Compose theo profile

Mỗi stack là một compose project riêng (`env/base.env` + `env/ports.<stack>.env` + `env/<stack>.env`), được điều khiển bằng `scripts/mp.ps1`. Chi tiết từng service ở [04-pipeline.md §4.9](04-pipeline.md#49-triển-khai-và-tài-nguyên).

**Hình 1.3 — Triển khai local: Docker Compose theo profile**

> Loại hình: flowchart trên → dưới, mỗi profile là một khung. Ý chính người xem phải thấy: lõi luôn chạy, mỗi profile bật thêm một nhóm service; batch được Airflow gọi bằng DockerOperator.

Khung (nhóm):

| Khung | Màu tầng | Chứa node |
|---|---|---|
| Lõi (luôn chạy) | lake | 1–5 |
| profile `crawl` / `crawl-browser` | thu thập | 6, 7 |
| profile `speed` | speed | 8 |
| profile `batch` / `orchestration` | batch, điều phối | 9, 10 |
| profile `serve` / `ops` | phục vụ | 11, 12, 13 |
| compose observability | giám sát | 14 |

Node:

| # | Nhãn trên hình | Hình dạng | Tầng/màu | Ghi chú |
|---|---|---|---|---|
| 1 | kafka :9092 | trụ | lake | |
| 2 | minio :9000 | trụ | lake | |
| 3 | postgres-dw :5433 | trụ | lake | |
| 4 | redis :6379 | trụ | lake | |
| 5 | elasticsearch :9200 | trụ | lake | |
| 6 | crawl-worker (Tiki) | bo góc | thu thập | |
| 7 | crawl-worker-lazada | bo góc | thu thập | |
| 8 | speed — 2 streaming query | bo góc | speed | |
| 9 | batch-once (công cụ) | bo góc | batch | |
| 10 | airflow-scheduler, api-server, dag-processor | bo góc | điều phối | |
| 11 | kibana :5601 | bo góc | phục vụ | |
| 12 | superset :8088 | bo góc | phục vụ | |
| 13 | es-projector | bo góc | phục vụ | |
| 14 | alloy · prometheus · loki · grafana :3001 | bo góc | giám sát | |

Cạnh:

| Từ | Tới | Nhãn trên cạnh | Nét |
|---|---|---|---|
| 6, 7 crawl-worker | 2 minio | | liền |
| 6, 7 crawl-worker | 1 kafka | | liền |
| 6, 7 crawl-worker | 3 postgres-dw | | liền |
| 8 speed | 1 kafka | | liền |
| 8 speed | 5 elasticsearch | | liền |
| 8 speed | 4 redis | | liền |
| 8 speed | 3 postgres-dw | | liền |
| 10 airflow | 9 batch-once | DockerOperator | đứt |
| 9 batch-once | 2 minio | | liền |
| 9 batch-once | 3 postgres-dw | | liền |
| 13 es-projector | 3 postgres-dw | | liền |
| 13 es-projector | 5 elasticsearch | | liền |
| 13 es-projector | 4 redis | | liền |
| 11 kibana | 5 elasticsearch | | liền |
| 12 superset | 3 postgres-dw | | liền |
| 14 observability | khung Lõi | scrape | đứt |

*Hình 1.3. Mỗi stack là một compose project riêng, điều khiển bằng `scripts/mp.ps1`.*

### Hybrid: batch và speed lakehouse trên Databricks

Từ 2026-11-05 (nhánh `phase-11/databricks`) phần tính toán chạy được trên Databricks; phần dính tới mạng nội bộ ở lại máy local.

**Hình 1.4 — Triển khai hybrid: batch và speed lakehouse trên Databricks**

> Loại hình: flowchart trái → phải, 3 khung theo nơi chạy. Ý chính người xem phải thấy: Bronze lên S3 bằng `mc mirror` chạy tay; Databricks tính trên S3; kết quả chưa tự động quay về PostgreSQL local.

Khung (nhóm):

| Khung | Màu tầng | Chứa node |
|---|---|---|
| Máy local (Docker) | theo từng node | 1–5 |
| AWS S3 (ap-southeast-1) | lake | 6, 7, 8 |
| Databricks (single node, DBR 17.3) | batch, speed | 9, 10 |

Node:

| # | Nhãn trên hình | Hình dạng | Tầng/màu | Ghi chú |
|---|---|---|---|---|
| 1 | crawler + frontier | bo góc | nguồn | |
| 2 | MinIO bronze | trụ | lake | |
| 3 | Kafka · speed Kafka | trụ | nguồn | |
| 4 | PostgreSQL `cache.*` | trụ | phục vụ | |
| 5 | Superset · Kibana | chữ nhật | phục vụ | |
| 6 | bronze: records, crawl_audit | trụ | lake | |
| 7 | silver Delta | trụ | lake | |
| 8 | gold: dw, runs, manifests, speed/* | trụ | lake | |
| 9 | `ecomdata-marketplace-batch` — silver→quality, `--skip-postgres` | bo góc | batch | |
| 10 | `ecomdata-marketplace-speed` — Auto Loader, available-now | bo góc | speed | |

Cạnh:

| Từ | Tới | Nhãn trên cạnh | Nét |
|---|---|---|---|
| 1 crawler | 2 MinIO | | liền |
| 2 MinIO | 6 S3 bronze | `mc mirror` (tay) | liền |
| 6 S3 bronze | 9 job batch | | liền |
| 9 job batch | 7 silver | | liền |
| 9 job batch | 8 gold | | liền |
| 6 S3 bronze | 10 job speed | | liền |
| 10 job speed | 8 gold | | liền |
| 8 gold | 4 PostgreSQL `cache.*` | publish chưa tự động | đứt |
| 4 PostgreSQL `cache.*` | 5 Superset · Kibana | | liền |

*Hình 1.4. Job batch trên Databricks dừng ở `quality` vì `publish` và `promote` cần PostgreSQL. Job speed chạy available-now hoặc trong một cửa sổ demo, không chạy 24/7 (`databricks/resources/jobs.yml`).*

## 1.7 Nguyên tắc thiết kế

| Nguyên tắc | Thực hiện ở đâu | Hệ quả |
|---|---|---|
| **Bronze là sự thật, raw-first** | `ingestion/pipeline.py`: ghi raw trước khi kiểm HTTP status, trước khi parse | Trang lỗi 4xx/5xx vẫn còn bằng chứng; parser đổi thì `reparse` dựng lại records |
| **ID tất định** | `domain/identity.py` (`deterministic_id` = SHA-256 các phần nối bằng `\x1f`) | Ghi lại, replay Kafka, chạy lại batch đều ra cùng byte; không cần exactly-once |
| **Publish Kafka sau khi file đã ghi** | `ingestion/service.py:388-411` | Kafka không bao giờ có record mà Bronze không có |
| **Một normalizer cho batch và speed** | `processing/normalize/` | Hai đường không lệch định nghĩa |
| **Cổng chất lượng chặn publish** | `processing/batch/quality.py::decide` | Run lỗi không bao giờ tới Superset; cache giữ bản tốt cuối cùng |
| **Star đã chứng nhận** (ADR-0007) | stage `quality`: `RESTORE TABLE … VERSION AS OF` | Run bị từ chối không để lại star bẩn cho run sau |
| **Publish trước promote** (ADR-0005) | stage `publish` → `promote` | `current.json` không bao giờ trỏ vào Gold mà cache chưa có |
| **Luật trước, ML chỉ xếp hạng** (ADR-0009) | `processing/batch/discount.py` | Phán quyết đọc được lý do; Isolation Forest chỉ để sắp thứ tự xem tay |

## 1.8 Quy mô đã đo

| Số đo | Giá trị | Ngày, nguồn |
|---|---|---|
| Lượt chạy liên tục dài nhất | 9 giờ 16 phút, 10 run batch, 10/10 PROMOTED, quality PASS | 13–14/11/2026, `docs/evidence/fullrun-20261114/KET-QUA.md` |
| Dòng Gold mỗi run | 1.432.017 → 1.966.878 | như trên |
| Thời gian stage nặng nhất (`dimensions`) | khoảng 217–259 s; `marts` 88–105 s | như trên |
| Độ trễ speed p95 | khoảng 59–62 s sau khi đuổi kịp tồn đọng (trigger 30 s) | như trên |
| Task SWEEP thành công một lượt | 14.023 | như trên |
| Micro-batch speed | 6.429, 0 thất bại; p50 8,8 s với trigger 5 s | 31/10/2026, `docs/DANH_GIA_HE_THONG_20261113.md` |
| Tăng trưởng Bronze | 385 MB raw + 74 MB records trong 6 giờ crawl Tiki | 31/10/2026, như trên |
