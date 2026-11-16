# 4. Các pipeline: tổng quan và chi tiết

> Đúng với: commit `9c73660` · Cập nhật: 2026-11-16 · Nguồn chân lý: `src/ecomdata/orchestration/`, `src/ecomdata/ingestion/service.py`, `src/ecomdata/processing/{batch,speed}/`, `compose/docker-compose.yml`, `databricks/resources/jobs.yml`, `scripts/mp.ps1`

## 4.1 Danh mục pipeline

| Mã | Pipeline | Kiểu | Kích hoạt | Chạy ở đâu | Đầu vào → đầu ra | Entrypoint |
|---|---|---|---|---|---|---|
| P1 | Thu thập | tiến trình dài hạn | liên tục, theo `scheduled_for` trong frontier | container `crawl-worker`, `crawl-worker-lazada` | site → Bronze, Kafka `records.v1`, `audit.crawl_*` | `python -m ecomdata.ingestion.service --sources tiki` |
| P2 | Discovery, seed | theo lịch | DAG `source_discovery`, `@daily` | DockerOperator (image python) | sitemap/`categories.json` → `audit.crawl_frontier` (tier SWEEP) | `python -m ecomdata.ingestion.seed --from-discovery` |
| P3 | Speed (Kafka) | streaming | trigger 30 s | container `speed` | `records.v1` → `observations.v1` → changes, ES, Redis | `python -m ecomdata.processing.speed.service` |
| P4 | Speed lakehouse | streaming, available-now | job Databricks (lịch 15 phút, đang PAUSED) hoặc chạy tay | Databricks | Bronze/records → Delta `gold/speed/*` | `ecomdata-speed-lakehouse --available-now --auto-loader` |
| P5 | Batch theo giờ | theo lịch | DAG `marketplace_batch`, `@hourly` | DockerOperator (image Spark), 8 task | Bronze → Silver → `dw` → mart → `cache.*` → `current.json` | `python -m ecomdata.processing.batch run --stage …` |
| P6 | Reprocess | chạy tay | DAG `marketplace_reprocess` (không lịch) | như P5 | như P5, cửa sổ `--silver-since` hoặc `--rebuild` | như P5 |
| P7 | Batch trên Databricks | chạy tay, có tham số | job `ecomdata-marketplace-batch` | Databricks | S3 Bronze → Silver, `dw`, mart, quality (không publish) | `ecomdata-batch run … --skip-postgres` |
| P8 | Bảo trì | theo lịch | DAG `marketplace_maintenance`, `@weekly` | DockerOperator (image python) | prune raw, backup, đo dung lượng | `python -m ecomdata.ops prune-raw / backup / storage-snapshot` |
| P9 | Chiếu audit sang ES | tiến trình dài hạn | vòng lặp 30 s | container `es-projector` | `audit.*`, DLQ, Redis → ES `marketplace-*-v1` | `python -m ecomdata.serving.es_projector` |

**Hình 4.1 — Bản đồ pipeline**

> Loại hình: flowchart trái → phải. Ý chính người xem phải thấy: 9 pipeline P1–P9 đọc/ghi những kho nào, và ai khởi động chúng (Airflow, Databricks Jobs hoặc tự chạy liên tục).

Node:

| # | Nhãn trên hình | Hình dạng | Tầng/màu | Ghi chú |
|---|---|---|---|---|
| 1 | Airflow | chữ nhật | điều phối | bộ lập lịch |
| 2 | Databricks Jobs | chữ nhật | điều phối | bộ lập lịch cloud |
| 3 | P2 discovery · @daily | chữ nhật bo góc | thu thập | |
| 4 | P1 thu thập · liên tục | chữ nhật bo góc | thu thập | |
| 5 | P3 speed Kafka · 30 s | chữ nhật bo góc | speed | |
| 6 | P4 speed lakehouse · available-now | chữ nhật bo góc | speed | |
| 7 | P5 batch · @hourly | chữ nhật bo góc | batch | |
| 8 | P6 reprocess · tay | chữ nhật bo góc | batch | |
| 9 | P7 batch Databricks · tay | chữ nhật bo góc | batch | |
| 10 | P8 bảo trì · @weekly | chữ nhật bo góc | điều phối | |
| 11 | P9 es-projector · 30 s | chữ nhật bo góc | phục vụ | |
| 12 | frontier | trụ | thu thập | `audit.crawl_frontier` |
| 13 | Bronze | trụ | lake | |
| 14 | Kafka | lục giác | Kafka | |
| 15 | Silver · dw · runs | trụ | lake | |
| 16 | cache.\* · audit.\* | trụ | phục vụ | PostgreSQL |
| 17 | ES · Redis | trụ | phục vụ | |
| 18 | S3 lake | trụ | lake | |

Cạnh:

| Từ | Tới | Nhãn trên cạnh | Nét |
|---|---|---|---|
| Airflow | P2 | | đứt |
| Airflow | P5 | | đứt |
| Airflow | P6 | | đứt |
| Airflow | P8 | | đứt |
| Databricks Jobs | P4 | | đứt |
| Databricks Jobs | P7 | | đứt |
| P2 | frontier | | liền |
| frontier | P1 | | liền |
| P1 | Bronze | | liền |
| P1 | Kafka | | liền |
| P1 | cache.\* · audit.\* | | liền |
| Kafka | P3 | | liền |
| P3 | ES · Redis | | liền |
| P3 | cache.\* · audit.\* | | liền |
| Bronze | P5 | | liền |
| Bronze | P6 | | liền |
| P5 | Silver · dw · runs | | liền |
| P6 | Silver · dw · runs | | liền |
| P5 | cache.\* · audit.\* | | liền |
| P6 | cache.\* · audit.\* | | liền |
| Bronze | S3 lake | mc mirror | đứt |
| S3 lake | P4 | | liền |
| S3 lake | P7 | | liền |
| P4 | S3 lake | | liền |
| P7 | S3 lake | | liền |
| P8 | Bronze | | liền |
| P8 | Silver · dw · runs | backup | đứt |
| cache.\* · audit.\* | P9 | | liền |
| P9 | ES · Redis | | liền |

*Hình 4.1. Bản đồ pipeline. Nét đứt là điều khiển (lập lịch). P1, P3, P9 là tiến trình dài hạn; P2, P5, P6, P8 do Airflow khởi động; P4, P7 do Databricks khởi động.*

### Một giờ vận hành điển hình

**Hình 4.2 — Một giờ vận hành điển hình**

> Loại hình: Gantt, trục ngang là giờ UTC từ 10:00 tới 11:00, stack local đầy đủ. Ý chính người xem phải thấy: P1, P3, P9 chạy suốt giờ; run batch `mp-…T1000Z` chờ 10 phút rồi chạy tuần tự các stage và xong trong khoảng nửa đầu giờ.

| Làn (section) | Bước | Bắt đầu | Thời lượng | Phụ thuộc | Ghi chú vẽ |
|---|---|---|---|---|---|
| P1 thu thập | ACTIVE (25 trang/giờ) + SWEEP đến hạn | 10:00 | 60 phút | | thanh liên tục |
| P3 speed | micro-batch mỗi 30 s | 10:00 | 60 phút | | thanh liên tục |
| P5 batch (run `mp-…T1000Z`) | window_settled (chờ 10 phút) | 10:00 | 10 phút | | tô nổi bật (đường găng) |
| P5 batch | silver | 10:10 | 2 phút | window_settled | |
| P5 batch | dimensions | 10:12 | 4 phút | silver | |
| P5 batch | facts | 10:16 | 2 phút | dimensions | |
| P5 batch | marts | 10:18 | 2 phút | facts | |
| P5 batch | quality → publish → promote | 10:20 | 3 phút | marts | |
| P9 projector | vòng 30 s | 10:00 | 60 phút | | thanh liên tục |

*Hình 4.2. Thời lượng từng stage chỉ là minh hoạ theo bậc độ lớn đo ngày 13–14/11: `dimensions` 217–259 s, `marts` 88–105 s, `silver` 38–189 s (`docs/evidence/fullrun-20261114/KET-QUA.md`). `max_active_runs = 1` nên các run không chồng lên nhau.*

## 4.2 P1 — Thu thập (crawl)

**Mục đích.** Biến một task của frontier thành bằng chứng trong Bronze và một luồng record trên Kafka.

**Hình 4.3 — Một lượt thu thập (P1)**

> Loại hình: sequence, các cột (lifeline) xếp trái → phải theo bảng dưới. Ý chính người xem phải thấy: raw được lưu trước khi kiểm HTTP status, rồi tới records, rồi mới gửi Kafka; kết quả cuối cùng quyết định trạng thái task trong frontier.

Cột (lifeline), theo thứ tự trái → phải:

| Ký hiệu | Nhãn | Tầng/màu |
|---|---|---|
| F | PostgreSQL `audit.crawl_frontier` | thu thập |
| W | crawl-worker | thu thập |
| S | Site (Tiki/Lazada) | nguồn |
| R | Bronze/raw | lake |
| C | Bronze/records | lake |
| K | Kafka `records.v1` | Kafka |
| A | `audit.crawl_*` + lake `crawl_audit` | thu thập |

Thông điệp (đánh số trên hình):

| # | Từ | Tới | Thông điệp | Ghi chú |
|---|---|---|---|---|
| 1 | W | F | `recover_expired_leases()` | |
| 2 | W | F | `lease_due(10) FOR UPDATE SKIP LOCKED` | |
| 3 | F | W | task (LEASED, attempts+1) | mũi tên trả về (nét đứt) |
| 4 | W | A | circuit của nguồn đang mở? | |
| — | | | **khối alt: circuit mở** | |
| 5 | W | F | `mark_retry(RATE_LIMITED, 900 s)` | kết thúc nhánh |
| — | | | **khối alt (else): circuit đóng** | các bước 6–… nằm trong nhánh này |
| 6 | W | A | `start_run` (RUNNING) | |
| 7 | W | S | robots.txt (cache 1 h), throttle ≥ 2 s + jitter | |
| 8 | W | S | GET trang (hoặc Playwright + bắt XHR) | |
| 9 | S | W | phản hồi | mũi tên trả về (nét đứt) |
| 10 | W | R | `part-N.bin.gz` rồi `metadata.json` | |
| — | | | Note trên W: kiểm HTTP status **SAU** khi đã lưu raw | ô ghi chú |
| 11 | W | W | extract → bỏ PII → build_records | tự gọi |
| 12 | W | C | `{raw_artifact_id}.jsonl.gz` | |
| — | | | **khối loop: từng listing_row** | bao quanh bước 13 |
| 13 | W | K | `send(acks=all).get(30 s)` | |
| — | | | **khối alt: thành công** | |
| 14 | W | F | SUCCEEDED + chèn READY lần sau (cùng transaction) | |
| — | | | **else: lỗi tạm thời** | |
| 15 | W | F | RETRY_WAIT, backoff 30·2^(n-1) s | |
| — | | | **else: lỗi vĩnh viễn / hết 5 lần** | |
| 16 | W | F | FAILED | |
| 17 | W | A | attempt + `finish_run`, ghi bản lake JSONL.gz | sau khối alt, vẫn trong nhánh circuit đóng |

*Hình 4.3. Một lượt thu thập. Thứ tự raw → records → Kafka đảm bảo không tầng sau nào có dữ liệu mà tầng trước không có.*

| Hạng mục | Giá trị |
|---|---|
| Tài nguyên | 1 container mỗi nguồn; Lazada `shm_size` 1 GB, batch 1 |
| Healthcheck | heartbeat file, tuổi tối đa 600 s; dừng êm trong 120 s |
| Đo được | 14.023 task SWEEP thành công trong một lượt quét, khoảng 4 giờ (13/11, `docs/evidence/fullrun-20261114/KET-QUA.md`); Lazada bị chặn sau 76 trang (31/10, `docs/evidence/e2e-run-20261031/BAO-CAO-CHI-SO.md`) nên chỉ dùng để minh hoạ đa nguồn |
| Khi lỗi | xem §3.1; worker khởi động lại phải chờ lease cũ hết hạn (5 phút) |

## 4.3 P2 — Discovery và seed

DAG `source_discovery` (`@daily`, `catchup=False`) có **một task cho mỗi nguồn khai báo `discovery`**, các task chạy song song: `python -m ecomdata.ingestion.seed --source <code> --from-discovery --tier SWEEP`. Seed chèn một task READY cho mỗi danh mục × trang, `scheduled_for` neo ở `1970-01-01Z`, `ON CONFLICT DO NOTHING`, nên seed lại nhiều lần cũng vô hại. Tier `ACTIVE` được seed tay bằng `mp.ps1 seed`.

## 4.4 P3 — Speed qua Kafka

| Hạng mục | Giá trị |
|---|---|
| Tiến trình | 1 container `speed`, 2 query Spark (§3.4); vòng watchdog dừng tiến trình nếu query normalizer chết |
| Trigger | 30 s (`MARKETPLACE_STREAM_TRIGGER`); `spark.sql.shuffle.partitions = 4` |
| Checkpoint | volume `speed_checkpoints` → `/data/checkpoints/v1` và `/v1-normalizer` |
| Khởi động lại | `restart: unless-stopped`; tiếp tục từ checkpoint, ID tất định nên replay không sinh trùng |
| Reset | đổi `MARKETPLACE_STREAM_CHECKPOINT_VERSION` thì đọc lại từ `earliest` với state rỗng: mọi offer phát lại `NEW_OFFER` |
| Đo được | p95 khoảng 59–62 s sau khi đuổi kịp; `failed = 0`, `invalid = 0`, DLQ = 0 trong 9 giờ (13–14/11, `fullrun-20261114/KET-QUA.md`) |

## 4.5 P4 — Speed lakehouse (Databricks)

**Hình 4.4 — Speed lakehouse trên Databricks (P4)**

> Loại hình: flowchart trái → phải. Ý chính người xem phải thấy: hai query nối nhau qua bảng Delta `gold/speed/observations` thay cho Kafka; query 2 dùng cùng luật với speed local.

Node:

| # | Nhãn trên hình | Hình dạng | Tầng/màu | Ghi chú |
|---|---|---|---|---|
| 1 | S3 bronze · `marketplace/records/` | trụ | lake | |
| 2 | Query 1 · Auto Loader cloudFiles (text) · `normalize_micro_batch` | chữ nhật bo góc | speed | |
| 3 | `gold/speed/observations` | trụ | lake | |
| 4 | `gold/speed/records_dlq` | trụ | lake | |
| 5 | Query 2 · Delta stream → cột giả Kafka · `service.start_query` (cùng luật) | chữ nhật bo góc | speed | |
| 6 | outputs | trụ | lake | |
| 7 | offer_state · MERGE tiến | trụ | lake | |
| 8 | price_changes · MERGE theo change_id | trụ | lake | |
| 9 | stream_progress | trụ | lake | |

Cạnh (tất cả nét liền):

| Từ | Tới | Nhãn trên cạnh | Nét |
|---|---|---|---|
| S3 bronze | Query 1 | | liền |
| Query 1 | `gold/speed/observations` | | liền |
| Query 1 | `gold/speed/records_dlq` | | liền |
| `gold/speed/observations` | Query 2 | | liền |
| Query 2 | outputs | | liền |
| Query 2 | offer_state | | liền |
| Query 2 | price_changes | | liền |
| Query 2 | stream_progress | | liền |

*Hình 4.4. Speed lakehouse: Auto Loader đọc Bronze/records trên S3, query 2 chạy lại đúng luật của speed local trên Delta.*

Với `--available-now`, query 2 chỉ bắt đầu khi query 1 đã xử lý hết file hiện có, sau đó cả hai dừng. Job: `ecomdata-marketplace-speed`, timeout 6 giờ, lịch quartz `0 0/15 * * * ?` (Asia/Ho_Chi_Minh), **đang PAUSED**. Biến thể này không ghi Kafka, ES hay Redis.

Rủi ro chưa kiểm (`docs/DATABRICKS_READINESS.md` §8): `ProcessingTimeTimeout` có thể không bắn dưới `availableNow` (nghĩa là có thể không có `OFFER_STALE`); pyarrow bị wheel ghi đè; S3A trên DBR 17.3.

## 4.6 P5 — Batch theo giờ

### DAG `marketplace_batch`

**Hình 4.5 — DAG `marketplace_batch`**

> Loại hình: flowchart trái → phải, một chuỗi thẳng 8 task. Ý chính người xem phải thấy: thứ tự stage cố định, `quality` là cổng chặn (không retry) đứng trước `publish` và `promote`.

Node:

| # | Nhãn trên hình | Hình dạng | Tầng/màu | Ghi chú |
|---|---|---|---|---|
| 1 | window_settled · TimeDeltaSensor 10 phút | chữ nhật bo góc | điều phối | |
| 2 | silver | chữ nhật bo góc | batch | |
| 3 | dimensions | chữ nhật bo góc | batch | |
| 4 | facts | chữ nhật bo góc | batch | |
| 5 | marts | chữ nhật bo góc | batch | |
| 6 | quality · retries = 0 | chữ nhật bo góc | cổng: tô đỏ nhạt (màu Kafka) để nổi bật | |
| 7 | publish | chữ nhật bo góc | batch | |
| 8 | promote | chữ nhật bo góc | batch | |

Cạnh: window_settled → silver → dimensions → facts → marts → quality → publish → promote, mỗi mũi tên một cạnh, nét liền, không nhãn.

*Hình 4.5. DAG theo giờ: sensor chờ cửa sổ dữ liệu ổn định 10 phút rồi chạy 7 stage tuần tự.*

| Thuộc tính | Giá trị |
|---|---|
| Lịch | `@hourly`, bắt đầu 2026-10-25 UTC, `catchup=False`, `max_active_runs=1`, `dagrun_timeout` 3 h |
| Task | `DockerOperator`, image `ecommerce/spark-marketplace:4.0.1-r4`, `auto_remove=success`, timeout 45 phút mỗi stage |
| Lệnh | `python3 -m ecomdata.processing.batch run --stage <s> --run-id mp-<YYYYMMDDTHHMMZ> --as-of <data_interval_end> [--resume] [--allow-backfill]` |
| Retry | 2 lần, cách 2 phút; lần thử thứ 2 trở đi tự thêm `--resume`; `quality` không retry |
| Tham số | `allow_backfill` (mặc định False) |
| Khoá | advisory lock `pg_try_advisory_lock(820801)` trên kết nối riêng, giữ suốt lệnh; bị chiếm thì thoát mã 75 |

### State machine của một run (ADR-0005)

**Hình 4.6 — State machine của một run batch**

> Loại hình: state diagram, đường chính đi thẳng từ trái sang phải (CREATED → … → PROMOTED), các trạng thái lỗi/giữ đặt bên dưới. Ý chính người xem phải thấy: mỗi stage đẩy run lên một mốc; lỗi đưa về FAILED hoặc QUALITY_FAILED và `--resume` mở lại từ mốc cuối.

Trạng thái:

| Trạng thái | Loại | Ghi chú vẽ |
|---|---|---|
| (bắt đầu) | điểm đầu | chấm đen |
| CREATED | thường | |
| SILVER_BUILT | thường | |
| DIMENSIONS_BUILT | thường | |
| FACTS_BUILT | thường | |
| GOLD_WRITTEN | thường | |
| QUALITY_PASSED | thường | |
| QUALITY_FAILED | cuối (lỗi) | đặt dưới đường chính |
| PUBLISHED | thường | |
| HELD | giữ lại | đặt dưới đường chính |
| PROMOTED | cuối (thành công) | |
| FAILED | cuối (lỗi) | đặt dưới đường chính |
| (kết thúc) | điểm cuối | chấm đen có viền, sau PROMOTED |

Chuyển trạng thái:

| Từ | Tới | Điều kiện (nhãn cạnh) |
|---|---|---|
| (bắt đầu) | CREATED | |
| CREATED | SILVER_BUILT | silver |
| SILVER_BUILT | DIMENSIONS_BUILT | dimensions |
| DIMENSIONS_BUILT | FACTS_BUILT | facts |
| FACTS_BUILT | GOLD_WRITTEN | marts |
| GOLD_WRITTEN | QUALITY_PASSED | quality PASS |
| GOLD_WRITTEN | QUALITY_FAILED | quality FAIL (RESTORE star) |
| QUALITY_PASSED | PUBLISHED | publish |
| QUALITY_PASSED | HELD | quality-only / scratch / backfill bị từ chối / pointer conflict |
| PUBLISHED | PROMOTED | promote (CAS) |
| PUBLISHED | HELD | PROMOTION_CONFLICT |
| HELD | PUBLISHED | chạy lại (vd. `--allow-backfill`) |
| HELD | PROMOTED | |
| CREATED | FAILED | ngoại lệ ở bất kỳ stage |
| SILVER_BUILT | FAILED | |
| DIMENSIONS_BUILT | FAILED | |
| FACTS_BUILT | FAILED | |
| QUALITY_FAILED | GOLD_WRITTEN | `--resume` (chạy lại quality) |
| FAILED | CREATED | `--resume` về `last_completed_status` |
| PROMOTED | (kết thúc) | |

*Hình 4.6. Trạng thái của run trong `audit.marketplace_batch_run`. `FAILED`, `QUALITY_FAILED`, `PROMOTED` là trạng thái cuối. `--resume` mở lại run ở `last_completed_status`; stage nào đã đạt mốc thì bị bỏ qua.*

### Đầu vào, đầu ra từng stage

| Stage | Đọc | Ghi | Trạng thái sau |
|---|---|---|---|
| `silver` | Bronze/records của cửa sổ ngày (không cắt `as_of`) | 4 bảng Silver, `_runs/<run>/silver.json` | `SILVER_BUILT` |
| `dimensions` | toàn bộ Silver (gồm quảng cáo), bảng rộng, page context, crawl attempts | 7 dim + 1 bridge (`MERGE`), `dimensions.json` | `DIMENSIONS_BUILT` |
| `facts` | Silver, `dim_product_listing`, crawl audit | 6 fact (`MERGE`, partition `date_key`), `facts.json` | `FACTS_BUILT` |
| `marts` | star tại version vừa dựng, cắt `as_of`, bỏ quảng cáo; audit cắt `as_of` | 12 mart vào `runs/run_id=<run>/`, `marts.json` | `GOLD_WRITTEN` |
| `quality` | Silver cắt `as_of`, mart, star, audit | `audit.marketplace_quality_result`, `quality.json`, manifest | `QUALITY_PASSED` / `QUALITY_FAILED` / `HELD` |
| `publish` | mart, manifest | `staging.*` → `cache.marketplace_*`, `audit.marketplace_cache_version` | `PUBLISHED` / `HELD` |
| `promote` | manifest, `current.json` | `current.json` (CAS) | `PROMOTED` / `HELD` |

Sau mỗi stage, runner ghi `timings/<stage>.json` và chép dòng audit ra `_runs/run_id=<run>/run.json`, để lake tự mô tả được lịch sử run.

### Cờ dòng lệnh

| Cờ | Tác dụng |
|---|---|
| `--stage <s>` (lặp được) / `--all` | chọn stage |
| `--resume` | mở lại run FAILED/QUALITY_FAILED |
| `--force` | chạy lại một stage đã đạt mốc, không đổi trạng thái |
| `--allow-backfill` | cho promote run có `as_of` cũ hơn run đang current |
| `--quality-only` | dừng ở HELD sau quality, không publish |
| `--silver-since D`, `--late-days N`, `--rebuild` | cửa sổ của stage `silver` |
| `--silver-uri`, `--gold-root-uri` | đổi gốc lưu; gold root khác mặc định → scratch run (không manifest, HELD) |
| `--skip-postgres` | audit và quality trong bộ nhớ, không lock, không publish (dùng cho Databricks) |

## 4.7 P6 — Reprocess

DAG `marketplace_reprocess` (không lịch, chạy bằng *Trigger DAG w/ config*) có 7 stage như P5, nhưng không có sensor, timeout 4 giờ mỗi task, 1 retry. `run_id = mp-reprocess-<ts>`. Tham số: `since` (thêm `--silver-since`) hoặc `rebuild` (dựng lại Silver từ toàn bộ Bronze). Dùng khi normalizer đổi, khi Bronze được bổ sung ngày cũ, hoặc khi dựng lại sau sự cố. Kết hợp với `ingestion/reparse.py`: parser đổi thì reparse dựng lại records từ raw, rồi reprocess dựng lại mọi tầng sau.

## 4.8 P7 — Batch trên Databricks

| Hạng mục | Giá trị |
|---|---|
| Job | `ecomdata-marketplace-batch`, một task `python_wheel_task`, entry point `ecomdata-batch` |
| Lệnh | `run --run-id dbx-{{job.run_id}} --as-of {{job.parameters.as_of}} --stage silver … --stage quality --skip-postgres` |
| Cluster | single node (`num_workers: 0`), `SINGLE_USER`, `local[*,4]`, `m5d.large`, DBR `17.3.x-scala2.13` |
| Cấu hình | `DATA_LAKE_PROFILE=s3`, `DATA_LAKE_TABLE_FORMAT=delta`, `MARKETPLACE_CRAWL_AUDIT_SOURCE=lake`; khoá S3 lấy từ secret scope `ecomdata` |
| Đồng thời | `max_concurrent_runs: 1`, thay cho advisory lock |
| Không gồm | `publish`, `promote` (cần PostgreSQL `cache.*` ở local) |

Lệnh: `databricks bundle deploy -t dev`, rồi `databricks bundle run -t dev marketplace_batch --params as_of=…`. Bronze được đưa lên S3 bằng `mc mirror` chạy tay.

## 4.9 Triển khai và tài nguyên

| Profile compose | Service | Kiểu |
|---|---|---|
| (lõi) | `kafka` (4.2.0, KRaft), `kafka-init`, `minio`, `minio-init` (bucket `ecommerce-bronze/silver/gold`), `redis` 8.6.2, `postgres-dw` 18.3 (chạy `init_postgres.sql`), `elasticsearch` 8.18.1 (heap 1 GB) | dịch vụ, `unless-stopped` |
| `crawl` / `crawl-browser` | `crawl-worker`, `crawl-worker-lazada` | dài hạn |
| `speed` | `speed` | dài hạn |
| `batch` | `batch-once` | công cụ một lần (`run --rm`) |
| `orchestration` | `airflow-init`, `airflow-api-server` (:8084), `airflow-scheduler`, `airflow-dag-processor` | dịch vụ |
| `serve` | `kibana`, `kibana-marketplace-setup`, `superset`, `superset-init` | dịch vụ, khởi tạo một lần |
| `ops` | `ops` (`python -m ecomdata.ops`), `es-projector` | công cụ, dài hạn |
| `smoke` | `stub-source` (site giả, cổng 8000) | kiểm thử offline |
| `legacy-scheduler` | `batch-scheduler` (vòng lặp cũ trước Airflow) | không dùng nữa |
| file observability | `loki`, `prometheus`, `alloy`, `grafana` (:3001), bind 127.0.0.1 | dịch vụ |

Mọi service chạy **một bản**. Tham số chung do Airflow truyền vào container theo tiền tố (`KAFKA_`, `POSTGRES_`, `MINIO_`, `DATA_LAKE_`, `CRAWL_`, `MARKETPLACE_`, `SPARK_`…). Container gắn nhãn `ecomdata.project/layer/service` để Alloy nhận diện.

Điều khiển hằng ngày qua `scripts/mp.ps1 -EnvFile env/<stack>.env <verb>`:

| Verb | Việc |
|---|---|
| `up [-With speed,serve,…]` | bật lõi + các profile |
| `status`, `validate` | trạng thái crawl/speed/batch/pointer; kiểm tra tính đúng |
| `seed`, `migrate` | thêm task frontier; áp lại DDL |
| `batch -AsOf … [-AllowBackfill] [-QualityOnly]` | chạy batch `--all` ngoài Airflow |
| `dags list\|pause\|unpause` | điều khiển DAG |
| `backup`, `restore -BackupId` | sao lưu, khôi phục |
| `smoke`, `drill d1..d11`, `bench`, `evaluate`, `evidence`, `demo` | kiểm thử, diễn tập sự cố, đo, gom bằng chứng |

## 4.10 P8 — Bảo trì, sao lưu, khôi phục

DAG `marketplace_maintenance` (`@weekly`): `prune_raw` → `backup` → `storage_snapshot`.

**Hình 4.7 — DAG `marketplace_maintenance` và nội dung backup**

> Loại hình: flowchart trái → phải. Ý chính người xem phải thấy: ba task chạy tuần tự; backup giữ batch lock và chép bốn nhóm dữ liệu.

Node:

| # | Nhãn trên hình | Hình dạng | Tầng/màu | Ghi chú |
|---|---|---|---|---|
| 1 | prune-raw · xoá bronze/raw > 30 ngày | chữ nhật bo góc | điều phối | |
| 2 | backup · (giữ batch lock) | chữ nhật bo góc | điều phối | |
| 3 | storage-snapshot · kích thước từng kho, chỉ đọc | chữ nhật bo góc | điều phối | |
| 4 | Gold: manifests, current.json, run được trỏ tới | trụ | lake | |
| 5 | Bronze, Silver | trụ | lake | |
| 6 | pg_dump schema audit, cache | trụ | lake | |
| 7 | SHA-256 từng file · /reports/backups | trụ | lake | |

Cạnh:

| Từ | Tới | Nhãn trên cạnh | Nét |
|---|---|---|---|
| prune-raw | backup | | liền |
| backup | storage-snapshot | | liền |
| backup | Gold: manifests… | | liền |
| backup | Bronze, Silver | | liền |
| backup | pg_dump | | liền |
| backup | SHA-256 từng file | | liền |

*Hình 4.7. Bảo trì hằng tuần: dọn raw cũ, sao lưu, đo dung lượng.*

- Backup chép Gold **trước**, sau đó tới Bronze và Silver. Backup bị từ chối nếu `run_id` của pointer khác `audit.marketplace_cache_version.run_id`.
- Restore ghi `current.json` **cuối cùng**, sau đó chạy các kiểm tra `validate --restored`.
- Không sao lưu: Kafka, Elasticsearch, Redis, checkpoint speed. Những thứ này dựng lại được từ Bronze.
- Không có bảo trì Delta (`OPTIMIZE`, `VACUUM`) và không xoá run Gold cũ. Xem rủi ro ở [05](05-sai-lech-va-rui-ro.md).

## 4.11 Ma trận lỗi: hỏng chỗ nào thì mất gì

| Thành phần hỏng | Ảnh hưởng tức thời | Dữ liệu mất? | Phục hồi |
|---|---|---|---|
| Site chặn, rate limit | circuit mở 15 phút, task chuyển RETRY_WAIT | không | tự động; cảnh báo `crawl-blocked`, `circuit-open` |
| MinIO | crawler lỗi `STORAGE_ERROR` (retry), batch dừng | không, vì raw chưa ghi thì task chưa thành công | tự động khi MinIO lên lại |
| Kafka | crawler `PUBLISH_ERROR` (retry); speed dừng | không: Bronze đã có; batch không phụ thuộc Kafka | tự động; speed đọc tiếp từ checkpoint |
| Speed container | không có thay đổi mới trên Kibana | không | restart, tiếp tục từ checkpoint (đã diễn tập E1–E5 ngày 31/10, `docs/evidence/e2e-run-20261031/`) |
| ES, Redis | micro-batch speed lỗi, replay | không | restart; ghi idempotent |
| Một stage batch lỗi | run FAILED; cache giữ run trước | không | Airflow retry với `--resume` |
| Quality FAIL | run QUALITY_FAILED, star được RESTORE | không; Superset giữ bản tốt cuối | sửa nguyên nhân, `--resume` |
| PostgreSQL | frontier dừng, batch không publish | cache dựng lại được từ Gold; audit có bản trong lake | khôi phục từ backup, hoặc publish lại từ lake |
