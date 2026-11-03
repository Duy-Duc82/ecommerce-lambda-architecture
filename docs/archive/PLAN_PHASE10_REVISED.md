# Phase 10 (bản sửa): pipeline ELT đầy đủ, star schema, Airflow

Ngày 2026-10-24. Trạng thái: **bản nháp, chờ người dùng duyệt.**

- **Thay cho:** thứ tự và phạm vi của `docs/PLAN_PHASE10_DIMENSIONAL_MODEL.md`.
- **Vẫn dùng từ plan cũ:** thiết kế dim/fact (mục 3), quy tắc khoá và SCD2, ràng buộc ở mục 0 của plan đó (không đụng stack demo, commit không có `Co-Authored-By`, không commit dữ liệu thô, đặt `spark.driver.memory` tường minh).
- **Hạn hoàn thành:** 2026-11-24 (4 tuần). Ngày demo khoảng cuối tháng 12.

---

## 1. Vì sao sửa

Góp ý của giáo viên hướng dẫn ngày 2026-10-24:

1. **Thêm Apache Airflow vào pipeline.**
2. **Ingestion lấy mọi trường lấy được; Spark chuẩn hoá sau (ELT).** Bài toán phân tích rút ra từ dữ liệu đó.
3. **Bỏ bài toán so sánh sản phẩm giữa các sàn.** Shopee và TikTok Shop không crawl được; chỉ còn 2 sàn thì bài toán yếu.
4. **Ưu tiên một pipeline big data đầy đủ, chỉn chu như của một DE, và có đủ dữ liệu:**
   - batch phải chịu được dữ liệu nhiều tháng, nhiều năm;
   - speed chỉ cần chạy thỉnh thoảng để kiểm;
   - trước demo chạy full stack khoảng 2 tuần là đủ.

Hiện trạng đo trên mẫu thật (`docs/evidence/fullstack-run-20261024/samples/`):

| Điểm | Hiện tại | Hệ quả |
|---|---|---|
| Trường lấy về | Trang category Tiki có **133 trường lá** mỗi sản phẩm. `crawler/sites/tiki.py:198` (`_event_from_row`) giữ khoảng **17**. | Mất `seller_name`, `brand_id`, `category_ids`, `sku`, `origin`, `is_authentic`, badges, thời gian giao, `category_l1..l4`, `search_rank`… Dữ liệu cấp trang (`filters`, `sellers`, `paging`) cũng bị bỏ. |
| Nơi chuẩn hoá | Python trong crawler: ép kiểu `Decimal`, validate, dựng canonical observation. | Đổi parser thì phải crawl lại. Không có lớp Spark nào xử lý phần chuẩn hoá. |
| Lưu raw | HTML **không nén**, 812 KB/trang (`body.bin`). | 300 category × 48 lượt/ngày ≈ **11 GB/ngày**. Ổ C còn 229 GB, tức khoảng 20 ngày. |
| Silver | `silver-sink` ghi mỗi observation thành 1 file JSON. | Khoảng 160.000 file sau 8 giờ. |
| Phạm vi crawl | 300 category, chỉ trang 1 (`?page=2` lặp lại trang 1). | Sitemap Tiki có khoảng **4.200 category**. Một lượt quét toàn bộ ≈ 4.200 request ≈ 130.000 snapshot sản phẩm (khảo sát ngày 12/10). |
| Điều phối | Vòng lặp `batch_layer/marketplace_scheduler.py` (quyết định D2: không dùng Airflow). | Đảo ngược D2, xem mục 3.5. |
| Số nguồn | Chỉ Tiki. | Giáo viên muốn **nhiều nguồn, ưu tiên sàn TMĐT**. Kết quả probe ở mục 3.6. |

## 2. Kiến trúc đích

```
                      Airflow (LocalExecutor, metadata DB = postgres-dw/airflow)
                      ├─ tiki_category_discovery   (hằng ngày)
                      ├─ marketplace_batch         (hằng giờ)
                      └─ marketplace_maintenance   (hằng tuần)

crawl-worker ──fetch──► Bronze/raw      HTML.gz, giữ 30 ngày        (MinIO)
             └────────► Bronze/records  JSONL.gz, đủ mọi trường,     (MinIO, master dataset, giữ vĩnh viễn)
             └────────► Kafka marketplace.records.v1  (đủ mọi trường)
                                │
          speed (bật khi cần) ◄─┘   dùng chung hàm normalize với batch

Bronze/records ──Spark──► Silver (Delta, đã chuẩn hoá, partition observed_date)
               ──Spark──► Gold/dw (dim, fact, Delta MERGE) ──► Gold marts
               ──publish─► Postgres dw.* + cache.* ──► Superset
```

Bỏ `silver-sink`: Silver giờ do Spark sinh ra từ Bronze.

**Hai loại crawl worker, dùng chung frontier, Bronze và record envelope:**

- **`crawl-worker-http`:** `requests`, parse JSON nhúng trong HTML. Dùng cho Tiki, Chợ Tốt và các nhà bán lẻ có SSR.
- **`crawl-worker-browser`:** image `mcr.microsoft.com/playwright/python:v1.55.0-noble`, headless Chromium, không stealth. Ghi lại JSON mà chính trang tự tải trong lúc render (`page.on("response")`), không tự gọi API. Dùng cho Lazada và các trang CSR.

Thêm một nguồn chỉ cần một adapter nhỏ: URL listing, cách tìm mảng sản phẩm, cách nhận diện trang chặn, và danh sách trường PII cần bỏ.

## 3. Thiết kế

### 3.1 Ingestion theo ELT

**Crawler chỉ làm 4 việc:**

1. Fetch trang.
2. Kiểm tra trang hợp lệ: có `__NEXT_DATA__`, có `props.initialState.catalog.data`, không phải trang challenge. Trang không hợp lệ vẫn đi vào nhánh retry/circuit hiện có.
3. Lưu raw HTML ở dạng gzip. Thêm `content_encoding: gzip` vào metadata; `reparse.py` đọc được cả bản cũ không nén.
4. Ghi **record envelope**, mỗi sản phẩm một dòng, vào Bronze/records và Kafka.

**Crawler không chọn trường, không ép kiểu, không tính giá.** Ngoại lệ duy nhất là **PII**: trường thông tin cá nhân (ví dụ của Chợ Tốt: `full_name`, `account_name`, `avatar`, số điện thoại, toạ độ `latitude`/`longitude`) bị bỏ ngay trong record. Riêng ID người bán thì băm bằng `sha256` có salt. Raw HTML có PII chỉ giữ 30 ngày rồi xoá.

Record envelope (`marketplace-record.v1`):

| Trường | Nguồn |
|---|---|
| `record_id` | `deterministic_id(raw_artifact_id, row_index)` |
| `raw_artifact_id`, `crawl_run_id`, `marketplace_code`, `adapter_version` | sẵn có |
| `request_target`, `request_page`, `fetched_at`, `http_status` | sẵn có |
| `record_kind` | `listing_row` hoặc `page_context` |
| `row_index` | vị trí trong `catalog.data` (thứ hạng hiển thị) |
| `payload` | **nguyên object JSON của dòng**, không sửa |

- **Mỗi trang có thêm 1 record `page_context`:** `categoryData`, `filters`, `sellers`, `paging`, `sort_options`, `amplitude`, `metaTitle`, `h1`. Đây là nguồn cho tên danh mục và breadcrumb (giải rủi ro R1 của plan cũ).
- **Dòng quảng cáo (`advertisement`) cũng giữ,** đánh dấu ở Silver chứ không lọc ở crawler.
- **Đường dẫn Bronze/records:** `bronze/marketplace/records/marketplace=tiki/observed_date=…/hour=…/crawl_run_id=…/raw_artifact_id=….jsonl.gz`. Mỗi trang 1 file, ghi xong mới publish Kafka, giữ thứ tự raw-first.

### 3.2 Phạm vi và nhịp crawl

| Tầng | Category | Nhịp | Trang/ngày |
|---|---|---|---|
| `HOT` | 300 category hiện có | 2 giờ | 3.600 |
| `SWEEP` | toàn bộ khoảng 4.200 category trong `sitemap_categories_*.xml` | 24 giờ | 4.200 |

- **Khoảng cách giữa request:** giữ `CRAWL_REQUEST_DELAY_SECONDS=2.0`. Khoảng 7.800 trang/ngày ≈ 4,3 giờ tải mỗi ngày nếu chạy tuần tự.
- **Ước lượng sản lượng:** khoảng 275.000 dòng sản phẩm/ngày, tức **khoảng 8 triệu/tháng**. Số thật sẽ đo ở W1.
- **Ước lượng dung lượng:**

| Vùng | Mỗi trang (gzip) | Mỗi ngày | Giữ |
|---|---|---|---|
| Bronze/raw HTML | 206 KB | ≈ 1,6 GB | 30 ngày (≈ 48 GB) |
| Bronze/records | 59 KB | ≈ 0,46 GB | vĩnh viễn (≈ 14 GB/tháng, ≈ 170 GB/năm) |
| Silver Parquet | đo ở W2 | | vĩnh viễn |

- **Category discovery là một DAG Airflow:** tải sitemap rồi upsert frontier tầng `SWEEP` (dùng lại `seed_frontier`, idempotent). Category biến mất khỏi sitemap thì đánh dấu, không xoá.

### 3.3 Spark Bronze → Silver

Job `batch_layer/marketplace_silver_build.py`:

- **Đọc** Bronze/records của các partition `observed_date`/`hour` chưa xử lý (watermark lưu trong manifest).
- **Schema khai báo tường minh** (`config/tiki_record_schema.py`) cho cả 133 trường cùng các mảng lồng (`badges_new`, `impression_info`, `visible_impression_info.amplitude`).
  - Trường lạ chưa có trong schema thì giữ nguyên trong cột `payload_json`.
  - Báo cáo schema drift: các key mới xuất hiện kèm tần suất.
- **Normalize** (`batch_layer/marketplace_normalize.py`, chỉ dùng hàm của Spark, dùng chung cho batch và speed):
  - ép kiểu tiền về `decimal(38,6)`;
  - tách `quantity_sold.value`;
  - `primary_category_path` → mảng category id;
  - dịch các cờ boolean;
  - chuẩn hoá chuỗi (trim, NFC);
  - `is_advertisement`;
  - quy về đúng các cột canonical hiện có, cộng thêm các cột mới.
- **Kiểm dữ liệu:** dòng thiếu `id`/`price` hoặc giá âm đi vào `silver/marketplace/quarantine` kèm lý do, không làm hỏng cả batch.
- **Dedup** theo `(marketplace_code, platform_listing_id, fetched_at)`, phá hoà như `marketplace_marts._ordered`.
- **Ghi** Delta `silver/marketplace/offer_observations_v2/`, partition `observed_date`. Có thêm bảng `silver/marketplace/page_context/`.
- **Tương thích:** Silver v2 chứa mọi cột canonical cũ, nên `marketplace_marts.py`, quality và ops vẫn chạy được trong lúc chuyển sang star.

### 3.4 Star schema

Giữ thiết kế ở mục 3 của plan cũ, với các thay đổi do có thêm trường:

- **`dim_seller`:** thêm `seller_name`, `seller_type`, `is_official_store`, `is_trusted_store` (SCD1).
- **`dim_brand` (mới):** `brand_id`, `brand_name`. Tách khỏi `dim_product_listing`.
- **`dim_category`:** có tên và breadcrumb từ `page_context` cùng `category_l1..l4_name`.
- **`dim_product_listing` (SCD2):** thêm `sku`, `master_product_sku`, `origin`, `is_authentic`, `tiki_verified`. Thuộc tính SCD2 gồm `product_title`, `brand_key`, `category_key`, `seller_key`.
- **Junk `dim_listing_flags`:** gộp các cờ giao hàng (`is_tikinow_delivery`, `is_nextday_delivery`, `freeship`, `is_flash_deal`…), thay cho `dim_offer_status`.
- **`fact_offer_observation`:** thêm `search_rank`, `row_index`, `quantity_sold`, `review_count`, `rating`, `is_advertisement`.
- **`fact_category_page_daily` (mới, periodic snapshot):** danh mục × ngày, gồm số sản phẩm, giá median, tỷ lệ giảm giá, số người bán.
- **Bỏ** các mart thuần so sánh liên sàn. `dim_marketplace` vẫn thiết kế cho nhiều nguồn.

Tài liệu P0 (`docs/DATA_MODEL.md` §4, `docs/model/*`) sửa theo các điểm này.

### 3.5 Airflow (đảo ngược quyết định D2)

**Lý do đảo ngược:**

- D2 bỏ Airflow vì khi đó chỉ có **một** job định kỳ.
- Phase 10 có ba nhóm việc nối nhau, mỗi nhóm cần retry riêng, log riêng và backfill theo khoảng thời gian:
  - Bronze → Silver → dim → fact → mart → quality → publish;
  - discovery category hằng ngày;
  - bảo trì hằng tuần.
- Ghi đảo ngược vào `docs/PROGRESS.md` §5.

**Hạ tầng:**

- Airflow 3.x, `LocalExecutor`, chạy 3 container: `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`. Không dùng Celery.
- Metadata là database `airflow` trên `postgres-dw` có sẵn.
- Profile compose `orchestration`. Dự tính 1,5–2 GB RAM.
- Task Spark chạy bằng `DockerOperator` với image `spark-marketplace`, giống `batch-once`, nên image Airflow không phải chứa Java hay pyspark. Mỗi task đặt `--driver-memory` riêng. Cần mount `/var/run/docker.sock`; Docker Desktop trên Windows hỗ trợ.

**`marketplace_batch`** (`schedule="@hourly"`, `catchup=False`, `max_active_runs=1`):

```
wait_crawl_settled (sensor, audit) → build_silver → build_dims → build_facts
  → build_marts → quality_gate → publish_postgres
```

- `as_of = data_interval_end`; `run_id` giữ dạng `mp-YYYYMMDDTHHMMZ` (hàm `run_id_for` có sẵn).
- Advisory lock vẫn nằm trong job, làm lớp bảo vệ thứ hai.
- `--allow-backfill` chỉ bật khi trigger tay với param `allow_backfill=true`.
- `marketplace_warehouse.py` thêm `--stage {silver,dims,facts,marts,quality,publish}`. Mỗi stage đọc và ghi Delta, nên chạy lại riêng một stage được.

**`tiki_category_discovery`** (hằng ngày): tải sitemap → diff → upsert frontier → báo số category thêm/bớt.

**`marketplace_maintenance`** (hằng tuần):

- Delta `OPTIMIZE` và `VACUUM`;
- xoá Bronze/raw HTML quá 30 ngày;
- `ops backup`;
- báo cáo dung lượng từng vùng.

**`batch-scheduler` cũ** chuyển sang profile `legacy-scheduler`. Test của nó giữ nguyên.

### 3.6 Nguồn dữ liệu (probe ngày 24/10/2026)

Script và kết quả probe: scratchpad của session, `probe/scripts/probe_marketplaces.py` và `probe_xhr.py`. Chạy trong container Playwright, headless, không stealth, có kiểm robots.txt.

| Nguồn | Loại | Kết quả | Dữ liệu | Số trường/SP | Phân trang |
|---|---|---|---|---|---|
| **Tiki** | sàn B2C | OK, HTTP | `__NEXT_DATA__` | 133 | không (`?page=2` lặp lại trang 1) → quét theo khoảng 4.200 category |
| **Lazada** | sàn B2C | OK, trình duyệt | JSON trang tự tải `?ajax=true&page=N` → `mods.listItems` (40 SP/trang), kèm `filter`, `breadcrumb` | 55 | **có**, `page=2` ra dữ liệu khác |
| **Chợ Tốt** | sàn C2C | OK, HTTP | `__NEXT_DATA__` → `props.pageProps.initialState.adlisting.data.ads` (20 tin/trang) | 115, có PII | có |
| Muaban.net | rao vặt | homepage OK, có `__NEXT_DATA__` | URL category cần tìm lại (bản probe trả 404) | chưa đo | chưa đo |
| Shopee | sàn | **chặn**: chuyển tới `/verify/traffic/error` | — | — | — |
| TikTok Shop | sàn | **chặn**: "Security Check" (captcha) | — | — | — |
| Sendo | sàn | **không kết nối được** (`ERR_CONNECTION_REFUSED`) | — | — | — |
| Vatgia | rao vặt | chỉ còn trang rao vặt, ít dữ liệu | — | — | — |

Nhà bán lẻ online đã khảo sát ngày 11/10 (`docs/evidence/source-hunt-20261011/`) dùng làm **nguồn bổ sung để tăng lượng dữ liệu**:

- JSON-LD: Thế Giới Di Động, Nguyễn Kim.
- Thuộc tính `data-*` trong HTML: Điện Máy Xanh.
- `__NUXT__`: CellphoneS.
- Next RSC: FPT Shop, GearVN, Di Động Việt.
- `__NEXT_DATA__`: Phong Vũ.
- Cần trình duyệt: Hasaki, Bách Hoá Xanh (đều render được giá trong lần probe này).

**Thứ tự đưa vào:**

1. Tiki và Chợ Tốt (worker HTTP).
2. Lazada (worker trình duyệt).
3. Phong Vũ, CellphoneS, Thế Giới Di Động, Điện Máy Xanh (có JSON nhúng sẵn, adapter nhỏ).
4. Các nguồn còn lại nếu còn thời gian.

Shopee, TikTok Shop và Sendo đưa vào luận văn như phần **phân tích khả thi**, có bằng chứng; không vượt anti-bot.

**Silver đa nguồn:**

- Mỗi nguồn có một bảng Silver rộng riêng (`silver/marketplace/<source>_listing`), giữ mọi trường đã ép kiểu.
- Có thêm một bảng chung `silver/marketplace/offer_observations_v2`, gồm các cột chung: giá, giá gốc, người bán, thương hiệu, danh mục, rating, đã bán, tồn kho. Star schema đọc bảng chung này.
- `dim_marketplace` lúc này có nhiều dòng thật. Thêm cột `source_type` (`b2c_marketplace`, `c2c_classified`, `retailer`).

### 3.7 Speed

- Chuyển sang profile `speed`, chỉ bật khi cần test hoặc demo.
- Đọc `marketplace.records.v1`, dùng chung `marketplace_normalize.py` với batch. Một đoạn code chuẩn hoá chạy cả batch lẫn stream: đây là luận điểm Lambda khi bảo vệ.
- Topic `marketplace.observations.v1` cũ: giữ đến hết W4 rồi bỏ.

## 4. Lịch 4 tuần

Mỗi phase: test viết trước hoặc cùng code; chạy toàn bộ suite; số test không thấp hơn mốc 1179 passed (trừ test bị thay có chủ đích, ghi rõ trong commit); mỗi phase một PR vào `develop`.

### W1 (26/10 – 01/11): ingestion ELT và bắt đầu thu thập dài hạn

Việc quan trọng nhất của tuần: dữ liệu chỉ tích luỹ kể từ ngày bắt đầu chạy.

- **E0:** duyệt plan này cùng phần contract record envelope (mục 3.1).
- **E1:**
  - crawler ghi raw gzip và Bronze/records, publish `marketplace.records.v1`;
  - `reparse.py` đọc được cả bản gzip;
  - giữ nhánh canonical cũ đến khi Silver v2 chạy, để stack không gãy.
- **E2:**
  - parser sitemap category;
  - tầng `SWEEP`;
  - seed khoảng 4.200 category;
  - đo sản lượng và dung lượng thật trong 24 giờ đầu.
- **E2b:** adapter **Chợ Tốt** (HTTP, bỏ PII).
- **E3:**
  - dựng stack thu thập `mp-collect` (project và port riêng) chỉ gồm `minio`, `postgres-dw`, `kafka`, các crawl worker;
  - chạy liên tục tới ngày demo.
  - **Nghiệm thu:** 24 giờ không lỗi; báo số trang, số dòng, GB/ngày, tỷ lệ lỗi.

### W2 (02/11 – 08/11): Silver bằng Spark và nền Airflow

- **S1:**
  - `tiki_record_schema.py`, `marketplace_normalize.py`, `marketplace_silver_build.py`;
  - quarantine, báo cáo drift;
  - Delta cho host và image tag mới (mục 4.2 plan cũ).
- **S2:**
  - `marketplace_datasets.py` (registry, mục 4.1 plan cũ);
  - sửa P0 `DATA_MODEL.md` theo mục 3.4.
- **A1:**
  - profile `orchestration`;
  - DAG `marketplace_batch` bản một task (bọc batch hiện tại, đọc Silver v2);
  - DAG `tiki_category_discovery`.
- **E4:**
  - `crawl-worker-browser` (Playwright) và adapter **Lazada**;
  - thêm vào `mp-collect`.
- **E5 (W2–W3, xen kẽ):** adapter Phong Vũ, CellphoneS, Thế Giới Di Động, Điện Máy Xanh.
- **Nghiệm thu:**
  - Silver v2 từ toàn bộ Bronze của W1, kèm số dòng, số dòng quarantine, số drift;
  - Airflow chạy 24 run liên tục PASS trên stack thử.

### W3 (09/11 – 15/11): star schema

- **D1:** dims, gồm SCD2 và closure danh mục (P1 plan cũ, cộng `dim_brand` và `dim_listing_flags`).
- **D2:** facts, gồm `fact_category_page_daily` (P2 plan cũ).
- **D3:**
  - ghi Gold Delta bằng `MERGE` theo kiểu incremental;
  - quality gate cho mô hình;
  - manifest v2 (P3 và P5 plan cũ).
- **Nghiệm thu:**
  - chạy trên khoảng 2 tuần dữ liệu thật;
  - incremental chạy 2 lần cho kết quả bằng full rebuild;
  - thời gian batch không tăng theo tổng dung lượng Silver.

### W4 (16/11 – 22/11): serving, DAG đầy đủ, bằng chứng

- **V1:**
  - Postgres `dw.*` cùng `cache.*`;
  - dựng lại mart từ fact;
  - Superset có dashboard "Star schema" và dashboard "Data platform" (sản lượng, dung lượng, drift, quarantine) (P4 plan cũ).
- **A2:**
  - tách `marketplace_batch` thành từng stage;
  - DAG `marketplace_maintenance`;
  - test DAG (`DagBag` import không lỗi, cấu trúc phụ thuộc đúng).
- **K1:** speed đọc `marketplace.records.v1`; bỏ `silver-sink` và topic cũ.
- **R1:**
  - báo cáo HTML: kiến trúc, số đo thu thập 3–4 tuần, ảnh Airflow Graph/Grid, dung lượng;
  - cập nhật `PROGRESS.md`.

**23–24/11:** dự phòng.

**Sau 24/11:** `mp-collect` tiếp tục chạy. Khoảng 2 tuần trước demo, bật full stack (thêm Airflow, speed, Superset, Kibana) chạy liên tục để có số liệu demo.

## 5. Rủi ro

| | Rủi ro | Cách xử lý |
|---|---|---|
| R1 | Máy phải bật liên tục trên 1 tháng; Windows Update hoặc sleep làm ngắt crawl. | Tắt sleep, đặt giờ hoạt động cho Windows Update, đặt `restart: unless-stopped`. Frontier đã resume được sau khi khởi động lại (drill D1–D10). Báo cáo ghi rõ các khoảng thủng dữ liệu. |
| R2 | RAM: `mp-collect` chạy song song với stack đang phát triển. | `mp-collect` không có Spark. Ước khoảng 2–3 GB, cộng khoảng 1 GB cho Chromium của worker trình duyệt. `mp-tikihtml` đã dừng ngày 24/10. |
| R7 | Lazada siết anti-bot khi chạy lâu (script "baxia"). | Giãn nhịp (≥ 5 giây/trang, chỉ một tab). Phát hiện trang chặn thì mở circuit, không thử vượt qua. Nếu bị chặn hẳn thì Lazada trở thành phân tích khả thi như Shopee. |
| R8 | PII của Chợ Tốt. | Bỏ ở crawler (mục 3.1). Có test kiểm record không còn trường PII. |
| R3 | Tiki đổi cấu trúc `__NEXT_DATA__`. | Raw vẫn được lưu và validate chỉ kiểm khung ngoài. Spark báo drift; sửa schema rồi chạy lại từ Bronze. |
| R4 | Tốc độ crawl 4.200 trang/ngày gây chú ý hoặc bị chặn. | Giữ delay 2 giây, tôn trọng robots, có circuit breaker sẵn. Nếu có 429 thì giãn tầng `SWEEP` thành 48 giờ. |
| R5 | Đổi contract làm gãy nhiều test. | Silver v2 giữ mọi cột canonical cũ; nhánh cũ chỉ bỏ ở W4. |
| R6 | 4 tuần là ít. | Thứ tự cắt bớt nếu trễ: K1 (speed) → `fact_category_page_daily` → dashboard "Data platform". Không cắt E1–E3, S1, D1–D3, A1. |

## 6. Cần người dùng quyết định

1. Duyệt thứ tự W1–W4 và record envelope ở mục 3.1.
2. Danh sách nguồn và thứ tự ở mục 3.6. Có tính nhà bán lẻ (Thế Giới Di Động, CellphoneS…) là "nguồn" không, hay chỉ lấy sàn?
3. Chợ Tốt (C2C, hàng cũ): đưa vào chung bảng offer, hay để bảng riêng?
