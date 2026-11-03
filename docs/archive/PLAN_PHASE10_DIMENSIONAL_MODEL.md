# Phase 10: mô hình dữ liệu chiều (dim/fact) cho nhánh marketplace

Ngày 2026-10-24. Trạng thái: **đã duyệt; 4 quyết định ở mục 8 đã chốt.** Bắt đầu từ P0. Mô hình đề xuất có trong `docs/model/` (DBML, PlantUML, SQL, Mermaid); bản xem nhanh là `docs/model/marketplace_star_schema.png`.

---

## 0. Bàn giao cho session thực hiện (đọc trước)

| Mục | Giá trị |
|---|---|
| Worktree | `C:\code\data-phase10` |
| Branch | `phase-10-dimensional-model`, tạo từ `origin/develop` (`673173b`) rồi merge `origin/phase-9-tiki-category-html` → `92386ac`. **Chưa push.** |
| Python | `C:\code\data\.venv\Scripts\python.exe` (Python 3.12.13, pyspark 4.0.4). Dùng chung venv với repo chính. |
| Mốc test | `C:\code\data\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider` chạy từ `C:\code\data-phase10`: **1179 passed, 11 deselected**, mất 14 phút 19 giây. Spark test chạy `local[1]` qua fixture `spark` trong `tests/spark_support.py:72`. |
| Image Spark | `docker/spark-marketplace/Dockerfile` (`apache/spark:4.0.1`, jar đóng sẵn trong `/opt/spark/jars`) |

**Ràng buộc bắt buộc:**

1. **Không đụng vào stack demo đang chạy.** Project Docker là `mp-tikihtml` (container `th-*`; port 9201, 5602, 8089, 3001, 3101, 9011, 5434, 9192, 6380). Stack đó chạy từ một worktree tạm khác, dùng để demo cho giáo viên.
   - Không `docker compose down` / `stop` project này.
   - Không rebuild image `ecommerce/spark-marketplace:4.0.1` hoặc `ecommerce/marketplace-python:1` dưới cùng tag, vì demo đang dùng các tag đó.
   - Khi cần chạy end-to-end, dùng project và port riêng, ví dụ thêm `env/phase10.env` với `COMPOSE_PROJECT_NAME=mp-phase10`, `MP_CONTAINER_PREFIX=p10-`, port khác mọi file `env/*.env`. Image dùng tag mới, ví dụ `ecommerce/spark-marketplace:4.0.1-delta`.
2. **Không đụng stack `mp-live`** (dữ liệu live từ 10/04), cũng không đụng `mp-mlbench`.
3. **Commit:** đứng tên người dùng, **không** có dòng `Co-Authored-By`. Commit nhỏ, mỗi commit một việc; test đi trước hoặc đi cùng code. Mỗi phase là một PR riêng vào `develop`. Chỉ push hoặc mở PR khi người dùng yêu cầu.
4. **Repo public:** không commit dữ liệu crawl thô (HTML Tiki, dump Silver).
5. **Trao đổi với người dùng bằng tiếng Việt.** Báo cáo xuất ra file HTML hoặc MD trên máy, không dùng claude.ai artifact.
6. **Đặt `spark.driver.memory` tường minh** cho batch. Ngày 24/10, batch chạy với heap mặc định 1 GB đã bị `OutOfMemoryError`; demo đang tạm vá bằng `PYSPARK_SUBMIT_ARGS="--driver-memory 3g pyspark-shell"`.

**Tài liệu cần đọc:**
- `docs/DATA_MODEL.md`: contract và 10 mart hiện có;
- `docs/PROGRESS.md` §4c–4d: thiết kế star đã chốt từ trước;
- `batch_layer/marketplace_marts.py`, `marketplace_warehouse.py`, `marketplace_postgres.py`;
- `config/quality_rules.py`;
- `docs/evidence/fullstack-run-20261024/bao-cao-chay-thu.html`: số đo của lượt chạy 8 giờ. Bản này nằm ở worktree `C:\code\data` vì chưa commit.

---

## 1. Vấn đề

Batch layer của marketplace (`batch_layer/marketplace_marts.py:223`) sinh 10 mart tổng hợp, nối với nhau bằng natural key. Không có lớp mô hình dữ liệu nào ở giữa:

- **Không có dim, không có fact:** grain chỉ được ngầm định trong từng mart; không có surrogate key, không có SCD, không có bus matrix.
- **Thuộc tính mô tả chỉ có ở `offer_current`:** `product_title`, `brand`, `category_path` không được lưu theo lịch sử, nên khi giá trị đổi thì bản cũ mất.
- **Mỗi run xử lý lại toàn bộ** Silver (`marketplace_warehouse.py:123`, `observations_as_of`). Thời gian batch tăng từ 1,0 lên 5,1 phút trong 8 giờ.
- **Silver lưu mỗi observation thành một file JSON:** khoảng 160.000 file sau 8 giờ.
- **Không có registry dataset.** Tên 10 dataset bị hard-code ở:
  - `config/quality_rules.py:27` (`GOLD_DATASETS`)
  - `batch_layer/marketplace_postgres.py:18–30` (`DATASET_COLUMNS`, `DATASETS`)
  - `batch_layer/marketplace_warehouse.py:197,201`
  - `scripts/init_postgres.sql:209–290`
  - `display/superset/datasources.yaml:27–45`, `create_marketplace_batch_dashboard.py:6`, `create_marketplace_quality_dashboard.py:12`
  - `ops/evaluate.py:302–304`, `ops/evidence.py:143–145`, `ops/drills.py:396,432,1066`
  - các test `test_marketplace_postgres.py:133`, `test_marketplace_quality.py:113`, `test_marketplace_manifest.py:65,101`, `test_marketplace_warehouse.py:68,144`, `test_marketplace_anomaly.py:303`

  `ops/validate.py` và `ops/backup.py` thì dùng chung, chúng đọc danh sách dataset từ manifest.

## 2. Mục tiêu

1. **Dựng star schema Kimball ở Gold:** mỗi fact có câu mô tả grain; dùng conformed dimension, surrogate key deterministic, SCD2, và unknown member `-1`.
2. **Dựng mart BI từ fact, không đọc thẳng Silver.** Giữ nguyên 10 bảng `cache.marketplace_*` (cùng tên, cùng cột) để dashboard, `ops/*` và test không gãy.
3. **Quality gate kiểm cả mô hình:** toàn vẹn tham chiếu, grain duy nhất, SCD2 hợp lệ, đối soát giữa các fact.
4. **Chạy incremental bằng Delta Lake;** Silver gom thành Parquet.
5. **Tài liệu đủ để bảo vệ luận văn:** ERD, bus matrix, grain, lý do của từng lựa chọn.

**Ngoài phạm vi:** ghép sản phẩm giữa sàn, thêm sàn mới, ML, `fact_listing_lifecycle`.

## 3. Thiết kế mô hình

### 3.1 Bus matrix

| Quy trình \ Dim | `dim_date` | `dim_marketplace` | `dim_product_listing` | `dim_seller` | `dim_category` | `dim_offer_status` |
|---|:-:|:-:|:-:|:-:|:-:|:-:|
| `fact_offer_observation` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `fact_price_snapshot_daily` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `fact_price_change` | ✓ | ✓ | ✓ | ✓ | ✓ | |
| `fact_counter_daily` | ✓ | ✓ | ✓ | | | |
| `fact_crawl_attempt` | ✓ | ✓ | | | ✓ | |
| `fact_behavior_event` (Kaggle, đã có) | ✓ | | | | | |

Chỉ `dim_date` dùng chung (conformed) với star Kaggle: sản phẩm Kaggle và sản phẩm Tiki là hai tập thực thể khác nhau (`PROGRESS.md` §4c).

### 3.2 Dimension (cột chi tiết: `docs/model/star-schema.dbml`)

| Dim | Natural key | Kiểu thay đổi | Ghi chú |
|---|---|---|---|
| `dim_date` | `date_key = yyyymmdd` | tĩnh | Sinh theo khoảng ngày cố định. Có `is_weekend`, `is_double_day_sale` (1/1 … 12/12), `is_vn_holiday` (danh sách cố định trong code). Conformed với Kaggle. |
| `dim_marketplace` | `marketplace_code` | SCD1 | Gồm `platform_type`, `country`, `currency`, `first_crawled_at`. Đây là `dim_site` của `PROGRESS.md`. |
| `dim_seller` | `seller_id` | SCD1 | Gồm `first_seen_at`, `last_seen_at`, `observed_listing_count`. |
| `dim_category` + `bridge_category_ancestry` | `category_id` (node) | SCD1 / rebuild | Cây danh mục Tiki sâu 4–8 cấp, không đều. Đây là snowflake có chủ đích, kèm closure table. `category_name` cho phép rỗng (rủi ro R1). |
| `dim_product_listing` | `offer_id` + `valid_from` | **SCD2** trên `product_title`, `brand`, `category_key`, `seller_key`; SCD1 trên `source_url`, `platform_listing_id` | Gồm `valid_from`, `valid_to` (exclusive, `9999-12-31` cho bản hiện hành), `is_current`, `version`. `brand` cho phép rỗng (13,9% sản phẩm không có). |
| `dim_offer_status` (junk) | (`availability`, `active_status`, `has_discount`, `has_rating`) | tĩnh | Khoảng 30 dòng. |

**Quy tắc khoá:**
- `surrogate = xxhash64(natural_key [, valid_from])`, cách làm giống `batch_layer/warehouse_job.py:240`. Chạy lại cùng dữ liệu cho ra cùng khoá.
- Mỗi dim có một dòng `-1` "Unknown". Fact không bao giờ chứa FK `NULL`.

**Quy tắc SCD2:**
- Sắp observation của từng `offer_id` theo `observed_at`. Mỗi khi tổ hợp thuộc tính được theo dõi đổi giá trị thì mở một version mới, với `valid_from = observed_at` của lần quan sát đó.
- Nếu thuộc tính đổi rồi quay về giá trị cũ thì vẫn tính là version mới.
- Nếu hai lần quan sát cùng `observed_at`, phá hoà theo `fetched_at` rồi `observation_id`, giống `marketplace_marts._ordered`.
- `fact_offer_observation.listing_key` trỏ tới version đang hiệu lực tại `observed_at`.

### 3.3 Fact

| Fact | Loại | Grain | Thay thế hoặc là nguồn của |
|---|---|---|---|
| `fact_offer_observation` | Transaction | một lần crawler nhìn thấy một listing tại một thời điểm | nguồn cho mọi mart giá |
| `fact_price_snapshot_daily` | Periodic snapshot | listing × ngày × tiền tệ, có ít nhất một quan sát | `offer_price_history_daily`; giá đóng/mở/min/max/avg, `in_stock_ratio` |
| `fact_price_change` | Transaction (dẫn xuất) | một lần giá khác lần quan sát liền trước | `offer_change_daily` dẫn xuất từ fact này |
| `fact_counter_daily` | Periodic snapshot | listing × ngày × tên bộ đếm | `counter_delta_daily` (giữ quy tắc không clamp giá trị âm hiện có) |
| `fact_crawl_attempt` | Transaction | một lần thử crawl một trang | `crawl_reliability_daily`, `source_coverage_daily` |

### 3.4 Mart (Postgres `cache.*`)

- **10 mart hiện có:** giữ nguyên tên và cột, dựng lại từ fact và dim.
- **3 mart mới:**
  - `category_price_index_daily`: median giá theo danh mục, ngày đầu là mốc 100; dùng bridge để cộng dồn theo nhánh danh mục.
  - `listing_price_volatility`: độ lệch chuẩn, hệ số biến thiên và số lần đổi giá trong cửa sổ 7 và 30 ngày.
  - `discount_daily`: tỷ lệ sản phẩm đang giảm giá và phân phối mức giảm, theo danh mục.

### 3.5 Lưu trữ

- **Gold trên MinIO, dạng Delta Lake:**
  - `gold/marketplace/dw/<table>/` cho star, cập nhật bằng `MERGE`;
  - `gold/marketplace/runs/run_id=<id>/marts/<mart>/` cho mart, giữ cơ chế run-scoped cùng manifest và pointer như hiện nay.

  Manifest v2 ghi phiên bản Delta (`version`) của từng bảng star mà run đó đọc, để replay đúng trạng thái.
- **Postgres:** schema `dw` giữ bản sao star (DDL lấy từ `docs/model/star-schema.sql`), schema `cache` giữ mart. Publish vẫn trong một transaction như `marketplace_postgres.publish`.

## 4. Thay đổi kỹ thuật

1. **Registry** `batch_layer/marketplace_datasets.py`. Với mỗi dataset, ghi: tên, `kind` (`dim` / `fact` / `bridge` / `mart`), grain, khoá, cột partition, cột Postgres, schema Postgres. Mọi chỗ ở mục 1 đọc từ registry; một test đối chiếu `scripts/init_postgres.sql` và `datasources.yaml` với registry.
2. **Delta Lake:**
   - Host: thêm `delta-spark==4.0.0` vào `requirements.txt`; fixture test dùng `configure_spark_with_delta_pip`, hoặc jar đóng sẵn nếu máy không có mạng.
   - Image: thêm `io.delta:delta-spark_2.13:4.0.0` và `io.delta:delta-storage:4.0.0` vào `/opt/spark/jars` bằng cách tải về có kiểm checksum như các jar Kafka hiện có, rồi đặt tag image mới.
   - Bật `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` và `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`.
3. **Gom Silver thành Parquet:** job `batch_layer/marketplace_silver_compact.py` ghi mỗi giờ (`observed_date`, `hour`) thành một bảng Delta `silver/marketplace/offer_observations_delta/`. Batch đọc bảng này. JSON vẫn là bản gốc idempotent, không đổi.
4. **Incremental:**
   - watermark là `max(observed_at)` đã nạp, lưu trong manifest;
   - `fact_offer_observation`: `MERGE` theo `observation_id`;
   - fact theo ngày: tính lại các ngày bị ảnh hưởng, ghi bằng `replaceWhere`;
   - SCD2: chỉ tính lại các `offer_id` có quan sát mới, rồi `MERGE` vào `dim_product_listing` (đóng version cũ, mở version mới).

   Run đầy đủ (`--full-rebuild`) vẫn dùng được cho backfill và replay.
5. **Quality** (thêm vào `config/quality_rules.py` và `batch_layer/marketplace_quality.py`):
   - **Mandatory:**
     - toàn vẹn tham chiếu (mọi FK của fact có trong dim);
     - grain duy nhất cho từng fact;
     - SCD2: mỗi `offer_id` có đúng một `is_current`, các khoảng không chồng nhau và liền mạch;
     - đối soát: tổng `observation_count` của `fact_price_snapshot_daily` bằng số dòng `fact_offer_observation` theo từng ngày.
   - **Advisory:** tỷ lệ FK trỏ vào dòng `-1` vượt ngưỡng; mart dựng từ fact khác mart cũ (chỉ trong lúc chuyển đổi).

## 5. Các phase: việc cần làm và tiêu chí nghiệm thu

Thứ tự trong mỗi phase: test viết trước hoặc cùng lúc, code, chạy toàn bộ suite (không được thấp hơn mốc 1179 passed), rồi commit.

### P0. Tài liệu thiết kế (cần người dùng duyệt trước khi code)
- Viết lại phần marketplace của `docs/DATA_MODEL.md`:
  - ERD bằng Mermaid, chép từ `docs/model/star-schema.mmd`;
  - bus matrix;
  - câu mô tả grain cho từng fact;
  - quy tắc khoá và SCD2;
  - bảng ánh xạ từ mart cũ sang fact/dim;
  - lý do của từng lựa chọn: vì sao star; vì sao snowflake riêng cây danh mục; vì sao SCD2 chứ không phải SCD1; vì sao surrogate deterministic.
- Sửa câu "Dimensions are SCD Type 1" của phần Kaggle cho đúng phạm vi áp dụng.
- **Nghiệm thu:** người dùng duyệt tài liệu.

### P1. Registry và dimension
- **File mới:** `batch_layer/marketplace_datasets.py`, `batch_layer/marketplace_dimensions.py` (`build_dim_date`, `build_dim_marketplace`, `build_dim_seller`, `build_dim_category`, `build_bridge_category_ancestry`, `build_dim_product_listing`, `build_dim_offer_status`), `batch_layer/marketplace_keys.py` (surrogate key, unknown member).
- **Chuyển các chỗ hard-code ở mục 1 sang đọc registry.** Chưa đổi hành vi.
- **Test:**
  - `tests/test_marketplace_datasets.py`: registry khớp SQL, khớp YAML, khớp với `GOLD_DATASETS`.
  - `tests/test_marketplace_dimensions.py` với các tình huống SCD2: đổi tên; đổi brand từ rỗng sang có giá trị; đổi người bán; A → B → A; hai quan sát cùng thời điểm; listing chỉ có một quan sát.
  - Cây danh mục 4 cấp và 8 cấp, closure đầy đủ.
  - Chạy hai lần cho ra cùng khoá.
- **Nghiệm thu:** test pass. Chạy trên Silver thật (đọc bằng mount chỉ-đọc hoặc bản copy, **không** ghi vào bucket của demo) và báo số dòng từng dim, số listing có từ 2 version trở lên.

### P2. Fact
- **File mới:** `batch_layer/marketplace_facts.py` (5 fact ở mục 3.3).
- **Test:** `tests/test_marketplace_facts.py`, gồm grain duy nhất, lookup FK theo đúng version SCD2, đối soát snapshot với observation, `fact_price_change` khớp `offer_change_daily` cũ trên cùng fixture.
- **Nghiệm thu:** chạy trên dữ liệu thật và báo số dòng cùng kết quả đối soát.

### P3. Delta, Gold, quality, manifest
- Thêm Delta (mục 4.2) cho host và cho image tag mới.
- Ghi `gold/marketplace/dw/*` bằng `MERGE`; manifest v2 (`batch_layer/marketplace_manifest.py`, tăng `manifest_schema_version` và giữ đọc được v1).
- Thêm các rule ở mục 4.5.
- **Test:** cố tình làm hỏng FK, tạo SCD2 chồng thời gian, tạo trùng grain: gate phải chặn.
- **Nghiệm thu:** một run đầy đủ trên stack `mp-phase10` riêng ra PASS.

### P4. Serving
- DDL schema `dw` thêm vào `scripts/init_postgres.sql`, lấy từ `docs/model/star-schema.sql` và thêm dòng unknown.
- `marketplace_postgres.py` publish cả `dw.*` và `cache.*` trong một transaction.
- Dựng lại 10 mart cũ từ fact; thêm 3 mart mới.
- Superset: thêm dataset cho `dw.*`, dashboard "Star schema" (các chart đọc dim/fact), và datasource cho 3 mart mới.
- **Nghiệm thu:**
  - mart cũ dựng từ fact khớp từng dòng với mart cũ trên cùng `as_of`; chỗ nào khác thì ghi lại lý do;
  - 13 chart của dashboard `marketplace-batch-gold` vẫn có dữ liệu trên stack `mp-phase10`.

### P5. Incremental và gom Silver
- `marketplace_silver_compact.py`; watermark; SCD2 incremental; tuỳ chọn `--full-rebuild`.
- **Test:** chạy incremental hai lần liên tiếp cho kết quả giống một lần full rebuild.
- **Nghiệm thu:** trên stack riêng, sau khoảng 24 giờ dữ liệu, thời gian một lượt batch không tăng theo tổng lượng Silver. So với mức 1,0 → 5,1 phút đo ngày 24/10.

### P6. Bằng chứng
- Chạy full stack riêng khoảng 8 giờ.
- Viết báo cáo HTML theo mẫu `bao-cao-chay-thu.html`: số dòng từng dim và fact, số version SCD2, thời gian từng bước, kết quả từng check.
- Cập nhật `docs/PROGRESS.md`.

## 6. Rủi ro

- **R1. Tên danh mục:** `category_path` chỉ có id. Tên có thể lấy từ slug của frontier (chỉ có cho các danh mục đang crawl) hoặc từ breadcrumb trong `__NEXT_DATA__` (chưa kiểm tra; xem `tests/fixtures/tiki_category_page_sample.html`). Nếu không lấy được thì để rỗng.
- **R2. Lịch sử SCD2 còn mỏng:** dữ liệu mới có vài giờ nên sẽ ít version thứ hai. Kiểm bằng fixture và báo số đo thật.
- **R3. Delta trên host Windows:** `configure_spark_with_delta_pip` cần tải jar từ Maven. Nếu không có mạng thì dùng jar đã tải về trong thư mục tests.
- **R4. RAM:** máy ảo Docker có 13,65 GB và demo đang dùng khoảng 10 GB. Stack `mp-phase10` chỉ bật khi cần, và khi bật thì không chạy song song với batch của demo.

## 7. Cách báo tiến độ

- Cuối mỗi phase: chạy suite, ghi lại số test, viết một mục ngắn trong `docs/PROGRESS.md`, rồi báo người dùng bằng tiếng Việt.
- Báo cụ thể: đã làm gì, số đo, việc còn treo, và cần người dùng quyết định gì.

## 8. Quyết định đã chốt (người dùng, 2026-10-24)

- **Lưu trữ:** Delta Lake (`MERGE`, time travel), theo roadmap ngày 26/7.
- **Postgres:** đẩy cả star (`dw`) lẫn mart (`cache`).
- **Phạm vi:** chỉ Tiki; `dim_marketplace` vẫn thiết kế cho nhiều sàn.
- **`fact_listing_lifecycle`:** để phase sau, làm khi thêm Chợ Tốt.
