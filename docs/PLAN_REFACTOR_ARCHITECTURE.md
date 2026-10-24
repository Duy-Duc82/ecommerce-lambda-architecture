# Plan refactor kiến trúc: pipeline ELT đa nguồn (Tiki + Lazada), gọn và đúng pattern

Ngày 2026-10-24. Trạng thái: **giao cho session thực thi.**

Plan này đi trước, và là nền cho:
- `docs/PLAN_PHASE10_REVISED.md`: ELT, thu thập dài hạn, Airflow;
- `docs/PLAN_PHASE10_DIMENSIONAL_MODEL.md`: star schema.

Phần star schema (dim/fact, SCD2, Delta) **không** thuộc plan này. Ở đây chỉ chừa chỗ cho nó trong khung stage.

---

## 0. Bàn giao (session thực thi đọc trước)

| Mục | Giá trị |
|---|---|
| Repo | `C:\code\data` (git). Worktree nguồn: `C:\code\data-phase10`, branch `phase-10-dimensional-model`, HEAD `53a94bf`, chưa push. |
| Worktree làm việc | **Tạo mới:** `git -C C:\code\data worktree add C:\code\data-refactor -b refactor/architecture phase-10-dimensional-model`. Commit đầu tiên chép file plan này và `PLAN_PHASE10_REVISED.md` sang `docs/`. |
| Python | `C:\code\data\.venv\Scripts\python.exe` (3.12.13, pyspark 4.0.4, Java có sẵn; Spark test chạy `local[1]`). |
| Mốc test | Chạy từ worktree: `C:\code\data\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider`, kết quả **1179 passed, 11 deselected**, khoảng 14 phút. **Chạy lại mốc này ở worktree mới trước khi sửa gì**, rồi ghi lại số passed và skipped. |
| Docker | Docker Desktop (WSL2, 14 GB). Hiện không stack nào chạy: `mp-tikihtml` đã `stop` ngày 24/10. Image `mcr.microsoft.com/playwright/python:v1.55.0-noble` đã kéo về. |
| Bằng chứng probe | `C:\Users\Admin\AppData\Local\Temp\claude\C--code-data\2b60680c-bddc-45c9-bb6d-5a850e04dd30\scratchpad\probe\` chứa script `probe_marketplaces.py` và `probe_xhr.py`, cùng output (HTML Tiki/Lazada, JSON XHR Lazada). Dùng để cắt fixture nhỏ. |

**Ràng buộc bắt buộc:**

1. **Không đụng các stack khác.** Không `down`, `stop` hay `rm` các project `mp-tikihtml`, `mp-live`, `mp-bench`, `mp-mlbench`, `mp-demo`, và không xoá volume của chúng. Không build đè các tag image `ecommerce/marketplace-python:1`, `ecommerce/spark-marketplace:4.0.1`, `ecommerce/spark-warehouse:3.5.1`. Image mới dùng tag mới (mục 5, R8).
2. **Commit:** commit nhỏ, mỗi commit một việc, đứng tên người dùng (git config sẵn). **Tuyệt đối không có dòng `Co-Authored-By`.** Không push, không mở PR. Không commit dữ liệu crawl thô. Fixture phải nhỏ (≤ 5 sản phẩm), đã cắt bớt, không có PII.
3. **Crawl lịch sự:** tôn trọng robots.txt, không stealth, không vượt captcha hay anti-bot. Tiki: ≥ 2 giây/request. Lazada: ≥ 5 giây/trang, 1 tab. Gặp trang chặn thì mở circuit, không thử lại dồn dập.
4. **Báo cáo bằng tiếng Việt**, viết kỹ thuật (thư viện, hàm, cách làm), không dùng phép so sánh ví von. Không dùng claude.ai artifact.
5. **Sau E2E (R9) phải `docker compose -p mp-refactor stop`.** Người dùng muốn máy được nghỉ.
6. Không làm phần **Shopee** ngoài những gì ghi ở mục 3.6.
7. Mỗi phase xong thì suite phải xanh rồi mới sang phase sau. Nếu phải bỏ một test, ghi tên và lý do vào `docs/REFACTOR_REPORT.md`.

---

## 1. Hiện trạng: vấn đề cần sửa

Kết quả audit ngày 24/10. Trích dẫn file:dòng theo HEAD `53a94bf`.

### 1.1 Cấu trúc và phân tầng

- **Không có packaging.** Không có `pyproject.toml`, `setup.py`, `setup.cfg`, `tox.ini` hay Makefile.
  - 13 file dùng `sys.path.insert`, ví dụ `crawler/runner.py:19`.
  - `display/` và `observability/` không có `__init__.py`.
  - `pytest.ini` không có `testpaths`, nên các script `batch_layer/test_ml_*.py` bị pytest gom nhầm.
- **`config/` thực chất là tầng domain**, nằm lẫn với settings:
  - `marketplace_schema.py` (620 dòng) chứa entity, invariant và factory ID;
  - các catalog dataset và quality cũng nằm ở đây;
  - `settings.py` có khoảng 120 biến cho mọi tầng, chạy `load_dotenv` và validate ngay khi import.
  - `config/storage.py:34` đọc lại env và gọi `load_dotenv` lần nữa.
- **Phụ thuộc vòng và sai chiều:**
  - `common` ↔ `config` phụ thuộc lẫn nhau: `common/object_store.py:16` đọc settings, còn `config/marketplace_schema.py:13` import `common.identity`.
  - `common/dlq.py:41` phụ thuộc topic của domain.
  - Crawler import ngược vào ingestion: `crawler/reparse.py:232` dùng `data_ingestion.marketplace_silver_sink`, `crawler/service.py:176` dùng producer.
  - Module chung import thẳng site cụ thể: `runner.py:32`, `reparse.py:364`.
- **Code Kaggle cũ trộn với marketplace** trong `data_ingestion/` (`producer.py`, `es_indexer.py`, `BEHAVIOR_EVENT_SCHEMA`), `config/schema.py`, `batch_layer/` (`warehouse_job.py`, `ml_job.py`, `analytics/`), `speed_layer/speed_layer.py`, `serving_layer/` (toàn bộ), `display/` (3 file), `scripts/` (3 ps1 và 1 sql), `ML_Pipeline.ipynb` và `notebooks/`.
- **6 file `.pyc` bị track trong git.**

### 1.2 Ingestion

- **Adapter có hai thế hệ.** Trong `crawler/base.py:36-38,173-270`, các phương thức legacy `fetch_listing`, `parse_product`, `crawl` là **abstract**. Ngược lại, interface đang dùng thật (`fetch_listing_page`, `parse_listing_page`) chỉ `raise NotImplementedError`.
- **Crawler chuẩn hoá ngay lúc crawl (ETL):**
  - `ParsedListingPage.observations` bắt buộc là `MarketplaceObservationV1` (`contracts.py:256`);
  - wire decoder từ chối mọi key lạ.
  - Hệ quả: trang Tiki có 133 trường/sản phẩm nhưng chỉ khoảng 17 trường được giữ.
- **`FetchResult` chỉ chứa một HTTP response** (`contracts.py:176`), và `raw_store` chỉ ghi một `body.bin` không nén. Cách này không chứa được kết quả trình duyệt (HTML cộng nhiều JSON XHR).
- **Registry site bị lặp ở 6 chỗ:** `runner.py:40`, `reparse.py:366`, `ops/backup.py:563`, `seed_frontier.py:28`, `config/counter_semantics.py:15`, `batch_layer/marketplace_dimensions.py:39`.
- **Bug:**
  - (a) `data_ingestion/marketplace_silver_sink.py:27,42` hard-code đường dẫn Silver, không đọc `MARKETPLACE_SILVER_DATASET`.
  - (b) `runner.py:82` tạo adapter mới cho mỗi task, và constructor (`base.py:67,104`) tải robots.txt bằng `requests.get` với timeout cứng.
  - (c) `acquisition.py:142` gọi `request_url()` ngoài `try`, nên `ValueError` thoát ra ngoài thay vì thành report FAILED.
  - (d) `tiki.py:116` có `except Exception: raise` vô nghĩa.
- **Helper validate bị viết lại khoảng 9 lần** (`_require_text`, `_utc`…). Logic lặp hàng giữa `tiki.py:309-361` và `tiki_html.py:95-136` gần như y hệt.

### 1.3 Batch và speed

- **`_run_marketplace_warehouse` (`batch_layer/marketplace_warehouse.py:235-358`) là một hàm 120 dòng**, giữ lock suốt toàn bộ:
  - các stage truyền DataFrame trong bộ nhớ cho nhau, nên không tách ra chạy riêng được;
  - trạng thái audit là một state machine ngầm, rải khắp hàm;
  - repository được tạo bằng `from_settings()` bên trong hàm, không inject được.
  - URI của Gold run được tính ở 3 chỗ.
  - `MarketplaceBatchContext` gánh tham số của mọi stage.
- **Batch và speed lặp logic, và đã lệch nhau:**
  - validation (`marketplace_warehouse.py:139` và `marketplace_speed_layer.py:73-87`);
  - thứ tự sắp xếp và tie-break: batch dùng `(observed_at, fetched_at, observation_id)`, speed dùng `(event_time, observation_id)`;
  - change rules (`marketplace_marts.py:60-72` và `marketplace_change_rules.py:107-124`);
  - freshness và stale dùng hai setting khác nhau.
- **Check `observation_id_unique` không bao giờ fail được,** vì nó chạy sau khi đã dedup (`marketplace_quality.py:203`, `marketplace_warehouse.py:267,277`).
- **Có 4 chỗ tạo SparkSession, cấu hình theo 3 cách khác nhau.** Chuỗi JDBC URL được dựng 3 lần; lambda `psycopg2.connect` bị chép lại.
- **Có action Spark nằm bên trong các hàm "pure":** `count` và `collect` trong `flatten_*`, `deduplicate_*`, `build_source_coverage_daily`, `build_counter_transitions`.
- **Import và setting thừa:**
  - `marketplace_warehouse.py:28`: import `MARKETPLACE_PERCENTILE_ACCURACY` nhưng không dùng, trong khi `marketplace_marts.py:177` hard-code `10000`;
  - `MARKETPLACE_STREAM_WATERMARK`: import nhưng không dùng.

### 1.4 Ops, hạ tầng, test

- **`ops/` (5.454 dòng) lặp nhiều:**
  - `Stack` có 2 bản (`drills.py:138`, `bench.py:304`); `wait_until` và `container()` cũng có 2 bản;
  - `LIVE_PROJECT` và `refuse_live()` có 3 bản, mỗi bản một luật khác nhau;
  - `CheckResult` có 2 bản;
  - pattern `postgres_connection_factory()()` bị chép khoảng 15 lần.
  - `es_projector.py` và `stub_source.py` thực chất là service, không phải công cụ vận hành.
  - `stub_source.py:38` nạp fixture từ `tests/`.
  - `__main__.py` chặn `seed` và `evaluate` trước argparse; lệnh lạ thì rơi về `status`.
- **Compose:**
  - service legacy và marketplace nằm chung một file;
  - key mã hoá của Kibana bị hard-code;
  - `stream.override.yml` (ghi là "không commit" nhưng đã commit) chứa bản vá heap thuộc về file chính;
  - `env/tiki-html.env` trùng port với `env/bench.env`;
  - khối port khoảng 25 dòng bị chép qua 3 file env.
- **Requirements:**
  - `requirements.txt` còn nhánh cho Python < 3.9, có `streamlit`, `plotly`, `faker` không ai dùng, và trộn pytest vào runtime;
  - pin phiên bản nằm ở 3 chỗ khác nhau.
- **Test:**
  - 57 file nằm phẳng trong `tests/`, đặt tên lẫn lộn; chỉ có marker `drill`;
  - `spark_support.py` vẫn ghi "pyspark 3.5.1".
- **Tài liệu:**
  - `docs/` có khoảng 20.000 dòng; 9 file `PHASE_*_IMPLEMENTATION_PLAN` là lịch sử;
  - docstring trong code trích "Phase 8 plan section N".

---

## 2. Kiến trúc đích

### 2.1 Bố cục package

```
pyproject.toml                  # PEP 621, setuptools, src layout, extras: spark, browser, airflow, dev
src/ecomdata/
  domain/                       # THUẦN: không I/O, không pyspark/requests/psycopg2/kafka/playwright
    validation.py               # 1 bộ helper duy nhất: require_text, aware_utc, non_bool_int, ...
    identity.py                 # deterministic_id, make_*_id (từ common/identity.py)
    sources.py                  # SourceSpec + SourceRegistry (MỘT registry duy nhất)
    records.py                  # SourceRecord envelope (marketplace-record.v1) + wire encode/decode
    acquisition.py              # ListingPageRequest, FetchPart, FetchBundle, ExtractedPage, report, error tree
    observations.py             # MarketplaceOffer/OfferObservation/ObservationV1/ChangeV1 (từ config/marketplace_schema.py)
    ordering.py                 # MỘT quy tắc thứ tự + tie-break: (observed_at, fetched_at, observation_id)
    change_rules.py             # rule thay đổi thuần (dùng chung batch + speed)
    datasets.py                 # dataset registry (từ config/marketplace_datasets.py)
    quality_rules.py, counter_semantics.py, topics.py, dlq.py
    batch_run.py                # BatchRunStatus enum + bảng chuyển trạng thái hợp lệ
  settings/                     # dataclass frozen theo concern; load_settings(env) KHÔNG side effect lúc import
    __init__.py                 # Settings(kafka, storage, postgres, redis, es, crawl, batch, speed, quality, airflow)
  infra/                        # adapter ra ngoài, hiện thực các Protocol (port)
    object_store.py             # ObjectStore protocol + MinIO/S3/local impl, hỗ trợ gzip
    postgres.py                 # ConnectionFactory, jdbc_url() (MỘT chỗ)
    kafka.py                    # producer factory (MỘT chỗ)
    spark.py                    # SparkSessionFactory (MỘT chỗ: S3A, shuffle, driver memory, timezone UTC)
    http.py                     # HttpClient + RobotsPolicy (cache theo host) + Throttle
    browser.py                  # BrowserSession (Playwright, vòng đời theo worker)
  ingestion/                    # ứng dụng: thu thập (không import processing)
    fetchers/{base,http,browser}.py      # Strategy: Fetcher.fetch(request) -> FetchBundle
    sources/tiki/{spec,extractor,discovery}.py
    sources/lazada/{spec,extractor,discovery}.py
    pipeline.py                 # Template Method: acquire_listing_page (robots→throttle→fetch→persist→check→extract→report)
    raw_store.py                # Bronze raw: parts gzip + metadata manifest
    record_store.py             # Bronze records: JSONL.gz theo trang
    frontier.py, scheduling.py, audit.py, worker.py
    publisher.py                # Kafka marketplace.records.v1 (Decorator quanh executor)
    reparse.py                  # đọc raw → extract lại → so sánh record
    service.py, seed.py, cli.py # composition root + CLI
  processing/                   # ứng dụng: Spark (không import ingestion)
    normalize/
      common.py                 # cột offer chung + biểu thức Spark cho rule chung (sinh từ domain)
      sources/tiki.py, sources/lazada.py   # schema khai báo + map payload → cột chung + bảng rộng
    batch/
      stages/{silver,marts,quality,promote,publish}.py   # mỗi stage = class hiện thực Stage protocol
      ports.py                  # Protocol: ObjectStore, AuditRepo, QualityRepo, CachePublisher, Lock, SparkProvider
      context.py                # RunContext(run_id, as_of, paths) + config theo stage
      runner.py                 # chạy 1 stage hoặc --all; kiểm state machine
      marts.py, anomaly.py, dimensions.py, keys.py, dim_date.py, manifest.py
      repositories.py           # Postgres audit/quality/cache publisher
      lock.py, scheduler.py     # scheduler vòng lặp cũ giữ lại (profile legacy-scheduler)
      cli.py, bootstrap.py
    speed/
      stream.py, sinks.py, service.py, bootstrap.py
  orchestration/
    dags/marketplace_batch.py, dags/marketplace_maintenance.py
  serving/                      # provisioning Superset/Kibana (từ display/), es_projector
  ops/                          # CLI vận hành, drills, bench, evidence (dùng ops/stackctl.py chung)
services/stub_source/           # stub nguồn cho smoke/drill (fixture riêng, không đọc tests/)
legacy/kaggle/                  # pipeline Kaggle cũ: code, notebook, script, Dockerfile, compose riêng
docker/{python,spark,crawler-browser,airflow}/Dockerfile
compose/{docker-compose.yml, docker-compose.legacy.yml, docker-compose.observability.yml}
env/{base.env, ports.<stack>.env, <stack>.env}
tests/{unit/<package>, spark/, integration/, architecture/, legacy/, fixtures/}
docs/{ARCHITECTURE.md, adr/, archive/, ...}
```

Có thể đổi tên chi tiết nếu thấy hợp lý hơn. Nhưng **ranh giới tầng (mục 2.2)** và **"một chỗ duy nhất"** cho registry, settings, Spark session, JDBC và ordering là bắt buộc.

### 2.2 Luật phụ thuộc

Kiểm bằng `import-linter`, cấu hình trong `pyproject.toml`, và có test trong suite.

- **Layers, từ trên xuống:**
  1. `orchestration` | `ops` | `serving`
  2. `ingestion` | `processing`. Hai package này **độc lập**, không import lẫn nhau.
  3. `infra`
  4. `settings`
  5. `domain`
- **Cấm:** `ecomdata.domain` không import `pyspark`, `requests`, `psycopg2`, `kafka`, `minio`, `boto3`, `playwright`, `redis`, `elasticsearch`, `ecomdata.settings`.
- **Cấm:** `ecomdata` không import `legacy`.
- **Settings chỉ được đọc ở composition root** (`bootstrap.py`, `service.py`, `cli.py`, `dags/*`). Viết test kiến trúc tự quét import để kiểm.
- **Code riêng của từng site chỉ nằm ở** `ingestion/sources/<site>/`, `processing/normalize/sources/<site>.py` và `domain/sources.py`. Test kiến trúc quét các literal `"tiki"`, `"lazada"`, `tiki.vn`, `lazada.vn` ngoài các chỗ đó. Được phép trong tests, ops/drills, env và docs.

### 2.3 Pattern và chỗ dùng

Mục này phải có trong `docs/ARCHITECTURE.md`, kèm đường dẫn file.

| Pattern | Chỗ dùng |
|---|---|
| Ports & Adapters (hexagonal) | `processing/batch/ports.py`, `infra/*`. Logic nhận Protocol; composition root inject bản thật; test inject bản fake. |
| Strategy | `Fetcher` (`HttpFetcher`, `BrowserFetcher`; sau này `ApiFetcher`). `Extractor` theo site. Normalizer Spark theo site. |
| Registry + Factory | `SourceRegistry` (mã nguồn → `SourceSpec`: id, base URL, loại fetcher, extractor, normalizer, tiền tệ, counter semantics, discovery, PII drop list). `DatasetRegistry`. |
| Template Method | `ingestion/pipeline.py`: các bước thu thập cố định, các bước thay đổi được inject vào. |
| Pipeline of Stages + State Machine | Mỗi `Stage` khai báo `name`, `requires`, `produces`, `allowed_from: set[BatchRunStatus]`, `run(ctx) -> StageResult`. Stage idempotent, đọc và ghi dữ liệu đã lưu, không truyền DataFrame trong bộ nhớ. `BatchRunStatus` có bảng chuyển trạng thái tường minh. |
| Repository | frontier, crawl audit, batch audit, quality, cache publisher. Đều nằm sau Protocol. |
| Decorator | publisher Kafka bọc executor; `non_decreasing_clock`. |
| Value Object | dataclass `frozen=True`, validate bằng một helper chung `domain/validation.py`. |
| Composition Root | mỗi entrypoint có một `bootstrap.py`. Không gọi `from_settings()` trong logic. |

---

## 3. Thiết kế chi tiết các thay đổi chức năng

### 3.1 Record envelope (`marketplace-record.v1`): chuyển sang ELT

`SourceRecord` gồm các trường:

| Trường | Nội dung |
|---|---|
| `schema_version` | `"marketplace-record.v1"` |
| `record_id` | `deterministic_id(raw_artifact_id, record_kind, record_index)` |
| `source_code` | lấy từ registry |
| `marketplace_id` | lấy từ registry |
| `record_kind` | `listing_row` hoặc `page_context` |
| `record_index` | thứ hạng hiển thị trên trang |
| lineage | `raw_artifact_id`, `raw_uri`, `body_sha256`, `crawl_run_id`, `adapter_version`, `extractor_version`, `request_target`, `request_page`, `fetched_at`, `http_status` |
| `payload` | object JSON nguyên vẹn của dòng |
| `dropped_fields` | danh sách tên trường PII đã bỏ |

Quy tắc:

- **Extractor chỉ tìm mảng dòng** và tách `page_context`. Không ép kiểu, không chọn trường, không lọc quảng cáo (quảng cáo được đánh dấu ở Silver).
- **Nguồn dữ liệu theo site:**
  - Tiki: `__NEXT_DATA__` → `props.initialState.catalog.data`. `page_context` gồm `categoryData`, `filters`, `sellers`, `paging`, `sort_options`, `amplitude`, `metaTitle`, `h1`.
  - Lazada: phần XHR `?ajax=true` → `mods.listItems`. `page_context` gồm `mods.filter`, `mods.breadcrumb`, `mods.sortBar`, `mainInfo`, `seoInfo`.
- **Bronze/raw:**
  - Mỗi part ghi thành `part-<n>.bin.gz`.
  - `metadata.json` (v2) liệt kê các part: `url`, `status`, `content_type`, `role` (`document` hoặc `xhr`), `sha256`, `bytes`, `encoding=gzip`.
  - Đọc được cả bản v1 (`body.bin` không nén).
  - `RawArtifact` có `parts`; `body_sha256` là sha của manifest.
- **Bronze/records:** `bronze/marketplace/records/source=<code>/observed_date=<d>/hour=<h>/raw_artifact_id=<id>.jsonl.gz`. Ghi xong file thì mới publish lên Kafka `marketplace.records.v1`.
- **Silver không còn do `silver-sink` ghi nữa.** Bỏ service `silver-sink` (đưa vào `legacy` hoặc xoá). Topic `marketplace.observations.v1` không còn producer.
- **`reparse`:** đọc raw (v1 hoặc v2) → extract lại → so `record_id` và `payload` với Bronze/records.

### 3.2 Fetcher và nguồn

**Tách vai trò của adapter cũ thành 3 thành phần:**
- `SourceSpec`: dữ liệu khai báo của nguồn.
- `Fetcher`: cách tải.
- `Extractor`: cách tìm dữ liệu trong cái đã tải.

**`HttpFetcher`:**
- dùng `HttpClient` inject vào;
- `RobotsPolicy` tải lười và cache theo host, có TTL;
- `Throttle` tách riêng theo host.

**`BrowserFetcher`:**
- `BrowserSession` được tạo **một lần cho mỗi worker** và đóng khi nhận signal dừng;
- mỗi task mở một context hoặc page mới;
- bắt các response khớp `capture_patterns` của spec (`page.on("response")`);
- chờ theo `wait_for` của spec, có timeout;
- trả về `FetchBundle`, gồm HTML của trang cộng các XHR đã bắt.
- Không stealth. Phát hiện trang chặn bằng regex khai báo trong spec → `FetchBlockedError` → `FailureKind.BLOCKED` → mở circuit.

**Tiki:**
- `HttpFetcher`, trang category chỉ lấy `page=1`.
- `discovery`: đọc `https://tiki.vn/sitemap_categories*.xml` → danh sách category (khoảng 4.200; kiểm robots trước).
- Lệnh `seed --source tiki --from-discovery [--limit N] --tier SWEEP|HOT`.

**Lazada:**
- `BrowserFetcher`, URL `https://www.lazada.vn/<category>/?page=N`, `capture_patterns=[r"[?&]ajax=true"]`.
- Có phân trang (`page=N` cho dữ liệu khác nhau).
- `discovery` bước đầu là danh sách category cấu hình sẵn: `env/sources/lazada_categories.json`, khoảng 20 category lớn.

**Shopee (mục 3.6):** chỉ dừng ở mức có interface `ApiFetcher` trong thiết kế, không hiện thực.

**Worker:**
- `crawl-worker` nhận `--source <code>` (hoặc `--sources a,b`); lease frontier lọc theo `marketplace_id`.
- Image trình duyệt riêng (`docker/crawler-browser`, nền `mcr.microsoft.com/playwright/python:v1.55.0-noble`) chạy worker cho Lazada.
- Worker HTTP dùng image python thường.

### 3.3 Silver bằng Spark và normalize dùng chung

**`processing/normalize/sources/<site>.py`**

Mỗi site có:
- `PAYLOAD_SCHEMA`: một `StructType` khai báo **đủ** các trường đã thấy. Tiki có 133 trường lá, Lazada có 55.
- `to_wide(records_df)`: trả bảng rộng của site, giữ thêm cột `payload_json` gốc.
- `to_offer_observation(wide_df)`: trả về các **cột canonical hiện có** của Silver. Đây là các cột mà `marts.py` đang đọc, gồm `observation_id` và `offer_id`, sinh theo quy tắc cũ cho Tiki.
- Các cột mới dùng chung: `source_code`, `seller_name`, `brand_id`, `category_ids`, `sku`, `search_rank`, `is_advertisement`, `quantity_sold`, `location`…

**Quy tắc chung (`normalize/common.py`):**
- ép kiểu tiền về `decimal(38,6)`;
- chuẩn hoá chuỗi (`trim`, NFC qua UDF nếu cần);
- tiền tệ lấy từ registry;
- áp quy tắc thứ tự của `domain/ordering.py`;
- dòng không hợp lệ (thiếu id hoặc giá, giá âm) vào `quarantine` kèm `reason`.

**Stage `silver`:**
- **Đầu vào:** Bronze/records của khoảng `[watermark, as_of)`.
- **Ghi:**
  - `silver/marketplace/offer_observations_v2/` (Parquet, partition `source_code`, `observed_date`; `partitionOverwriteMode=dynamic`);
  - `silver/marketplace/<source>_listing_wide/`;
  - `silver/marketplace/page_context/`;
  - `silver/marketplace/quarantine/`.
- **Báo cáo drift:** key có trong payload nhưng không có trong `PAYLOAD_SCHEMA` (dùng `json_object_keys` hoặc tương đương), kèm tần suất. Ghi báo cáo vào manifest.
- **Dedup:** **trước khi** dedup phải đếm trùng `observation_id` và trùng nội dung, rồi ghi số liệu. Check quality `observation_id_unique` đọc số liệu này để không còn vô nghĩa.

**Speed** đọc `marketplace.records.v1` và dùng cùng normalizer cho mỗi micro-batch. Change rules thì gọi `domain/change_rules.py`.

### 3.4 Batch theo stage và Airflow

**Thứ tự stage:**

```
silver → marts → quality → promote → publish
```

Stage `dims` và `facts` của Phase 10 sẽ cắm vào sau `silver`, chưa làm ở plan này.

**Yêu cầu:**
- Mỗi stage đọc đầu vào từ storage: Silver v2, run-scoped Gold `gold/marketplace/runs/run_id=<id>/`, và manifest. Không nhận DataFrame trong bộ nhớ.
- **Trạng thái run:** `CREATED → SILVER_BUILT → GOLD_WRITTEN → QUALITY_PASSED | QUALITY_FAILED → PUBLISHED → PROMOTED`, cùng `HELD` và `FAILED`.
  - Stage kiểm `allowed_from` trước khi chạy.
  - Chạy lại một stage đã xong thì là no-op, trừ khi có `--force`.
  - `expected_current_run_id` (cho thao tác CAS pointer) được lưu vào audit row lúc tạo run.
- **Đổi thứ tự `promote` và `publish`:** hiện tại publish đặt `SUCCEEDED` rồi mới promote. Giữ đúng ngữ nghĩa cũ; nếu đổi thì ghi lại lý do.
- **Lock:**
  - mỗi stage giữ advisory lock trong lúc chạy;
  - không có hai run chạy song song: Airflow đặt `max_active_runs=1`, còn CLI dựa vào state machine.
- **CLI:**
  - `python -m ecomdata.processing.batch run --run-id … --as-of … --stage <name>|--all [--resume] [--allow-backfill] [--quality-only]`;
  - `--all` chạy tuần tự trong một process và **phải cho cùng kết quả** với chạy từng stage riêng.
- **Sửa phần cấu hình:**
  - SparkSession chỉ tạo qua `infra/spark.py`, đặt `spark.driver.memory` tường minh (mặc định `3g`; bỏ bản vá trong `stream.override.yml`);
  - percentile accuracy đọc từ setting;
  - bỏ import thừa.

**Airflow:**
- **Image:** `docker/airflow` dựa trên `apache/airflow:<bản 3.x mới nhất ổn định có thật trên Docker Hub; ghi tag cụ thể>`, thêm `apache-airflow-providers-docker`.
- **Compose:** profile `orchestration` gồm `airflow-init`, `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`, (cùng `airflow-triggerer` nếu bản 3.x cần).
  - `LocalExecutor`;
  - metadata DB là database `airflow` trên `postgres-dw` (tạo trong init SQL hoặc ở `airflow-init`);
  - mount `/var/run/docker.sock`.
- **DAG `marketplace_batch`:**
  - `schedule="@hourly"`, `catchup=False`, `max_active_runs=1`, `retries=2`;
  - mỗi stage là một task `DockerOperator`, image Spark tag mới, network của stack, env từ file;
  - `as_of = data_interval_end`; `run_id` theo `run_id_for(as_of)` hiện có;
  - param `allow_backfill` mặc định `false`.
- **DAG `marketplace_maintenance`** (`@weekly`): xoá Bronze/raw cũ hơn N ngày (cấu hình được), `ops backup`, `ops storage-snapshot`.
- **DAG `source_discovery`** (`@daily`): chạy discovery của Tiki → seed tầng `SWEEP`.
- **Scheduler vòng lặp cũ** chuyển sang profile `legacy-scheduler`; giữ test của nó.

### 3.5 Ops, hạ tầng, test, tài liệu

**Ops:**
- `ops/stackctl.py` gom về một chỗ: `Stack`, `wait_until`, `container`, `refuse_live` (một luật duy nhất: từ chối khi project là `mp-live` **hoặc** container mang nhãn của project đó).
- `CheckResult` chỉ có một bản.
- Tách CLI `python -m ecomdata.ops` bằng argparse subcommands: lệnh lạ thì báo lỗi; `seed` và `evaluate` có trong `--help`.
- `es_projector` → `serving/`; `stub_source` → `services/stub_source/` với fixture riêng.

**Compose:**
- tách `compose/docker-compose.legacy.yml` cho Kaggle;
- `observability` để file riêng như hiện tại;
- key Kibana chuyển ra env;
- env ghép từ nhiều lớp: `env/base.env` + `env/<stack>.env`; port của mỗi stack không trùng nhau (`test_compose_isolation` kiểm việc này);
- thêm `env/refactor.env`: project `mp-refactor`, prefix `rf-`, port riêng.

**Requirements:**
- `pyproject.toml` có extras `spark`, `browser`, `airflow`, `dev`, `legacy`;
- bỏ `streamlit`, `plotly`, `faker` nếu không ai import;
- pin chỉ ở một chỗ: `requirements/*.txt` sinh từ pyproject, hoặc `constraints.txt`.
- Dockerfile cài từ đó.

**Test:**
- `tests/unit/<package>/`, `tests/spark/` (marker `spark`), `tests/integration/` (marker `integration`), `tests/architecture/`, `tests/legacy/` (marker `legacy`);
- `testpaths=["tests"]`; mặc định bỏ `drill`;
- sửa docstring "3.5.1" trong `spark_support`.

**Vệ sinh repo:**
- `git rm --cached` các `.pyc`;
- chuyển `ML_Pipeline.ipynb` và `notebooks/` sang `legacy/kaggle/`;
- file sinh tự động (`lambda-logs.json`, `marketplace_dashboards.ndjson`, `.png`) thì giữ, nhưng ghi lệnh sinh trong `docs/ARCHITECTURE.md`.

**Tài liệu:**
- **`docs/ARCHITECTURE.md` viết lại:** sơ đồ tầng, sơ đồ luồng dữ liệu (Mermaid), bảng module, bảng pattern ở mục 2.3, luật phụ thuộc, cách thêm một nguồn mới (checklist).
- **`docs/adr/`** có ít nhất 6 ADR:
  1. ELT và record envelope;
  2. đảo ngược D2 để dùng Airflow;
  3. Ports & Adapters cùng luật import;
  4. một SourceRegistry;
  5. batch theo stage cùng state machine;
  6. Shopee: không crawl, chỉ Affiliate API khi có tài khoản.
- Chuyển `docs/PHASE_*_IMPLEMENTATION_PLAN.md` sang `docs/archive/`, sửa các tham chiếu trong code cho đúng đường dẫn mới.
- `README.md` cập nhật lệnh chạy.

### 3.6 Shopee

Ghi vào ADR-6, không viết code:

- **Crawl trang:** bị chặn. Probe ngày 24/10 cho thấy chuyển hướng tới `/verify/traffic/error`.
- **Shopee Open Platform:** chỉ đọc được dữ liệu của shop đã uỷ quyền cho app, nên không dùng được cho dữ liệu toàn sàn.
- **Shopee Affiliate Open API** (GraphQL `https://open-api.affiliate.shopee.vn/graphql`, query `productOfferV2`):
  - trả về tên, giá min/max, đã bán, rating, shop, category, link;
  - cần tài khoản Shopee Affiliate được duyệt để có AppID và Secret; ký request bằng SHA256.
  - **Việc tiếp theo cần người dùng:** đăng ký Shopee Affiliate.
  - Khi có AppID, thêm `ApiFetcher` và `sources/shopee/` theo checklist "thêm nguồn".

---

## 4. Thứ tự thực hiện

Mỗi phase kết thúc khi:
- suite xanh;
- `ruff` và `lint-imports` sạch (kể từ R1);
- có commit;
- có một mục ngắn trong `docs/REFACTOR_REPORT.md`.

| Phase | Nội dung |
|---|---|
| **R0** | Tạo worktree và branch. Chép 2 plan vào worktree và commit. Chạy mốc test, ghi vào REFACTOR_REPORT. |
| **R1** | `pyproject.toml` (src layout, extras, cấu hình ruff, import-linter, pytest). Chuyển package vào `src/ecomdata/` bằng `git mv`, sửa import, bỏ `sys.path.insert`. Đưa Kaggle sang `legacy/kaggle/`. `pip install -e .[dev,spark]` vào `.venv`. Sửa Dockerfile và compose `command` (dùng `PYTHONPATH=/app/src` hoặc cài package). Hành vi không đổi. |
| **R2** | `domain/` (gom validation, identity, entity, registry nguồn, ordering, change rules, batch_run). `settings/` không có side effect khi import. `infra/` (Spark factory, JDBC, Kafka factory, object store có gzip, HTTP/robots/throttle). Bật luật import. Sửa bug (a)(b)(c)(d) ở mục 1.2, mỗi bug có test hồi quy. |
| **R3** | Ingestion: `FetchBundle`, Fetcher strategy, `SourceSpec`, Extractor, raw store v2 và record store, publisher envelope, reparse v2. Tiki chuyển sang khung mới; discovery qua sitemap. Bỏ các phương thức legacy của `SiteCrawler`. |
| **R4** | Lazada: `BrowserFetcher` + `BrowserSession`, spec, extractor, fixture nhỏ (cắt từ output probe, ≤ 5 sản phẩm, không PII). Image `crawler-browser`. Test `BrowserFetcher` bằng page giả (không cần mạng). Một test integration thật (marker `integration`) lấy 1 trang Lazada. |
| **R5** | Processing: `normalize/` (Tiki + Lazada), stage `silver`, quarantine, báo cáo drift. Test parity (AC7). |
| **R6** | Batch theo stage: ports, context, runner, state machine, tách `_run_marketplace_warehouse`. Sửa check `observation_id_unique`. Thống nhất ordering và rule giữa batch và speed. |
| **R7** | Speed đọc envelope và dùng normalize chung. Bỏ `silver-sink`. |
| **R8** | Hạ tầng: Dockerfile mới (tag `ecommerce/marketplace-python:2`, `ecommerce/spark-marketplace:4.0.1-r2`, `ecommerce/crawler-browser:1`, `ecommerce/airflow:<ver>-r1`), compose tách lớp, env ghép lớp, `env/refactor.env`, Airflow cùng 3 DAG và test DAG. Ops gom `stackctl`, CLI subcommands. Chuyển stub source ra `services/`. |
| **R9** | E2E trên `mp-refactor` (AC12). Tài liệu (AC13). Tổng kết `docs/REFACTOR_REPORT.md`. `docker compose -p mp-refactor stop`. |

---

## 5. Tiêu chí nghiệm thu

**Tất cả các tiêu chí dưới đây phải đạt.** `docs/REFACTOR_REPORT.md` có một bảng: mỗi AC kèm lệnh kiểm, kết quả và đường dẫn bằng chứng.

| AC | Tiêu chí | Cách kiểm |
|---|---|---|
| AC1 | Đóng gói chuẩn | `pip install -e .[dev,spark]` chạy được trong `.venv`. `git grep -n "sys.path.insert" -- src tests services` không ra gì. Có `src/ecomdata/**/__init__.py`. Không còn `.pyc` bị track. |
| AC2 | Luật phụ thuộc | `lint-imports` pass với các contract ở mục 2.2 (layers, independence ingestion↔processing, forbidden cho domain, cấm import `legacy`). `tests/architecture/` pass: settings chỉ được import ở composition root; literal riêng của site chỉ nằm đúng chỗ. |
| AC3 | Lint | `ruff check src tests services` sạch (bộ rule tối thiểu `E,F,I,B,UP`; ghi các ngoại lệ nếu có). |
| AC4 | Suite xanh | `pytest` toàn bộ (trừ `drill`): **0 failed, 0 error**. Số passed ≥ mốc R0 trừ các test bỏ có chủ đích (liệt kê kèm lý do). Test Spark **thật sự chạy**, không bị skip (ghi số skipped và lý do). Có thêm test cho mọi module mới. |
| AC5 | Một registry, một chỗ cấu hình | Mỗi thứ sau chỉ có đúng một định nghĩa: `SourceRegistry`, SparkSession factory, `jdbc_url`, Kafka producer factory, helper validate, quy tắc ordering. Kiểm bằng `git grep`, ghi kết quả. Thêm một nguồn giả trong test chỉ cần: spec + extractor + normalizer + đăng ký. |
| AC6 | ELT không mất dữ liệu | Với fixture Tiki và Lazada: số record `listing_row` bằng số dòng nguồn (kể cả quảng cáo). `payload` của mỗi record **bằng đúng** object JSON nguồn (so sánh sâu). Có `page_context`. Bronze/raw là gzip, đọc lại được cả v1 lẫn v2. `reparse` v2 cho kết quả `IDENTICAL`. |
| AC7 | Parity với hệ cũ | Cho cùng một fixture Tiki, 10 mart sinh từ đường mới (Bronze records → normalize → Silver v2 → marts) khớp với mart của đường cũ (canonical observations → marts) theo từng dòng, trên các cột cũ. Chỗ nào khác nhau thì phải có lý do ghi trong report (ví dụ thứ tự tie-break đã thống nhất). |
| AC8 | Batch theo stage | Mỗi stage chạy độc lập được qua CLI. Chạy `--all` và chạy từng stage riêng cho Gold, manifest và audit giống nhau (test). Có test cho mọi chuyển trạng thái hợp lệ và không hợp lệ. Chạy lại stage đã xong là no-op. `observation_id_unique` fail khi đầu vào có trùng (test). Không còn `from_settings()` trong `processing/batch/stages`. |
| AC9 | Sửa bug | Có test hồi quy cho bug (a), (b), (c) ở mục 1.2. Bug (d) đã xoá. |
| AC10 | Lazada | Có test extractor trên fixture. Có test `BrowserFetcher` với page giả: bắt đúng XHR, phát hiện trang chặn, đóng session khi dừng. Một test `integration` chạy thật lấy được ≥ 30 sản phẩm từ 1 trang category. |
| AC11 | Image và compose | 4 image tag mới build thành công; tag cũ không bị đổi (`docker image inspect` trước và sau giống nhau). `docker compose ... config` hợp lệ cho mọi profile. `test_compose_isolation` pass với `refactor.env`, và không stack nào trùng port. |
| AC12 | E2E thật trên `mp-refactor` | Xem các mục con bên dưới bảng. |
| AC13 | Tài liệu | `docs/ARCHITECTURE.md` mới (có đủ các phần ở mục 3.5). Ít nhất 6 ADR. `docs/archive/` đã chứa các plan phase cũ, và tham chiếu trong code đã sửa (`git grep "docs/PHASE_"` chỉ còn ra `docs/archive`). README cập nhật. `docs/PROGRESS.md` có mục mới. |
| AC14 | Git | Branch `refactor/architecture` có các commit nhỏ, author là người dùng, **không có `Co-Authored-By`** (`git log --format=%B \| grep -i co-authored` không ra gì), không push. Các stack và volume khác không bị đụng (so sánh `docker ps -a` và `docker volume ls` trước và sau). |

**AC12: E2E thật trên `mp-refactor`.** Cả 6 mục con phải đạt:

- (a) Crawl liên tục **≥ 60 phút**:
  - Tiki ≥ 20 category (seed từ discovery);
  - Lazada ≥ 5 category × 2 trang;
  - giữ đúng các mức politeness ở mục 0.
- (b) Bronze/raw và Bronze/records có dữ liệu của **cả hai nguồn**. Ghi số trang, số record, số byte và tỷ lệ lỗi theo nguồn. Circuit không mở, hoặc mở nhưng có giải thích.
- (c) Airflow `marketplace_batch` chạy **≥ 2 DAG run liên tiếp**, scheduled hoặc trigger với logical date phủ khoảng crawl:
  - mọi task `success`;
  - quality PASS;
  - Postgres `cache.marketplace_offer_current` có dòng của **cả `tiki` và `lazada`**;
  - dashboard Superset `marketplace-batch-gold` có dữ liệu (chụp màn hình hoặc truy vấn API).
- (d) DAG `source_discovery` chạy ít nhất 1 lần thành công. DAG `marketplace_maintenance` parse không lỗi; nếu chạy thì không xoá gì trong thời gian giữ dữ liệu.
- (e) Speed chạy **≥ 10 phút** trên `marketplace.records.v1`. Có key `rt:offer:*` trong Redis và có change event cho cả hai nguồn. DLQ bằng 0, hoặc có giải thích.
- (f) Bằng chứng nằm trong `docs/evidence/refactor-<yyyymmdd>/`: `README.md`, số đo, ảnh chụp Airflow Graph/Grid và Superset. **Không có dữ liệu thô.** Sau đó chạy `docker compose -p mp-refactor stop`.

---

## 6. Rủi ro

| | Rủi ro | Xử lý |
|---|---|---|
| K1 | Đổi tên package làm vỡ nhiều test và lệnh. | Làm R1 bằng `git mv` và sửa import hàng loạt; chưa đổi hành vi. Chạy suite ngay sau đó. |
| K2 | Lazada chặn khi chạy E2E. | Giảm nhịp, giảm số category. Nếu vẫn bị chặn thì ghi bằng chứng, giữ test bằng fixture, và báo người dùng. **Không vượt chặn.** AC12 lúc đó coi là không đạt cho Lazada, phải dừng lại hỏi người dùng. |
| K3 | Airflow 3.x với `DockerOperator` trên Docker Desktop Windows (socket, network). | Thử sớm ở R8 bằng một DAG `hello`. Nếu socket không dùng được thì dùng `BashOperator` gọi `docker` CLI trong image Airflow có docker client. Ghi ADR. |
| K4 | RAM khi chạy E2E (Spark, Airflow, Chromium, ES, Superset). | Đặt `--driver-memory 3g`. Speed và batch không chạy cùng lúc nếu thiếu RAM. Theo dõi bằng `docker stats`. |
| K5 | Parity khó đạt tuyệt đối do thống nhất tie-break. | Được phép lệch, miễn có lý do rõ ràng và test chứng minh. |

## 7. Báo cáo

- Cuối mỗi phase: thêm một mục vào `docs/REFACTOR_REPORT.md` gồm việc đã làm, số test, và các quyết định nhỏ đã tự chốt.
- Khi xong: bảng AC1–AC14, mỗi dòng ghi đạt hay không, kèm bằng chứng.
- Nếu có AC không đạt được vì lý do ngoài tầm (ví dụ bị chặn), ghi rõ và dừng lại để hỏi. Không tự hạ tiêu chí.
