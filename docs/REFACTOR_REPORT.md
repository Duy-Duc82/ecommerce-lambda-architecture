# Báo cáo refactor kiến trúc (`refactor/architecture`)

Plan: `docs/PLAN_REFACTOR_ARCHITECTURE.md`. Worktree `C:\code\data-refactor`, branch `refactor/architecture`, tạo từ `phase-10-dimensional-model`.

Mỗi phase có một mục: việc đã làm, số test, các quyết định nhỏ tự chốt. Bảng nghiệm thu AC1–AC14 ở cuối file.

---

## R0. Worktree, branch, mốc test

- `git -C C:\code\data worktree add C:\code\data-refactor -b refactor/architecture phase-10-dimensional-model`.
- **Lệch so với plan:** HEAD của `phase-10-dimensional-model` lúc tạo branch là `f991af5`, không phải `53a94bf` như ghi trong plan mục 0. Ba commit thêm vào sau `53a94bf`: `b39deac` (DATA_MODEL star schema), `67bf754` (registry Gold dataset `config/marketplace_datasets.py`), `f991af5` (các dimension của star). Refactor làm trên `f991af5`, nên các module này cũng được chuyển chỗ như phần còn lại.
- Commit đầu: chép `PLAN_REFACTOR_ARCHITECTURE.md` và `PLAN_PHASE10_REVISED.md` từ `C:\code\data-phase10\docs\` (không đụng các file chưa commit ở worktree đó).
- **Mốc test** (chạy ở worktree mới, `C:\code\data\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider -rs`):

  | | Giá trị |
  |---|---|
  | passed | **1244** |
  | skipped | 0 |
  | deselected | 11 (11 drill, marker `drill`) |
  | thời gian | 952 s |

  Số passed cao hơn mốc 1179 ghi trong plan vì branch đã có thêm test của registry dataset và dimension (hai commit nói trên). Mốc dùng cho AC4 là **1244**.
- **Trạng thái Docker trước khi làm** (để so ở AC11/AC14): `docs/evidence/refactor-20261024/baseline/`
  - `ps_before_short.txt`: 49 container, tất cả `exited`;
  - `volumes_before.txt`: 76 volume;
  - `images_before.txt`: ID của 3 tag cũ
    - `ecommerce/marketplace-python:1` = `sha256:23e5681c…`
    - `ecommerce/spark-marketplace:4.0.1` = `sha256:b81bc0fc…`
    - `ecommerce/spark-warehouse:3.5.1` = `sha256:dadc7cda…`

## R1. Đóng gói, `src/ecomdata`, `legacy/kaggle`

- `pyproject.toml` (PEP 621, setuptools, src layout): dependency ở dạng khoảng phiên bản; extras `spark`, `browser`, `airflow`, `dev`, `legacy`; cấu hình `pytest` (`testpaths=["tests"]`, `pythonpath=["."]`, marker `drill`, `spark`, `integration`, `legacy`, `airflow`; mặc định bỏ `drill` và `integration`), `ruff`, `import-linter`. `pytest.ini` bị bỏ.
- `pip install -e ".[dev,spark]"` vào `C:\code\data\.venv` thành công (cài thêm `ruff 0.17.2`, `import-linter 2.15`, `PyYAML 6.0.3`; pyspark 4.0.4, pandas, numpy giữ nguyên bản đang có).
- Chuyển chỗ bằng `git mv` (script ánh xạ `old path -> new path`, sau đó viết lại mọi import, chuỗi `monkeypatch`, lệnh `python -m` trong compose/ps1):
  - `common/`, `config/`, `crawler/`, `data_ingestion/marketplace_*`, `batch_layer/marketplace_*`, `speed_layer/marketplace_*`, `display/` (phần marketplace), `ops/` → `src/ecomdata/{domain,infra,settings,ingestion,processing,serving,ops}`;
  - Kaggle (`batch_layer/{warehouse_job,ml_job,evaluate_ml,postgres_cache,analytics}`, `data_ingestion/{producer,es_indexer}`, `config/schema.py`, `speed_layer/speed_layer.py`, `serving_layer/`, dashboard Kaggle, 3 script ps1 + 1 sql, `ML_Pipeline.ipynb`, `notebooks/`, Dockerfile Spark 3.5.1) → `legacy/kaggle/`;
  - file runtime của Superset (`superset_config.py`, `patch_runtime.py`, `setup_superset.sh`, `datasources.yaml`) → `docker/superset/` (thư mục mount, không phải module Python).
  - `BEHAVIOR_EVENT_SCHEMA` tách khỏi `processing/schemas.py` sang `legacy/kaggle/data_ingestion/schemas.py`.
- Bỏ mọi `sys.path.insert` (13 chỗ). Đường dẫn tương đối theo file sửa lại (`parents[3]` cho module trong `src/ecomdata/*/`); test dùng `tests/paths.py` (`REPO_ROOT`, `FIXTURES`).
- Test chuyển vào `tests/unit/<package>/`, `tests/spark/`, `tests/legacy/`; `tests/conftest.py` gắn marker theo thư mục.
- Compose: `command` đổi sang `python -m ecomdata....`; `PYTHONPATH=/app/src:/app:...` đặt trong `x-marketplace-env`, nên image cũ (source mount ở `/app`) chạy được code mới mà không build lại.
- `git rm --cached` 6 file `.pyc` (commit riêng). `.gitignore` thêm `*.egg-info/`, `.ruff_cache/`.
- `ruff`: bộ `E,F,I,B,UP`, `line-length=120`. Autofix + `ruff format` cho 61 file có câu lệnh ghép (`if x: y`, `;`), phần còn lại sửa tay (biến vòng lặp trong lambda của drill, `pytest.raises(Exception)` → lỗi cụ thể, `raise ... from None`). **Ngoại lệ ghi lại:** `E501` (độ dài dòng do formatter quyết định, không tách tay chuỗi SQL dài), `UP042` (giữ `class X(str, Enum)`: `StrEnum` đổi kết quả `str()`/`format()` của member, mà text của member đi vào wire và SQL); `E402` chỉ trong `tests/**`.
- `lint-imports`: contract đầu tiên "ecomdata không import legacy", tạm bỏ qua 1 import của `SiteCrawler.crawl()` cũ (xoá ở R3).
- **Số test:** 1244 passed, 0 skipped, 11 deselected (bằng mốc R0). `ruff check src tests`: sạch. `lint-imports`: 1 kept, 0 broken.

## R2. `domain/`, `settings/`, `infra/`, luật import, bug (a)–(d)

Commit: `dd3ba4e`, `a3a0f61`, `3b7fe08`, `3d61209`, `099de8b`, `8542b9d`, `3eeff99` (và `e49b43c` cho test kiến trúc).

- **`domain/validation.py`** (`dd3ba4e`): một bộ helper (`require_text`, `aware_utc`, `non_bool_int`, `sha256_hex`, ...) thay cho khoảng 9 bản `_require_text`/`_utc`. `acquisition.py`, `observations.py`, `identity.py`, `change_rules.py` và `ingestion/{audit,frontier,raw_store,scheduling,worker}.py` dùng chung. Test: `tests/unit/domain/test_validation.py`.
- **`domain/sources.py`** (`a3a0f61`): `SourceSpec` + `SourceRegistry` (`SOURCES`, `TIKI`, `LAZADA`). Thay 6 registry lặp (plan mục 1.2). `counter_semantics`, `seed`, `batch/dimensions` đọc từ đây. Test: `tests/unit/domain/test_sources.py`.
- **`domain/ordering.py`, `domain/batch_run.py`** (`3b7fe08`): quy tắc thứ tự `(observed_at, fetched_at, observation_id)` và state machine `BatchRunStatus` với bảng chuyển trạng thái. Test: `tests/unit/domain/test_ordering_and_batch_run.py`. **Chưa có mart nào dùng `ordering.py`** (việc của R6).
- **`infra/`** (`099de8b`): `spark.py` (một `SparkSession` factory), `postgres.py` (`ConnectionFactory`, một `jdbc_url()`), `kafka.py` (một producer factory), `http.py` (`HttpClient`, `RobotsPolicy` cache theo host, `Throttle` theo host), `object_store.py` (có gzip). Test: `tests/unit/infra/`.
- **`settings/`** (`8542b9d`): dataclass frozen, `load_settings(env)`, không `load_dotenv` hay validate lúc import. `domain/topics.py` là hằng số.
- **Composition root** (`3eeff99`): `ingestion/bootstrap.py`, `ops/bootstrap.py`, `processing/batch/bootstrap.py`, `serving/bootstrap.py` đọc settings một lần và inject factory.
- **Bug** (mỗi bug một test hồi quy):
  - (a) Silver path hard-code: `test_the_silver_path_follows_the_configured_dataset` (`test_marketplace_silver_sink.py`);
  - (b) robots.txt tải lại mỗi task: `test_robots_are_fetched_once_per_host_and_cached` (`test_http.py`), `test_robots_are_downloaded_once_for_every_page_a_worker_acquires` (`test_fetchers_and_runtime.py`);
  - (c) `request_url()` ngoài `try`: `test_a_target_the_adapter_cannot_turn_into_a_url_is_a_failed_report` (`test_marketplace_acquisition.py`);
  - (d) `except Exception: raise` biến mất cùng `tiki.py` ở R3.
- **Luật import** (`e49b43c`): `lint-imports` có 4 contract (`ecomdata` không import `legacy`; layers; `domain` thuần; `ingestion` và `processing` độc lập). `tests/architecture/`: `test_import_rules.py` (settings chỉ ở composition root), `test_single_definitions.py`, `test_site_literals.py` (literal `tiki`/`lazada` chỉ ở chỗ cho phép).
- Legacy Kaggle đọc settings và storage profile đóng băng riêng (`3d61209`).

## R3. Ingestion ELT (Tiki)

Commit: `cad93e4`, `ac98f8f`, `e49b43c`.

- `domain/acquisition.py` viết lại quanh `FetchBundle`/`FetchPart`/`ExtractedPage`; `domain/records.py` có `SourceRecord` (`marketplace-record.v1`), `build_records`, `record_to_wire`/`record_from_wire`, `record_json`.
- `ingestion/fetchers/{base,http}.py` (Strategy), `ingestion/sources/tiki/{extractor,discovery}.py` (extractor chỉ tìm mảng dòng trong `__NEXT_DATA__` và tách `page_context`; discovery đọc sitemap nêu trong robots.txt), `ingestion/pipeline.py` (Template Method), `ingestion/runtime.py` (runtime theo worker), `ingestion/raw_store.py` (v2: `part-<n>.bin.gz` + `metadata.json`, đọc được v1), `ingestion/record_store.py` (Bronze/records JSONL.gz), `ingestion/publisher.py` (Kafka `marketplace.records.v1`), `ingestion/reparse.py` (đọc raw v1/v2, extract lại, so record).
- Xoá `SiteCrawler` và các phương thức legacy (`site_crawler.py`, `sources/tiki/api.py`, `sources/tiki/html.py`); các test của chúng được viết lại thành `test_crawler_contracts.py`, `test_source_records.py`, `test_fetchers_and_runtime.py`, `test_raw_store.py`, `test_record_store.py`, `test_tiki_source.py`, `test_marketplace_reparse.py`, `test_marketplace_acquisition.py`.
- **Test bỏ có chủ đích** (code mà chúng kiểm đã bị xoá; hành vi tương ứng có test mới ở trên): `tests/unit/ingestion/test_crawler.py`, `test_tiki_category_html.py`, `test_tiki_contract.py`, `tests/unit/ops/test_stub_source_ids.py` (adapter API cũ của Tiki và stub cũ).
- Stub nguồn chuyển sang `services/stub_source/` (`server.py`, `fixture.json` riêng, phục vụ trang category Tiki).
- **Quyết định:** quảng cáo không bị lọc ở extractor; được đánh dấu ở Silver (`is_advertisement`) và mart chỉ đọc hàng organic (`inputs.organic_only`).
- **Chưa làm (thuộc R7):** `ingestion/silver_sink.py` và `silver_service.py` vẫn còn và vẫn đọc `marketplace.observations.v1`; service `silver-sink` trong compose chưa bỏ.

## R4. Lazada

Commit: `fed2b3d`.

- `infra/browser.py` (`BrowserSession`, Playwright, một lần cho mỗi worker), `ingestion/fetchers/browser.py` (`BrowserFetcher`: bắt response khớp `capture_patterns`, chờ `wait_for`, phát hiện trang chặn bằng regex của spec → `FetchBlockedError`), `ingestion/sources/lazada/{extractor,discovery}.py` (`mods.listItems` và `page_context`; discovery từ `env/sources/lazada_categories.json`, 20 category).
- Fixture ≤ 5 sản phẩm: `tests/fixtures/sources/lazada/{category_page.html,listing_xhr.json}`.
- Test: `test_browser_session.py`, `test_browser_fetcher.py` (page giả, không cần mạng), `test_lazada_source.py`; một test `integration` thật (`tests/integration/test_lazada_live.py`, bị bỏ chọn mặc định, **chưa chạy** vì cấm crawl trong session này).
- **Chưa làm:** image `docker/crawler-browser` (R8).

## R5. Processing: normalize, stage `silver`, quarantine, drift, parity

Commit: `0accc39` (normalize), `93f8f64`, `7545432`, `421fda5`, `6983f6a`.

Session trước bị dừng giữa chừng, để lại khoảng 48 file chưa commit, gồm R5 và cả bản nháp R6 (runner, 4 stage còn lại, memory/wire/cli, bootstrap viết lại, repositories, scheduler) và R8 (tách compose, Dockerfile mới, env ghép lớp, Airflow). Phần nháp R6/R8 **chưa có test** và làm hỏng các test cũ (xoá `warehouse.py`), nên không đưa vào R5: đã cất nguyên trạng ở nhánh `wip/r6-r8-draft` (commit `4fc95a4`). Chỉ phần R5 được giữ lại.

- `processing/normalize/common.py`, `normalize/sources/{tiki,lazada}.py`: `PAYLOAD_SCHEMA` (Tiki 133 trường lá, Lazada 55), `to_wide`, `to_offer_observation`, `split_quarantine`, `payload_drift`.
  - Sửa so với `0accc39` (`7545432`): `observation_id` của hàng quảng cáo có thêm `"ad"` và `record_index`, nên một quảng cáo lặp listing organic cùng response không đụng id với hàng organic; listing organic lặp trên một trang vào quarantine với lý do `duplicate_listing_on_page` (hệ cũ giữ hàng đầu và đếm phần còn lại là `duplicate_count`).
- `processing/batch/stages/base.py` (`Stage`, `StageContext`, `RunOptions`, `Outcome`), `ports.py` (Protocol), `context.py` (`MarketplaceBatchContext` chuyển từ `warehouse.py`, thêm `gold_run_uri`; `warehouse.py` re-export tên cũ), `lake.py` (`SparkLakeIO`), `inputs.py` (`organic_only`, `observations_as_of`, ...).
- `stages/silver.py` (`SilverStage`): đọc Bronze records của `[silver_since, as_of]`; bỏ bản sao trùng `record_id` (đếm vào summary); ghi `offer_observations_v2` (partition `source_code`, `observed_date`), `quarantine`, `<source>_listing_wide`, `page_context`, và `silver/_runs/run_id=<id>/silver.json`.
  - Summary có `quarantine_by_reason`, `payload_drift` (key, số lần), `duplicates_before_dedup` = `{record_id, observation_id, identical_content}`. `observation_id` và `identical_content` được đếm trên hàng hợp lệ **trước** mọi dedup (`duplicate_counts`); khi so nội dung bỏ `record_id` và `raw_artifact_id`. Không hàng nào bị bỏ vì `observation_id` trùng: R6 cho check `observation_id_unique` đọc số này.
- **AC7 (parity)**: `tests/spark/test_silver_stage.py`. Đường cũ được dựng lại từ ánh xạ đã đóng băng của `TikiCrawler._event_from_row` (lấy từ `3eeff99`) với factory của `domain/observations.py`, rồi `canonical_json`, `flatten_marketplace_observations`, `deduplicate_marketplace_observations`. Đường mới: Bronze records → `SilverStage` → `organic_only`. Cả hai chạy qua `build_marketplace_marts`.
  - Kết quả: cả 10 mart khớp từng dòng trên mọi cột; bảng observation khớp trên mọi cột cũ. Không có chỗ lệch nào cần ghi.
  - Phạm vi: fixture Tiki (4 hàng organic + 1 quảng cáo). Hai đường dùng cùng frame audit nên các mart phụ thuộc audit khớp theo định nghĩa.
- **Giới hạn môi trường:** Spark trên host không có filesystem, nên test dùng `LakeIO` và `ObjectStore` in-memory (`MemoryLake`, `MemoryStore` trong test). `SparkLakeIO` (Parquet, `partitionOverwriteMode=dynamic`) **chưa chạy thật**; cần chạy trong stack Docker (R9).
- Tiện thể: `git rm --cached src/ecomdata.egg-info` (đang bị track nhầm).

## Số test hiện tại

Chạy ở worktree sạch tại commit `6983f6a` (`PYTHONPATH=src`, `pytest -q -p no:cacheprovider -rs`): 1474 passed, 1 skipped, 11 deselected (11 drill), 1839 s (máy đang chạy song song một session khác). 1 skipped: `tests/integration/test_lazada_live.py`, `importorskip("playwright.sync_api")` vì `.venv` chưa cài Playwright (extra `browser`); test này chạm mạng nên không được chạy. Ruff sạch, `lint-imports` 4 kept. Mốc R0: 1244 passed, 0 skipped.

Test đã bỏ có chủ đích từ R0 đến R5: xem R3 (4 file test của adapter Tiki cũ và stub cũ).

## Bảng nghiệm thu AC1–AC14 (tại commit `6983f6a`)

| AC | Trạng thái | Bằng chứng / lệnh kiểm |
|---|---|---|
| AC1 Đóng gói | **pass** | `pip install -e ".[dev,spark]"` (R1). `git grep -n "sys.path.insert" -- src tests services` không ra gì. `git ls-files \| grep -c "\.pyc$"` = 0. `src/ecomdata.egg-info` đã `git rm --cached` (`6983f6a`). |
| AC2 Luật phụ thuộc | **pass** | `lint-imports`: 4 contract kept. `pytest tests/architecture`: `test_import_rules.py`, `test_single_definitions.py`, `test_site_literals.py`. |
| AC3 Lint | **pass** | `python -m ruff check src tests services`: All checks passed. Ngoại lệ: `E501`, `UP042`, `E402` trong `tests/**` (R1). |
| AC4 Suite xanh | **pass**: 1505 passed, 2 skipped, 11 deselected (drill, integration), 0 failed | `python -m pytest -q -p no:cacheprovider -rs`. Skip 1: `tests/airflow/test_dags.py` (host không cài `airflow`; 3 DAG được kiểm thật trong container, xem AC12c). Skip 2: `tests/integration/test_lazada_live.py` (host không có Playwright; Lazada thật đã chạy trong E2E). |
| AC5 Một chỗ cấu hình | **pass** | `SourceRegistry`, Spark factory, `jdbc_url`, Kafka producer factory, `domain/validation.py`, và `domain/ordering.py` (`marts.py` dùng `ORDER_COLUMNS`, `speed/stream.py` dùng `order_key`). `percentile_accuracy` lấy từ `context`. Kiểm: `tests/architecture/test_single_definitions.py`. |
| AC6 ELT không mất dữ liệu | **pass (mức test, fixture)** | `tests/unit/domain/test_source_records.py`, `tests/unit/ingestion/test_{tiki_source,lazada_source,raw_store,record_store,marketplace_reparse}.py`; quarantine đếm đủ hàng ở Silver: `tests/spark/test_silver_stage.py`. Chưa kiểm trên dữ liệu crawl thật. |
| AC7 Parity | **pass (Tiki)** | `pytest tests/spark/test_silver_stage.py -k "marts_of_the_new_path or observation_rows_agree"`: 10 mart và các cột cũ khớp từng dòng, không có chỗ lệch. |
| AC8 Batch theo stage | **pass (mức test, in-memory)** | `tests/unit/processing/test_batch_runner.py` (chuyển trạng thái, no-op, `--force`, lock); `tests/spark/test_batch_stages.py`: `--all` và từng stage cho cùng Gold, manifest, audit; `observation_id` trùng thì quality FAIL. `git grep from_settings -- src/ecomdata/processing/batch/stages` rỗng. Chưa chạy trên Parquet/MinIO thật (R9). |
| AC9 Sửa bug | **pass** | (a) `test_the_silver_path_follows_the_configured_dataset`; (b) `test_robots_are_fetched_once_per_host_and_cached`, `test_robots_are_downloaded_once_for_every_page_a_worker_acquires`; (c) `test_a_target_the_adapter_cannot_turn_into_a_url_is_a_failed_report`; (d) `git grep -n "except Exception:" -A1 src/ecomdata/ingestion/sources` không còn `raise` trần. |
| AC10 Lazada | **pass (E2E thật)** | Test fixture: `test_lazada_source.py`, `test_browser_fetcher.py`, `test_browser_session.py`. Chạy thật trong E2E: 27 trang thành công, 11 lỗi `PARSE_ERROR` (nguyên nhân chưa rõ, không có dấu hiệu bị chặn, circuit không mở); xem `docs/evidence/refactor-20261025/README.md`. |
| AC11 Image và compose | **pass (mức cấu hình và build)** | Image mới: `ecommerce/marketplace-python:2`, `ecommerce/spark-marketplace:4.0.1-r2`, `ecommerce/crawler-browser:1`, `ecommerce/airflow:3.3.2-r1`; ba tag cũ giữ nguyên id so `images_before.txt`. `docker compose --env-file env/base.env --env-file env/ports.<stack>.env --env-file env/<stack>.env config -q` sạch với 4 stack. `pytest tests/unit/ops/test_compose_isolation.py test_compose_services.py`: 99 passed. Airflow lên được trên `mp-refactor` (3 DAG parse, socket Docker truy cập được); chưa chạy một DAG thật (cần stack lõi, R9). |
| AC12 E2E thật trên `mp-refactor` | **pass** | (a) crawl 84 phút; (b) Bronze cả hai nguồn; (c) 2 DAG run `success`, quality PASS, cache có tiki và lazada, dashboard 13 chart; (d) `source_discovery` success; (e) speed ~70 phút, Redis 32.171 key, change event của cả hai nguồn, DLQ ES 0; (f) bằng chứng ở `docs/evidence/refactor-20261025/`, stack đã `stop`. Hai điểm mở: nguyên nhân 11 lỗi Lazada, và Tiki category rỗng bị gán `PARSE_ERROR`. |
| AC13 Tài liệu | **pass** | `docs/ARCHITECTURE.md` mới (sơ đồ tầng, Mermaid luồng dữ liệu, bảng module, pattern, luật phụ thuộc, checklist thêm nguồn); 6 ADR ở `docs/adr/`; `docs/archive/` chứa plan Phase 1–9 và `PHASE_INDEX.md`, `git grep "docs/PHASE_"` ngoài archive chỉ còn câu mô tả việc này trong plan và handoff; README viết lại; `docs/PROGRESS.md` §34. |
| AC14 Git | **pass** | 45 commit, tác giả chỉ có người dùng; `git log --format=%B ... \| grep -ci co-authored` = 0; không push. Docker: không container hay volume có từ trước bị xoá, ba image cũ giữ id (chi tiết trong evidence). |


## Cập nhật 2026-10-25 16:40: trạng thái R6/R7

- Một session nền bị dừng giữa chừng đã để lại bản nháp R6/R7 chưa commit. Bản nháp được lưu nguyên trạng ở nhánh `wip/r6-r7-draft` (`1abef01`). Nhánh `refactor/architecture` giữ R0–R5 sạch.
- Kiểm bản nháp (`PYTHONPATH` trỏ vào `src` của bản nháp):
  - `pytest`: 1386 passed, 4 failed, 2 skipped, 11 deselected. Có 1 file test bị cắt ngang (`tests/unit/ops/test_ops_validate.py`, `SyntaxError`).
  - `lint-imports`: 4/4 kept.
  - `ruff`: 5 lỗi.
- Chi tiết từng mục và các bước tiếp theo: `docs/HANDOFF_REFACTOR.md` mục 0.
- AC chưa đổi so với bảng ở trên: AC8 vẫn "chưa xong" cho tới khi bản nháp được đưa vào và suite xanh.

## R6/R7. Batch theo stage, speed đọc envelope (2026-10-25)

Nguồn: bản nháp `wip/r6-r7-draft` squash vào `refactor/architecture`, sửa cho xanh, commit `a80984f`..`6d2d072`.

- Sửa test: `test_ops_validate.py` (helper bị cắt), `test_ops_backup.py` (bỏ check `kafka_to_silver_lag`, vì `silver-sink` đã bỏ), `test_retention.py` (ngày bằng cutoff được giữ), test drills (D3 là drill của `silver-sink`, đã bỏ; còn D1–D2, D4–D11).
- Lỗi thật: `scheduler.main()` bỏ qua `sys.argv`, nên `python -m ecomdata.processing.batch.scheduler --help` chạy scheduler thật. Đã sửa, test `test_the_entrypoint_answers_help` xanh.
- Ordering: `marts.py` dựng window từ `domain/ordering.ORDER_COLUMNS`; `speed/stream.py` sắp theo `order_key(observed_at, fetched_at, observation_id)`.
- `build_crawl_reliability_daily` nhận `percentile_accuracy` từ `MarketplaceBatchContext`.
- `observability/`: bỏ `silver-sink` khỏi promtail và tiêu đề dashboard.
- **Chưa làm:** `build_offer_change_daily` chưa gọi `domain/change_rules.py` (luật đó là Python theo từng offer, mart là Spark; cần quyết định thiết kế). `collect()` còn ở `marts.py` (max date, danh sách marketplace) và `inputs.py` (kiểm xung đột `observation_id`).
- `docs/ARCHITECTURE.md` do bản nháp viết lại; chưa rà, thuộc R9.
- Tiếp theo: R8 (Dockerfile, compose tách lớp, image Airflow), R9 (E2E `mp-refactor`).

## R8. Hạ tầng (2026-10-25)

Nguồn: lấy từng file từ `wip/r6-r8-draft` (compose, docker, env, `mp.ps1`, `constraints.txt`), không merge nguyên; bỏ hết phần còn `silver-sink`.

- Image: build lại `marketplace-python:2`, `spark-marketplace:4.0.1-r2`; build mới `airflow:3.3.2-r1`. Không build đè tag cũ.
- `docker/airflow/Dockerfile`: image Airflow từ chối `pip` chạy bằng root, nên `COPY --chown=airflow`, không đổi user; không cài lại `python-dotenv` (Airflow có sẵn bản mới hơn bản `constraints.txt` ghim).
- Compose: service Airflow chạy `user: "50000:0"` và `PYTHONPATH` rỗng. Chạy bằng root làm module `airflow` biến mất (nằm ở user-site của `airflow`), còn `PYTHONPATH` của `marketplace-env` che module của Airflow.
- `scripts/mp.ps1` gọi `python -m ecomdata.processing.batch run --all` thay cho `batch.warehouse`; smoke không còn `silver-sink`, `batch-scheduler` chạy theo profile `legacy-scheduler`.
- Test: `test_compose_isolation.py` tính cả file legacy (cổng Spark), nhận ghim `@sha256:` không phải là secret, nhận `profiles:` có thụt lề tuỳ ý; `test_demo.py` đọc `compose/docker-compose.yml`.
- `DEMO_RUNBOOK.md`, `RUNBOOK.md` theo tên Dockerfile mới; `SPARK_DRIVER_MEMORY` thay cho `stream.override.yml`.
- Suite: 1504 passed, 2 skipped, 11 deselected; `ruff` sạch; `lint-imports` 4 kept.
- **Hoãn:** gộp `Stack`, `wait_until`, `refuse_live` của `ops/bench.py`, `bench_load.py`, `drills.py` vào `ops/stackctl.py`. Các bản khác ngữ nghĩa (kiểu exception, giá trị trả về), và drill cần stack Docker đầy đủ nên không kiểm được ở R8. Làm cùng R9 khi có stack chạy.

## R9. E2E thật, tài liệu (2026-10-25)

- E2E trên `mp-refactor` với Tiki và Lazada thật; kết quả và số đo ở `docs/evidence/refactor-20261025/README.md`. Lần đầu `SparkLakeIO`, Parquet trên MinIO, DAG Airflow với `DockerOperator` và speed đọc `marketplace.records.v1` chạy thật.
- Năm lỗi mà test không bắt được, đã sửa: image Spark Python 3.10 (cần 3.12), thiếu topic `marketplace.observations.v1` trong `kafka-init`, file category Lazada nằm ngoài package, `setup_superset.sh` bị CRLF (thêm `.gitattributes`), hai lỗi Airflow ở R8.
- Áp lực bộ nhớ lúc chạy nằm ở máy chủ Windows, không phải VM Docker (số đo trong evidence). Giả thuyết "Lazada lỗi vì thiếu RAM" đã rút lại.
- Suite cuối: 1505 passed, 2 skipped, 11 deselected; `ruff` sạch; `lint-imports` 4 kept.
- Còn nợ: `build_offer_change_daily` chưa gọi `domain/change_rules.py`; gộp `Stack`/`wait_until`/`refuse_live` của `ops` (hoãn); loại lỗi riêng cho trang category rỗng; smoke offline (`mp.ps1 smoke`) và drill D1–D11 chưa chạy lại sau refactor; Shopee chờ AppID Affiliate.
