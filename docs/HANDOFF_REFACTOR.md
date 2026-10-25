# Bàn giao refactor kiến trúc: tiếp tục R6–R9

Ngày 2026-10-25. Plan: `docs/PLAN_REFACTOR_ARCHITECTURE.md`. Nhật ký: `docs/REFACTOR_REPORT.md` (có bảng AC1–AC14 hiện tại).

## 0. BẮT ĐẦU Ở ĐÂY (cập nhật 2026-10-25 tối, sau R8)

**R6, R7, R8 đã xong** trên `refactor/architecture`: suite 1504 passed, `ruff` sạch, `lint-imports` 4 kept. Chi tiết ở `docs/REFACTOR_REPORT.md` (mục R6/R7, R8). Các nhánh `wip/*` đã được thay thế, có thể xoá.

**Việc tiếp theo: R9** (mục 4.4): E2E trên `mp-refactor` (crawl thật Tiki và Lazada, DAG Airflow chạy thật ít nhất 2 lần, speed), bằng chứng, `ARCHITECTURE.md`, ADR, `docs/archive/`. Image đã build; lệnh lên stack: `.\scripts\mp.ps1 up -EnvFile env/refactor.env -With crawl,speed,batch,serve` rồi profile `orchestration`. Hiện không có container nào chạy.

**Còn nợ:** `build_offer_change_daily` chưa gọi `domain/change_rules.py`; gộp `Stack`/`wait_until`/`refuse_live` vào `ops/stackctl.py` (hoãn); `docs/ARCHITECTURE.md` là bản nháp chưa rà; AC8 chưa chạy trên Parquet/MinIO thật (R9 là lần đầu `SparkLakeIO` chạy thật). Shopee chờ AppID Affiliate của người dùng.

**Lưu ý môi trường:** file trong repo dùng CRLF; sửa bằng script phải giữ CRLF. Trong Git Bash, chuỗi có dấu gạch chéo ngược theo sau là n trong heredoc python bị đổi thành xuống dòng; dùng `Edit` hoặc `chr(92)`.

### (cũ) 16:40

**Việc tiếp theo:** đưa bản nháp R6/R7 ở nhánh `wip/r6-r7-draft` (`1abef01`) về `refactor/architecture`, sửa cho suite xanh, rồi làm nốt những ô chưa xong ở bảng dưới. Sau đó sang R8 (mục 4.3), rồi R9 (mục 4.4).

**Trạng thái các nhánh:**

- `refactor/architecture` (`C:\code\data-refactor`) sạch; R0–R5 đã xong và đã kiểm. HEAD là commit docs sau `f529d45`.
- `wip/r6-r7-draft` dựa trên `6983f6a`. Nó do một session bị dừng giữa chừng viết ra, chưa commit theo từng phần và chưa xanh.

**Không còn session hay tiến trình nền nào.** Luôn chỉ cho **một** session ghi vào worktree tại một thời điểm.

### 0.1 Kết quả kiểm bản nháp (25/10, worktree `C:\code\data-refactor-verify`, detached ở `1abef01`)

**Lưu ý môi trường:** `.venv` cài editable trỏ vào `C:\code\data-refactor\src`. Muốn test một worktree khác thì phải đặt `PYTHONPATH=<worktree>/src`. Nếu không, Python sẽ import code của worktree chính.

**`pytest` (bỏ `tests/unit/ops/test_ops_validate.py`):** 1386 passed, **4 failed**, 2 skipped, 11 deselected. Mất 34 phút.

Các test fail:

- `tests/unit/ops/test_ops_backup.py::test_the_derived_stores_are_allowed_to_be_empty_after_a_restore`
- `tests/unit/ops/test_ops_backup.py::test_the_tolerated_checks_are_exactly_the_stores_that_are_not_backed_up`
  - Rất có thể do `silver-sink` và Silver JSON đã bị bỏ, nên danh sách store được backup đổi theo.
- `tests/unit/ops/test_retention.py::test_only_raw_older_than_the_window_is_deleted`
  - `ops/retention.py` mới, test mới.
- `tests/unit/processing/test_marketplace_scheduler.py::test_the_entrypoint_answers_help`
  - Scheduler cũ còn gọi entrypoint của `warehouse.py`, file này đã bị xoá. Phải chuyển sang `python -m ecomdata.processing.batch run --all`.

**`tests/unit/ops/test_ops_validate.py` hỏng:** `SyntaxError` ở dòng 26 (`gzip.compress("` bị cắt ngang). Session trước bị dừng đúng lúc đang ghi file này. Viết lại hàm helper đó.

**Kiểm tĩnh:**

- `lint-imports`: 4 kept, 0 broken.
- `ruff check src tests services`: **5 lỗi**, trong đó 2 sửa được bằng `--fix`.

### 0.2 Checklist R6/R7 đối chiếu bản nháp

| Mục (plan §3.4 / §4) | Trạng thái trong `wip/r6-r7-draft` |
|---|---|
| `stages/{marts,quality,publish,promote}.py` + `stages/base.py` | Có; có test `tests/spark/test_batch_stages.py`, `test_batch_inputs.py`. |
| `runner.py`, `cli.py`, `__main__.py` (`--stage` lặp, `--all`, `--force`, `--resume`, `--allow-backfill`, `--quality-only`) | Có; test `tests/unit/processing/test_batch_runner.py`. |
| Xoá `warehouse.py` và cập nhật các chỗ tham chiếu | Đã xoá. **Còn sót** entrypoint của scheduler (test fail ở trên). Rà lại `ops/bench.py`, `ops/drills.py` (chuỗi `python -m`). |
| `batch/bootstrap.py` inject adapter thật; không còn `from_settings()` trong `stages/` | Có; grep `from_settings` trong `stages/` không ra kết quả. |
| `spark.driver.memory` tường minh | Có, `infra/spark.py:24` mặc định `3g`. |
| Sửa check `observation_id_unique` (đọc số trùng trước dedup) | Có (`batch/quality.py` khoảng dòng 217 và 460, tham số `accounted_rows`). Cần xác nhận có test "đầu vào trùng thì FAIL". |
| Thống nhất ordering qua `domain/ordering.py` | **Chưa.** `marts.py` dòng 22, 29, 97, 346, 559, 568 vẫn viết thẳng `orderBy(observed_at, fetched_at, observation_id)`. |
| `build_offer_change_daily` gọi `domain/change_rules.py`; một setting freshness/stale | **Chưa kiểm**, có thể chưa làm. |
| Thay `10000` cứng ở `marts.py:474` bằng `context.percentile_accuracy` | **Chưa.** |
| Bỏ action Spark (`count`, `collect`) trong hàm thuần | **Chưa kiểm.** |
| AC8: `--all` và từng stage riêng cho cùng Gold, manifest, audit; chạy lại là no-op; test chuyển trạng thái | Test runner đã có; **cần xác nhận** có test so `--all` với từng stage riêng. |
| R7: speed đọc `marketplace.records.v1` | Có. `processing/speed/service.py:81` `read_marketplace_records` dùng `normalizer.normalize_micro_batch`, ghi sang `marketplace.observations.v1` (giờ là topic dẫn xuất của speed), rồi `stream.py` đọc tiếp. Test `tests/spark/test_speed_normalizer.py`. |
| R7: bỏ `silver-sink` | Đã bỏ khỏi `src/` và compose. **Còn sót** tham chiếu ở `observability/{build_grafana_dashboard.py,promtail.yml,run_report_metrics.py}`. |
| Một phần của R8/R9 đã nằm trong bản nháp | 3 DAG ở `orchestration/dags/` (`DockerOperator`, `@hourly`, `max_active_runs=1`), `orchestration/tasks.py`, `ops/{bronze,retention}.py`, `docs/adr/0001–0006`. Chưa có image Airflow và compose `orchestration`. |

### 0.3 Các bước cụ thể cho session tiếp theo

1. Trong `C:\code\data-refactor`, chạy `git merge --squash wip/r6-r7-draft`. Nếu có conflict thì giữ bản của `refactor/architecture` cho `docs/`.
2. Sửa:
   - `tests/unit/ops/test_ops_validate.py`;
   - 4 test fail;
   - 5 lỗi ruff;
   - tham chiếu `silver-sink` trong `observability/`.
3. Làm nốt các ô **Chưa** ở bảng 0.2.
4. Chạy toàn bộ suite (khoảng 35 phút), `ruff check src tests services` và `lint-imports`. Cả ba phải sạch.
5. Commit thành từng phần: stages + runner; ordering + rules; speed; ops; observability. Mỗi commit đứng tên người dùng, **không** có `Co-Authored-By`.
6. Thêm mục R6/R7 vào `docs/REFACTOR_REPORT.md`, cập nhật bảng AC, sửa mục 0 của file này.
7. Sang R8 (mục 4.3), rồi R9 (mục 4.4).

## 1. Trạng thái hiện tại

| Mục | Giá trị |
|---|---|
| Worktree | `C:\code\data-refactor` |
| Branch | `refactor/architecture` (tạo từ `phase-10-dimensional-model` @ `f991af5`), chưa push |
| Commit code cuối của session này | `6983f6a` (`chore: stop tracking the generated egg-info`); các commit docs nằm sau đó |
| Phase xong | R0, R1, R2, R3, R4, R5 |
| Phase chưa làm | R6, R7, R8, R9 |
| Test (commit `6983f6a`, chạy ở worktree sạch) | 1474 passed, 1 skipped (Lazada integration, thiếu Playwright), 11 deselected (drill) |
| `ruff check src tests services` | sạch |
| `lint-imports` | 4 contract kept, 0 broken |
| Bản nháp R6/R8 | nhánh `wip/r6-r8-draft`, commit `4fc95a4`, **chưa test** (mục 5.1) |

**Cập nhật lúc 15:45 ngày 25/10:**

- Mọi session nền đã dừng. Không còn tiến trình `claude -p` hay vòng lặp chạy lại nào.
- Phần R6/R7 dở dang (chưa kiểm) mà session chạy song song để lại đã được commit nguyên trạng vào nhánh **`wip/r6-r7-draft`** (`1abef01`). Nhánh này gồm:
  - `processing/batch/{runner,cli,__main__,memory}.py`;
  - `stages/{marts,quality,promote,publish}.py`;
  - `orchestration/` (3 DAG và `tasks.py`);
  - `processing/speed/normalizer.py`, `processing/normalize/wire.py`;
  - `ops/{bronze,retention}.py`;
  - `docs/adr/0001–0006`;
  - các test `tests/spark/test_batch_*`, `tests/airflow/`, `tests/unit/orchestration/`.

  Bản nháp này **chưa chạy suite**, và đã xoá `warehouse.py` cùng `test_marketplace_warehouse.py`.
- Bản nháp cũ hơn (R6/R8) nằm ở `wip/r6-r8-draft` (`4fc95a4`).
- `refactor/architecture` hiện **sạch**, dừng ở `f529d45`, đúng trạng thái R5 trong mục này.
- Cách tiếp tục R6:
  1. `git diff refactor/architecture wip/r6-r7-draft --stat`;
  2. lấy lại từng phần bằng `git checkout wip/r6-r7-draft -- <file>`, hoặc `git cherry-pick -n 1abef01`;
  3. chạy suite;
  4. commit theo từng phần.
- Luôn chỉ cho **một** session ghi vào worktree tại một thời điểm.

## 2. Bản đồ `src/ecomdata/`

Một dòng cho mỗi package hoặc module (trạng thái commit `6983f6a`).

### `domain/` (thuần, không I/O)

- `acquisition.py`: hợp đồng thu thập một trang (`ListingPageRequest`, `FetchBundle`, `FetchPart`, `ExtractedPage`, report, cây lỗi, `FailureKind`).
- `batch_run.py`: `BatchRunStatus` và bảng chuyển trạng thái hợp lệ.
- `change_rules.py`: luật so sánh hai observation của một offer (thuần).
- `counter_semantics.py`: counter nào của nguồn nào được tin để tính delta (đọc từ `sources.py`).
- `datasets.py`: registry dataset Gold (star `dw` và 10 mart `cache`): `MARTS`, `MART_NAMES`, `dataset()`.
- `dlq.py`: bản ghi quarantine có version của observation.
- `identity.py`: `deterministic_id`, `make_offer_id`, `make_observation_id`, `make_seller_id`, `make_change_id`.
- `observations.py`: `MarketplaceOffer`/`OfferObservation`/`MarketplaceObservationV1`/`MarketplaceChangeV1` và factory `create_*`.
- `ordering.py`: một thứ tự `(observed_at, fetched_at, observation_id)`. **Chưa mart/speed nào gọi.**
- `quality_rules.py`: registry luật chất lượng bắt buộc.
- `records.py`: `SourceRecord` (`marketplace-record.v1`), `build_records`, `record_to_wire`/`record_from_wire`, `record_json`.
- `serialization.py`, `wire.py`: mã hoá JSON wire của hợp đồng v1 (`canonical_json`, `marketplace_observation_from_wire`).
- `sources.py`: `SourceSpec`, `SourceRegistry`, `SOURCES`, `TIKI`, `LAZADA` (một registry duy nhất).
- `topics.py`: tên topic Kafka.
- `validation.py`: một bộ helper kiểm giá trị.

### `settings/`, `infra/`

- `settings/__init__.py`: `Settings` (dataclass frozen theo concern), `load_settings(env)`, không side effect lúc import.
- `infra/browser.py`: `BrowserSession` (Playwright, một lần mỗi worker).
- `infra/dlq.py`, `infra/heartbeat.py`, `infra/lifecycle.py`: helper DLQ, liveness và tắt êm cho service dài.
- `infra/http.py`: `HttpClient`, `RobotsPolicy` (cache theo host), `Throttle`.
- `infra/kafka.py`: producer factory duy nhất, gửi có ack.
- `infra/object_store.py`: ghi/đọc object ngoài Spark, có gzip.
- `infra/postgres.py`: `ConnectionFactory` và `jdbc_url()` duy nhất.
- `infra/spark.py`: `SparkSession` factory duy nhất.
- `infra/storage.py`: cách Spark và SDK object store chạm tới lake.

### `ingestion/` (không import `processing`)

- `bootstrap.py`, `cli.py`: composition root và CLI (thu thập một lần, `seed`).
- `fetchers/{base,http,browser}.py`: Strategy tải trang (`HttpFetcher`, `BrowserFetcher`).
- `sources/tiki/{extractor,discovery}.py`, `sources/lazada/{extractor,discovery}.py`: extractor và discovery theo site.
- `pipeline.py`: Template Method thu thập một trang (robots → throttle → fetch → lưu raw → extract → report).
- `runtime.py`: runtime theo worker (fetcher, extractor, robots cho các nguồn).
- `raw_store.py`: Bronze/raw v2 (`part-<n>.bin.gz` + `metadata.json`), đọc được v1.
- `record_store.py`: Bronze/records JSONL.gz theo trang.
- `publisher.py`: publish `marketplace.records.v1` lên Kafka sau khi ghi file.
- `reparse.py`: extract lại từ raw, so với Bronze/records.
- `frontier.py`, `scheduling.py`, `audit.py`, `worker.py`, `service.py`, `seed.py`: frontier PostgreSQL, luật retry, audit và circuit, worker, service dài, seed.
- `silver_sink.py`, `silver_service.py`: **cũ**, Kafka `marketplace.observations.v1` → Silver JSON; xoá ở R7.

### `processing/`

- `schemas.py`: schema Spark của wire observation cũ (`MARKETPLACE_OBSERVATION_WIRE_SCHEMA`).
- `normalize/common.py`: đọc record envelope, `CANONICAL_COLUMNS`, quy tắc chung, `split_quarantine`, `payload_drift`, `page_contexts`.
- `normalize/sources/{tiki,lazada}.py`: `PAYLOAD_SCHEMA`, `to_wide`, `to_offer_observation`.
- `batch/stages/base.py`: `Stage`, `StageContext`, `RunOptions`, `Outcome`, `StageNotAllowed`.
- `batch/stages/silver.py`: `SilverStage` (stage duy nhất đã có).
- `batch/ports.py`: Protocol `ObjectStore`, `AuditRepo`, `QualityRepo`, `CachePublisher`, `LakeIO`, `RunRecord`.
- `batch/context.py`: `MarketplaceBatchContext`, `GoldWriteResult`, `MarketplaceBatchResult`, `ConflictingObservationError`.
- `batch/lake.py`: `SparkLakeIO`, `read_crawl_audit`, `silver_schemas`.
- `batch/inputs.py`: `organic_only`, `observations_as_of`, `crawl_audit_as_of`, `deduplicate_observations`.
- `batch/warehouse.py`: **cũ**, `_run_marketplace_warehouse` (hàm lớn cần tách), `flatten_marketplace_observations`, `WarehouseDependencies`, `main`; re-export các tên của `context.py`.
- `batch/marts.py`, `anomaly.py`, `quality.py`: 10 mart, mart anomaly, cổng chất lượng.
- `batch/manifest.py`, `repositories.py`, `lock.py`, `scheduler.py`, `bootstrap.py`: manifest Gold và pointer, repository PostgreSQL (staging cache, audit), advisory lock, scheduler vòng lặp cũ, composition root.
- `batch/dimensions.py`, `keys.py`, `dim_date.py`: dimension của star (Phase 10, chưa nối vào stage).
- `speed/{stream,sinks,service,change_producer}.py`: speed layer; hiện vẫn đọc `marketplace.observations.v1`.

### `serving/`, `ops/`, ngoài `src/`

- `serving/es_projector.py`, `serving/kibana/*`, `serving/superset/*`, `serving/bootstrap.py`: projector và provisioning.
- `ops/{backup,bench,bench_load,demo,drills,evaluate,evidence,smoke,storage,validate}.py`, `ops/bootstrap.py`: CLI vận hành (`python -m ecomdata.ops`). Vẫn còn `Stack`, `wait_until`, `refuse_live` nhân bản (R8).
- `services/stub_source/{server.py,fixture.json}`: stub nguồn offline cho smoke/drill.
- `legacy/kaggle/`: pipeline Kaggle cũ. `docker/`, `docker-compose.yml`, `observability/`, `env/`, `scripts/`: hạ tầng hiện hành (chưa tách lớp).
- `tests/{unit,spark,architecture,integration,legacy,drills,fixtures}`.

## 3. Cài đặt và chạy

Python: `C:\code\data\.venv\Scripts\python.exe` (3.12.13, pyspark 4.0.4).

```powershell
$py = "C:\code\data\.venv\Scripts\python.exe"
& $py -m pip install -e ".[dev,spark]"                   # đã cài; chạy lại khi đổi pyproject
& $py -m pytest -q -p no:cacheprovider                    # toàn bộ (bỏ drill, integration), khoảng 17 phút
& $py -m pytest -q -p no:cacheprovider tests/spark/test_silver_stage.py   # một file
& $py -m ruff check src tests services
& "C:\code\data\.venv\Scripts\lint-imports.exe"           # 4 contract
& $py -m pytest -q -m integration tests/integration       # CHẠM MẠNG (Lazada thật), chỉ chạy khi chủ ý
```

Lưu ý:
- Một worktree khác cùng dùng `.venv`, và `pip install -e` trỏ `ecomdata` vào worktree đã cài. Muốn chạy test ở worktree khác: `$env:PYTHONPATH = "src"` (session này kiểm bằng `git worktree add --detach C:\code\data-refactor-verify <commit>`, đã xoá sau khi chạy).
- Spark trên host không có filesystem (không `winutils`): test Spark truyền dữ liệu in-memory (xem `MemoryLake` trong `tests/spark/test_silver_stage.py`). Parquet/S3A chỉ chạy được trong Docker.
- Stack hiện hành vẫn là `docker-compose.yml` ở gốc và `scripts\mp.ps1` (chưa tách compose):

```powershell
.\scripts\mp.ps1 up -EnvFile env/bench.env -With crawl,ingest,speed,batch,serve
.\scripts\mp.ps1 smoke -EnvFile env/bench.env
.\scripts\mp.ps1 validate
docker compose --env-file env/bench.env config -q          # kiểm cú pháp compose, không chạm container
```

  Không chạy `down`, `stop`, `rm` lên `mp-tikihtml`, `mp-live`, `mp-bench`, `mp-mlbench`, `mp-demo`. `env/refactor.env` (project `mp-refactor`) **chưa có** trên branch này; bản nháp ở `wip/r6-r8-draft`.

## 4. Việc còn lại

### 4.1 R6: batch theo stage

Điểm xuất phát: `SilverStage`, `StageContext`, ports, `SparkLakeIO`, `inputs.py` đã có và có test (`tests/spark/test_silver_stage.py`). `warehouse.py` vẫn là đường chạy thật và là cái cần thay. Bản nháp của phần còn lại ở `wip/r6-r8-draft` (xem 5.1); dùng làm tham khảo, không merge nguyên.

- [ ] `stages/marts.py`: đọc Silver v2 qua `LakeIO.read_silver`, `observations_as_of`, `organic_only`, chạy `build_marketplace_marts`, ghi `gold/marketplace/runs/run_id=<id>/` bằng `LakeIO.write_gold`. Milestone `GOLD_WRITTEN`.
- [ ] `stages/quality.py`: đọc Gold và Silver từ storage, gọi `evaluate_quality_gates`, ghi `QualityRepo`. Trạng thái `QUALITY_PASSED` hoặc `QUALITY_FAILED`.
- [ ] `stages/publish.py` (staging cache Postgres) và `stages/promote.py` (manifest, CAS pointer). Giữ đúng ngữ nghĩa hiện tại: publish đặt `SUCCEEDED` rồi promote; nếu đổi thì ghi lý do. `expected_current_run_id` lưu vào audit lúc tạo run.
- [ ] `runner.py` + `cli.py` + `__main__.py`: `python -m ecomdata.processing.batch run --run-id … --as-of … --stage <name>|--all [--resume] [--allow-backfill] [--quality-only] [--force]`. Kiểm `allowed_from` bằng `domain/batch_run.py`; stage đã xong thì no-op trừ `--force`; advisory lock (`batch/lock.py`) mỗi stage.
- [ ] Hợp nhất `warehouse.py`: tách `_run_marketplace_warehouse`; xoá `warehouse.py`, `flatten_marketplace_observations`, `WarehouseDependencies`, `deduplicate_marketplace_observations`, `observations_as_of`, `crawl_audit_as_of` (bản sao của `inputs.py`), `read_crawl_audit` và `write_run_scoped_gold` (bản sao của `lake.py`). Cập nhật chỗ còn nhắc `batch.warehouse`: `ops/bench.py:527`, `ops/drills.py:435,1517` (chuỗi lệnh `python -m`), và tests `tests/spark/test_marketplace_{warehouse,quality}.py`, `tests/unit/processing/test_marketplace_{lock,manifest,postgres,scheduler}.py`.
- [ ] `bootstrap.py`: tạo adapter thật (Postgres, MinIO, Spark qua `infra/spark.py`) và inject; không `from_settings()` trong `stages/`. `spark.driver.memory` tường minh (mặc định `3g`).
- [ ] Sửa check `observation_id_unique` (`batch/quality.py`): đọc `duplicates_before_dedup.observation_id` trong `silver/_runs/run_id=<id>/silver.json` (hoặc đếm trên Silver v2 trước dedup), không đo sau dedup. Test: đầu vào trùng thì check FAIL. Check reconciliation đếm cả hàng quarantine (bản nháp có tham số `accounted` cho việc này).
- [ ] Thống nhất ordering và rule: các `Window...orderBy(observed_at, fetched_at, observation_id)` trong `marts.py` (dòng 22, 29, 97, 346, 559, 568) lấy từ `domain/ordering.py`; `build_offer_change_daily` (`marts.py`) gọi `domain/change_rules.py`; `speed/stream.py` dùng cùng thứ tự (hiện `(event_time, observation_id)`); một setting cho freshness/stale.
- [ ] Bỏ action Spark trong hàm "thuần": `count`/`collect` ở `deduplicate_observations`, `build_source_coverage_daily`, `build_counter_transitions`, `flatten_*`. Thay `10000` cứng ở `marts.py:474` bằng `context.percentile_accuracy`. Bỏ import thừa.
- [ ] Test (AC8): mọi chuyển trạng thái hợp lệ và không hợp lệ; chạy lại stage đã xong là no-op; `--all` và từng stage cho cùng Gold, manifest, audit; stage chạy độc lập qua CLI; scheduler cũ giữ test.
- [ ] Giữ `tests/spark/test_silver_stage.py` xanh (đặc biệt parity AC7: khi xoá `flatten_marketplace_observations`, đường cũ phải được dựng lại ở một helper test, vì parity cần nó).

### 4.2 R7: speed đọc envelope, bỏ `silver-sink`

- [ ] `processing/speed/stream.py`: đọc `marketplace.records.v1`, dùng `normalize/sources/<site>.to_offer_observation` cho mỗi micro-batch (`foreachBatch`); change rule gọi `domain/change_rules.py`.
- [ ] Xoá `ingestion/silver_sink.py`, `ingestion/silver_service.py`, test của chúng (`tests/unit/ingestion/test_marketplace_silver_{sink,service}.py`), service `silver-sink` trong compose và `scripts/mp.ps1`, các tham chiếu trong `ops/` (drills D-series, smoke). Topic `marketplace.observations.v1` không còn producer.
- [ ] Test: speed trên fixture Tiki và Lazada cho cùng offer id với batch.

### 4.3 R8: hạ tầng

- [ ] Dockerfile mới: `docker/python` (`ecommerce/marketplace-python:2`), `docker/spark` (`ecommerce/spark-marketplace:4.0.1-r2`), `docker/crawler-browser` (`ecommerce/crawler-browser:1`, nền `mcr.microsoft.com/playwright/python:v1.55.0-noble`), `docker/airflow` (`ecommerce/airflow:<ver>-r1`, tag Airflow 3.x có thật). **Không build đè** tag cũ; ghi `docker image inspect` trước và sau.
- [ ] Compose tách lớp: `compose/docker-compose.yml`, `.legacy.yml`, `.observability.yml`; key mã hoá Kibana ra env; bỏ `stream.override.yml` (heap vào Dockerfile/CLI); `env/base.env` + `env/ports.<stack>.env` + `env/<stack>.env`; `env/refactor.env` (project `mp-refactor`, prefix `rf-`, port riêng); `tests/unit/ops/test_compose_isolation.py` kiểm không trùng port.
- [ ] Airflow: profile `orchestration` (`airflow-init`, `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`), `LocalExecutor`, DB `airflow` trên `postgres-dw`, mount docker.sock; thử sớm bằng DAG `hello` (rủi ro K3). `orchestration/dags/marketplace_batch.py` (`@hourly`, `catchup=False`, `max_active_runs=1`, `retries=2`, mỗi stage một `DockerOperator`, `as_of = data_interval_end`, param `allow_backfill`), `marketplace_maintenance.py` (`@weekly`), `source_discovery.py` (`@daily`). Test DAG (marker `airflow`).
- [ ] Ops: `ops/stackctl.py` gom `Stack`, `wait_until`, `container`, `refuse_live`; một `CheckResult`; CLI argparse subcommands (lệnh lạ báo lỗi, `seed` và `evaluate` có trong `--help`).
- [ ] `requirements.txt`/`requirements-marketplace.txt` thay bằng `constraints.txt`; bỏ `streamlit`, `plotly`, `faker`.
- [ ] Vệ sinh: `ML_Pipeline.ipynb` và `notebooks/` sang `legacy/kaggle/`; sửa docstring "3.5.1" trong `tests/spark_support.py` nếu còn.

### 4.4 R9: E2E và tài liệu

- [ ] E2E trên `mp-refactor` đủ 6 mục con AC12 (crawl ≥ 60 phút, Tiki ≥ 20 category, Lazada ≥ 5 category × 2 trang, DAG chạy ≥ 2 lần, speed ≥ 10 phút). Đây là lần đầu `SparkLakeIO` và Parquet chạy thật. Politeness: Tiki ≥ 2 s/request, Lazada ≥ 5 s/trang, 1 tab; bị chặn thì dừng và hỏi người dùng (K2). Xong thì `docker compose -p mp-refactor stop`.
- [ ] Bằng chứng ở `docs/evidence/refactor-<yyyymmdd>/` (không dữ liệu thô).
- [ ] `docs/ARCHITECTURE.md` viết lại (sơ đồ tầng, Mermaid luồng dữ liệu, bảng module, bảng pattern mục 2.3, luật phụ thuộc, checklist thêm nguồn). Hiện vẫn là bản cũ.
- [ ] `docs/adr/` ≥ 6 ADR (ELT và envelope; đảo D2 để dùng Airflow; Ports & Adapters và luật import; một SourceRegistry; batch theo stage và state machine; Shopee).
- [ ] Chuyển `docs/PHASE_*_IMPLEMENTATION_PLAN.md` sang `docs/archive/` và sửa tham chiếu (`git grep "docs/PHASE_"`). Mục mới trong `docs/PROGRESS.md`.
- [ ] Bảng AC1–AC14 cuối `docs/REFACTOR_REPORT.md` cập nhật lần cuối.

## 5. Vấn đề đã biết, lệch plan, quyết định cần người dùng

### 5.1 Bản nháp R6/R8 ở `wip/r6-r8-draft`

Commit `4fc95a4`, tác giả người dùng, nội dung do session bị dừng để lại: `runner.py`, `stages/{marts,quality,promote,publish}.py`, `memory.py` (fake in-memory của các port), `wire.py`, `cli.py`, `__main__.py`, `bootstrap.py` viết lại, `repositories.py`, `scheduler.py`, `quality.py` (tham số `accounted`), `compose/*`, `docker/{python,spark,crawler-browser,airflow}`, `env/base.env`, `env/ports.*.env`, `env/refactor.env`, `scripts/create_airflow_db.py`, `scripts/init_postgres.sql`, `scripts/mp.ps1`, các test compose. Không có test cho phần R6, và xoá `warehouse.py` làm vỡ test cũ. Lấy từng file: `git checkout wip/r6-r8-draft -- <path>`.

### 5.2 Lệch plan

- Mục 3.3: `Silver` chưa chạy qua `SparkLakeIO` thật (Parquet), chỉ qua fake in-memory. Rủi ro chính của R9.
- AC7 chỉ cover Tiki (Lazada không có đường cũ). Đường cũ được dựng lại trong test từ ánh xạ đã đóng băng, vì code cũ đã bị xoá ở R3.
- `observation_id` của hàng quảng cáo có thêm `"ad"` và `record_index` (plan 3.3 không nói). Hàng organic giữ công thức cũ nên id liên tục với Silver cũ.
- Listing organic lặp trên một trang vào quarantine `duplicate_listing_on_page` (hệ cũ đếm `duplicate_count`).
- Bản sao `record_id` trùng bị bỏ (và đếm) ở stage `silver`, để chạy lại cho cùng kết quả; `observation_id` trùng thì chỉ đếm, không bỏ.
- `src/ecomdata.egg-info` từng bị track nhầm; đã `git rm --cached`.
- Nhánh dựa trên `f991af5` chứ không phải `53a94bf` (xem R0).

### 5.3 Quyết định cần người dùng

- **Shopee:** chỉ có Affiliate Open API (GraphQL `productOfferV2`) là đường hợp lệ; cần **AppID và Secret** của tài khoản Shopee Affiliate đã được duyệt (việc của người dùng: đăng ký). Chưa có code, chỉ ghi ở ADR-6 khi viết.
- Lazada có thể chặn khi E2E (K2): nếu chặn, AC12 cho Lazada không đạt; phải dừng và hỏi, không vượt chặn.
- Đổi thứ tự `promote`/`publish` (plan 3.4): mặc định giữ ngữ nghĩa cũ.
- Hai session ghi cùng một worktree (mục 1): người dùng cần quyết định session nào tiếp tục, và có nhận bản R6 của session kia hay không.

## 6. Luật cứng

- Commit nhỏ, đứng tên người dùng (git config sẵn). **Không bao giờ thêm `Co-Authored-By` hay dòng ghi công AI.** Không push, không mở PR.
- Không đụng project Docker hay tag image ngoài những cái mình tạo; không đụng `C:\code\data-phase10`. Không `down`/`stop`/`rm`/xoá volume của `mp-tikihtml`, `mp-live`, `mp-bench`, `mp-mlbench`, `mp-demo`. Không build đè `ecommerce/marketplace-python:1`, `ecommerce/spark-marketplace:4.0.1`, `ecommerce/spark-warehouse:3.5.1`.
- Không crawl site thật khi chưa tới R9; test dùng fixture, test `integration` giữ bị bỏ chọn. Crawl lịch sự: robots.txt, không stealth, không vượt captcha/anti-bot; Tiki ≥ 2 s/request, Lazada ≥ 5 s/trang, 1 tab; trang chặn thì mở circuit, không thử dồn.
- Không commit dữ liệu crawl thô; fixture ≤ 5 sản phẩm, không PII.
- Tài liệu bằng tiếng Việt, kỹ thuật (thư viện, hàm, cách làm), không ví von. Không dùng claude.ai artifact.
- Python: `C:\code\data\.venv\Scripts\python.exe`.
- Mỗi phase xong thì suite xanh (kèm `ruff` và `lint-imports`) mới sang phase sau. Bỏ test nào thì ghi tên và lý do vào `docs/REFACTOR_REPORT.md`. Mốc AC4: 1244 passed, 0 skipped (R0).
- Sau E2E (R9): `docker compose -p mp-refactor stop`.
- Không làm Shopee ngoài ADR-6.
