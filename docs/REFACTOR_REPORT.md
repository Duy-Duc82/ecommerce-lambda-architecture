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
