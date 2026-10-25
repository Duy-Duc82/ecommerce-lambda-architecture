# Phase 10 trên kiến trúc mới: star schema sau refactor

Ngày 2026-10-25. Trạng thái: **bản nháp, chờ người dùng duyệt trước khi viết mã.** Nhánh `phase-10/star-on-refactor` (từ `refactor/architecture`).

Thay thế phần "thứ tự W2–W4" của `docs/PLAN_PHASE10_REVISED.md`: ingestion ELT, Silver bằng Spark, Airflow, đa nguồn đã xong ở refactor (R0–R9). Giữ nguyên thiết kế dim/fact của `PLAN_PHASE10_DIMENSIONAL_MODEL.md` §3, cộng các điều chỉnh của bản revised §3.4.

## 1. Hiện trạng đo trên mã

| Mục | Có | Thiếu |
|---|---|---|
| Registry dataset | `domain/datasets.py`: `STAR` (7 dim và bridge, 5 fact) và `MARTS` (10 mart) | `dim_brand`, `dim_listing_flags`, `fact_category_page_daily`; cột mới của `dim_seller`, `dim_category`, `dim_product_listing`, `fact_offer_observation` (revised §3.4) |
| Builder dim | `processing/batch/dimensions.py`: `build_dim_marketplace`, `dim_seller`, `dim_category`, `bridge_category_ancestry`, `dim_product_listing` (SCD2), `dim_offer_status`; `keys.py`, `dim_date.py` | chưa nối vào stage nào; chỉ đọc cột canonical, chưa đọc bảng `<source>_listing_wide` |
| Builder fact | không có | cả 5 fact |
| Stage | `silver`, `marts`, `quality`, `publish`, `promote` | stage `dimensions`, `facts` |
| Lưu trữ | Gold ghi theo run (`runs/run_id=<id>/`), ghi đè cả thư mục | star cần bảng bền qua các run, cập nhật tăng dần (MERGE) |
| Định dạng bảng | Parquet; **Delta đã có** (`DATA_LAKE_TABLE_FORMAT=delta`, image `r3`, smoke MERGE đạt) | adapter ghi MERGE trong `LakeIO` |
| Airflow | hoạt động (2 run `success`, `source_discovery` success) | task cho stage mới |
| Dữ liệu thật | một đợt E2E 84 phút | chưa có dữ liệu nhiều ngày; Phase 10 cần ít nhất 2 tuần để kiểm incremental |

## 2. Thiết kế đề xuất

**Thứ tự stage:** `silver -> dimensions -> facts -> marts -> quality -> publish -> promote`.

- `dimensions` đọc Silver v2 (bảng chung và bảng rộng theo nguồn), dựng dim, ghi vào `gold/marketplace/dw/<bảng>/` dạng Delta, cập nhật bằng `MERGE` (SCD2 cho `dim_product_listing`, SCD1 cho phần còn lại).
- `facts` đọc Silver v2 và dim, ghi fact vào cùng `dw/`, partition theo `date_key`, `MERGE` theo khoá fact; chạy lại cùng cửa sổ cho cùng kết quả.
- `marts` dựng 10 mart cũ **từ fact** (thay vì từ Silver), giữ nguyên tên và cột để Postgres `cache.*`, Superset và quality không đổi. Parity: mart mới bằng mart cũ trên cùng dữ liệu (như AC7 của refactor).
- `publish` thêm bản sao `dw.*` vào PostgreSQL (star) bên cạnh `cache.*`.

**Tính tái lập theo `as_of`:** manifest của run ghi phiên bản Delta của từng bảng `dw/` sau khi run chạy; đọc lại run cũ dùng `VERSION AS OF`. Bảng `dw/` ghi theo thứ tự cố định nên chạy lại một run cho cùng phiên bản dữ liệu.

**Trạng thái run:** thêm `DIMENSIONS_BUILT` và `FACTS_BUILT` vào `domain/batch_run.py` (bảng chuyển trạng thái, test AC8, ADR-0005 cập nhật, DAG thêm hai task). Chạy `--all` và từng stage vẫn cho cùng kết quả.

**Đa nguồn:** `dim_marketplace` có hai dòng thật (tiki, lazada); `dim_category` đọc `page_context` của từng nguồn; `dim_brand` và `dim_listing_flags` đọc bảng rộng (`tiki_listing_wide`, `lazada_listing_wide`) vì các trường đó không nằm trong cột canonical.

## 3. Gói công việc

| | Việc | Kiểm |
|---|---|---|
| D0 | Cập nhật `DATA_MODEL.md` §4, `docs/model/*` và registry theo mục 1 (dim mới, cột mới) | review của người dùng |
| D1 | Builder cho `dim_brand`, `dim_listing_flags`, `dim_seller` và `dim_category` mới, đọc bảng rộng; test từng builder bằng fixture Tiki và Lazada | test Spark trong bộ nhớ |
| D2 | `LakeIO.merge(...)` (Delta) và `MemoryLake` tương ứng; stage `dimensions` | test chạy lại hai lần cho cùng kết quả; smoke Delta trong Docker |
| D3 | Builder 5 fact (+ `fact_category_page_daily`), stage `facts`, khoá và quan hệ giữa fact và dim | test khoá ngoại: mọi `*_key` của fact có trong dim |
| D4 | `marts` dựng từ fact; parity 10 mart | test parity trên fixture, rồi trên dữ liệu E2E |
| D5 | `publish` ghi `dw.*`; dashboard Superset "Star schema"; DAG thêm hai task; quality gate cho mô hình | chạy trên `mp-refactor` |
| D6 | Incremental: chạy 2 lần bằng full rebuild; thời gian batch không tăng theo tổng Silver | trên ≥ 2 tuần dữ liệu |

Mỗi gói: test viết cùng mã, chạy toàn bộ suite, một PR vào `develop`. Mốc test hiện tại: 1505 passed.

## 4. Rủi ro

| | Rủi ro | Xử lý |
|---|---|---|
| P1 | Delta trên S3A (MinIO) chưa kiểm; MERGE cần ghi nguyên tử | kiểm ở D2 trên MinIO trong Docker trước khi phụ thuộc vào nó |
| P2 | SCD2 và MERGE tăng dần khó đạt "bằng full rebuild" | viết test so sánh từ D2; không cắt |
| P3 | Dữ liệu chưa đủ để kiểm incremental | mở lại stack thu thập dài hạn sớm (mục 5, câu 2) |
| P4 | Đổi thứ tự stage làm gãy test AC8, DAG, drill | cập nhật cùng lúc; chạy suite sau mỗi gói |
| P5 | Databricks: `dw/` Delta là bảng chuẩn của Databricks, nhưng publish và audit vẫn cần Postgres | xem `docs/DATABRICKS_READINESS.md` §3 |

## 5. Cần người dùng quyết định

1. **Duyệt thiết kế mục 2**, đặc biệt: star nằm ngoài thư mục run và dùng Delta `MERGE`; thêm hai trạng thái run; mart dựng từ fact.
2. **Thu thập dài hạn:** mở lại một stack thu thập (Tiki, Lazada) chạy liên tục để có dữ liệu cho D6? Cần quyết định về RAM của máy (xem evidence, mục áp lực bộ nhớ) và có bật sleep hay không.
3. **Databricks:** các câu hỏi ở `docs/DATABRICKS_READINESS.md` §6 quyết định `dw/` nằm ở bucket nào. Nếu chưa biết, D0–D3 vẫn làm được vì chúng chạy trên MinIO và Delta cục bộ.
