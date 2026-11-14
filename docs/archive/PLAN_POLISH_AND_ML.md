# Kế hoạch hoàn thiện: dọn repo, ML, dashboard

- **Ngày lập:** 2026-11-03. **Hạn:** 2026-11-24. Demo khoảng cuối tháng 12.
- **Nhánh gốc:** `develop` (Phase 10 đã merge qua PR #18, CI xanh).
- **Quyết định của người dùng (2026-11-03):**
  - Bài toán: **phát hiện giảm giá ảo** là chính, **dự báo nhu cầu cấp danh mục** là phụ.
  - `legacy/` (demo Kaggle) xoá khỏi nhánh chính, gắn tag `kaggle-demo-final` trước khi xoá.

## 0. Điều kiện tiên quyết: dữ liệu

Không có gì đang thu thập (`mp-live` dừng từ 06/10). Cả hai bài toán ML cần lịch sử nhiều tuần, nên đây là việc gấp nhất.

- P0.1 Chạy H7 (`scripts/h7.ps1`), gói cuối của `PLAN_PIPELINE_HARDENING.md`. Ổ C: còn 196 GB (03/11), không còn bị chặn.
- P0.2 Bật stack thu thập dài hạn (crawler Tiki + Lazada, Kafka, MinIO, Postgres), chạy liên tục tới demo.
  - Tắt sleep của Windows, bật Docker Desktop tự khởi động (lý do `mp-live` chết lần trước).
  - Batch Airflow chạy hằng ngày để Gold có dữ liệu tăng dần.

## 1. Dọn repo (P1, khoảng 1 ngày, một PR)

| Việc | Chi tiết |
|---|---|
| Gốc repo | xoá file `Brief để xây dựng plan ...md`; gộp `docker-compose.yml`, `run-fullstack.sh` vào `compose/`, `scripts/` hoặc xoá nếu trùng |
| `legacy/` | tag `kaggle-demo-final`, xoá `legacy/`, `tests/legacy`, `requirements.txt`, extra `legacy` trong `pyproject.toml`, `compose/docker-compose.legacy.yml`, job CI `legacy` |
| `docs/` | chuyển `PLAN_*`, `HANDOFF_REFACTOR`, `REFACTOR_REPORT`, `PROGRESS` vào `docs/archive/`; giữ README, ARCHITECTURE, DATA_MODEL, RUNBOOK, OBSERVABILITY, DEMO_*, SOURCE_FEASIBILITY, `adr/`, `model/` |
| Trùng lặp | `observability/build_superset_batch_dashboard.py` vs `src/ecomdata/serving/superset/` |
| README | viết lại: một câu bài toán, sơ đồ kiến trúc, cách chạy trong 5 lệnh, badge CI, ảnh dashboard |
| Khác | `.gitignore` cho `data/`, `__pycache__`, file tạm; kiểm tra không còn secret trong `env/` |

## 2. Audit và trả nợ code (P2, khoảng 2 ngày)

- Nợ đã biết: `build_offer_change_daily` chưa dùng `domain/change_rules.py`; `ops/drills.py` 71 KB cần tách; 11 lỗi parse Lazada chưa rõ nguyên nhân.
- Một lượt audit có giới hạn (code chết, hàm quá dài, đặt tên, docstring lệch code), sửa theo danh sách, không refactor lại kiến trúc.

## 3. ML (P3, khoảng 1 tuần)

Chạy như một stage của batch (Spark đọc Gold `dw`), có DAG Airflow riêng `marketplace_ml`, kết quả ghi vào Gold và publish sang Postgres `cache` để Superset đọc. Tham số và số đo của mỗi lần chạy ghi vào `_runs/` như các stage khác.

### 3.1 Phát hiện giảm giá ảo (chính)

- **Câu hỏi:** "Đợt giảm giá này có thật không?" Dấu hiệu: nâng giá gốc/giá bán ngay trước khi giảm, % giảm hiển thị lệch xa so với giá thực tế trong N ngày trước, giảm giá bất thường so với cùng danh mục.
- **Đầu vào:** `fact_price_snapshot_daily` (giá, giá gốc, % giảm), `fact_price_change`, `fact_counter_daily`, `dim_category`.
- **Cách làm:**
  1. Luật có giải thích được (ví dụ: giá gốc tăng > X% trong 14 ngày trước đợt giảm; % giảm thực tế so với giá trung vị 30 ngày nhỏ hơn nhiều so với % hiển thị).
  2. Mô hình không giám sát nhiều biến (Isolation Forest) trên các đặc trưng đó, để xếp hạng mức đáng ngờ.
  3. Mỗi cảnh báo kèm lý do (luật nào khớp, đặc trưng nào lệch).
- **Đánh giá:** không có nhãn thật; gán nhãn tay khoảng 100 mẫu top-ranked để đo precision@k, so với tầng MAD/IQR hiện có làm baseline.
- **Bảng ra:** `mart_discount_suspicion` (listing, ngày, điểm, lý do).

### 3.2 Dự báo nhu cầu cấp danh mục (phụ)

- **Câu hỏi:** doanh số 7 ngày tới của mỗi danh mục.
- **Đầu vào:** chênh lệch ngày của số đã bán cộng dồn (`fact_counter_daily`), gộp theo danh mục.
- **Cách làm:** baseline naive/seasonal-naive so với một mô hình (ví dụ LightGBM trên lag hoặc thư viện chuỗi thời gian), backtest trượt; chọn mô hình theo MAE/sMAPE.
- **Rủi ro:** cần đủ lịch sử. Nếu tới 20/11 chưa đủ khoảng 3 tuần dữ liệu thì chỉ trình bày baseline và pipeline, ghi rõ giới hạn.
- **Bảng ra:** `mart_category_demand_forecast`.

## 4. Dashboard (P4, khoảng 3–4 ngày)

Một bộ màu và bố cục thống nhất, dựng bằng code như hiện nay.

- **Superset (batch):**
  1. Tổng quan thị trường (số listing, giá trung vị, phân bố theo danh mục, theo sàn).
  2. Giá và khuyến mãi (biến động giá, đợt giảm giá, top giảm giá đáng ngờ).
  3. Dự báo nhu cầu (thực tế vs dự báo theo danh mục).
  4. Chất lượng dữ liệu (quarantine, quality rules, độ tươi).
- **Kibana (speed và vận hành):** quan sát thời gian thực và log pipeline.
- **Grafana:** giữ cho sức khoẻ hệ thống.

## 5. Chốt (P5)

Bằng chứng cuối (ảnh dashboard, số đo thu thập, kết quả ML), merge `develop` vào `master`, tag feature-freeze.

## 6. Lịch

| Tuần | Việc |
|---|---|
| 03–09/11 | P0 (H7 + bật thu thập), P1, P2 |
| 10–16/11 | P3 |
| 17–23/11 | P4, P5 |
| 24/11 | dự phòng |
