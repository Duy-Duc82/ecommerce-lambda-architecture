# Tiến độ đồ án — nâng cấp Lambda architecture thành sản phẩm DE end-to-end

> Đọc file này đầu tiên khi bắt đầu một session mới (Claude Code hoặc người).
> Nó là nguồn sự thật về "đang làm gì / đã xong gì / quyết định gì đã chốt" —
> không cần đọc lại toàn bộ lịch sử chat để tiếp tục.

## 1. Mục tiêu & câu chuyện nghiệp vụ (đã chốt, không đổi trừ khi có lý do mạnh)

Biến project từ lambda-architecture demo (Kaggle CSV replay) thành đồ án tốt
nghiệp + portfolio Data Engineer, xoay quanh một câu chuyện duy nhất:

> **"Nền tảng theo dõi giá đa sàn (Tiki/Shopee/Lazada) + phát hiện bất thường
> & dự báo nhu cầu"**

Mọi layer — kể cả xử lý exception/outlier — phải phục vụ câu chuyện này,
không phải các module rời rạc không liên quan tới nhau.

Timeline: ~5 tháng (20 tuần), bắt đầu 2026-07-26. Không giới hạn cứng, nhưng
lộ trình ở mục 3 giả định tốc độ này.

## 2. Ràng buộc kỹ thuật đã chốt (đừng vi phạm khi implement tiếp)

- Máy dev đã chạy 8+ container (Kafka, 2×Spark, MinIO, Redis, Postgres, ES,
  Kibana, Superset). Service mới (Airflow, crawler headless browser...) phải
  chạy qua `docker-compose --profile`, không resident 24/7.
- `.venv` là Python 3.14 — **không** cài được `darts`/`pyod`/`torch`. ML thật
  (N-BEATS/LSTM/AutoEncoder) cần venv riêng 3.10–3.12. Đừng cố "fix" việc này,
  chỉ tận dụng cơ chế graceful-fallback đã có.
- Không thêm hệ bổ trợ nặng (Vault, Kubernetes, Kafka Connect...). Ưu tiên
  thư viện nhúng (Delta Lake = Spark lib, MLflow = sqlite backend) hơn service
  mới.
- `kafka-python`/`kafka-python-ng` **không** hỗ trợ idempotent producer thật —
  đừng claim "exactly-once", chỉ "at-least-once + DLQ quan sát được".
- Crawler: rate-limit + robots.txt + UA rotation, KHÔNG né chặn quy mô lớn —
  đây là crawler nghiên cứu cho đồ án, ghi rõ giới hạn này khi viết thesis.
- Crawler **không** tạo ra event hành vi thật (view/cart/purchase) — chỉ có
  catalog/price. Vì vậy có contract riêng (`PRICE_SNAPSHOT_FIELDS`), không
  gộp vào contract hành vi cũ.

Chi tiết đầy đủ + audit gốc: `C:\Users\Admin\.claude\plans\shimmering-rolling-blanket.md`
(file plan được duyệt, không sửa) và memory `graduation-thesis-roadmap.md`.

## 3. Trạng thái từng phase

| # | Phase | Trạng thái | Ghi chú |
|---|---|---|---|
| 0 | Foundation cleanup (config/secrets, `.env.example`, docs, CI) | ⏸️ Chưa làm (đã tạm hoãn 1 lần theo yêu cầu user) | `.env` đã dọn dở dang (xoá var rác, thêm MINIO_ROOT_USER/PASSWORD) nhưng **chưa** có `.env.example`, chưa sửa `docker-compose.yml` dùng `${VAR}`, chưa có CI |
| 1 | Multi-site crawler | 🔶 Đang làm — **Tiki xong, verified live**; Shopee/Lazada chưa bắt đầu | Xem chi tiết mục 4 |
| 2 | Ingestion hardening (DLQ cho producer.py/es_indexer.py) | ⏳ Chưa bắt đầu | `common/dlq.py` đã có sẵn (build sớm cho crawler) — Phase 2 chỉ còn việc retrofit vào `data_ingestion/producer.py` và `es_indexer.py` |
| 3 | Speed layer hardening | ⏳ Chưa bắt đầu | |
| 4 | Lakehouse (Delta Lake) + incremental + `price_warehouse_job.py` | ⏳ Chưa bắt đầu | |
| 5 | ML maturity (MLflow, prediction history, price-anomaly) | ⏳ Chưa bắt đầu | |
| 6 | Orchestration (Airflow) | ⏳ Chưa bắt đầu | |
| 7 | Serving API (FastAPI) | ⏳ Chưa bắt đầu | |
| 8 | Dashboards + thesis polish | ⏳ Chưa bắt đầu | |

Legend: ✅ xong · 🔶 đang làm/dở · ⏳ chưa bắt đầu · ⏸️ tạm hoãn

## 4. Chi tiết Phase 1 (crawler) — phase đang làm

### Đã xong (commit `727bcf6`, đã push lên `origin/master`)

- `config/schema.py`: thêm `PRICE_SNAPSHOT_FIELDS` + `normalize_price_snapshot`
  + `validate_price_snapshot` — contract riêng cho dữ liệu crawl, tách khỏi
  `CANONICAL_FIELDS`/`EVENT_TYPES` (hành vi người dùng).
- `common/dlq.py`: `publish_to_dlq()` — publish record lỗi sang topic
  `<topic>.dlq`, never raises. Build sớm hơn kế hoạch (vốn ở Phase 2) vì
  crawler cần ngay; Phase 2 sẽ retrofit vào ingestion cũ.
- `common/object_store.py`: `put_bytes(zone, path, data)` — ghi object vào
  MinIO bronze (mode `s3a`) hoặc local filesystem (mode `local`), không cần
  Spark session, mirror lại logic dual-mode của `config.settings.data_lake_uri`.
- `crawler/base.py`: `SiteCrawler` (Template Method, cùng khuôn với
  `analytics/base.py::Forecaster`) — robots.txt check, rate-limit + jitter,
  cô lập lỗi theo từng category/product (1 lỗi không chặn cả run).
- `crawler/sites/tiki.py`: `TikiCrawler` — gọi
  `https://tiki.vn/api/personalish/v1/blocks/listings`, field mapping đã
  **verify bằng response live thật** (2026-07-26). Ghi chú rõ trong docstring:
  Tiki không công bố API contract ổn định, cần re-verify field trước khi
  dùng cho việc khác.
- `crawler/runner.py`: CLI `python -m crawler.runner --site tiki [--dry-run]`
  — crawl, ghi raw JSON vào bronze (`crawl_raw/{site}/{date}/...`), publish
  snapshot chuẩn hoá lên Kafka topic `ecommerce_price_snapshots`, lỗi ra DLQ.
  1 site lỗi không chặn site khác (try/except quanh từng site trong `main()`).
- Test: `tests/test_crawler.py`, `tests/test_object_store.py`, + mở rộng
  `tests/test_schema.py`. **27/27 test pass** (12 test mới).
- **Verified thật**: `python -m crawler.runner --site tiki --dry-run` gọi
  live Tiki API thành công, lấy 40 sản phẩm, 0 lỗi.
- Dọn kèm: `.gitignore` thêm `data.zip` + `lakehouse/`; gỡ `data.zip` (633MB)
  khỏi git tracking VÀ khỏi lịch sử của 2 commit local (dùng `git filter-branch`
  vì GitHub chặn file >100MB) trước khi push — xem log chat để biết chi tiết
  nếu cần lặp lại thao tác tương tự.

### Còn lại của Phase 1

- **Shopee adapter** (`crawler/sites/shopee.py`): JS-rendered, cần Playwright
  (chưa có trong `requirements.txt` — cần thêm `playwright` + chạy
  `playwright install chromium`). Rủi ro anti-bot cao hơn Tiki nhiều.
- **Lazada adapter** (`crawler/sites/lazada.py`): tương tự Shopee.
- Đăng ký cả hai vào `SITE_CRAWLERS` dict trong `crawler/runner.py`.
- Cân nhắc thêm test cho Playwright-based adapter (có thể cần fixture HTML
  tĩnh thay vì gọi API JSON như Tiki).
- **Chưa** viết audit trail cho crawl run vào Postgres `audit` schema — theo
  plan gốc, việc này gộp vào Phase 4 (`price_warehouse_job.py`'s quality
  gate), không phải Phase 1.

### Cách tiếp tục ngay (nếu resume ở đây)

```bash
# xác nhận vẫn xanh trước khi code tiếp
.venv/Scripts/python.exe -m pytest tests/ -q

# thử lại Tiki (đã hoạt động) để chắc chưa bị Tiki đổi API
.venv/Scripts/python.exe -m crawler.runner --site tiki --dry-run
```
Sau đó viết `crawler/sites/shopee.py` theo đúng khuôn `SiteCrawler` (xem
`crawler/sites/tiki.py` làm mẫu) — điểm khác biệt chính: `fetch_listing` cần
Playwright (mở trang, chờ JS render, đọc DOM/network response) thay vì
`requests.get` đơn giản.

## 5. Quyết định kỹ thuật đã chốt (tránh hỏi lại / đổi ý giữa đường)

- **Lakehouse**: Delta Lake cho Gold (không phải Iceberg, không phải tự viết
  incremental logic tay) — vì là thư viện Spark (`delta-spark`), không cần
  thêm service.
- **ML tracking**: MLflow với sqlite backend (không cần container mới).
- **Orchestration**: Airflow 1 container LocalExecutor, tái dùng `postgres-dw`
  làm metadata DB — không Celery cluster.
- **Serving**: FastAPI nhỏ (3-4 endpoint), không phải microservice đầy đủ.
- **Schema**: hành vi (view/cart/purchase) và giá crawl (price snapshot) là
  **hai contract riêng**, không gộp — vì bản chất dữ liệu khác nhau (crawler
  không thể quan sát hành vi người dùng thật).

## 6. Việc đang tạm hoãn / chờ quyết định của user

- Phase 0 (foundation cleanup) bị hoãn 1 lần khi user nói "nghiên cứu và chốt
  duyệt phương án trước" — rồi user chuyển sang yêu cầu làm Phase 1 trước.
  **Chưa quay lại Phase 0** — nhắc user nếu họ quên.
- Chưa hỏi lại: có muốn tiếp tục Shopee/Lazada ngay, hay dừng ở Tiki để kiểm
  tra thủ công trước (câu hỏi này đã đặt ra ở cuối phần Tiki, chưa có câu trả
  lời khi file này được viết).

## 7. Tham chiếu

- Plan đã duyệt (đầy đủ, không sửa): `C:\Users\Admin\.claude\plans\shimmering-rolling-blanket.md`
- Memory cross-session: `graduation-thesis-roadmap.md`, `architecture-redesign.md`,
  `canonical-event-contract.md`, `ml-python-version-caveat.md` (trong
  `C:\Users\Admin\.claude\projects\C--code-data\memory\`)
- Commit crawler layer: `727bcf6` (branch `master`, đã push)
