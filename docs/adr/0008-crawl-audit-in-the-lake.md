# ADR-0008: Bản lưu của audit nằm trong lake; PostgreSQL giữ bản vận hành

Trạng thái: chấp nhận (2026-11-02). Kế hoạch: `docs/PLAN_PIPELINE_HARDENING.md` (H5), `docs/PLAN_PHASE10_ON_REFACTOR.md` (D7).

## Bối cảnh

Quyết định ngày 26/10: PostgreSQL chỉ là bộ nhớ đệm phục vụ; mọi lưu trữ dữ liệu nằm trong object storage. Trước H5, crawl audit (`crawl_run`, `crawl_request_attempt`) chỉ có trong PostgreSQL, và batch đọc nó bằng JDBC, quét toàn bảng, cộng thêm `crawl_frontier` để biết mã sàn của từng attempt. Run của batch và kết quả quality cũng chỉ có trong PostgreSQL.

Sau quyết định đó, ngày 31/10 stack có thêm Grafana. Dashboard và cảnh báo của nó đọc các bảng `audit.*` trong PostgreSQL, và circuit breaker của crawler đọc `crawl_source_state` ở mỗi task. Chuyển hẳn audit ra khỏi PostgreSQL sẽ phá cả hai.

## Quyết định

1. **Crawl audit ghi hai nơi.** Crawler vẫn ghi PostgreSQL như trước (bản vận hành: breaker, Grafana). Khi một crawl run kết thúc, worker ghi thêm một object gzip JSON-lines vào Bronze:
   `bronze/marketplace/crawl_audit/observed_date=<ngày bắt đầu>/hour=<HH>/crawl_run_id=<id>.jsonl.gz`.
   Object có một dòng `run` và một dòng `attempt` cho mỗi attempt. Dòng `attempt` tự mang `marketplace_code`, `request_target`, `resource_type` của task, nên batch không cần đọc frontier. Bố cục nằm ở `domain/crawl_audit.py`.
2. **Batch đọc audit từ lake** (`MARKETPLACE_CRAWL_AUDIT_SOURCE=lake`, mặc định). `postgres` vẫn dùng được cho một lake ghi trước H5.
3. **Ghi lake hỏng thì không làm hỏng task.** Fetch, Bronze records và bản PostgreSQL đều đã ghi. Worker log cảnh báo và đi tiếp. `ops audit-export [--since]` chép các run đã xong từ PostgreSQL vào lake theo đúng bố cục đó, và chạy lại thì không đổi gì. Lệnh này cũng dùng để chuyển dữ liệu cũ.
4. **Run đang chạy chưa vào lake.** Nó vào lake khi kết thúc. Luật đối soát (check 8) chỉ xét run đã kết thúc và đã qua thời gian settle, nên không bị ảnh hưởng.
5. **`fact_crawl_attempt.attempt_id` = `xxhash64(crawl_run_id, task_id, attempt_number)`**, không còn là sequence của PostgreSQL. Fact vì thế giống nhau dù dựng từ bản nào.
6. **Run và kết quả quality của batch:** runner ghi lại `_runs/run_id=<run>/run.json` sau mỗi stage và khi run hỏng. Stage `quality` ghi `_runs/run_id=<run>/quality.json`, gồm mọi kết quả và quyết định. Máy trạng thái và khoá của run vẫn ở PostgreSQL, giống frontier: đó là trạng thái vận hành.
7. **Giữ lại trong PostgreSQL:** `crawl_frontier` (hàng đợi `FOR UPDATE SKIP LOCKED`), `crawl_source_state` (breaker), và máy trạng thái của run.

## Hệ quả

- Mart `crawl_reliability_daily`, `source_coverage_daily` và `fact_crawl_attempt` dựng từ lake khớp với bản dựng từ JDBC. `tests/spark/test_crawl_audit_lake.py` kiểm điều này trên cùng dữ liệu.
- Batch không còn quét JDBC toàn bảng. Lake audit được đọc toàn bộ ở mỗi run, đúng với quyết định H2 rút gọn (dựng lại toàn bộ mỗi run).
- Số object trong Bronze tăng thêm một cho mỗi crawl run, ngang với số object records.
- Chưa làm (D7c): `marketplace_speed_batch`, `marketplace_stream_progress`, `storage_snapshot` vẫn chỉ nằm trong PostgreSQL. Đây là số đo vận hành mà Grafana đọc, không phải dữ liệu phân tích.
