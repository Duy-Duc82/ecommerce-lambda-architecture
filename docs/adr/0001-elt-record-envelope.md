# ADR-0001: ELT và envelope `marketplace-record.v1`

Trạng thái: chấp nhận (2026-10-24). Nguồn quyết định: `docs/PLAN_REFACTOR_ARCHITECTURE.md` mục 3.1, 3.3.

## Bối cảnh

Pipeline cũ chuẩn hoá ngay lúc crawl (ETL). `ParsedListingPage.observations` bắt buộc là `MarketplaceObservationV1`, wire decoder từ chối mọi khoá lạ. Trang category Tiki có 133 trường lá cho mỗi sản phẩm, nhưng chỉ khoảng 17 trường được giữ. Mọi trường bị bỏ ở bước crawl không thể khôi phục nếu không crawl lại. `FetchResult` chỉ chứa một HTTP response, nên không biểu diễn được kết quả của trình duyệt (HTML cộng nhiều phản hồi XHR).

## Quyết định

1. Crawler chỉ **tìm mảng dòng** trong phần tải về và tách `page_context`. Extractor không ép kiểu, không chọn trường, không lọc quảng cáo.
2. Mỗi dòng là một `SourceRecord` (`domain/records.py`), envelope `marketplace-record.v1`:
   - định danh: `record_id = deterministic_id(raw_artifact_id, record_kind, record_index)`, `source_code`, `marketplace_id`;
   - `record_kind` là `listing_row` hoặc `page_context`;
   - lineage: `raw_artifact_id`, `raw_uri`, `body_sha256`, `crawl_run_id`, `adapter_version`, `extractor_version`, `request_target`, `request_page`, `fetched_at`, `http_status`;
   - `payload`: object JSON nguyên vẹn của dòng; `dropped_fields`: tên các trường PII đã bỏ (danh sách khai báo trong `SourceSpec`).
3. Bronze có hai tầng: `bronze/raw` (các part gzip và `metadata.json` v2, đọc được cả bản v1) và `bronze/marketplace/records` (JSONL.gz theo trang). File records được ghi xong rồi mới publish lên Kafka `marketplace.records.v1`.
4. Chuẩn hoá chuyển sang Spark: `processing/normalize/sources/<site>.py` khai báo `PAYLOAD_SCHEMA` đủ các trường đã thấy, và `to_offer_observation` map payload sang các cột canonical mà `marts.py` đọc. Stage `silver` ghi `offer_observations_v2`, bảng rộng theo nguồn, `page_context`, `quarantine`, và báo cáo drift (khoá có trong payload mà schema không khai báo).
5. Quảng cáo được đánh dấu ở Silver (`is_advertisement`); mart và quality chỉ xét hàng organic.
6. `reparse` đọc raw (v1 hoặc v2), extract lại, so `record_id` và `payload` với Bronze/records.
7. Service `silver-sink` bị bỏ. Speed layer đọc `marketplace.records.v1` và dùng cùng normalizer (`processing/speed/normalizer.py`).

## Hệ quả

- Không mất dữ liệu nguồn: số record `listing_row` bằng số dòng nguồn (kể cả quảng cáo), `payload` bằng đúng object JSON nguồn (`tests/unit/ingestion`, `tests/spark/test_normalize.py`).
- Thêm trường mới của nguồn không cần crawl lại: chạy lại stage `silver` với `--silver-since`.
- Thêm một nguồn chỉ cần spec, extractor, normalizer và đăng ký (ADR-0004).
- Chi phí: Bronze/records lớn hơn Silver cũ; payload đầy đủ nằm trong Parquet qua cột `payload_json`.
- Topic `marketplace.observations.v1` không còn producer bên ngoài. Speed layer dùng nó làm topic dẫn xuất giữa hai query của chính nó (`processing/speed/service.py`), vì truy vấn có trạng thái (`applyInPandasWithState`) đọc event canonical.
