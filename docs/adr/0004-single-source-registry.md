# ADR-0004: Một `SourceRegistry`

Trạng thái: chấp nhận (2026-10-24).

## Bối cảnh

Registry các site bị lặp ở 6 chỗ (`runner.py`, `reparse.py`, `ops/backup.py`, `seed_frontier.py`, `config/counter_semantics.py`, `batch_layer/marketplace_dimensions.py`). Thêm một site nghĩa là sửa cả 6 chỗ và dễ bỏ sót.

## Quyết định

`domain/sources.py` có `SourceSpec` và `SourceRegistry` (`SOURCES`, `TIKI`, `LAZADA`). Một `SourceSpec` chứa mọi dữ kiện riêng của nguồn: `code`, `marketplace_id`, URL gốc, kiểu fetcher (`http` hoặc `browser`), extractor, normalizer, tiền tệ, ngữ nghĩa counter, discovery, danh sách trường PII bỏ, `capture_patterns` và `wait_for` của trình duyệt, regex nhận trang bị chặn, mức politeness tối thiểu.

Mọi nơi khác tra registry: `ingestion/runtime.py` (fetcher, extractor, robots theo nguồn), `processing/batch/stages/silver.py` (`registry.normalizer(code)`), `domain/counter_semantics.py`, `ops`, `seed`.

Checklist thêm một nguồn:

1. `ingestion/sources/<site>/extractor.py` (tìm mảng dòng và `page_context`) và `discovery.py`.
2. `processing/normalize/sources/<site>.py`: `PAYLOAD_SCHEMA`, `to_wide`, `to_offer_observation`.
3. Một `SourceSpec` trong `domain/sources.py` và đăng ký vào `SOURCES`.
4. Fixture nhỏ (tối đa 5 sản phẩm, không PII) trong `tests/fixtures/sources/<site>/`, test extractor và normalizer.

## Hệ quả

- `tests/architecture/test_single_definitions.py` kiểm registry chỉ có một định nghĩa; `tests/unit/domain/test_sources.py` kiểm thêm một nguồn giả chỉ cần spec, extractor, normalizer và đăng ký.
- Literal tên site ngoài các vị trí trên bị `tests/architecture/test_site_literals.py` chặn.
