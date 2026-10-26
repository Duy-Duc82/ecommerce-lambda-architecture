# ADR-0005: Batch theo stage và state machine

Trạng thái: chấp nhận (2026-10-24), hiện thực 2026-10-25.

## Bối cảnh

`_run_marketplace_warehouse` là một hàm 120 dòng giữ lock suốt toàn bộ: các stage truyền DataFrame trong bộ nhớ cho nhau nên không tách ra chạy riêng được; trạng thái audit là state machine ngầm; repository được tạo bằng `from_settings()` ngay trong hàm. Check `observation_id_unique` chạy sau dedup nên không bao giờ fail.

## Quyết định

Pipeline gồm 5 stage, mỗi stage là một lớp hiện thực `Stage` (`processing/batch/stages/base.py`) với `name`, `requires`, `produces`, `allowed_from` và `milestone`:

```
silver -> marts -> quality -> publish -> promote
```

- Stage đọc đầu vào từ storage và ghi đầu ra vào storage (Silver v2, Gold theo run `gold/marketplace/runs/run_id=<id>/`, manifest), không truyền DataFrame cho stage sau; chạy lại là idempotent.
- `domain/batch_run.py` có `BatchRunStatus` và bảng chuyển trạng thái: `CREATED → SILVER_BUILT → GOLD_WRITTEN → QUALITY_PASSED | QUALITY_FAILED → PUBLISHED → PROMOTED`, cùng `HELD` và `FAILED`. Runner (`processing/batch/runner.py`) kiểm `allowed_from`, chạy lại stage đã xong là no-op trừ `--force`, đánh dấu `FAILED` khi stage lỗi, và chỉ `--resume` mới cho tiếp tục run đã `FAILED` hoặc `QUALITY_FAILED`.
- `expected_current_run_id` (token compare-and-swap của pointer) được ghi vào audit row lúc tạo run.
- Advisory lock (`processing/batch/lock.py`) giữ suốt một lần gọi runner, nên `--all` và từng stage dùng lock như nhau.
- Phụ thuộc inject qua `BatchDependencies` (Protocol trong `ports.py`); `bootstrap.py` là composition root. Không còn `from_settings()` trong `stages/`.
- CLI: `python -m ecomdata.processing.batch run --run-id … --as-of … --stage <name>|--all [--resume] [--allow-backfill] [--quality-only] [--force] [--silver-since]`.
- Sửa `observation_id_unique`: stage `marts` đếm `observation_id` lặp **trước** dedup, ghi vào `silver/_runs/run_id=<id>/marts.json`; stage `quality` đọc số đó. Phép đối chiếu với crawl audit đếm cả hàng quảng cáo và hàng quarantine, vì audit đếm hàng lúc extract.

## Lệch plan (có lý do)

Plan liệt kê `promote → publish`. Giữ đúng ngữ nghĩa cũ: cache Postgres được publish **trước** khi pointer `current` đổi, để pointer không bao giờ trỏ vào Gold run mà cache chưa được ghi. Vì vậy thứ tự stage là `publish → promote`, và `PUBLISHED → PROMOTED` trong state machine. Run chỉ là `PROMOTED` khi pointer đã trỏ vào nó; run thua đua hoặc là backfill cũ bị `HELD` kèm `held_reason`.

## Hệ quả

- Airflow lập lịch từng stage (ADR-0002).
- Test: mọi chuyển trạng thái hợp lệ và không hợp lệ (`tests/unit/domain/test_ordering_and_batch_run.py`), runner và stage publish/promote (`tests/unit/processing/test_batch_runner.py`), pipeline qua các stage thật (`tests/spark/test_batch_stages.py`: `--all` và từng stage riêng cho cùng Gold, manifest và audit).
