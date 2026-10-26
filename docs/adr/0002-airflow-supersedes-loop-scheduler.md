# ADR-0002: Dùng Airflow thay cho scheduler vòng lặp (đảo quyết định D2)

Trạng thái: chấp nhận (2026-10-24). Hướng dẫn của giảng viên ngày 24/10: thêm Airflow vào pipeline.

## Bối cảnh

Phase 8 quyết định D2: batch chạy bằng một vòng lặp (`processing/batch/scheduler.py`), không dùng Airflow. Quyết định đó hợp lý khi batch là một hàm 120 dòng giữ lock suốt quá trình. Sau khi batch tách thành stage độc lập (ADR-0005), mỗi stage có thể được lập lịch, thử lại và quan sát riêng.

## Quyết định

1. Airflow 3.x (`apache/airflow:3.3.2-python3.12`, image dự án `ecommerce/airflow:3.3.2-r1`) là bộ điều phối. Compose profile `orchestration` gồm `airflow-init`, `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`; `LocalExecutor`; metadata DB là database `airflow` trên `postgres-dw` (tạo bằng `scripts/create_airflow_db.py`).
2. Ba DAG trong `src/ecomdata/orchestration/dags/`:
   - `marketplace_batch` (`@hourly`, `catchup=False`, `max_active_runs=1`, `retries=2`): mỗi stage (`silver`, `marts`, `quality`, `publish`, `promote`) là một task `DockerOperator` chạy image Spark; `as_of = data_interval_end`; `run_id = run_id_for(as_of)`; tham số `allow_backfill` mặc định `false`;
   - `marketplace_maintenance` (`@weekly`): xoá Bronze/raw quá hạn, `ops backup`, `ops storage-snapshot`;
   - `source_discovery` (`@daily`): discovery của Tiki rồi seed tầng `SWEEP`.
3. Scheduler vòng lặp cũ giữ lại ở profile `legacy-scheduler`, kèm test của nó. Không có hai run chạy song song: Airflow đặt `max_active_runs=1`, còn CLI dựa vào state machine của run (ADR-0005) và advisory lock.

## Hệ quả

- Retry, lịch sử chạy và đồ thị phụ thuộc nằm ở Airflow thay vì trong code vòng lặp.
- Task chạy trong container riêng nên cần `/var/run/docker.sock`. Trên Docker Desktop, nếu socket không dùng được thì dùng `BashOperator` gọi `docker` CLI (rủi ro K3 của plan); kết quả thử ghi trong `docs/REFACTOR_REPORT.md`.
- Thêm tài nguyên cho Airflow (API server, scheduler, dag-processor) trên máy 14 GB; stack E2E đặt `--driver-memory 3g` và không chạy speed cùng lúc với batch nếu thiếu RAM (rủi ro K4).
