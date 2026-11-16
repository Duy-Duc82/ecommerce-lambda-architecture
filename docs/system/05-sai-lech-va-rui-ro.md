# 5. Sai lệch giữa tài liệu và code, rủi ro đã biết

> Đúng với: commit `9c73660` · Cập nhật: 2026-11-16

Khi viết bộ tài liệu này, code được đọc trực tiếp và đối chiếu với tài liệu cũ. Mục 5.1 liệt kê chỗ tài liệu hoặc comment nói khác code; bộ `docs/system/` đã theo code. Mục 5.2 là rủi ro **suy ra từ code**, chưa chạy để xác nhận.

## 5.1 Tài liệu, comment lệch code

| # | Ở đâu | Tài liệu nói | Code làm | Đề xuất |
|---|---|---|---|---|
| 1 | `docs/ARCHITECTURE.md` §2 | Silver, Gold là Parquet; mart dựng thẳng từ Silver | mặc định Delta (`settings/__init__.py:116`); star bắt buộc Delta; mart dựng từ star (`star_sightings.py`) | thay hình §2 bằng Hình 2.1 |
| 2 | `docs/ARCHITECTURE.md` §5 | `silver` đọc Bronze "đến `as_of`"; `dimensions`/`facts` đọc Silver "cắt tại `as_of`" | `silver` đọc cửa sổ ngày; `dimensions`/`facts` cố ý không cắt (`stages/dimensions.py:13-16`) | sửa bảng §5 theo [04 §4.6](04-pipeline.md#46-p5--batch-theo-giờ) |
| 3 | `docs/ARCHITECTURE.md` §5, `stages/marts.py:1`, `quality_rules.py:27`, `repositories.py:313,353`, `datasets.py:5` | "10 mart" | 12 mart (thêm `discount_credibility_daily`, `discount_daily`), cả 12 được publish | sửa số |
| 4 | `docs/ARCHITECTURE.md` §5 | "7 dimension và bridge" | 7 dimension + 1 bridge = 8 bảng | sửa |
| 5 | `docs/ARCHITECTURE.md` §5 | không nhắc RESTORE star khi quality FAIL; CLI thiếu `--late-days`, `--rebuild`, `--silver-uri`, `--gold-root-uri` | có (`stages/quality.py:105-108`, `cli.py:19-50`) | bổ sung |
| 6 | `docs/ARCHITECTURE.md` §3, §4 | decorator ở `ingestion/publisher.py`; thiếu `speed/lakehouse.py` | decorator là `publishing_executor` trong `ingestion/service.py:388`; `lakehouse.py` có từ phase 11 | sửa |
| 7 | `docs/ARCHITECTURE.md` §2 | đường dẫn `bronze/raw` | key thật là `marketplace/raw/…` trong bucket `ecommerce-bronze` | sửa |
| 8 | `docs/DATA_MODEL.md` §1, §2 | hợp đồng ở `config/marketplace_schema.py`; Silver là một JSON mỗi observation | hợp đồng ở `domain/observations.py`, `domain/wire.py`; Silver là Delta (ELT, ADR-0001) | viết lại §1–§2 |
| 9 | `docs/DATA_MODEL.md` §1.1 | Kafka key là `(marketplace, platform_listing_id)` | một chuỗi `"<code>:<listing_id>"` (`normalize/wire.py:78`) | sửa |
| 10 | `docs/DATA_MODEL.md` §1.3 | DLQ do Silver sink ghi, stage `DECODE` hoặc `CONTRACT_VALIDATION`; không nhắc `records.v1.dlq` | chỉ speed ghi, chỉ `CONTRACT_VALIDATION`; `DlqStage.DECODE` và `infra/dlq.py` không được dùng; có thêm `records.v1.dlq` | sửa, hoặc xoá code chết |
| 11 | `docs/DATA_MODEL.md` §6 | projector ở `ops/es_projector.py` | `serving/es_projector.py` | sửa |
| 12 | `domain/topics.py:29`, `compose/docker-compose.yml:199-201`, `ingestion/service.py:1-6` | `marketplace.observations.v1` "không còn producer/consumer"; service publish `observations.v1` | normalizer của speed sinh topic này, query có trạng thái đọc nó; crawler publish `records.v1` | sửa comment |
| 13 | `domain/ordering.py:6-9` | speed xét LATE theo `order_key` (gồm `fetched_at`) | `change_rules.py:167-172` chỉ dùng `(observed_at, observation_id)`; hiện không sai vì `fetched_at = observed_at` | thống nhất một cách |
| 14 | `observability/build_kibana_demo_dashboard.py` | trigger 5 s | mặc định 30 s | sửa mô tả |
| 15 | `serving/superset/*.py` | spec 3 dashboard (batch, promotion, quality) | không có code provisioning gọi tới; dashboard thật do `observability/build_superset_batch_dashboard.py` dựng, chưa có chart discount | thêm chart discount vào builder thật, hoặc xoá spec |
| 16 | `scripts/init_postgres.sql:475-491` | CHECK của `error_kind` trong DDL inline thiếu `BLOCKED` | được bổ sung bằng `ALTER … ADD CONSTRAINT` phía sau | gộp vào DDL inline |
| 17 | `compose/docker-compose.yml:14-16, 422` | danh sách DAG thiếu `marketplace_reprocess`; es-projector có "daily storage snapshot" | có 4 DAG; projector không có snapshot | sửa comment |
| 18 | Bảng Delta `gold/speed/price_changes` | tên gợi ý chỉ chứa thay đổi giá | chứa cả 7 loại thay đổi | đổi tên thành `changes` ở phase sau |

## 5.2 Rủi ro trong code (chưa kiểm chứng bằng chạy)

| # | Rủi ro | Cơ chế | Hệ quả có thể | Mức |
|---|---|---|---|---|
| R1 | **Frontier vượt `max_attempts`** | `lease_due` tăng `attempts` (`frontier.py:147`); khi circuit mở, `mark_retry` trả task về `RETRY_WAIT` mà không hoàn lại lượt (`worker.py:159-160` nói ngược lại). `recover_expired_leases` cũng có thể để task ở `RETRY_WAIT` với `attempts == max_attempts`. Lần lease tiếp theo đưa `attempts` lên `max+1`, vi phạm CHECK `attempts <= max_attempts` | câu lệnh lease lỗi cho **cả lô** 10 task; worker có thể kẹt lặp lại. Dễ xảy ra khi bị chặn lâu | cao |
| R2 | `BLOCKED` không chờ lâu | comment `scheduling.py:69-70` nói chờ lâu, code dùng backoff thường | gõ cửa lại site đang chặn sớm hơn dự kiến; chỉ circuit breaker che được | trung bình |
| R3 | Lỗi MinIO mở circuit của sàn | `STORAGE_ERROR` được tính vào chuỗi lỗi của nguồn (`worker.py:44`) | sự cố lưu trữ nội bộ làm dừng crawl 15 phút như thể site chặn | thấp |
| R4 | Không bảo trì Delta | không có `OPTIMIZE`/`VACUUM`; không xoá run Gold cũ | file nhỏ và version cũ tích luỹ; mỗi giờ một thư mục run, 12 mart. Ước tính Bronze tăng 1,5–2 GB/ngày (`DANH_GIA_HE_THONG_20261113.md`), Silver/Gold chưa đo riêng | trung bình khi chạy ≥ 30 ngày |
| R5 | Speed sập khi sink lỗi tạm thời | ES/Kafka lỗi thì ném ngoại lệ, Docker khởi động lại container thay vì thử lại có backoff | dữ liệu vẫn đúng nhờ checkpoint; log nhiễu, cảnh báo giả (đã thấy ngày 31/10, E2–E3) | thấp |
| R6 | Worker khởi động lại chờ lease cũ | lease 300 s gắn với `hostname:pid` cũ | mất tới 5 phút sau mỗi lần restart | thấp |
| R7 | Lakehouse speed: không có `OFFER_STALE` | `ProcessingTimeTimeout` có thể không bắn dưới `availableNow` | bảng `price_changes` trên Databricks thiếu loại `OFFER_STALE` | thấp, chưa kiểm |
| R8 | Hybrid chưa khép vòng | job Databricks không publish; Bronze lên S3 bằng `mc mirror` chạy tay | dashboard local không thấy kết quả của Databricks nếu không publish tay | đã biết, theo thiết kế |
| R9 | Code định nghĩa mà không dùng | trạng thái `DISABLED`, tier `HOT`, fetcher `api` (ném `NotImplementedError`), attempt status `PARTIAL` | đọc code dễ hiểu nhầm năng lực hệ thống | thấp |

Nên xử lý R1 trước tiên: thêm điều kiện `attempts < max_attempts` vào `lease_due`, hoặc cho `mark_retry` của nhánh circuit hoàn lại lượt đã tính, kèm một test tái hiện.
