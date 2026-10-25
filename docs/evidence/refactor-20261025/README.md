# Bằng chứng E2E trên `mp-refactor` (AC12), 2026-10-25

Stack: project `mp-refactor`, prefix `rf-`, `env/refactor.env`. Nguồn thật: Tiki (HTTP, `__NEXT_DATA__`, ≥ 2 s/request) và Lazada (Chromium một tab, ≥ 5 s/trang). Không có dữ liệu thô trong thư mục này; ảnh chụp là giao diện Airflow và Superset.

## Kết luận theo mục con

| Mục | Kết quả | Ghi chú |
|---|---|---|
| (a) Crawl ≥ 60 phút | **đạt**: 13:43:33Z đến 15:07:56Z, 84 phút | Tiki 1.445 trang (25 category seed ban đầu, rồi discovery nạp 4.237 category vào tầng SWEEP); Lazada 38 trang (6 category × 2 trang seed). |
| (b) Bronze có cả hai nguồn | **đạt**, có hai điểm cần giải thích (bên dưới) | Số liệu ở bảng 1. Circuit không mở. |
| (c) `marketplace_batch` ≥ 2 run liên tiếp | **đạt** | `scheduled__2026-10-25T14:00` và `scheduled__2026-10-25T15:00`, 5 task mỗi run đều `success`. Quality: mọi check MANDATORY PASS. Cache có `tiki` 28.881 và `lazada` 752 offer. Dashboard `marketplace-batch-gold`: 13 chart, script dựng dashboard gọi chart-data API cho từng chart và không có chart lỗi hay rỗng. |
| (d) `source_discovery`, `marketplace_maintenance` | **đạt** | `source_discovery` chạy `success` (lần đầu fail, xem phát hiện 3). `marketplace_maintenance` parse không lỗi, chưa đến lịch `@weekly`, không chạy, không xoá gì. |
| (e) Speed ≥ 10 phút | **đạt** | Chạy ổn định từ 13:57:40Z đến khi dừng 15:07Z (~70 phút), sau hai lỗi khởi động đã sửa. Redis có 32.171 key `rt:offer:*`. ES `marketplace-changes-v1`: tiki 31.451, lazada 752 change event. DLQ ES `marketplace-dlq-v1` = 0. |
| (f) Bằng chứng, dừng stack | **đạt** | `docker compose -p mp-refactor stop` đã chạy; hiện không container nào chạy. |

## Bảng 1: crawl và Bronze theo nguồn

Nguồn số liệu: `audit.crawl_request_attempt` join `audit.crawl_frontier`, `audit.crawl_source_state`, và liệt kê bucket MinIO.

| | Tiki | Lazada |
|---|---|---|
| Trang thành công | 1.352 | 27 |
| Trang lỗi (HTTP 200, `PARSE_ERROR`) | 93 (6,4%) | 11 (28,9%) |
| Record đã parse | 41.891 | 1.080 |
| Byte thô tải về | 649,8 MB | 68,5 MB |
| Độ trễ trung bình một trang | 2,3 s | 5,5 s (thành công), 49,5 s (lỗi) |
| Bronze/raw (đối tượng, nén) | 2.890 đối tượng, 157,4 MB | 103 đối tượng, 6,3 MB |
| Bronze/records (tệp, nén) | 1.352 tệp, 32,5 MB | 27 tệp, 0,7 MB |
| `consecutive_failures`, `opened_until` | 0, trống | 0, trống |

Giải thích hai điểm:

- **Tiki, 93 lỗi:** tất cả có thông báo "Tiki category page has no products", HTTP 200, trong khoảng 14:40–14:57. Đó là category rỗng trong đợt sweep 4.237 category, không phải bị chặn. Extractor đang phân loại trang rỗng là `PARSE_ERROR`; nên có một loại riêng (ghi vào nợ kỹ thuật).
- **Lazada, 11 lỗi:** thông báo "none of the 0 captured responses holds mods.listItems", mỗi lần chờ ≈ 49 s (timeout 45 s), tất cả trong 14:23–14:33. Khoảng này trùng lúc máy thiếu RAM (batch Spark chạy cùng speed và Chromium; hệ thống đã tắt hai lệnh nền vì thiếu bộ nhớ). Sau 14:33 các trang đều thành công, nên khả năng cao là Chromium chậm do thiếu RAM. **Không có bằng chứng nào cho thấy bị chặn** (không captcha, không HTTP 4xx, circuit không mở), nhưng tôi không chứng minh được nguyên nhân; còn là giả thuyết.

## Bảng 2: batch, quality, serving

| | Run 14:00 | Run 15:00 |
|---|---|---|
| Trạng thái | PROMOTED | PROMOTED |
| Thời gian chạy | 1 phút 41 giây | 2 phút 43 giây |
| Hàng Silver | 1.087 | 35.474 |
| Hàng Gold | 8.487 | 241.809 |
| Check quality | 16/17 PASS | 16/17 PASS |

Check không PASS duy nhất là `price_anomaly_evaluation_coverage` (mức ADVISORY): chưa có đủ lịch sử giá để đánh giá bất thường sau 1,5 giờ dữ liệu. Mọi check MANDATORY đều PASS.

Run 14:00 chạy lúc 14:23 (lúc đó mới bật `source_discovery` và unpause DAG), nên cửa sổ dữ liệu của nó là 13:43–14:00. Run 15:00 chạy đúng giờ.

Speed: Kafka `marketplace.records.v1` ~43.000 message; `marketplace.records.v1.dlq` có 15 message, tất cả lý do `duplicate_listing_on_page` (quarantine theo thiết kế, listing organic lặp trên một trang).

## Phát hiện từ lần chạy thật (đã sửa, có commit)

1. **Image Spark chạy Python 3.10** (`apache/spark:4.0.1` trên Ubuntu 22.04), trong khi package cần 3.11+ (`from datetime import UTC`). Speed rơi vào vòng khởi động lại. Sửa: cài Python 3.12 từ deadsnakes, `python3` trỏ về 3.12 (`72d8e45`).
2. **Topic `marketplace.observations.v1` không được `kafka-init` tạo.** Speed ghi vào topic dẫn xuất này rồi đọc lại; Spark báo `UnknownTopicOrPartitionException`. Sửa: thêm vào vòng tạo topic, kèm test (`72d8e45`).
3. **`source_discovery` của Lazada fail trong container Airflow:** đường dẫn `env/sources/lazada_categories.json` tính từ vị trí file, đúng trên repo nhưng sai với package đã cài. Sửa: chuyển file vào package (`categories.json`, khai báo `package-data`), test đổi theo (`3e28e3e`).
4. **`setup_superset.sh` bị CRLF** khi checkout trên Windows (`core.autocrlf=true`): shebang hỏng, `superset-init` thoát 127. Sửa: `.gitattributes` với `*.sh text eol=lf`.
5. **Hai lỗi ở R8, đã sửa trước E2E:** Airflow chạy bằng root không thấy module `airflow` (đổi `user: "50000:0"`, `PYTHONPATH` rỗng), và image Airflow không cho `pip` bằng root.

## Các mục chưa làm hoặc còn mở

- Chưa kiểm hồi phục (backup/restore) và các drill D1–D11 trên stack này.
- `marketplace_maintenance` chưa chạy lần nào (đã parse); chưa thử xoá thật.
- Dashboard Superset chụp trong cùng stack; phần lệnh `superset-init` in `Column sync skipped` và `No such option: -c` ở một bước phụ, chưa tìm nguyên nhân (dashboard vẫn dựng và có dữ liệu).
- Chưa đo tải RAM có hệ thống. Khi speed, Chromium và batch Spark chạy cùng lúc trên máy 14,6 GB đã xảy ra thiếu bộ nhớ; nên không chạy batch cùng speed trên máy này (rủi ro K4 của plan).
- Tiki sweep 4.237 category vẫn còn dở khi dừng (1.352 trang thành công). Đợt crawl dùng để kiểm kiến trúc, không phải để thu đủ dữ liệu.

## Ảnh chụp

- `screenshots/airflow-batch-grid.png`: DAG `marketplace_batch`, 5 task × 2 run, 0 task lỗi.
- `screenshots/airflow-batch-run1.png`: chi tiết run 14:00.
- `screenshots/airflow-discovery-grid.png`: DAG `source_discovery`.
- `screenshots/superset-batch-gold.png`: dashboard `marketplace-batch-gold` có dữ liệu cả hai nguồn.

## Docker trước và sau (AC14)

Không container hay volume nào có từ trước bị xoá; image `ecommerce/marketplace-python:1`, `ecommerce/spark-marketplace:4.0.1`, `ecommerce/spark-warehouse:3.5.1` giữ nguyên id (`23e5681cec3c`, `b81bc0fcbec7`, `dadc7cdab282`). Phần thêm mới chỉ thuộc project `mp-refactor` (container `rf-*`, volume `mp-refactor_*` và vài volume ẩn danh của chúng).
