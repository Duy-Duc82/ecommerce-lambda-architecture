# Chạy E2E toàn luồng, 31/10/2026

> **Báo cáo chỉ số đầy đủ của cả đợt chạy 02:30 → 17:24 UTC** (tốc độ crawl từng sàn, giới hạn biên, thí nghiệm lỗi E1–E5, các trường hợp biên): [`BAO-CAO-CHI-SO.md`](BAO-CAO-CHI-SO.md). Mở lại BI: `sh docs/evidence/e2e-run-20261031/xem-lai-bi.sh`. Backup cuối: `data/ops/backups/bk-20261031T172502Z`.

Stack `mp-refactor`, nhánh `phase-10/star-on-refactor`. Cửa sổ đánh giá: 02:30 → 08:30 UTC (09:30 → 15:30 giờ VN). Mọi số liệu lấy từ bảng `audit.*`, Elasticsearch, Loki và Prometheus của stack; số thô ở `raw/`, truy vấn SQL ở `raw/report.sql`.

## Luồng

Crawl Tiki (HTML danh mục) → Bronze (MinIO) + Kafka → speed (Spark Structured Streaming) → Elasticsearch/Redis → Kibana. Airflow chạy batch mỗi giờ: Silver → dimension → fact → mart → quality → publish (Postgres `cache`) → promote → Superset. Alloy gom log và metrics vào Loki/Prometheus, Grafana hiển thị và cảnh báo.

| Xem ở đâu | URL |
|---|---|
| Grafana (tổng quan, pipeline, tài nguyên, Airflow, log) | http://localhost:3004 |
| Airflow | http://localhost:8384 |
| Kibana: "Demo — dữ liệu chảy liên tục qua pipeline", "Marketplace — realtime changes", "Marketplace — source health and freshness" | http://localhost:5604 |
| Superset: "Marketplace — batch layer (Gold, chất lượng, lịch sử)" | http://localhost:8086/superset/dashboard/marketplace-batch-gold/ |

## Kết quả

### Crawl (theo giờ, UTC)

| Giờ | Lượt tải | Thành công | Lỗi | Bản ghi parse | Raw (MB) | p95 (ms) |
|---|---|---|---|---|---|---|
| 02 (từ 02:30) | 475 | 471 | 4 | 12.620 | 191,9 | 3.300 |
| 03 | 1.255 | 1.236 | 19 | 32.556 | 522,1 | 3.269 |
| 04 | 1.261 | 1.261 | 0 | 30.906 | 521,5 | 3.151 |
| 05 | 1.265 | 1.265 | 0 | 33.908 | 552,8 | 3.161 |
| 06 | 42 | 41 | 1 | 601 | 14,5 | 3.064 |
| 07 | 25 | 25 | 0 | 601 | 10,7 | 2.914 |
| 08 (đến 08:30) | 25 | 25 | 0 | 601 | 10,7 | 2.991 |

- 02:30 → 06:00 là lượt quét SWEEP: 4.227 danh mục lấy từ sitemap, khoảng 1.260 trang/giờ (một trang mỗi 2 giây theo `CRAWL_REQUEST_DELAY_SECONDS`). Sau đó các danh mục này hẹn quét lại sau một ngày, nên từ 06:00 chỉ còn tier ACTIVE (25 trang mỗi giờ). Đây là lịch của frontier, không phải sự cố.
- 24 lỗi, đều `PARSE_ERROR`. 23 lỗi là danh mục rỗng (`paging.total = 0`), xảy ra trước bản sửa lỗi lúc 03:53. Lỗi còn lại là `sach-van-phong-pham-qua-luu-niem/c21298`: Tiki báo 284 sản phẩm nhưng HTML không kèm sản phẩm nào, nên vẫn tính là lỗi (đúng thiết kế).
- Không có `RATE_LIMITED` hay `BLOCKED`; DLQ trống trong cả cửa sổ.

### Speed

| Chỉ số | Giá trị |
|---|---|
| Micro-batch | 701, thành công 701 |
| Dòng vào | 105.116 |
| Thời gian micro-batch | trung bình 0,11 s, tối đa 11,5 s |
| Lag so với Kafka (`offsets_behind_latest`) | lớn nhất 0 |
| Elasticsearch | `marketplace-changes-v1` 115.385 tài liệu, `marketplace-offers-current-v1` 75.457 |

### Batch (Airflow `marketplace_batch`, mỗi giờ)

| Run | Trạng thái | Quality | Silver | Gold | Phút |
|---|---|---|---|---|---|
| mp-20261031T0200Z | PROMOTED | PASS | 38.808 | 262.665 | 24,3 (gồm thời gian sửa lỗi) |
| mp-20261031T0300Z | FAILED | – | – | – | 6,1 |
| mp-reprocess-20261031T032842Z | PROMOTED | PASS | 65.840 | 448.601 | 8,2 |
| mp-20261031T0400Z | PROMOTED | PASS | 81.543 | 543.580 | 8,7 |
| mp-20261031T0500Z | PROMOTED | PASS | 110.529 | 693.421 | 7,0 |
| mp-20261031T0600Z | PROMOTED | PASS | 142.012 | 792.408 | 6,7 |
| mp-20261031T0700Z | PROMOTED | PASS | 142.612 | 792.424 | 6,7 |
| mp-20261031T0800Z | PROMOTED | PASS | 143.212 | 792.424 | 6,7 |

- Từ 04:00, 5 run theo giờ liên tiếp chạy hết 8 task mà không cần can thiệp.
- Run 03:00 lỗi vì layout Silver cũ (mục "Lỗi đã sửa", số 3). Trong lúc đó Superset vẫn phục vụ bản 02:00; DAG `marketplace_reprocess` (`rebuild=true`) dựng lại Silver từ Bronze.
- Quality của run mới nhất: 16/16 luật bắt buộc PASS, 4/5 luật advisory PASS. Luật FAIL là `price_anomaly_evaluation_coverage = 0`, vì phát hiện giá bất thường chưa có đủ số ngày lịch sử làm baseline.
- Cache BI (run 08:00): 75.455 offer, 3.908 người bán, 105.329 dòng lịch sử giá theo ngày, 5.615 dòng giá theo danh mục. 13/13 chart Superset đều trả dữ liệu.
- `marketplace_maintenance` (prune, backup, storage snapshot) đã chạy thành công lúc 04:32 và tạo backup `bk-20261031T043158Z`.

### Giám sát

| Chỉ số (6 giờ) | Giá trị |
|---|---|
| Log trong Loki theo layer | orchestration 24.536, storage 3.927, batch 2.684, serving 2.490, observability 2.193, speed 1.670, crawl 745, ops 228, ingest 10 |
| Log task Airflow | có ở mọi task của 4 DAG, gắn nhãn `dag_id`, `task_id`, `run_id` |
| Endpoint lên | Elasticsearch, Grafana, MinIO, Superset 100%; Kibana, Loki 99,86%; Airflow 99,42% (do tạo lại container khi đổi image) |
| RAM VM Docker cao nhất | 93,5%, lúc batch chạy (batch dùng tối đa 8,7 GB). Không bật cảnh báo vì không kéo dài 10 phút |
| CPU trung bình | batch 2,8 core, speed 0,7 core, các layer khác dưới 0,3 core |
| Series trong Prometheus | 12.976 |

Cảnh báo đã bật trong cửa sổ, và đều đúng:

- "Batch run thất bại", "Batch không chạy đúng khung giờ", "DAG run gần nhất thất bại": bật khi batch lỗi, tự hết sau khi sửa.
- "Crawl ngừng" (06:23): báo nhầm khi crawler rảnh giữa hai chu kỳ. Luật đã được viết lại (mục "Lỗi đã sửa", số 9).

Lúc kết thúc không còn cảnh báo nào bật.

## Lỗi đã sửa trong lượt chạy

| # | Lỗi | Nguyên nhân | Sửa |
|---|---|---|---|
| 1 | Task `dimensions` báo `invalid choice` | Task Airflow chạy code đóng sẵn trong image `spark-marketplace:4.0.1-r3`, cũ hơn H0 | Build image `4.0.1-r4`, `marketplace-python:3`, `airflow:3.3.2-r3` và tăng tag. Stack dev đặt `AIRFLOW_SOURCE_MOUNT` để task chạy working tree như các service khác |
| 2 | `DeltaDataSource source does not support user-specified schema` | `read_silver` gắn schema khai báo sẵn cho bảng Delta | Chỉ gắn schema khi đọc Parquet |
| 3 | Silver: `Partition columns do not match` | Silver do code cũ ghi (theo `source_code`), code mới ghi theo `source_code, observed_date` | Chạy `marketplace_reprocess` với rebuild. `write_silver` giờ kiểm tra layout trước và báo `SilverLayoutMismatch`, chỉ rõ cần rebuild |
| 4 | Maintenance lỗi từ 25/10 | Lệnh `ops prune-raw` chưa từng được nối vào CLI | Thêm lệnh, kèm test kiểm tra mọi lệnh của DAG maintenance đều có trong CLI |
| 5 | Backup: `Permission denied: /reports` | Container task không có `/reports` ghi được | `AIRFLOW_REPORTS_MOUNT` bind `data/ops`, đúng chỗ service `ops` đọc và restore |
| 6 | Backup: `_staged_commits/ is named by the stack but absent` | Listing MinIO trả về cả object đánh dấu thư mục rỗng mà Delta tạo | `S3ObjectStore.stat` bỏ qua key kết thúc bằng `/` |
| 7 | 116 danh mục Tiki `FAILED` vì "no products" | Danh mục rỗng thật (`paging.total = 0`) bị coi là lỗi parse | Coi là trang hợp lệ có 0 dòng, giữ bản ghi page-context làm bằng chứng; trang rỗng mà paging báo có hàng vẫn là lỗi |
| 8 | `observability/healthcheck.sh` chỉ chạy được với `mp-tikihtml` | Hardcode cổng và tên container | Nhận `env/<stack>.env`; chỉ báo crawl khi có việc quá hạn |
| 9 | "Crawl ngừng" báo nhầm mỗi giờ | Luật chỉ xét "20 phút không tải" | Chỉ báo khi marketplace đã crawl trong 6 giờ qua có task `READY` quá hạn hơn 10 phút |
| 10 | Grafana ghi `level=error` "SMTP not configured" | Không cấu hình kênh nhận thì Grafana vẫn thử gửi email | Alloy gắn `level=expected` cho dòng này, giống lỗi cố ý của batch |

Test: 1.385 unit và architecture test pass, 2 skip (cần package `docker`, đã chạy trong image Airflow); 19 test Spark Silver pass; 19 test DAG và orchestration chạy trong image `airflow:3.3.2-r3` pass.
