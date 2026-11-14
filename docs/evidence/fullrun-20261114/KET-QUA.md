# Kết quả lượt chạy toàn luồng 14/11/2026

## 1. Thời gian chạy và số liệu tổng

- **Chạy thực:** từ 01:53 VN (18:53 UTC 13/11) đến 11:09 VN (04:09 UTC 14/11), tức **khoảng 9 giờ 16 phút**, không đủ 10 giờ. Lý do dừng sớm: RAM host xuống 2,6 GB, hệ thống dừng lệnh chờ nền của giờ 10 và yêu cầu không tự chạy lại (xem `NHAT-KY.md`, mục giờ 9,2). Run 0400Z chưa có trong kết quả.
- **Lớp đã bật:** crawl Tiki, Kafka, speed, es-projector, ES, Redis, MinIO, Postgres, Airflow (batch mỗi giờ, discovery), Loki/Prometheus/Grafana/Alloy, Superset, Kibana (Kibana dừng lúc 11:05 VN để giải phóng RAM).
- **Batch:** 10 run (`mp-20261113T1800Z` đến `mp-20261114T0300Z`), **10/10 PROMOTED, quality PASS**. Gold 1.432.017 dòng (1800Z) tăng lên 1.966.878 (0300Z).
- **Speed:** trong mọi lượt theo dõi `failed=0`, `invalid=0`, DLQ=0; p95 giảm từ 173,6 s (giờ 0, đang đuổi phần tồn Kafka) xuống khoảng 59–62 s.
- **Crawl:** khoảng 1.270 lượt OK/giờ trong 4 giờ đầu, sau đó chỉ 25 lượt/giờ (xem mục 4). Audit cũ xuất sang lake: 10.846 lượt.
- **Serving:** Superset và Kibana lên `healthy` lúc bật; tôi chưa mở giao diện để kiểm bảng `marketplace_discount_credibility_daily` (mục 3 của kịch bản, dòng Serving), nên mục này **chưa kiểm**.

## 2. Tiêu chí của CHIEN-LUOC-DANH-GIA-ML.md (mục 1-3), từ `bao-cao-ml.json` (04:09 UTC)

| Tiêu chí | Số đo | Ngưỡng | Kết quả |
|---|---|---|---|
| batch_runs | 10 run xong, 10 PROMOTED | mọi run sau run đầu | PASS |
| coverage_rule | quality PASS, observed 0,148 (0300Z) | PASS và > 0 | PASS |
| grain | 253.198 = 253.198 | bằng nhau | PASS |
| row_invariants | 0 vi phạm ở cả 4 kiểm tra | 0 | PASS |
| history_rules | ngày 14/11: 190 dòng có lý do từ lịch sử (`CLAIM_EXCEEDS_HISTORY` 39, `NOT_BELOW_REFERENCE` 151) | > 0 | PASS |
| category_reconciles | không lệch | lệch 0 | PASS |
| stability | tệ nhất 99,997 % giữ nguyên (7 cặp run) | ≥ 99 % | PASS |
| marts (hiệu năng) | cơ sở 88,4 s (run đầu), tệ nhất 105,4 s, tăng 19,3 % | ≤ 30 % | PASS |
| fit | `fit_seconds` khoảng 0,11–0,13 s, mẫu 56.243–66.575 dòng | < 60 s, mẫu ≤ 200.000 | PASS |
| ranked_rows | 66.824 dòng có hạng, đủ `showing_reduction` | khớp | PASS |
| **flag_share** | ngày 14/11: 151/249 = **60,6 %** (ngày 13/11 đầy đủ lúc giờ 3: 5.686/16.284 = 34,9 %) | 1–10 % | **FAIL** |
| rule_ml_agreement | lift 6,15 (13/11 lúc giờ 3: 2,17) | ≥ 2,0 | PASS |

Ngưỡng không đổi. Hai lưu ý: (a) ngày 14/11 mới có 249 dòng "hiển thị giảm giá" lúc đo nên con số 60,6 % rất nhỏ mẫu; (b) lift 6,15 cũng trên mẫu rất nhỏ (39 dòng mạnh).

**Nguyên nhân flag_share FAIL (quan sát, chưa kiểm chứng sâu):** ngày 13/11 có 5.568 dòng `SUSPECT` đều mang lý do `CLAIM_EXCEEDS_HISTORY` và 8.066 lý do `NOT_BELOW_REFERENCE` trên 2.937 dòng `QUESTIONABLE`. Nghĩa là luật lịch sử của `discount-rules.v1` gắn cờ một phần lớn giảm giá hiển thị trên dữ liệu 4 ngày lịch sử. Có thể do luật quá chặt, hoặc do dữ liệu thật; chỉ phân biệt được bằng việc gán nhãn (mục 6 kịch bản, chưa làm). Không đổi luật hay ngưỡng ở lượt này.

## 3. Ghi chú hiệu năng

Mốc `marts` lấy từ run đầu của chính lượt này (88,4 s), không có mốc cũ có ML. Đây là đo độ ổn định, không phải so trước/sau: 10 run nằm trong 88–105 s. Stage nặng nhất là `dimensions` (khoảng 217–259 s). Stage `silver` tăng từ 38 s lên 100–189 s ở các run sau; chưa tìm nguyên nhân.

## 4. Lỗi gặp trong lượt chạy (tóm tắt `NHAT-KY.md`)

1. `theo-doi.sh`: `GROUP BY 1` trỏ vào biểu thức có hàm gộp. Sửa thành `GROUP BY credibility_status`.
2. `danh_gia_ml.py` chết khi in tiếng Việt qua pipe (cp1252). Sửa bằng `PYTHONIOENCODING=utf-8` trong `theo-doi.sh`; chạy tay phải đặt biến này.
3. Crawl về 0 lượt từ giờ 4: không phải lỗi, frontier hết task đến hạn (lượt quét SWEEP xong, lượt sau hẹn khoảng 19 giờ nữa). Chỉ còn 25 task ACTIVE mỗi giờ.
4. RAM trống 2,6 GB ở giờ 9,2: dừng `rf-kibana`.

Chưa gán nhãn (mục 6). `nhan-tay.csv` có 30 dòng top luật và 30 dòng top ML nhưng **0 dòng baseline `ANOMALOUS_LOW`**, vì ngày mới nhất không có dòng nào ở trạng thái đó; cần xem lại khi bàn cách gán nhãn.
