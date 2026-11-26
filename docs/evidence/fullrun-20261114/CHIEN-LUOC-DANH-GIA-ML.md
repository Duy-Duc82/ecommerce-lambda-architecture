# Chiến lược đánh giá phần ML trong lượt chạy toàn luồng 14/11/2026

Lượt chạy: stack `mp-refactor` (container `rf-*`), code nhánh `feat/discount-credibility` mount từ `C:\code\data-refactor`, mục tiêu khoảng 10 giờ. Phần ML là hai mart của ADR-0009 (`discount_credibility_daily`, `discount_daily`) với Isolation Forest xếp hạng, chạy trong stage `marts` của batch mỗi giờ.

Ba câu hỏi, mỗi câu có số đo và ngưỡng chấp nhận trước khi chạy, để không "đọc kết quả rồi mới đặt tiêu chí".

## 1. Tính đúng: mart dựng được và nhất quán trong pipeline thật

| Số đo | Cách lấy | Chấp nhận |
|---|---|---|
| Run batch PROMOTED, quality PASS | `audit.marketplace_batch_run` | mọi run theo giờ sau run đầu tiên |
| Luật `discount_credibility_evaluation_coverage` | `audit.marketplace_quality_result` | PASS, `observed_value` > 0 (có offer-ngày đủ lịch sử) |
| Số dòng `cache.marketplace_discount_credibility_daily` = số dòng `cache.marketplace_offer_price_history_daily` | SQL | bằng nhau tuyệt đối (cùng grain) |
| Mọi dòng có `credibility_status` hợp lệ, `rule_score` trong 0..100, `credibility_reasons_json` là mảng | ràng buộc CHECK của DDL + SQL | 0 vi phạm |
| Luật lịch sử bắt đầu bật | đếm lý do `LIST_PRICE_RAISED`, `CLAIM_EXCEEDS_HISTORY`, `NOT_BELOW_REFERENCE` trong ngày 14/11 | > 0, vì offer tier ACTIVE đã được thấy 25/10, 31/10, 07/11 (3 ngày trong cửa sổ 30 ngày) |
| Ổn định giữa các run | cùng (offer, ngày) có cùng verdict ở hai run liên tiếp khi dữ liệu ngày đó không đổi | ≥ 99 % dòng giữ nguyên |
| `discount_daily` khớp với mart offer | tổng `questionable_offer_count` + `suspect_offer_count` theo ngày so với đếm trực tiếp từ mart offer | lệch 0 |

## 2. Hiệu năng: chi phí thêm vào batch có chấp nhận được không

| Số đo | Cách lấy | Chấp nhận |
|---|---|---|
| Thời gian stage `marts` | `audit.marketplace_batch_run` (cột thời gian stage, H2) so với run 07/11 (12 phút cả run, máy đủ RAM) | tăng không quá 30 % so với run 07/11 cùng khối lượng |
| Thời gian fit Isolation Forest và số dòng mẫu | dòng log JSON `discount_ml_fitted` mà `processing/batch/discount.py` in mỗi lần fit (`model`, `sample_rows`, `eligible_rows`, `fit_seconds`), đọc từ Loki | fit < 60 s với mẫu ≤ 200.000 dòng |
| Số dòng được chấm | `ml_method = ISOLATION_FOREST` trong mart | bằng số offer-ngày có `displayed_discount_percent > 0` |
| RAM driver | Grafana (container batch) | không OOM, run không FAILED vì bộ nhớ |

## 3. Chất lượng: kết quả có nghĩa không, trong giới hạn 4 ngày dữ liệu

Không có nhãn thật. Ba phép đo thay thế, ghi rõ là thay thế:

1. **Đồng thuận giữa luật và mô hình.** Trong các offer-ngày hiển thị giảm giá hôm nay: tỉ lệ dòng `rule_score ≥ 50` nằm trong top 5 % theo `ml_rank_percent`. Kỳ vọng cao hơn 5 % (ngẫu nhiên) rõ rệt; nếu không, mô hình không nhìn cùng tín hiệu với luật.
2. **Đọc tay có tiêu chí: để dành, chưa làm trong lượt này.** Isolation Forest học không giám sát nên huấn luyện không cần nhãn; nhưng muốn biết nó *đúng* hay không thì phải so với sự thật trên một mẫu nhỏ, và đó là việc của người. Cách gán nhãn sẽ được người dùng và Fable bàn lại sau (KICH-BAN-CHAY.md mục 6). Lượt này chỉ **xuất và giữ dữ liệu** cho việc đó: 30 dòng top theo `rule_score`, 30 dòng top theo `ml_rank_percent` (chỉ trong `CREDIBLE`/`INSUFFICIENT_HISTORY`), 30 dòng `ANOMALOUS_LOW` của `price_anomaly_daily` làm baseline, kèm giá, URL và toàn bộ lịch sử giá theo ngày của các offer đó. Đề xuất ban đầu để bàn: bốn nhãn theo mục 7 nhật ký 07/11 (A giá nhảy lên niêm yết rồi giảm lại; B niêm yết ảo rồi bật sale; C đổi biến thể/người bán; N bình thường), precision@30 = (A + B) / 30; `danh_gia_ml.py score-labels` đã tính sẵn theo đề xuất này nhưng có thể đổi.
3. **Tính hợp lý của phân bố.** Tỉ lệ `QUESTIONABLE` + `SUSPECT` trên số offer-ngày hiển thị giảm giá nằm trong 1–10 %. Dưới 1 % nghĩa là luật quá hiền, trên 10 % nghĩa là luật gắn cờ cả thị trường.

## 4. Kiểm tra các lớp khác (mọi thứ chạy ổn không)

Theo `observability/healthcheck.sh env/refactor.env` và script `theo-doi.sh` trong thư mục này, mỗi giờ một lần:

- crawl: số lượt tải 10 phút gần nhất > 0, không `RATE_LIMITED`/`BLOCKED`;
- speed: micro-batch SUCCEEDED trong 3 phút gần nhất, `invalid_rows` nhỏ, DLQ observations có bản ghi khi có dòng INVALID (kiểm tra sửa đổi của PR #20);
- batch: run theo giờ PROMOTED; luật bắt buộc không FAIL;
- serving: Superset đọc được hai mart mới; Kibana có thay đổi giá mới; Grafana không cảnh báo.

Kết quả ghi vào `KET-QUA.md` cùng thư mục, có số đo thật và kết luận đạt/không đạt theo từng ngưỡng trên.
