# ADR-0009: Bài toán phân tích là độ tin cậy của khuyến mãi; luật trước, mô hình chỉ xếp hạng

- **Ngày:** 2026-11-13
- **Trạng thái:** chấp nhận

## Bối cảnh

Kế hoạch 03/11 chọn hai bài toán dùng dữ liệu đầu ra của batch: phát hiện giảm giá ảo (chính) và dự báo nhu cầu theo danh mục (phụ). Dữ liệu hiện có 3 ngày crawl; bộ đếm "đã bán" chỉ cho tốc độ hợp lệ khi hai lần quan sát cách nhau dưới 48 giờ (`MARKETPLACE_COUNTER_MAX_GAP_SECONDS`), nên dự báo nhu cầu cần nhiều tuần lịch sử liên tục mới có gì để học. Benchmark 17/10 trên dữ liệu bán lẻ thật (`docs/evidence/ml-bench-20261017`) cho thấy thống kê robust đơn giản thắng các mô hình học máy về chất lượng, tốc độ và tính giải thích được; Isolation Forest chỉ có ích để xếp hạng.

Ngày 07/11 (`docs/evidence/run-20261107/NHAT-KY-LOI.md` mục 7) đã thấy ba kiểu giảm giá đáng ngờ trong dữ liệu thật: giá nhảy lên bằng giá niêm yết rồi "giảm" về giá cũ; giá niêm yết đặt rất cao rồi bật giảm; và đổi biến thể hoặc người bán làm cả hai giá cùng đổi.

## Quyết định

1. **Bài toán chính và duy nhất của giai đoạn này là độ tin cậy của khuyến mãi** (discount credibility): với mỗi offer-ngày, mức giảm mà trang hiển thị có đứng vững khi so với lịch sử giá của chính offer đó không. Dự báo nhu cầu để lại cho đến khi có ít nhất ba tuần dữ liệu liên tục.
2. **Luật có căn cứ ngoài quyết định**: giá tham chiếu là giá thấp nhất trong 30 ngày trước ngày xét (Chỉ thị EU 2019/2161 "Omnibus", điều 6a của 98/6/EC); mức giảm hiển thị trên 50 % là ngoài khung khuyến mãi thông thường (Nghị định 81/2018/NĐ-CP, điều 7). Năm lý do, mỗi lý do đọc được từ chính các cột của mart: `PRICE_ABOVE_LIST`, `DISCOUNT_OVER_CAP`, `LIST_PRICE_RAISED`, `CLAIM_EXCEEDS_HISTORY`, `NOT_BELOW_REFERENCE`. Trọng số và ngưỡng nằm ở `domain/discount_rules.py`.
3. **Mô hình chỉ xếp hạng, không phán xét**: Isolation Forest (scikit-learn) chấm độ bất thường của vector đặc trưng (mức giảm hiển thị, mức giảm thực, chênh lệch, cờ nâng giá niêm yết, số giá khác nhau trong ngày; cố ý không đưa mức giá vào, vì hàng đắt không phải là khuyến mãi lạ) để sắp thứ tự xem xét; cột `ml_rank_percent` đứng cạnh `rule_score`, và khi không có scikit-learn mart vẫn dựng được với `ml_method = NONE`.
4. **Hai mart mới**, dựng trong stage `marts` từ `offer_price_history_daily` (cùng nguồn với mart anomaly, nên ba mart không bao giờ bất đồng về giá của một ngày): `discount_credibility_daily` (offer × ngày × tiền tệ) và `discount_daily` (danh mục × ngày × tiền tệ). Cả hai qua cổng chất lượng và publish như mười mart cũ; thêm luật cảnh báo `discount_credibility_evaluation_coverage`.
5. **Ngôn từ là hợp đồng**: một verdict là phát biểu có cấp độ kèm lý do về một con số hiển thị, không phải kết luận về người bán. `SUSPECT` nghĩa là "nên xem", `INSUFFICIENT_HISTORY` nghĩa là "chưa có gì để nói".

## Hệ quả

- Giá trị của mart tăng theo số ngày thu thập: ngày đầu chỉ hai luật không cần lịch sử chạy được; từ ngày thứ tư của mỗi offer, ba luật lịch sử bắt đầu có hiệu lực; mốc Omnibus đủ nghĩa sau 30 ngày.
- Đánh giá không có nhãn thật: gán nhãn tay khoảng 100 offer-ngày top theo `rule_score` rồi theo `ml_rank_percent` để đo precision@k, so với tầng anomaly MAD/IQR làm baseline (kế hoạch 03/11 §3.1).
- Image Spark cần scikit-learn (`[ml]` trong `pyproject.toml`, pin trong `constraints.txt`); dựng lại image khi lên stack.
- Kiểu "đổi biến thể hoặc người bán" chưa tách được vì `offer_id` chưa mang biến thể; cả hai giá cùng đổi sẽ hiện ra như `LIST_PRICE_RAISED` kèm `CLAIM_EXCEEDS_HISTORY`. Ghi vào giới hạn, để lại cho pha sau.
