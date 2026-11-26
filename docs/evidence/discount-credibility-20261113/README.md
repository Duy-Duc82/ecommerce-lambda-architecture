# Chạy thử mart độ tin cậy khuyến mãi trên Gold thật (13/11/2026)

- **Dữ liệu:** `offer_price_history_daily` và `offer_current` của run `mp-20261031T1700Z`, trong backup `data/ops/backups/bk-20261031T172502Z` (2 ngày crawl: 25/10 và 31/10; Tiki 104.579 offer-ngày, Lazada 1.655).
- **Code:** `build_discount_credibility_daily` của `processing/batch/discount.py`, cấu hình mặc định (`DiscountRuleConfig`: cửa sổ 30 ngày, tối thiểu 3 ngày lịch sử, trần 50 %, chênh lệch 20 điểm, Isolation Forest bật). Script: `chay-thu.py` (chạy từ gốc repo, cần `.venv` có pyspark và scikit-learn).
- **Mục đích:** chứng minh mart dựng được trên dữ liệu thật và đọc được kết quả; không phải đánh giá chất lượng, vì mới có 2 ngày nên ba luật cần lịch sử chưa thể bật.

## Kết quả (`tom-tat.json`)

| Trạng thái | Offer-ngày | Nghĩa |
|---|---:|---|
| `NO_DISCOUNT` | 78.228 | không hiển thị giảm giá, không có gì mâu thuẫn |
| `INSUFFICIENT_HISTORY` | 27.468 | có hiển thị giảm giá, chưa có luật nào bật, chưa đủ 3 ngày lịch sử để xác nhận |
| `QUESTIONABLE` | 538 | `DISCOUNT_OVER_CAP`: mức giảm hiển thị trên 50 % |
| `SUSPECT` | 0 | chưa thể có: cần `CLAIM_EXCEEDS_HISTORY` hoặc hai lý do |

- 28.006 offer-ngày (26,4 %) hiển thị giảm giá; 538 trong số đó (1,9 %) trên trần 50 %. Khớp với phân bố đo tay hôm 07/11 (2,9 % lượt giảm từ 50 % trở lên, tính trên lượt đổi giá).
- Không có offer nào `PRICE_ABOVE_LIST` trong Gold này: normalizer đã null hoá discount âm từ 07/11, còn Gold 31/10 cũ hơn và Tiki hôm đó không có trường hợp giá trên niêm yết.
- Isolation Forest chấm 28.006 dòng hiển thị giảm giá (`ml-phu.csv`); 78.228 dòng còn lại `ml_method = NONE` đúng thiết kế.

## Đọc danh sách top (`top-40-de-xem-tay.csv`, `top-20-theo-isolation-forest.csv`)

- Top theo `rule_score` là 538 dòng `DISCOUNT_OVER_CAP`, phần lớn là văn phòng phẩm Deli (bút, tẩy, gôm) giảm 58–80 % so với giá niêm yết 3–4 lần giá bán, và đồ gia dụng Sunhouse. Đây đúng là kiểu B ghi ở nhật ký 07/11 mục 7 (giá niêm yết đặt rất cao rồi bật giảm).
- Top theo `ml_rank_percent` trên dữ liệu 2 ngày **chưa phải là thước đo đáng ngờ**: khi chưa có lịch sử, hai đặc trưng `effective_discount_percent` và `claim_gap_percent` bằng 0 cho mọi dòng, nên rừng chỉ còn tách theo mức giảm hiển thị và số giá khác nhau trong ngày. Kết quả là nó xếp cả hai đuôi: giảm sâu 75–80 % (trùng với luật) và giảm rất nông 5–7 % (hiếm, nhưng không đáng ngờ). Đây là đúng bản chất Isolation Forest và là lý do ADR-0009 cho nó xếp hạng chứ không phán xét. Khi có ≥ 3 ngày lịch sử cho mỗi offer, hai đặc trưng lịch sử có giá trị và thứ hạng mới có nghĩa để xem tay.
- Cột `category_l2` trong `theo-danh-muc-top25.csv` cắt từ `category_path` của `offer_current`; với Tiki, trường này là chuỗi id (`1/2/...`) nên gộp thô; `discount_daily` thật dùng `category_path` đầy đủ.

## Cách đánh giá tiếp (kế hoạch 03/11 §3.1)

1. Thu thập liên tục ≥ 7 ngày để ba luật lịch sử bật được (`LIST_PRICE_RAISED`, `CLAIM_EXCEEDS_HISTORY`, `NOT_BELOW_REFERENCE`).
2. Gán nhãn tay 100 offer-ngày: 50 top theo `rule_score` (rồi `ml_rank_percent`), 50 top chỉ theo `ml_rank_percent` trong số `CREDIBLE`/`INSUFFICIENT_HISTORY`. Mỗi dòng có `source_url` để mở trang và `credibility_reasons_json` để đối chiếu.
3. Đo precision@k cho từng cách xếp; baseline là `price_anomaly_daily` (MAD/IQR) trên cùng ngày.
4. Ghi kết quả cạnh file này, kèm phiên bản luật (`discount-rules.v1`).

## File

| File | Nội dung |
|---|---|
| `chay-thu.py` | script dựng mart từ backup và ghi các bảng dưới |
| `tom-tat.json` | số dòng, cấu hình, đếm theo trạng thái, lý do, phương pháp ML |
| `theo-trang-thai.csv` | đếm theo sàn, ngày, trạng thái |
| `theo-ly-do.csv` | đếm theo sàn, ngày, lý do |
| `ml-phu.csv` | số dòng rừng chấm được theo sàn |
| `top-40-de-xem-tay.csv` | 40 dòng QUESTIONABLE/SUSPECT đầu tiên theo `rule_score`, có tiêu đề, giá, lý do, URL |
| `top-20-theo-isolation-forest.csv` | 20 dòng có `ml_rank_percent` cao nhất |
| `theo-danh-muc-top25.csv` | gộp thô theo hai cấp danh mục |
