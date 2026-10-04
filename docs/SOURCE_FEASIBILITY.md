# Source feasibility matrix (Brief §3, P0-01)

> **Bản nháp, tổng hợp 2026-10-04, chưa được thầy hướng dẫn duyệt.** Mọi số
> liệu dưới đây là số đã đo và đã ghi trước đó trong `PROGRESS.md` (§4,
> §4a, các lượt chạy 2026-07-26, 2026-08-16, 2026-08-17) hoặc trong audit của
> stack `mp-live` (2026-10-04). File này **không** đưa ra phép đo mới. Ô nào
> chưa có bằng chứng thì ghi "chưa đo", không ước đoán.

## 1. Ứng viên đã thử

| Tiêu chí (Brief §3) | Tiki — `personalish/v1/blocks/listings` | WooCommerce Store API (`/wp-json/wc/store/v1/products`) | Shopify `/products.json` (cả Haravan/Sapo) | Shopee / Lazada |
|---|---|---|---|---|
| Truy cập public hợp lệ | Có. JSON, không cần auth. Live 2026-07-26: 40 SP, 0 lỗi | Có. 200 không auth trên woocommerce.com (2026-08-16); 2/4 site trả 403 do WAF, 1 site 404 vì không dùng Woo | Có. 200 trên allbirds.com, kith.com, yame.vn; gymshark.com 403 (WAF) | Không thử: JS-rendered, anti-bot mạnh (PROGRESS §4a); Brief §4.4 loại anti-bot bypass |
| Robots / crawl budget | Crawler đọc `robots.txt` từ đúng host được gọi (RFC 9309, Phase 8 WP1); delay 2 s/request; `mp-live` chạy 15 request/giờ | Chưa đo | Chưa đo | — |
| Listing ID ổn định | `id` số của Tiki. Ổn định qua các lượt 2026-08-17 (124/124 unique) và qua 3 chu kỳ đầu của `mp-live` | Chưa đo qua nhiều lượt | Chưa đo qua nhiều lượt | — |
| Title, URL, giá, timestamp | Có đủ. Hai lỗi dữ liệu đã sửa: URL nhân đôi `-p<id>`, `category_path` chỉ là id (2026-08-16) | Có: `prices.price` (đơn vị nhỏ nhất), `regular_price`, `currency_code`, `sku` | Có: price, sku; yame.vn trả VND | — |
| Pagination / category discovery | Có. Nhưng `total`/`last_page` không đáng tin: 17166 báo 307, chỉ trả 6 SP. Thứ tự do recommender quyết định nên trùng lặp xuyên trang (120 dòng, 118 unique). Trần thực tế khoảng 2.000 SP/category | Có header `X-WP-Total`, `X-WP-TotalPages` (1.701 SP / 567 trang trên woocommerce.com) | `limit`/`page` | — |
| Response size, latency, lỗi | Lượt đầy đủ 2026-08-17: 356/356 request OK, 0 lỗi, khoảng 2,9 s/trang (chủ yếu do throttle tự đặt). `mp-live` 2026-10-04: 60/60 OK, latency p50 3.060 ms, p95 3.910 ms, khoảng 157 KB/trang | Chưa đo | Chưa đo | — |
| Field ổn định 24–48 giờ | Chưa có lượt đo 24–48 giờ có chủ đích. `mp-live` (từ 2026-10-04) sẽ là bằng chứng: `rejected_count = 0` trên 2.400 observation đầu | Chưa đo | Chưa đo | — |
| Đủ listing cho monitored universe | Có: 9 category × 50 trang cho 14.117 SP unique trong 17m22s (2026-08-17). Universe đã chốt: 5 category × 3 trang, khoảng 600 offer | Theo từng shop: vài trăm tới vài nghìn SP/site | Theo từng shop | — |
| Overlap để so sánh | Không thuộc core (Brief §4.3, P3) | `sku` có nhưng thường là SKU nội bộ, không tự khớp xuyên site | Như Woo | — |

## 2. Quyết định (đã thực thi)

- **Source chính: Tiki**, category 1846, 8322, 1882, 1520 và 931 (`env/live.env`),
  mỗi category 3 trang listing, tier `ACTIVE` (60 phút). Thu thập liên tục từ
  **2026-10-04 10:16 UTC**.
- **Source thứ hai: chưa chọn.** WooCommerce và Shopify truy cập được mà không
  cần vượt anti-bot, nhưng cả hai là *shop*, không phải *sàn*. Brief §27
  không cho source thứ hai làm chậm việc thu thập source chính.
- **Shopee/Lazada: loại.** Lý do là anti-bot (Brief §4.4).

## 3. Giới hạn cần ghi vào quyển

- Endpoint Tiki là API nội bộ, không có tài liệu và không có version. Hợp
  đồng của nó có thể đổi bất cứ lúc nào. Dự án giảm rủi ro bằng raw-first
  Bronze, adapter version, fixture đóng băng và drill D7 (schema drift).
- `total`/`last_page` của endpoint không đáng tin, và thứ tự listing do
  recommender quyết định. Vì vậy một trang listing không phải tập offer cố
  định: offer có thể chuyển trang giữa hai lượt crawl.
- Tiêu chí "field ổn định 24–48 giờ" chưa có lượt đo riêng trước khi chốt
  source. Bằng chứng sẽ đến từ báo cáo `evaluate reliability` trên `mp-live`.
