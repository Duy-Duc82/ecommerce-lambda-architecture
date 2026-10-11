# Khảo sát crawl trực tiếp các sàn TMĐT Việt Nam

> Đo tối 2026-10-09 (khoảng 19:15–21:00 giờ Việt Nam), từ một máy Windows
> dùng mạng gia đình, IP Việt Nam. Mỗi phép đo chạy **một lần**. Không dùng
> proxy, không giả danh Googlebot, không dùng công cụ ẩn dấu tự động hóa,
> không giải captcha. Kết quả thô, ảnh chụp màn hình và script nằm trong
> `docs/evidence/crawl-survey-20261009/`.
>
> Bối cảnh: thầy hướng dẫn giao tìm hiểu crawl Shopee và yêu cầu **crawl
> thẳng trang web, không gọi API**.

## 1. Kết luận ngắn

1. **Shopee: không crawl thẳng được.** Bị chặn ở cả 4 mức đã thử: HTTP thuần,
   trình duyệt headless, trình duyệt có giao diện, và trình duyệt tự động
   do người thật đăng nhập (bị chặn ngay ở bước đăng nhập).
2. **TikTok Shop: không được.** Trả về captcha kéo mảnh ghép ngay ở request
   đầu, kể cả `robots.txt`.
3. **Lazada: crawl thẳng được bằng trình duyệt** (Playwright, headless).
   Lấy được 25 trang danh mục (phiên 1: 5 trang, 200 SP khác nhau; phiên 2: 20 trang, 800 SP khác nhau; tất cả đều có giá)
   mà không bị chặn. HTTP thuần thì bị lớp chống bot của Alibaba ("baxia")
   chặn.
4. **Tiki: crawl thẳng HTML được, không cần trình duyệt.** Trang danh mục và
   trang chi tiết đều có dữ liệu sản phẩm nhúng sẵn (`__NEXT_DATA__`).
5. **Chuỗi bán lẻ** (Thế Giới Di Động, Điện Máy Xanh, FPT Shop, CellphoneS,
   Phong Vũ, GearVN, Điện Máy Chợ Lớn, Nguyễn Kim, Hoàng Hà, Bách Hóa Xanh):
   đều trả HTML có giá ngay ở request thuần. Phần lớn trang chi tiết có
   JSON-LD `Product`/`Offer` theo chuẩn schema.org, nên **một bộ parser
   chung dùng được cho nhiều site**.
6. **Sendo:** trả HTTP 500 lúc đo, không đánh giá được.

**Đề xuất:** thay Shopee bằng **Lazada** làm sàn thứ hai, giữ **Tiki** (chuyển
từ gọi API sang parse HTML). Hai sàn lớn crawl thẳng được, đủ để so sánh giá
giữa các sàn. Chuỗi bán lẻ là phần mở rộng tùy chọn (mục 6, phương án B).

## 2. Các phương pháp đã thử

| Mã | Phương pháp | Mô tả |
|---|---|---|
| M1 | HTTP thuần + parse HTML | Tải trang như trình duyệt (User-Agent Chrome), không chạy JavaScript. Đọc giá trong HTML, JSON nhúng (`__NEXT_DATA__`, `self.__next_f`, `__NUXT__`), JSON-LD |
| M2 | Trình duyệt headless | Chromium qua Playwright, chạy JavaScript, chờ 8–12 giây, cuộn trang, đọc văn bản hiển thị |
| M3 | Trình duyệt có giao diện | Như M2 nhưng mở cửa sổ thật (headed) |
| M4 | Trình duyệt + người thật đăng nhập | Mở trang đăng nhập, người dùng tự đăng nhập bằng tài khoản của mình (chỉ thử với Shopee) |
| M5 | Phân trang | So sánh tập sản phẩm của trang 1 và trang 2/3 (`?page=`, `?pi=`, `?p=`) |
| M6 | Sitemap | Đọc `robots.txt` → sitemap → đếm URL sản phẩm |
| M7 | JSON-LD trang chi tiết | Đọc khối `<script type="application/ld+json">` có `@type: Product` |
| M8 | Độ bền | Tải liên tục nhiều trang (Lazada: 5 trang, sau đó thêm 20 trang, cách nhau 3–5 giây) |

**Cố ý không thử** (vượt anti-bot, vi phạm điều khoản sử dụng, hoặc nằm ngoài
phạm vi Brief §4.4): plugin "stealth"/undetected-chromedriver, điều khiển Chrome
thật qua cổng debug, proxy xoay vòng, dịch vụ giải captcha, giả User-Agent
Googlebot, tự động đăng nhập bằng tài khoản.

## 3. Bảng tổng hợp

| Site | Loại | M1 HTTP thuần | M2/M3 trình duyệt | Phân trang | JSON-LD chi tiết | Sitemap SP | Đánh giá |
|---|---|---|---|---|---|---|---|
| Shopee | Sàn | 200 nhưng là khung rỗng (0 giá); API 403 | Chuyển tới `/verify/traffic/error` hoặc `/buyer/login` | — | — | `sitemap.xml` là trang SPA, 0 URL | **Không khả thi** |
| TikTok Shop | Sàn | "Security Check" (captcha) | Captcha kéo mảnh ghép, headed: "Unable to verify" | — | — | `robots.txt` cũng bị captcha | **Không khả thi** |
| Lazada | Sàn | 200, 60 KB, 0 giá, có script "baxia" | **Có giá**: 40 SP/trang | `?page=N` đúng: 25 trang qua 2 phiên (200 + 800 SP khác nhau), không bị chặn | Có `Product`/`AggregateOffer`, nhưng bản HTTP thuần **không có giá** | 241 file; file đầu 10.000 URL (SP có đơn trong 30 ngày) | **Khả thi bằng trình duyệt** |
| Tiki | Sàn | **Có**: 49 SP/trang, 45 trường | Có | HTTP thuần: `?page=2` trả lại trang 1. Trình duyệt: trang 2 có 31 SP mới | Trang chi tiết có `__NEXT_DATA__` (giá, người bán, tồn kho) | 233 file (file đầu 1.844 URL) | **Khả thi bằng HTTP** |
| Sendo | Sàn | HTTP 500 | HTTP 500 | — | — | — | Không đánh giá được |
| Thế Giới Di Động | Bán lẻ | Có (48 SP) | — | `?pi=2` không đổi | `Product` + `Offer` (giá, VND, InStock) | Có sitemap | Khả thi |
| Điện Máy Xanh | Bán lẻ | Có (34 SP, có `data-price`, `data-brand`) | — | `?pi=2` không đổi | `Product` + `Offer` + rating | 11.428 URL trong sitemap danh mục | Khả thi |
| FPT Shop | Bán lẻ | Có (giá trong JSON RSC nhúng) | — | `?page=2`: 2 SP mới | Không có JSON-LD Product (dữ liệu nằm trong RSC) | Có | Khả thi |
| CellphoneS | Bán lẻ | Có (34–51 SP) | — | `?p=2` không đổi | `Product` + `Offer` | Có | Khả thi |
| Phong Vũ | Bán lẻ | Có (45 SP) | — | `?page=2`: 1 SP mới | `Product` + `Offer` (SKU, brand) | Có | Khả thi |
| GearVN | Bán lẻ | Có (25 SP) | — | **`?page=2`: 20 SP mới** | `Product` + `Offer` (SKU) | Có `sitemap-products.xml` | Khả thi, phân trang tốt nhất |
| Điện Máy Chợ Lớn | Bán lẻ | Có (44 SP) | — | `?page=2` không đổi | `Product` + `Offer` + tồn kho + rating | Có | Khả thi |
| Nguyễn Kim | Bán lẻ | Có giá (50 chuỗi giá) | — | — | Chưa lấy được link SP để thử | — | Chưa đủ dữ liệu |
| Hoàng Hà Mobile | Bán lẻ | Chứng chỉ SSL **hết hạn** | Có giá (16) | — | Chưa thử được trang chi tiết | 9.779 URL | Rủi ro kỹ thuật |
| Bách Hóa Xanh | Bán lẻ | HTML không có giá | Có giá (104) | — | — | — | Cần trình duyệt |

Ghi chú về phân trang: nhiều trang bán lẻ chỉ render sẵn trang đầu, phần còn
lại tải bằng AJAX khi bấm "Xem thêm". `robots.txt` của Điện Máy Xanh cấm
`/aj/` và của FPT Shop cấm `/ajax/`, nên không nên gọi các endpoint đó. Muốn
có danh sách sản phẩm đầy đủ thì đi qua sitemap rồi crawl từng trang chi tiết.

## 4. Chi tiết từng sàn lớn

### 4.1 Shopee

| Thử | Kết quả |
|---|---|
| `robots.txt` | Chỉ cấm `/cart`, `/checkout`, `/user`, `/me`, `/order`…; **không cấm** trang tìm kiếm, danh mục, sản phẩm |
| M1: `/api/v4/search/search_items`, `/api/v4/pdp/get_pc`, `/api/v4/recommend/recommend` | **403**, `"error": 90309999` (mã chặn anti-bot), khoảng 0,2 giây |
| M1: `/search?keyword=iphone` (UA desktop) | 200, 162 KB, chỉ là khung SPA: 0 tên SP, 0 giá, 0 JSON-LD |
| M1: cùng trang với UA iPhone | 200, 67 KB, 0 giá |
| M1: `/sitemap.xml` | Trả về khung SPA, không phải XML |
| M2 headless: tìm kiếm, danh mục | Chuyển tới `/verify/traffic/error`: *"Trang không khả dụng… đăng nhập lại"* |
| M3 headed: tìm kiếm | Chuyển tới `/buyer/login` |
| M3 headed: danh mục | *"Cần đăng nhập. Có vẻ như bạn chưa đăng nhập."* |
| M4: người thật đăng nhập trong trình duyệt tự động | Đăng nhập **không qua được**: Shopee báo lỗi liên tục, trình duyệt không rời trang đăng nhập/xác minh trong 7 phút |

Nguyên nhân: Shopee nhận ra trình duyệt tự động (`navigator.webdriver`,
fingerprint của Chromium "trần") và chấm điểm rủi ro ngay cả ở bước đăng
nhập. API được bảo vệ bằng header ký số (`af-ac-enc-*`) do JavaScript đã làm
rối mã sinh ra.

Muốn lấy được dữ liệu Shopee thì chỉ còn các cách ở mục "cố ý không thử".
Tất cả đều là vượt anti-bot, có rủi ro khóa tài khoản và vi phạm điều khoản
của Shopee.

### 4.2 Lazada

- M1 (HTTP thuần): 200 nhưng chỉ có 60 KB khung trang, 0 giá, kèm script
  `baxia` (lớp chống bot của Alibaba). Trang chi tiết tải thuần có JSON-LD
  `Product` (tên, SKU, tình trạng còn hàng) **nhưng không có giá**.
- M2 (headless) và M3 (headed) cho kết quả như nhau: danh mục Điện thoại
  40 SP/trang, đủ giá; tìm kiếm "iphone" 11 giá.
- M5 + M8: `?page=1…5` được 200 SP khác nhau, khoảng 7,7 giây/trang. Lượt
  thứ hai `?page=6…25`, cách nhau 3 giây: **20/20 trang thành công, 800 SP
  khác nhau, 2,8 phút (khoảng 8,4 giây/trang), không gặp captcha**.
- Trường lấy được từ thẻ sản phẩm: `data-item-id`, `data-sku-simple`, tên,
  URL `/products/pdp-i<id>.html`, giá. Trang chi tiết thêm JSON-LD.
- `robots.txt` chỉ cấm trang đăng nhập và `/undefined/`.
- Sitemap: `sitemap-products-order-last-30days-morethan0.xml` gồm 241 file
  `.xml.gz`; file đầu chứa đúng 10.000 URL. Đây là nguồn khám phá sản phẩm
  không cần duyệt danh mục.
- Quan sát dữ liệu: có nhiều listing giá bất thường, ví dụ "Xiaomi POCO C75
  8GB/256GB" giá 890.000 ₫ hay tên lách từ khóa "0PP0 A6 Pr0", "Xia0mi_P0C0".
  Đây là nguyên liệu tốt cho phần phát hiện bất thường giá của đề tài.

### 4.3 TikTok Shop

`https://www.tiktok.com/shop/vn` chuyển tới `shop.tiktok.com/vn` và trả trang
"Security Check" với captcha kéo mảnh ghép, ở cả HTTP thuần, headless và
headed. Ngay cả `robots.txt` cũng bị captcha.

### 4.4 Tiki (crawl HTML, không gọi API)

- Danh mục `c1789`: HTTP thuần 200, 526 KB, khoảng 0,14–0,7 giây. Khối
  `__NEXT_DATA__` chứa **49 sản phẩm**, mỗi SP 45 trường: `id`, `sku`, `name`,
  `price`, `original_price`, `discount_rate`, `rating_average`,
  `review_count`, `quantity_sold`, `seller_id`, `seller_name`, `brand_name`,
  `url_path`, `primary_category_path`…
- Phân trang: HTTP thuần với `?page=2`, `?page=3` vẫn trả 49 SP của trang 1
  (chỉ 1 SP mới). Trong trình duyệt, `?page=2` có 76 SP, trong đó 31 SP mới;
  bấm "Xem thêm" không tăng thêm.
- Trang chi tiết sản phẩm: HTTP thuần 200, khoảng 0,3 giây, `__NEXT_DATA__` có
  `price`, `list_price`, `original_price`, `inventory_status`,
  `current_seller`. Có JSON-LD nhưng không có khối `Product`.
- Sitemap: `tiki.vn/clover/sitemap_products_index.xml` gồm 233 file; file đầu
  có 1.844 URL sản phẩm.
- **Hệ quả cho đề tài:** crawler hiện tại gọi endpoint
  `personalish/v1/blocks/listings`, tức là API nội bộ. Muốn đúng yêu cầu
  "crawl thẳng" thì đổi sang: (a) parse `__NEXT_DATA__` trang danh mục để
  khám phá SP, và (b) crawl trang chi tiết của tập SP theo dõi (monitored
  universe) để lấy giá theo chu kỳ.

## 5. Demo chạy thật: so sánh giá xuyên sàn

Script `docs/evidence/crawl-survey-20261009/scripts/demo_compare.py` crawl thẳng
trang tìm kiếm của hai sàn với cùng một từ khóa: Tiki bằng HTTP thuần + parse
`__NEXT_DATA__`, Lazada bằng Playwright + đọc DOM. Kết quả xuất ra một CSV
chung (`site, method, listing_id, title, price_vnd, original_price_vnd, seller,
url, crawled_at`).

Kết quả với "samsung galaxy a27" (lọc bỏ phụ kiện, chỉ giữ máy 6GB/128GB):

| Sàn | Người bán | Giá (₫) |
|---|---|---|
| Tiki | Hồng Hạnh Mobile | 7.849.000 |
| Lazada | — | 7.869.000 |
| Lazada | — | 7.929.000 |
| Tiki | Tiki Trading | 7.990.000 |
| Lazada | — | 8.390.000 |
| Tiki | Long Hưng Official | 8.490.000 |
| Lazada | — | 8.990.000 |
| Lazada | — | 9.090.000 |

Cùng một mẫu máy, listing đắt nhất cao hơn listing rẻ nhất **15,8%**. Đây đúng
là loại tín hiệu đề tài muốn theo dõi.

Hai bài học từ demo:

- Tìm theo từ khóa cho ra rất nhiều phụ kiện. Với "iphone 16 128gb", 14 kết
  quả đầu trên Tiki toàn là ốp lưng và kính cường lực. Muốn so sánh xuyên sàn
  phải có bước **ghép sản phẩm** (product matching), không thể chỉ dựa vào từ
  khóa (xem `docs/CROSS_MARKETPLACE_PRODUCT_INTELLIGENCE_FUTURE_WORK.md`).
- Lazada có nhiều listing giá bất thường, ví dụ "OPPO A56 5G 8/256GB" giá
  1.799.000 ₫, hay tên lách từ khóa ("O.OPPO", "OP.PO", "Xia0mi"). Đây là dữ
  liệu thật cho phần phát hiện bất thường.

## 6. Phương án đề xuất

| Phương án | Nguồn | Ưu | Nhược |
|---|---|---|---|
| **A. Khuyến nghị** | Tiki (HTTP + parse HTML) + Lazada (Playwright) | Hai sàn lớn, có thể so sánh giá xuyên sàn. Đúng yêu cầu crawl thẳng. Tiki nhẹ, không cần trình duyệt | Lazada cần trình duyệt: chậm hơn (khoảng 5–8 giây/trang), tốn RAM, phải theo dõi khả năng bị chặn khi chạy lâu |
| B | A + bộ parser JSON-LD chung cho chuỗi bán lẻ (TGDD, ĐMX, Phong Vũ, GearVN, ĐMCL, CellphoneS) | Một adapter phủ nhiều site, dữ liệu sạch theo chuẩn schema.org (giá, VND, tồn kho) | Bán lẻ không phải "sàn"; chỉ trang đầu danh mục được render sẵn, phải đi qua sitemap |
| C | Shopee bằng trình duyệt thật đã đăng nhập hoặc công cụ stealth | Có Shopee | Vượt anti-bot, vi phạm điều khoản, dễ khóa tài khoản, không chạy hằng ngày được. **Không khuyến nghị** |

Tác động lên kiến trúc hiện tại (phương án A):

- Tầng thu thập: thêm adapter `TikiHtmlAdapter` (thay adapter API) và
  `LazadaBrowserAdapter`. Bronze vẫn lưu body thô (lúc này là HTML hoặc
  JSON nhúng), nên reparse và drill D7 (schema drift) vẫn áp dụng được.
- Thêm một worker Playwright (container riêng, chạy tuần tự, delay 3–5
  giây). RAM của worker này **chưa đo**.
- Kafka, Spark, warehouse, dashboard giữ nguyên vì hợp đồng
  `PRICE_SNAPSHOT_SCHEMA` không đổi.

## 7. Quy tắc crawl đề xuất ghi vào quyển

- Chỉ dữ liệu công khai, không đăng nhập, không thu thập dữ liệu cá nhân.
- Tuân thủ `robots.txt` của đúng host (crawler đã làm theo RFC 9309).
- Tốc độ thấp: một luồng cho mỗi site, delay ≥ 3 giây, tập SP theo dõi có
  giới hạn.
- Không vượt anti-bot: gặp captcha hoặc trang xác minh thì dừng site đó,
  ghi `error_kind`, không thử lại liên tục.

## 8. Câu hỏi cho thầy

1. Thầy đồng ý thay Shopee bằng Lazada làm sàn thứ hai không?
2. Phần Shopee đưa vào quyển như một phân tích khả thi (có bằng chứng ở mục
   4.1) có đủ không?
3. Có cần thêm chuỗi bán lẻ (phương án B) không, hay hai sàn là đủ?
4. Crawl Lazada bằng trình duyệt headless có được tính là "crawl thẳng" theo
   ý thầy không?

## 9. Giới hạn của khảo sát

- Mỗi phép đo chạy một lần, trên một máy, một IP, vào một buổi tối. Kết quả
  có thể khác khi chạy hằng ngày, từ IP datacenter, hoặc khi các sàn đổi cơ
  chế chống bot.
- Lazada mới thử tối đa 25 trang trong một phiên. Chưa thử chạy theo lịch
  nhiều ngày.
- Chưa đo RAM/CPU của worker trình duyệt.
- Số URL trong sitemap chỉ đếm file đầu tiên, không cộng dồn toàn bộ.
