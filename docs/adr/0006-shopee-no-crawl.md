# ADR-0006: Shopee: không crawl, chỉ dùng Affiliate API khi có tài khoản

Trạng thái: chấp nhận (2026-10-24). Không có code trong lần refactor này.

## Bối cảnh

Probe ngày 24/10 (`probe_marketplaces.py`) cho thấy Shopee chuyển hướng trang tới `/verify/traffic/error`. Vượt chặn bằng stealth hoặc giải captcha bị loại trừ theo nguyên tắc của dự án (tôn trọng robots.txt, không vượt anti-bot).

## Các đường đã xét

- **Crawl trang:** bị chặn. Không dùng.
- **Shopee Open Platform:** chỉ đọc được dữ liệu của shop đã uỷ quyền cho app, nên không cho dữ liệu toàn sàn.
- **Shopee Affiliate Open API** (GraphQL `https://open-api.affiliate.shopee.vn/graphql`, query `productOfferV2`): trả tên, giá min/max, đã bán, rating, shop, category, link. Cần tài khoản Shopee Affiliate được duyệt để có AppID và Secret; request được ký bằng SHA256.

## Quyết định

Chỉ Affiliate API là đường hợp lệ. Khi có AppID, thêm `ApiFetcher` (Strategy, cùng interface `Fetcher` với `HttpFetcher` và `BrowserFetcher`) và `ingestion/sources/shopee/` cùng `processing/normalize/sources/shopee.py` theo checklist ở ADR-0004.

**Việc cần người dùng:** đăng ký Shopee Affiliate để lấy AppID và Secret.
