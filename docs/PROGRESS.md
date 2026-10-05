# Tiến độ đồ án — nâng cấp Lambda architecture thành sản phẩm DE end-to-end

> Đọc file này đầu tiên khi bắt đầu một session mới (Claude Code hoặc người).
> Nó là nguồn sự thật về "đang làm gì / đã xong gì / quyết định gì đã chốt" —
> không cần đọc lại toàn bộ lịch sử chat để tiếp tục.

## 1. Mục tiêu & câu chuyện nghiệp vụ (đã chốt, không đổi trừ khi có lý do mạnh)

Biến project từ lambda-architecture demo (Kaggle CSV replay) thành đồ án tốt
nghiệp + portfolio Data Engineer, xoay quanh một câu chuyện duy nhất:

> **"Nền tảng theo dõi giá đa sàn (Tiki/Shopee/Lazada) + phát hiện bất thường
> & dự báo nhu cầu"**

Mọi layer — kể cả xử lý exception/outlier — phải phục vụ câu chuyện này,
không phải các module rời rạc không liên quan tới nhau.

Timeline: ~5 tháng (20 tuần), bắt đầu 2026-07-26. Không giới hạn cứng, nhưng
lộ trình ở mục 3 giả định tốc độ này.

## 2. Ràng buộc kỹ thuật đã chốt (đừng vi phạm khi implement tiếp)

- Máy dev đã chạy 8+ container (Kafka, 2×Spark, MinIO, Redis, Postgres, ES,
  Kibana, Superset). Service mới (Airflow, crawler headless browser...) phải
  chạy qua `docker-compose --profile`, không resident 24/7.
- ~~`.venv` là Python 3.14 — không cài được `darts`/`pyod`/`torch`~~
  **ĐÃ LỖI THỜI (kiểm chứng 2026-08-16).** `.venv` hiện là **Python 3.11.9**,
  `pyspark 3.5.1` (khớp `requirements.txt`), và `torch 2.5.1` / `darts 0.46.0` /
  `pyod 3.6.1` đều import được. Không cần venv riêng cho ML. `delta` và `mlflow`
  thì **chưa** cài (cần cho Phase 4/5).
- Phần cứng máy dev: **31.1 GB RAM, 8 nhân, 181 GB đĩa trống** — dư sức chạy
  batch job trên file Kaggle 8.6 GB. Nếu Spark chết, nghi cấu hình trước, đừng
  nghi phần cứng (xem mục 4b).
- Không thêm hệ bổ trợ nặng (Vault, Kubernetes, Kafka Connect...). Ưu tiên
  thư viện nhúng (Delta Lake = Spark lib, MLflow = sqlite backend) hơn service
  mới.
- `kafka-python`/`kafka-python-ng` **không** hỗ trợ idempotent producer thật —
  đừng claim "exactly-once", chỉ "at-least-once + DLQ quan sát được".
- Crawler: rate-limit + robots.txt + UA rotation, KHÔNG né chặn quy mô lớn —
  đây là crawler nghiên cứu cho đồ án, ghi rõ giới hạn này khi viết thesis.
- Crawler **không** tạo ra event hành vi thật (view/cart/purchase) — chỉ có
  catalog/price. Vì vậy có contract riêng (`PRICE_SNAPSHOT_FIELDS`), không
  gộp vào contract hành vi cũ.

Chi tiết đầy đủ + audit gốc: `C:\Users\Admin\.claude\plans\shimmering-rolling-blanket.md`
(file plan được duyệt, không sửa) và memory `graduation-thesis-roadmap.md`.

## 3. Trạng thái từng phase

> ⚠️ **Bảng dưới đây đã LỖI THỜI (đánh số từ trước khi có Brief).**
> Nguồn chân lý về việc chia phase là `docs/PHASE_INDEX.md` trên `master`,
> suy từ Brief §21 (12 tuần) và §22 (backlog P0–P3). Cách đánh số ở đây
> (Phase 3 = "Speed layer hardening") KHÔNG khớp với cách đánh số chuẩn
> (Phase 3 = Tuần 3 = Scheduler/retry/audit). Giữ lại làm lịch sử; đừng
> dùng để lập kế hoạch. Trạng thái hiện tại xem mục 7.

| # | Phase | Trạng thái | Ghi chú |
|---|---|---|---|
| 0 | Foundation cleanup (config/secrets, `.env.example`, docs, CI) | ⏸️ Chưa làm (đã tạm hoãn 1 lần theo yêu cầu user) | `.env` đã dọn dở dang (xoá var rác, thêm MINIO_ROOT_USER/PASSWORD) nhưng **chưa** có `.env.example`, chưa sửa `docker-compose.yml` dùng `${VAR}`, chưa có CI. *(đã sửa 1 phần: compose giờ dùng `${SPARK_WORKER_*}`)* |
| 1 | Multi-site crawler | 🔶 Đang làm — Tiki xong + sửa 2 lỗi data + pagination/multi-category (2026-08-17); **đổi hướng site tiếp theo** (xem 4a) | Xem chi tiết mục 4, 4a |
| 1b | Storage portability + sửa Spark OOM | 🔶 Code xong, **chưa chạy thật** (Docker tắt) | Xem mục 4b + `docs/CLOUD_MIGRATION.md` |
| 2 | Ingestion hardening (DLQ cho producer.py/es_indexer.py) | ⏳ Chưa bắt đầu | `common/dlq.py` đã có sẵn (build sớm cho crawler) — Phase 2 chỉ còn việc retrofit vào `data_ingestion/producer.py` và `es_indexer.py` |
| 3 | Speed layer hardening | ⏳ Chưa bắt đầu | |
| 4 | Lakehouse (Delta Lake) + incremental + `price_warehouse_job.py` | ⏳ Chưa bắt đầu | |
| 5 | ML maturity (MLflow, prediction history, price-anomaly) | ⏳ Chưa bắt đầu | |
| 6 | ~~Orchestration (Airflow)~~ → scheduler vòng lặp + advisory lock | ✅ Xong ở Phase 8 WP3 | Airflow bị bỏ, xem §5 (Phase 8 plan §2.1 D2) |
| 7 | Serving API (FastAPI) | ⏳ Chưa bắt đầu | |
| 8 | Dashboards + thesis polish | ⏳ Chưa bắt đầu | |

Legend: ✅ xong · 🔶 đang làm/dở · ⏳ chưa bắt đầu · ⏸️ tạm hoãn

## 4. Chi tiết Phase 1 (crawler) — phase đang làm

### Đã xong (commit `916d72b`, đã push lên `origin/master`)

- `config/schema.py`: thêm `PRICE_SNAPSHOT_FIELDS` + `normalize_price_snapshot`
  + `validate_price_snapshot` — contract riêng cho dữ liệu crawl, tách khỏi
  `CANONICAL_FIELDS`/`EVENT_TYPES` (hành vi người dùng).
- `common/dlq.py`: `publish_to_dlq()` — publish record lỗi sang topic
  `<topic>.dlq`, never raises. Build sớm hơn kế hoạch (vốn ở Phase 2) vì
  crawler cần ngay; Phase 2 sẽ retrofit vào ingestion cũ.
- `common/object_store.py`: `put_bytes(zone, path, data)` — ghi object vào
  MinIO bronze (mode `s3a`) hoặc local filesystem (mode `local`), không cần
  Spark session, mirror lại logic dual-mode của `config.settings.data_lake_uri`.
- `crawler/base.py`: `SiteCrawler` (Template Method, cùng khuôn với
  `analytics/base.py::Forecaster`) — robots.txt check, rate-limit + jitter,
  cô lập lỗi theo từng category/product (1 lỗi không chặn cả run).
- `crawler/sites/tiki.py`: `TikiCrawler` — gọi
  `https://tiki.vn/api/personalish/v1/blocks/listings`, field mapping đã
  **verify bằng response live thật** (2026-07-26). Ghi chú rõ trong docstring:
  Tiki không công bố API contract ổn định, cần re-verify field trước khi
  dùng cho việc khác.
- `crawler/runner.py`: CLI `python -m crawler.runner --site tiki [--dry-run]`
  — crawl, ghi raw JSON vào bronze (`crawl_raw/{site}/{date}/...`), publish
  snapshot chuẩn hoá lên Kafka topic `ecommerce_price_snapshots`, lỗi ra DLQ.
  1 site lỗi không chặn site khác (try/except quanh từng site trong `main()`).
- Test: `tests/test_crawler.py`, `tests/test_object_store.py`, + mở rộng
  `tests/test_schema.py`. **27/27 test pass** (12 test mới).
- **Verified thật**: `python -m crawler.runner --site tiki --dry-run` gọi
  live Tiki API thành công, lấy 40 sản phẩm, 0 lỗi.
- Dọn kèm: `.gitignore` thêm `data.zip` + `lakehouse/`; gỡ `data.zip` (633MB)
  khỏi git tracking VÀ khỏi lịch sử của 2 commit local (dùng `git filter-branch`
  vì GitHub chặn file >100MB) trước khi push — xem log chat để biết chi tiết
  nếu cần lặp lại thao tác tương tự.

### Bổ sung session 2026-08-16 (chưa commit)

Chạy thật lại crawler, kiểm tra chất lượng dữ liệu, phát hiện và sửa 2 lỗi:

- `crawler/runner.py`: thêm cờ **`--out-csv PATH`** — sink kiểm tra dữ liệu,
  độc lập với Kafka/MinIO. Ghép với `--dry-run` để soi output khi không có
  broker/object store. Encoding `utf-8-sig` cho Excel đọc đúng tiếng Việt.
  ```
  python -m crawler.runner --site tiki --dry-run --out-csv data/crawl/tiki_snapshots.csv
  ```
- `crawler/sites/tiki.py` — **lỗi 1: URL nhân đôi id.** `url_key` của Tiki đã
  sẵn hậu tố `-p<id>`, code cũ nối thêm `-p{id}.html` → `...-p279225805-p279225805.html`.
  Tiki vẫn resolve (trả 200, KHÔNG phải link chết) nhưng không phải URL
  canonical → hỏng khi join/dedupe với link từ nguồn khác. Đã sửa, 3 URL mới
  kiểm tra HTTP đều 200.
- `crawler/sites/tiki.py` — **lỗi 2: `category_path` chỉ là id số** của category
  truy vấn (`1846`), trong khi payload có sẵn `primary_category_path`
  (`1/2/1846/8095/2458`). Đã đổi sang dùng path đầy đủ, fallback về category
  truy vấn nếu thiếu.
- `tests/fixtures/tiki_listing_sample.json`: fixture viết tay **không giống
  response thật** (thiếu hậu tố `-p<id>` trong `url_key`, thiếu
  `primary_category_path`) — chính là lý do lỗi 1 lọt lưới. Đã sửa theo shape
  thật + thêm test nhánh fallback.
- `.gitignore`: thêm `data/crawl/` để CSV output không lọt vào repo.

**Kết quả kiểm tra chất lượng (40 dòng, 14 cột):** 0 null / 0 chuỗi rỗng ở mọi
cột, 0 trùng `product_id`, `price ≤ list_price` 100%, currency toàn `VND`,
`snapshot_time` UTC có timezone, chiết khấu TB 10.9% (max 61.7%).

### Điều tra robots.txt của Tiki (2026-08-16)

- robots.txt trả 200, **không cấm** endpoint listing. Chỉ cấm path riêng tư:
  `/customer/*`, `/checkout/*`, `/api/v2/me/`, `/v1/private/`, `/top/` + ~100
  pattern chặn spam SEO. **Không có `Crawl-delay`** → 2s+jitter là tự đặt.
- ⚠️ **Python `robotparser` đọc thiếu nửa cuối robots.txt của Tiki.** File có
  dòng trống ở line 108; theo spec dòng trống kết thúc một record, nên mọi rule
  sau đó bị bỏ. Bằng chứng: giữ nguyên → `can_fetch("/api/v2/me/")` = `True`;
  bỏ dòng trống → `False`. Endpoint đang dùng vẫn hợp lệ ở cả hai cách đọc nên
  **kết quả crawl không sai**, nhưng lá chắn yếu hơn vẻ ngoài.
- ⚠️ `crawler/base.py:60` `_allowed()` chỉ check `self.base_url` (trang chủ),
  **không** check URL thật sự fetch. Nếu Tiki cấm `/api/...` thì crawler vẫn
  chạy. Sửa đúng: truyền URL thực vào `can_fetch`.
- **Cả 2 điểm trên CHƯA sửa** — chờ user quyết.

### ✅ Pagination + multi-category (session 2026-08-17) — ĐÃ SỬA

Trước đó chỉ lấy 40 SP/lượt vì 3 giới hạn **của code, không phải của Tiki**:
`TIKI_CATEGORIES` mặc định 1 category, `PAGE_SIZE=40`, `tiki.py` hardcode
`"page": 1`. Đo thật: `paging` trả `total=2000, last_page=50` cho category 1846
→ chỉ lấy 2%. Test 9 category đều trả 200 (`1846/8322/1882/1520/931/915/4384`
mỗi cái `total=2000`; `1789`=116; `17166`=307).

Đã implement:

- `crawler/base.py::crawl()` — vòng lặp trang `1..max_pages` cho mỗi category,
  **dừng sớm khi trang trả rỗng**. Lỗi ở 1 trang chỉ kết thúc category đó
  (`break`), không chặn category sau; record lỗi vẫn ra DLQ kèm `{category, page}`.
- **Dedupe theo `product_id` cho cả lượt crawl** — grain của
  `fact_price_snapshot` là (site, product, ngày), nên 1 SP nằm ở 2 category
  không được thành 2 dòng. Đếm và log số trùng.
- **Chốt chặn endpoint bỏ qua `page`**: nếu trang N chỉ chứa id đã thấy ở
  category này thì dừng — nếu không sẽ đốt `max_pages` request vô ích.
- `crawler/sites/tiki.py` — `fetch_listing(category, page)` nhận số trang thật,
  **học `paging.last_page` từ trang 1** rồi trả `[]` (không tốn request) khi
  vượt quá. `__init__` mới giữ `self._last_page` theo category.
- `config/settings.py` — `CRAWL_MAX_PAGES` (mặc định 50) + `TIKI_CATEGORIES`
  mặc định thành **9 category đã verify** thay vì 1.
- `crawler/runner.py` — `--max-pages N` và `--category ID` (lặp được) để chạy
  nhanh; registry đổi từ lambda sang class + `SITE_CATEGORIES` để truyền tham số.

**Chạy thật (2026-08-17):** `--max-pages 3 --category 1846 --category 17166`
→ 5 request, **124 SP unique, 0 lỗi**, CSV 124 dòng × 14 cột: 0 null, 0 chuỗi
rỗng, 124/124 `product_id` unique, `price ≤ list_price` 100%.

⚠️ **Phát hiện mới, ngược với đo lần trước:** 3 trang của category 1846 trả 120
dòng nhưng chỉ **118 SP unique** — endpoint `personalish` xếp theo recommender
nên thứ tự đổi giữa các request, sinh trùng lặp xuyên trang (lần đo 2026-08-16
thấy "0 trùng" chỉ là may). Category 17166 chỉ trả 6 SP rồi báo hết trang, dù
`total=307` đo trước đó — **`total`/`last_page` của endpoint này không đáng tin**.
→ Dedupe không phải phòng xa, nó đang thực sự chặn dữ liệu bẩn. Đây cũng là
bằng chứng thêm cho lập luận **phải tách discovery/tracking** (mục 4c).

Test: **44 passed** (+8 test mới: page loop, max_pages, lỗi 1 trang, dedupe
xuyên category, chốt chặn trang lặp, 3 test `fetch_listing` của Tiki).

### ✅ Chạy full 9 category × 50 trang (2026-08-17) — SỐ THẬT CHO BÁO CÁO

```
python -m crawler.runner --site tiki --dry-run --out-csv data/crawl/tiki_full_snapshots.csv
→ 356 page(s) / 9 categories, 14.117 SP unique, failed=0, 4 trùng xuyên category
→ 13:15:08 → 13:32:30 UTC = 17 phút 22 giây
```

| Chỉ số | Giá trị |
|---|---|
| Sản phẩm unique | **14.117** (0 trùng `product_id` trong CSV) |
| Request thành công | 356/356, **0 lỗi, 0 DLQ** |
| Thời gian | **17m22s** (~2.9s/trang, chủ yếu là throttle tự đặt) |
| Tốc độ | ~13,5 SP/giây tính trên toàn lượt |
| `price ≤ list_price` | 100% |
| Currency | 100% `VND` |
| Giá | min 2.200đ · median 229.000đ · max 150.000.000đ (máy chiếu LG — giá thật, không phải outlier lỗi) |

**Ước tính "~18.000 SP/lượt" là SAI, số thật 14.117.** Lý do: 7 category chạm
trần `total=2000` của endpoint, `1789` chỉ có 119 SP, `17166` chỉ **6 SP** (dù
đo hôm 16/08 báo `total=307`). Chỉ 356/450 trang được fetch vì `last_page` cắt
sớm. → **Trần thật của endpoint `personalish` là ~2000 SP/category**, không phụ
thuộc `total` nó tự báo.

#### Bốn phát hiện chất lượng dữ liệu mới (ảnh hưởng thiết kế mart/ML)

1. **`brand` rỗng ở 1.968/14.117 dòng (13,9%)** — không phải lỗi crawler, mà là
   sự thật của catalog (hàng OEM/không nhãn). Mẫu 124 dòng trước đó cho "0 null"
   chỉ vì toàn SP top. → `dim_product_listing.brand` **phải nullable** hoặc gom
   vào nhóm `unknown`; đừng để quality gate đánh trượt dòng thiếu brand.
2. **`rating = 0` và `review_count = 0` ở ~6.600 dòng (47%)** — nghĩa là *chưa có
   đánh giá*, **không phải "bị đánh giá 0 điểm"**. Nếu mart tính `AVG(rating)`
   thẳng thì điểm trung bình bị kéo xuống một nửa. → phải `NULLIF(rating, 0)`
   khi tổng hợp. Đã kiểm: **không có dòng nào `rating` null mà `review_count > 0`**
   nên hai cột nhất quán với nhau.
3. **66% sản phẩm không giảm giá** (9.322/14.117 có `price == list_price`;
   median discount = 0%, mean 8,3%, max 88,9%, 100 SP giảm >50%). → mart
   `discount_daily` phải phân biệt "không sale" với "thiếu dữ liệu", và chuỗi
   `pct_change` sẽ **rất thưa** — càng củng cố việc cần `fact_price_change`
   riêng thay vì quét snapshot (mục 4c).
4. **Endpoint trả cả SP ngoài category truy vấn.** Có 1 dòng
   `category_path = 1/2/1343/7384/923` (quần short nam) lọt vào lượt crawl
   category điện tử — bằng chứng trực tiếp `personalish` là recommender, không
   phải filter theo category. → **không được coi category truy vấn là nhãn
   category**, phải dùng `primary_category_path` (đã sửa hôm 16/08 — nay có bằng
   chứng định lượng cho quyết định đó).

#### Cây ngành hàng: ragged hierarchy được xác nhận bằng số

`category_path` gồm **964 giá trị distinct**, tất cả gốc ở node `1`, **độ sâu
4–8 cấp** (5 cấp: 5.103 dòng · 6 cấp: 7.466 · 7 cấp: 1.326 · 4 cấp: 179 · 8 cấp:
43). Mục 4d ghi "biến thiên 5–6 cấp" — số thật là **4–8**. Không thể ép vào
`category_level_1..N` cố định ⇒ **closure table là lời giải đúng**, nay có số đo
để dẫn trong báo cáo. Ngoài ra: 1.797 brand distinct, 1.159 seller distinct.

## 4a. ĐỔI HƯỚNG: bỏ Shopee/Lazada, chuyển sang adapter theo NỀN TẢNG

**Quyết định của session 2026-08-16.** Vấn đề thật của `TikiCrawler` không phải
"ít category" mà là **1 adapter chỉ dùng được cho 1 site**, dựa trên endpoint
không tài liệu. Viết Shopee lại phải làm từ đầu, với rủi ro anti-bot cao.

Hướng mới: adapter khóa theo **nền tảng/chuẩn**, không theo từng site.

| Adapter | Phủ được | Độ ổn định |
|---|---|---|
| `TikiCrawler` (đã có) | 1 sàn | thấp — API nội bộ, không version |
| `WooCommerceCrawler` | mọi site WooCommerce | **cao** — API có tài liệu, có `v1` |
| `ShopifyCrawler` | mọi Shopify/Haravan/Sapo | cao — chuẩn de-facto |
| `JsonLdCrawler` (sau) | gần như mọi site TMĐT có SEO | cao — schema.org |

**Đã verify bằng request thật (2026-08-16):**

```
GET /wp-json/wc/store/v1/products?per_page=N     → 200, KHÔNG cần auth
     woocommerce.com  → X-WP-Total: 1701, X-WP-TotalPages: 567
     (2 site khác 403 do WAF, 1 site 404 vì không phải Woo)

GET /products.json?limit=N                        → Shopify, KHÔNG cần auth
     allbirds.com 200  price=91.00     sku=A11718M080
     kith.com     200  price=385.00    sku=2000419500
     yame.vn      200  price=228000    sku=0019526001   ← VN, VND, có SKU
     gymshark.com 403  (WAF chặn)
```

Field map Store API gần 1:1 với `PRICE_SNAPSHOT_FIELDS`, **cộng thêm `sku`**
(chìa khóa cho so sánh xuyên site):
`prices.price` / `prices.regular_price` / `prices.currency_code` /
`average_rating` / `review_count` / `is_in_stock` / `categories[]` / `brands[]`.

**Bẫy phải nhớ khi implement:** `prices.price` là **chuỗi theo đơn vị nhỏ nhất**
(`"4900"` + `currency_minor_unit: 2` = $49.00). Parse sai lệch 100 lần.

**Giới hạn phải ghi vào thesis:** WAF chặn (403) nên tỉ lệ thành công không phải
100%; shop Woo/Haravan VN nhỏ (vài trăm–vài nghìn SP/site) nên cần nhiều site;
**shop ≠ sàn** — so sánh giữa các shop khác về luận điểm với so sánh giữa các
sàn; `sku` không tự khớp xuyên site (nhiều shop dùng SKU nội bộ) nên vẫn còn
bài toán **product matching / entity resolution**.

**Nếu vẫn cần sàn lớn thứ hai:** ưu tiên Điện Máy Xanh / FPT Shop / CellphoneS
(SSR, dễ parse) hơn Shopee/Lazada (JS-rendered + anti-bot mạnh).

## 4b. Storage portability + sửa Spark OOM (session 2026-08-16)

### Chẩn đoán: crash KHÔNG đến từ tầng lưu trữ

- `grep -rn "hdfs|namenode|datanode"` → **0 kết quả ngoài `.venv`**. Dự án
  hiện không hề dùng HDFS (user từng dùng HDFS, crash, nên đã đổi sang MinIO).
- `docker-compose.yml` cũ: `SPARK_WORKER_MEMORY: 2g`, `SPARK_WORKER_CORES: 2`
- `data/data_kaggle/2019-Nov.csv` = **8589 MB**, `2019-Oct.csv` = 5406 MB
- Máy: 31.1 GB RAM (trống 15.3), 8 nhân

→ **Worker 2 GB nhai file 8.6 GB.** Đây nhiều khả năng cũng là lý do cụm HDFS
trước đây chết. **Lên cloud KHÔNG sửa được lỗi này** — nếu executor vẫn 2 GB,
đọc từ S3 chỉ chậm hơn vì thêm chặng mạng.

### Đã sửa

- `docker-compose.yml`: `SPARK_WORKER_CORES: ${SPARK_WORKER_CORES:-4}`,
  `SPARK_WORKER_MEMORY: ${SPARK_WORKER_MEMORY:-8g}`
- `batch_layer/warehouse_job.py:build_spark`: thêm `spark.executor.memory=6g`,
  `spark.driver.memory=4g`, `spark.sql.files.maxPartitionBytes=64MB`,
  `spark.sql.adaptive.enabled=true`. **Nâng worker mà không nâng executor là vô
  nghĩa** — worker chỉ quảng cáo dung lượng, executor phải được lệnh mới đòi.
- **`config/storage.py` (MỚI)** — 5 storage profile. Trước đây `build_spark`
  hardcode 2 thiết lập **chỉ đúng với MinIO** (`path.style.access=true`,
  `ssl.enabled=false`) nên tính portable của S3A chỉ có trên giấy.

  | Profile | Endpoint | TLS | path-style |
  |---|---|:--:|:--:|
  | `local` | — | — | — |
  | `minio` | `MINIO_ENDPOINT` | ✗ | ✓ |
  | `s3` | SDK phân giải theo region | ✓ | ✗ |
  | `r2` | `DATA_LAKE_ENDPOINT` | ✓ | ✓ |
  | `b2` | `DATA_LAKE_ENDPOINT` | ✓ | ✓ |

  Đổi nhà cung cấp = đổi `DATA_LAKE_PROFILE`. `DATA_LAKE_MODE` cũ vẫn hoạt động
  (`s3a` → `minio`) nên compose/`scripts/*.ps1` không phải sửa. Region mặc định
  `ap-southeast-1` (đi us-east-1 từ VN thêm ~200ms/request). Có
  `fs.s3a.committer.name=directory` vì object store **không có `rename` nguyên
  tử** — committer mặc định của Spark dựa vào rename.
- `common/object_store.py`, `config/settings.py:data_lake_uri`: dùng profile
  thay vì `DATA_LAKE_MODE` + `secure=False` hardcode.
- `tests/test_storage.py` (MỚI, 8 test) + cập nhật `tests/test_object_store.py`.

### ⚠️ CHƯA kiểm chứng chạy thật

Docker Desktop đang tắt → **chưa chạy `warehouse-job` để xác nhận file 5.4 GB
đi trót lọt với config mới.** Code đã sửa nhưng chưa có bằng chứng. Đây là việc
đầu tiên cần làm khi resume.

### Kết luận về "chuẩn big data phải dùng HDFS"

**Tiền đề này đã lỗi thời ~1 thập kỷ.** HDFS (2006) thiết kế quanh *data
locality* vì hồi đó mạng chậm hơn đĩa; nay mạng datacenter 10–100 Gbps nhanh
hơn đĩa. EMR/Databricks/Snowflake/BigQuery/Synapse đều đặt trên object storage.
Netflix/Uber/Airbnb đã rời HDFS sang S3. Hadoop 3 thêm S3A committer chính là
để chạy trên object store. **Đổi từ HDFS sang MinIO là nâng cấp, không phải
bước lùi.** Lập luận đầy đủ + bảng so sánh + câu trả lời mẫu cho hội đồng:
`docs/CLOUD_MIGRATION.md` mục 1.

## 4c. Thiết kế data mart cho nhánh giá (đã bàn, CHƯA implement)

### Bẫy phải xử lý trước khi dựng mart

Endpoint `personalish` xếp theo recommender → sản phẩm A có hôm nay, mai có thể
không nằm trong 2000 kết quả **dù vẫn còn bán**. Chuỗi giá sẽ thủng lỗ chỗ và
mô hình time-series hiểu nhầm khoảng trống là tín hiệu.

→ Tách crawler thành 2 pha: **discovery** (tuần/tháng, quét listing tìm SP mới,
sinh watchlist) + **tracking** (hằng ngày, crawl đúng watchlist theo id/URL).
Panel cân bằng thay vì rách. **Đây là điều kiện tiên quyết cho ML.**

### Ba fact, ba grain

| Fact | Loại | Grain | Nguồn |
|---|---|---|---|
| `fact_behavior_event` (đã có) | Transaction | 1 dòng / event | Kaggle |
| `fact_price_snapshot` (mới) | **Periodic snapshot** | 1 dòng / (site, product, ngày) | Crawler |
| `fact_price_change` (mới) | Transaction | 1 dòng / lần giá đổi | Dẫn xuất |

`fact_price_change` chỉ ghi khi giá **khác** snapshot trước
(`old_price, new_price, pct_change, change_type`) — thưa hơn snapshot cả trăm
lần nhưng chứa đúng thứ analytics cần.

### Dimension mới

- **`dim_site`**(`site_key, site_name, platform, country, is_marketplace,
  first_crawled_at`) — biến "5-10 sàn" thành **dữ liệu**, thêm sàn thứ 11 =
  insert 1 dòng, không sửa schema.
- **`dim_product_listing`** — **SCD Type 2** (`valid_from/valid_to/is_current`).
  Đo trên 14.117 SP thật (2026-08-17): `brand` rỗng 13,9% ⇒ **cột này phải
  nullable**, và `rating`/`review_count` = 0 ở 47% SP nghĩa là *chưa có đánh giá*
  ⇒ mọi tổng hợp phải `NULLIF(...,0)`. Xem mục 4.
  `docs/DATA_MODEL.md` hiện ghi "Dimensions are SCD Type 1" — đúng với Kaggle
  nhưng **sai với dữ liệu crawl** (tên/brand/seller đổi theo thời gian).

### ⚠️ Hai star này KHÔNG join được với nhau

Sản phẩm Kaggle (cửa hàng Nga) và sản phẩm crawl Tiki là **hai tập thực thể
khác nhau**. Dimension duy nhất thực sự conformed là `dim_date`. **Đừng vẽ sơ
đồ cho thấy user log nối vào price log.** Trình bày đúng = **bus matrix**:

| Business process | dim_date | dim_user | dim_product | dim_site | dim_product_listing |
|---|:--:|:--:|:--:|:--:|:--:|
| Hành vi người dùng | ✓ | ✓ | ✓ | | |
| Biến động giá | ✓ | | | ✓ | ✓ |

### Mart đề xuất

| Mart | Grain | Trả lời |
|---|---|---|
| `price_daily` | ngày × site × product | giá đóng/min/max, % discount, còn hàng |
| `price_change_daily` | ngày × site × category | số lần tăng/giảm, biên độ TB |
| `discount_daily` | ngày × site × category | tỉ lệ SP sale, phân phối độ sâu |
| `category_price_index` | ngày × site × category | **chỉ số giá median (base=100)** |
| `product_price_volatility` | product × site × cửa sổ | stddev, CV, số lần đổi giá |

`category_price_index` là mart quan trọng nhất: cho phép **so sánh giữa các sàn
mà KHÔNG cần product matching** — so chỉ số giá theo nhóm ngành là hợp lệ thống
kê, trong khi so từng sản phẩm bắt buộc phải giải entity resolution trước.

`product_price_volatility` nối thẳng vào ML: `forecasters.py` /
`anomaly_detector.py` hiện chỉ chạy trên 1 chuỗi `daily_revenue`; chuyển sang
hàng nghìn chuỗi giá là bài toán multi-series thật, xứng với darts/PyOD.

## 4d. Star vs Snowflake — đã chốt: GIỮ STAR

**Phát hiện: schema hiện tại ĐÃ snowflake sẵn một nhánh.** Trong
`warehouse_job.py:build_dimensions`, `dim_product` chỉ giữ `category_key` làm
FK, thuộc tính ngành hàng nằm riêng ở `dim_category`; `build_fact` **không**
mang `category_key` vào fact. Nên mọi query theo ngành hàng đi 2 chặng:
`fact → dim_product → dim_category`.

**Quyết định: không snowflake thêm.** Ba lý do kinh điển đều không áp dụng:

1. *Tiết kiệm dung lượng* — đã lỗi thời. Gold zone là Parquet, dictionary
   encoding nén chuỗi lặp gần như miễn phí.
2. *Tránh update dị thường* — không tồn tại. `write_gold` **rebuild dimension
   từ silver** mỗi lần chạy, không có `UPDATE` tại chỗ.
3. *Tái dụng dimension chung* — bằng không, vì hai star không chia sẻ
   `dim_product` (xem 4c).

Marts trong Postgres đang **denormalized** (`product_daily` có `brand` +
`category_code` inline) — đúng, **đừng đụng vào**.

**Ngoại lệ duy nhất đáng snowflake: cây category của Tiki.** `1/2/1846/8095/2458`
— đo trên 14.117 SP thật (2026-08-17), **độ sâu biến thiên 4–8 cấp**, 964
`category_path` distinct, tất cả cùng gốc node `1` (số cũ "5–6 cấp" là do mẫu
nhỏ). Không ép được vào `category_level_1..N` cố định. Đây là **ragged
hierarchy**, lời giải Kimball chuẩn là closure table:

```
dim_category_node(category_key, category_id, name, parent_key, level, full_path)
bridge_category(ancestor_key, descendant_key, depth)
```

Chỉ áp cho nhánh crawl. Kaggle dùng `electronics.smartphone` cố định 2 cấp, đã
split sẵn — để nguyên.

**Lưu ý trình bày:** snowflake KHÔNG phải bản nâng cấp của star, nó là đánh
đổi. Trả lời "để chuẩn hóa cho gọn" là mất điểm (đó chính là lý do đã lỗi thời).
Trả lời "giữ star, chỉ snowflake nhánh cây ngành hàng vì độ sâu biến thiên, kèm
số đo chi phí query" là được điểm.

*(Ghi chú: "snowflake hợp với data thay đổi trường liên tục" là nhầm lẫn — dữ
liệu thay đổi theo thời gian là địa hạt của **SCD Type 2**, không phải
snowflake. Snowflake là chuẩn hóa thuộc tính dimension, không liên quan tần
suất thay đổi. Cần nói đúng khi bảo vệ.)*

## 4e. Danh sách sàn/shop mục tiêu — ĐÃ CHỐT (2026-08-17)

User giao quyền quyết định. **Mọi site dưới đây đã được probe thật hôm nay** —
không có tên nào chỉ dựa trên phỏng đoán. Tổng: **9 site đang chọn / 4 adapter**.

| Adapter | Site | Bằng chứng đo được |
|---|---|---|
| `TikiCrawler` (có) | tiki.vn | đang chạy, 124 SP/2 category × 3 trang |
| `ShopifyCrawler` | **yame.vn** | 200, `price=277000` VND, `sku='2625086001'` |
| | **emwear.vn** | 200, `price=1250000` VND, `sku=None`, title `'Oliva Robe'` |
| | **allbirds.com** | 200, `price=91.00`, `sku='A11718M080'` |
| | **kith.com** | 200, `price=112.00`, `sku='14303111'` |
| `WooCommerceCrawler` | **woocommerce.com** | 200, `X-WP-Total: 1704`, `price="6900"` + `minor_unit=2` |
| | **barefootbuttons.com** | 200, `X-WP-Total: 61`, `price=195`, `sku='18-AC-AS-02'` |
| `JsonLdCrawler` | **cellphones.com.vn** | Product LD: `30990000 VND`, sku, brand Apple, rating 4.9/368 |
| | **fptshop.com.vn** | Product LD: `26490000 VND`, `sku='00914045'`, rating 4.9/162 |

Dự phòng đã verify 200 nếu site nào rụng: `gymshark.com`, `fashionnova.com`,
`dutchbulbs.com` (đều Shopify).

### Vì sao danh sách này, không phải danh sách khác

- **VN shop dùng Shopify/Haravan cực hiếm.** Probe 48 domain (28 VN):
  chỉ `yame.vn` + `emwear.vn` trả JSON. Phần lớn shop VN lớn là SPA/SSR tự viết
  → `/products.json` trả 404 hoặc HTML (`coolmate.me`, `routine.vn`,
  `nguyenkim.com`, `24hstore.vn`, `anphatpc.com.vn`, `bibomart.com.vn`...).
  Vì vậy **không thể đạt "5-10 sàn VN" chỉ bằng adapter Shopify** — đó là lý do
  thêm adapter thứ 4 (JSON-LD).
- **JSON-LD là cách vào đúng cho retailer VN lớn.** cellphones.com.vn và
  fptshop.com.vn nhúng `schema.org/Product` với **giá thật, sku, brand,
  aggregateRating** ngay trong HTML SSR — không cần Playwright, không cần API
  nội bộ không tài liệu. Cả hai đều có `robots.txt` khai báo `Sitemap:`
  (fptshop: sitemap index **259 sub-sitemap**; cellphones:
  `/sitemap/sitemap_index.xml?v=google`) → **discovery đi bằng sitemap**, khớp
  luôn với việc tách discovery/tracking ở mục 4c.
- **Giá trị cho câu chuyện đa sàn:** iPhone 16 Pro Max có ở cellphones
  (30.990.000đ), Galaxy S25 Ultra có ở fptshop (26.490.000đ), và cùng dòng máy
  cũng có trên Tiki → **so sánh giá cùng model xuyên site là làm được**, không
  chỉ so chỉ số giá theo ngành.

### ⚠️ Bẫy đã đo, phải xử lý khi implement

1. **`dienmayxanh.com` bị loại (tạm).** Có `Product` JSON-LD nhưng
   **`offers.price = 0.0`** — giá thật render bằng JS ở chỗ khác. Nếu tin JSON-LD
   mù quáng sẽ nạp giá 0 vào warehouse. Muốn lấy site này phải parse DOM/API
   riêng → hoãn. Bài học: **validate `price > 0` ngay ở adapter**, coi giá 0 là
   record lỗi đưa vào DLQ, không phải giá hợp lệ.
2. **WooCommerce `prices.price` là chuỗi theo đơn vị nhỏ nhất**: `"6900"` +
   `currency_minor_unit: 2` = $69.00. Sai 100 lần nếu parse thẳng (đã ghi ở 4a,
   nay xác nhận lại trên woocommerce.com).
3. **WAF chặn ngẫu nhiên**: `bombas.com` trả **429** (rate limit) trong lần probe
   đồng thời 8 luồng; `gymshark.com` từng 403 hôm 16/08 nhưng hôm nay 200.
   → Tỉ lệ thành công không phải 100%, và **không được crawl song song nhiều
   luồng vào cùng một host**.
4. **SSL/DNS chết thật**: `hc.com.vn` (SSLError), `dosi.vn`/`libe.vn`/`hnoss.vn`/
   `vietmax.vn` (ConnectionError) → adapter phải chịu được site chết, đúng như
   `crawl()` đang cô lập lỗi.
5. **`emwear.vn` không có `sku`** (`None`) → khoá join xuyên site không tồn tại
   ở mọi shop, đúng như đã ghi ở 4a: vẫn còn bài toán entity resolution.

### Thứ tự implement đề xuất

`ShopifyCrawler` trước (2 site VN + 2 intl, API sạch nhất, có `sku`) →
`JsonLdCrawler` (giá trị cao nhất cho câu chuyện VN, cần discovery bằng sitemap)
→ `WooCommerceCrawler` (chủ yếu để chứng minh tính đa nền tảng; catalog nhỏ).

## 4f. robots.txt — ĐÃ SỬA cả 2 điểm (2026-08-17)

User chốt "có sửa". Cả hai lỗi ghi ở mục "Điều tra robots.txt" đã xử lý:

- **`_allowed()` check sai URL.** Trước: luôn check `self.base_url` (trang chủ,
  gần như luôn được phép) → crawler được "cấp phép" đi vào path mà site cấm.
  Nay: `SiteCrawler.request_url(category, page)` là hook mới trả về **URL sẽ
  fetch thật**, `crawl()` check URL đó **trước từng trang**; `TikiCrawler`
  override trả `LISTING_URL`. Site cấm listing ⇒ dừng đúng category đó.
- **`robotparser` đọc thiếu nửa file.** `RobotFileParser.read()` theo bản nháp
  1994 (dòng trống kết thúc record) nên bỏ mọi rule sau dòng trống — Tiki có
  dòng trống ở line 108. Nay `_load_robots()` tự `requests.get` robots.txt (kèm
  UA của mình) rồi **lọc bỏ dòng trống trước khi `parse()`**, đúng RFC 9309:
  chỉ dòng `User-agent` mới mở group mới. Đã có test chứng minh **không** gộp
  rule của bot khác vào group của mình (`User-agent: EvilBot / Disallow: /` vẫn
  không áp lên ta).
- Kèm theo: `403/401` → coi là **cấm**; `404` → **không có robots.txt = không có
  giới hạn**; lỗi mạng → cấm (giữ nguyên hành vi cũ nhưng nay tường minh, trước
  đây chỉ *tình cờ* đúng nhờ `read()` raise sớm).

**Đối chứng thật trên tiki.vn:**

| URL | `read()` cũ | loader mới |
|---|---|---|
| `/api/personalish/v1/blocks/listings` (đang dùng) | allow | **allow** ✅ |
| `/api/v2/me/` | allow ❌ | **deny** ✅ |
| `/checkout/cart` | — | **deny** ✅ |

Ghi chú thêm: tiki.vn **trả robots.txt rỗng** nếu request không gửi header
`User-Agent` → loader mới luôn gửi UA, nếu không sẽ vô tình "được phép mọi thứ".

## 5. Quyết định kỹ thuật đã chốt (tránh hỏi lại / đổi ý giữa đường)

- **Lakehouse**: Delta Lake cho Gold (không phải Iceberg, không phải tự viết
  incremental logic tay) — vì là thư viện Spark (`delta-spark`), không cần
  thêm service.
- **ML tracking**: MLflow với sqlite backend (không cần container mới).
- ~~**Orchestration**: Airflow 1 container LocalExecutor, tái dùng
  `postgres-dw` làm metadata DB — không Celery cluster.~~
  **ĐẢO NGƯỢC 2026-10-04 (Phase 8 plan §2.1 D2). Không có Airflow.**
  Thay bằng `batch_layer/marketplace_scheduler.py`: một vòng lặp dịch vụ
  thường, cắt cửa sổ theo mốc interval, chạy batch dưới một PostgreSQL
  advisory lock. Lý do: Brief §27 không cho thêm hạ tầng nào không phục vụ
  một tiêu chí nghiệm thu, và tiêu chí duy nhất ở đây — "hai batch không bao
  giờ chồng nhau, một cửa sổ đã `SUCCEEDED` thì không chạy lại" — vòng lặp
  cộng advisory lock đáp ứng đủ. Một container Airflow sẽ thêm một metadata
  DB, một scheduler, một webserver và một mô hình DAG để lái đúng **một** job
  định kỳ. Hệ quả kiểm được: scheduler bỏ qua cửa sổ `SUCCEEDED`, khởi động
  cửa sổ chưa có, resume mọi trạng thái khác, và **không bao giờ** tự truyền
  `--allow-backfill` (test 15); run bị lock từ chối exit 75 và không ghi dòng
  audit nào (test 18, drill D10 chạy thật).
- **Serving**: FastAPI nhỏ (3-4 endpoint), không phải microservice đầy đủ.
- **Schema**: hành vi (view/cart/purchase) và giá crawl (price snapshot) là
  **hai contract riêng**, không gộp — vì bản chất dữ liệu khác nhau (crawler
  không thể quan sát hành vi người dùng thật).

Bổ sung 2026-08-16:

- **Crawler**: adapter khóa theo **nền tảng/chuẩn** (WooCommerce, Shopify),
  không theo từng site. Bỏ Shopee/Lazada khỏi kế hoạch gần. (mục 4a)
- **Dimensional model**: **giữ star**, chỉ snowflake nhánh cây ngành hàng của
  dữ liệu crawl (ragged hierarchy → closure table). (mục 4d)
- **Hai star KHÔNG join** — chỉ conformed qua `dim_date`, trình bày bằng bus
  matrix. (mục 4c)
- **`dim_product_listing` phải là SCD Type 2**, khác `dim_product` (Type 1).
- **Storage**: object storage (MinIO → S3/R2) là kiến trúc đúng, **không quay
  lại HDFS**. Chọn nhà cung cấp bằng `DATA_LAKE_PROFILE`, không sửa code.
  **Nhà cung cấp đã chốt (2026-08-17): AWS S3 `ap-southeast-1`** — local dev
  vẫn dùng MinIO, `DATA_LAKE_PROFILE=s3` chỉ bật khi deploy.
- **Dataset chính cho ML vẫn là Kaggle** (volume). Crawler đóng góp velocity +
  variety + chuỗi thời gian. **Đừng bán crawler như nguồn volume** — hội đồng
  sẽ bắt bẻ đúng chỗ đó.

## 6. Việc đang tạm hoãn / chờ quyết định của user

- Phase 0 (foundation cleanup) bị hoãn 1 lần khi user nói "nghiên cứu và chốt
  duyệt phương án trước" — rồi user chuyển sang yêu cầu làm Phase 1 trước.
  **Chưa quay lại Phase 0** — nhắc user nếu họ quên.
- ~~Shopee/Lazada ngay hay dừng ở Tiki~~ → **đã giải quyết**: bỏ Shopee/Lazada,
  chuyển sang adapter theo nền tảng (mục 4a).

### Đã được user trả lời (2026-08-17)

1. **Chạy Giai đoạn 1 (warehouse-job trên 5.4 GB)** — ⏸️ **vẫn chưa làm được:
   Docker Desktop đang tắt.** Đây là việc đầu tiên khi Docker bật lại.
2. **R2 hay S3?** → **S3, region `ap-southeast-1`.** Profile `s3` trong
   `config/storage.py` đã sẵn sàng; 4 biến `.env` cần đặt + cách kiểm soát phí
   egress ghi ở `docs/CLOUD_MIGRATION.md` §3.
3. **Danh sách sàn/shop** → user giao tôi quyết. **Đã chốt 9 site / 4 adapter,
   tất cả probe thật — xem mục 4e.**
4. **Sửa 2 điểm robots.txt?** → **có, đã sửa + có test + đối chứng thật trên
   tiki.vn — xem mục 4f.**

## 6b. Cách tiếp tục ngay (resume ở đây)

> ⚠️ **LỖI THỜI (2026-08-17).** Kỳ vọng "44 passed" và đường dẫn
> `.venv/Scripts/python.exe` đều không còn đúng: trên `develop` suite hiện có
> **519 test** (**624** sau khi PR #4 của Phase 7 merge) và chạy bằng `python`
> toàn cục (không có venv trong repo). Xem **§8**, **§9** và **§10** để biết
> trạng thái thật và việc tiếp theo.


```bash
# 1. Xác nhận vẫn xanh (kỳ vọng: 44 passed)
.venv/Scripts/python.exe -m pytest tests/ -q

# 2. Crawler còn sống? (kỳ vọng: ok=124 failed=0 với 2 category × 3 trang)
#    BỎ --max-pages/--category để chạy full: đã đo 14.117 SP / 356 trang / 17m22s
.venv/Scripts/python.exe -m crawler.runner --site tiki --dry-run \
    --max-pages 3 --category 1846 --category 17166 \
    --out-csv data/crawl/tiki_snapshots.csv

# 3. VIỆC QUAN TRỌNG NHẤT: kiểm chứng bản sửa Spark OOM (cần bật Docker Desktop)
docker compose up -d
docker compose --profile jobs run --rm warehouse-job \
    python -m batch_layer.warehouse_job --source data/data_kaggle/2019-Oct.sample.csv
# rồi lặp lại với 2019-Oct.csv (5.4 GB) và GHI LẠI thời gian chạy + RAM đỉnh
# — đây là số liệu đối chứng cho báo cáo
```

Thứ tự ưu tiên việc code tiếp, theo giá trị giảm dần:

1. **Kiểm chứng Spark OOM fix** (bước 3 ở trên) — mở khóa mọi quyết định cloud.
   Session 2026-08-17 **không làm được: Docker Desktop đang tắt.**
2. ~~Pagination + multi-category cho Tiki~~ → **XONG** (2026-08-17, xem mục 4)
3. **Tách discovery / tracking** — điều kiện tiên quyết cho ML chuỗi giá (4c).
   Bằng chứng mới ủng hộ việc này: xem phát hiện trùng lặp/`total` không đáng
   tin ở mục 4.
4. `WooCommerceCrawler` + `ShopifyCrawler` (4a) — nhớ bẫy `currency_minor_unit`
5. `dim_site` + `fact_price_snapshot` + `fact_price_change` (4c)
6. Cập nhật `docs/DATA_MODEL.md`: bus matrix + SCD2 + ragged hierarchy (4c, 4d)

## 7. Tham chiếu

- Plan đã duyệt (đầy đủ, không sửa): `C:\Users\Admin\.claude\plans\shimmering-rolling-blanket.md`
- **`docs/CLOUD_MIGRATION.md`** — chẩn đoán crash, lập luận HDFS vs object
  storage, bảng storage profile, ước lượng chi phí R2/B2/S3, kế hoạch 3 giai đoạn
- Memory cross-session: `graduation-thesis-roadmap.md`, `architecture-redesign.md`,
  `canonical-event-contract.md`, `ml-python-version-caveat.md` (trong
  `C:\Users\Admin\.claude\projects\C--code-data\memory\`)
- Commit crawler layer: `916d72b` (branch `master`, đã push)

### File thay đổi trong session 2026-08-16 (CHƯA COMMIT)

```
M  .gitignore                              + data/crawl/
M  batch_layer/warehouse_job.py            build_spark: profile + memory config
M  common/object_store.py                  profile-driven, bỏ secure=False
M  config/settings.py                      data_lake_uri dùng profile
M  crawler/base.py                         page loop + dedupe + chốt chặn trang lặp
M  crawler/runner.py                       + --out-csv, --max-pages, --category
M  crawler/sites/tiki.py                   sửa URL canonical + category_path; + pagination
M  docker-compose.yml                      SPARK_WORKER_* thành ${VAR}
M  tests/fixtures/tiki_listing_sample.json  khớp shape response thật
M  tests/test_crawler.py                   + test fallback category
M  tests/test_object_store.py              dùng profile thay DATA_LAKE_MODE
?? config/storage.py                       MỚI — 5 storage profile
?? tests/test_storage.py                   MỚI — 8 test
?? docs/CLOUD_MIGRATION.md                 MỚI — plan cloud
?? docs/PROGRESS.md                        file này
```

Thêm ở session 2026-08-17: `config/settings.py` (+`CRAWL_MAX_PAGES`, 9 category
mặc định), `tests/test_crawler.py` (+8 test).

Trạng thái test: **44 passed**.

---

---

## 8. Session 2026-09-29 — nguồn chân lý, Phase 3–6, mô hình `develop`

### 8.1 Vì sao có session này

Phát hiện Phase 5/6 bị triển khai **hai lần độc lập** trên hai nhánh. Nguyên
nhân: plan Phase 5/6 chỉ nằm ở ngọn nhánh `phase-5-6-speed-gold`, không có ở
commit gốc chung (`1d5b252`), nên phiên làm việc sau rẽ từ đó không nhìn thấy
và tự viết plan mới. Hai bộ tên biến, hai bộ tên module cho cùng một việc.

Bài học: **plan là hợp đồng, phải nằm ở nhánh chung.** Một hợp đồng chỉ một
nhánh nhìn thấy thì không phải hợp đồng.

### 8.2 Mô hình nhánh mới (chốt trong session này)

```text
phase-<n>-<tên>  --PR-->  develop  --PR (chỉ khi thầy hướng dẫn duyệt)-->  master
```

- `master`: **không nhận commit trực tiếp nữa**, kể cả docs. Chỉ nhận PR từ
  `develop` sau khi thầy duyệt. Hiện đang ở `3828eb5`, đúng bản gốc.
- `develop`: nhánh tích hợp, chạy full suite trước khi trình duyệt.
- Nhánh phase: **luôn rẽ từ `develop` mới nhất, không xếp chồng lên nhau.**
  Đã thử xếp chồng một lần và PR thứ hai hiện luôn diff của PR thứ nhất,
  không review được.

Chi tiết ở `docs/PHASE_INDEX.md` §3b. Ba PR đã chạy qua quy trình này:
**#1** (test phase 4/5/6), **#2** (Phase 3), **#3** (bug clock tiki).

### 8.3 Nguồn chân lý

`docs/PHASE_INDEX.md` là nguồn chân lý về chia phase: bảng Tuần ↔ Phase ↔
Backlog ID suy thẳng từ Brief §21/§22, các hợp đồng đóng băng (3 Kafka topic,
7 change type, Kafka key, `observation_id`, cột mart, counter delta), ràng buộc
môi trường và mô hình nhánh.

Bảng phase ở §3 của chính file này đã **lỗi thời** (đánh số từ trước Brief,
Phase 3 = "Speed layer hardening" thay vì "Scheduler/retry/audit"). Giữ làm
lịch sử, đừng dùng để lập kế hoạch.

### 8.4 Phase 1–6 đã xong, nằm trong `develop`

| Phase | Tuần | Nội dung | Test |
|---|---|---|---|
| 1 | 1 | Marketplace foundation | |
| 2 | 2 | Raw-first acquisition | |
| 3 | 3 | **Scheduler, retry, crawl audit** | 113 (đủ 30/30 item) |
| 4 | 4 | Canonical Kafka + Silver | |
| 5 | 5 | Speed layer | đủ 38/38 item |
| 6 | 6 | Batch temporal warehouse | 43/47 item |

**Nửa đầu lộ trình 12 tuần hoàn tất.**

Phase 3 (mới làm trong session này) gồm `crawler/scheduling.py` (quy tắc thuần),
`crawler/frontier.py` (lease `FOR UPDATE SKIP LOCKED`, thu hồi lease hết hạn),
`crawler/audit.py` (run/attempt + circuit breaker), `crawler/worker.py` (vòng
lặp inject clock/executor), 4 bảng DDL trong schema `audit`, và một adapter mỏng
ở `runner.py`. Đủ 14/14 mục Definition of Done, không đụng dòng nào của Phase 2.

Hai quyết định đáng nhớ:
- **Lỗi parser/validation là terminal và KHÔNG mở circuit** — nó nói response
  này hỏng, không phải nguồn đang chết.
- **Circuit mở thì trả task về nguyên trạng**, không tính là thất bại của task,
  nên `attempts` lẫn streak của nguồn đều không nhúc nhích.

### 8.5 Năm bug production, đều do test mới phát hiện

| Bug | Vị trí | Hệ quả |
|---|---|---|
| `_latest()` lấy bản ghi **cũ nhất** (`row_number()==1` trên window tăng dần) | `batch_layer/marketplace_marts.py` | `offer_current` báo giá đầu tiên là giá hiện tại; `offer_freshness` tính tuổi từ observation đầu → **không offer nào FRESH được** |
| Join trùng cột `first_seen_at` | `build_offer_current` | `AMBIGUOUS_REFERENCE` — **ném exception ở mọi input** |
| Đọc cờ `observed` từ frame không chứa nó | `build_source_coverage_daily` | `UNRESOLVED_COLUMN` — **ném exception ở mọi input** |
| `stage` gán sau khi decode thành công | `data_ingestion/marketplace_silver_sink.py` | Vi phạm hợp đồng bị dán nhãn `DECODE` thay vì `CONTRACT_VALIDATION`; hai loại lỗi đụng chung `dlq_id` |
| `kwargs.pop("clock")` không truyền xuống `super()` | `crawler/sites/tiki.py` | Clock inject bị ghi đè bằng giờ hệ thống → sai partition Bronze (`hour=`), sai `raw_artifact_id`, `fetched_at` nằm ngoài `[started_at, completed_at]` |

Hai bug giữa nghĩa là **2 trong 9 mart chưa từng chạy được lần nào**. Bug cuối
mắc kẹt trên một nhánh lẻ suốt 3 tuần, phát hiện khi dọn nhánh.

### 8.6 Môi trường

- `pyspark` 3.5.1 → **4.0.4**. 3.5.x chỉ hỗ trợ Python 3.8–3.11 + Java 8/11/17;
  trên Python 3.12 + Java 21 thì JVM chạy nhưng python worker chết ngay, triệu
  chứng (`EOFException`) không hề gợi ý nguyên nhân. Spark 4 bật ANSI mode mặc
  định — đã kiểm tra, không làm lệch mart nào.
- Test dùng Spark phải set `spark.sql.session.timeZone=UTC` cho khớp
  `build_spark()`; `collect()` trả timestamp naive theo **local zone của driver**,
  không theo session zone.
- Cài thêm `psycopg2-binary`, `scikit-learn`.

### 8.7 Trạng thái test

**519 passed, 0 failed, 0 skipped** — toàn bộ suite, không loại file nào.
Đầu session: 141 test, trong đó 7 file không collect được.

### 8.8 Trạng thái nhánh

```text
master              3828eb5   bản gốc, chờ thầy duyệt
develop             ✅ phase 1-6, 519 test xanh
phase-5-6-speed-gold          GIỮ làm tham chiếu — 15 commit riêng,
                              bản Phase 5/6 thay thế đã bị loại, có ~3000
                              dòng test cho một thiết kế khác
```

Đã xoá (đều nằm trọn trong `develop`): `phase-1-marketplace-foundation`,
`phase-3-4-scheduler-kafka-silver`, `phase-5-6-tests`, `phase-3-scheduler`,
`fix-tiki-clock-injection`, `fix-tiki-clock`.

### 8.9 Việc tiếp theo

**Phase 7 (Tuần 7)** — chưa bắt đầu. Theo Brief §21 và §22:
- P1-08 mandatory quality gates
- P1-09 gold publish manifest
- robust price anomaly: rolling median, MAD, IQR fallback, minimum sample size
  (Brief §15 nói rõ: MVP dùng robust statistics, không ML phức tạp; không gọi
  anomaly là scam hay incorrect price)
- raw reparse workflow
- batch replay/idempotency tests
- data-quality/audit dashboards

**Còn nợ:** 4/47 item Phase 6 (33–35, 43) cần chạy thật
`run_marketplace_warehouse` end-to-end với MinIO + PostgreSQL, không phải unit
test — cần Docker.

---

## 9. Session 2026-09-30 — Phase 7: quality gates, anomaly, manifest, reparse

### 9.1 Bắt đầu từ đâu

Mở session với yêu cầu "tiếp tục ở phase 7". Không có
`docs/PHASE_7_*.md` — đã tìm trên `develop`, `master`, `phase-5-6-speed-gold`
và toàn bộ lịch sử (`git log --all --diff-filter=A`): chưa từng tồn tại.
`PHASE_INDEX.md` §4.3 nói rõ "thiếu plan thì dừng lại và hỏi", nên đã dừng và
hỏi thay vì tự suy ra.

Sau khi user duyệt: viết plan trước, commit vào `develop`, rồi mới cắt nhánh.

### 9.2 Nhánh và PR

```text
develop                            19b1568  docs: adopt the phase 7 plan
phase-7-quality-anomaly-replay     8ce42fa  10 commit, PR #4 -> develop (MERGED 2026-09-30)
```

Plan nằm ở `docs/PHASE_7_QUALITY_ANOMALY_REPLAY_IMPLEMENTATION_PLAN.md`
(1334 dòng), **commit vào `develop` trước khi viết dòng code nào** — đúng
§4.2, quy tắc thêm sau sự cố 2026-09.

PR #4: https://github.com/Duy-Duc82/ecommerce-lambda-architecture/pull/4
20 file, +3601/−43. Đã merge vào `develop` (`610485e`) ngày 2026-09-30.

### 9.3 Phase 7 làm gì

Backlog **P1-08** (mandatory quality gates) + **P1-09** (gold publish manifest).
Không lấn P1-11 Kibana, P1-12 recovery drills hay P2.

```text
Phase 6 run-scoped Gold (9 mart)
  -> price_anomaly_daily        mart Gold thứ 10
  -> 13 mandatory + 4 advisory quality check
  -> quality result lưu lại, kể cả khi run bị từ chối
  -> gold publish manifest, con trỏ current chỉ tiến khi gate xanh
  -> PostgreSQL cache refresh chịu chung một quyết định
  -> raw reparse chứng minh cùng raw ra cùng observation
```

Module mới:

| File | Vai trò |
|---|---|
| `batch_layer/marketplace_anomaly.py` | rolling median / MAD / IQR fallback |
| `config/quality_rules.py` | registry 13 MANDATORY + 4 ADVISORY |
| `batch_layer/marketplace_quality.py` | `evaluate_quality_gates()`, `decide()` |
| `batch_layer/marketplace_manifest.py` | manifest + con trỏ `current.json` |
| `crawler/reparse.py` | verify reparse, read-only tuyệt đối |
| `display/superset/create_marketplace_quality_dashboard.py` | dashboard quality/audit |

Bảng mới: `cache.marketplace_price_anomaly_daily`,
`audit.marketplace_quality_result`. `audit.marketplace_batch_run` thêm
`quality_status`, `mandatory_failure_count`, `manifest_uri`,
`manifest_promoted` và status mới `QUALITY_FAILED`.

### 9.4 Năm quyết định thiết kế — đừng đảo ngược mà không đọc lý do

**Baseline anomaly loại ngày đang xét.** Frame là
`ROWS BETWEEN n PRECEDING AND 1 PRECEDING`. Nếu gộp ngày đang xét vào baseline
của chính nó, một giá cực đoan kéo median về phía nó rồi **tự ẩn**. Test ghim:
đặt giá ngày cuối `999999`, median vẫn phải là 30.

**Order statistic chính xác, không `percentile_approx`.** Frame bị chặn bởi
`anomaly_window_days` nên `collect_list` + `array_sort` rẻ; chính xác mới cho
phép kiểm chứng lại một verdict đã lưu từ tham số đã lưu. Một helper
`_ordered_percentile()` phục vụ cả median/p25/p75 để ba định nghĩa không trôi
khỏi nhau.

**MAD = 0 **và** IQR = 0 thì không phán anomaly** (`INSUFFICIENT_DISPERSION`).
Mọi sai lệch so với lịch sử phẳng tuyệt đối đều trông vô cùng đáng kể; báo
anomaly ở đây sẽ gắn cờ **lần đổi giá đầu tiên của mọi offer ổn định**.

**Mandatory check bị SKIP = gate đỏ.** `decide()` tính cả `SKIPPED` lẫn kết quả
thiếu hẳn vào `mandatory_failures`. Audit DB rỗng làm check 8 trả `SKIPPED` và
chặn publish. Coi skip là pass tức là để một dependency chết publish được run
chưa hề được kiểm.

**Run bị từ chối ghi bằng chứng TRƯỚC khi lỗi lan ra.** Thứ tự trong
`run_marketplace_warehouse` là hợp đồng, không phải chi tiết cài đặt:
evaluate → lưu mọi quality result → ghi run manifest → mới xử lý verdict.
Trạng thái `QUALITY_FAILED` cố ý khác `FAILED`: run tạo ra Gold đầy đủ, soi
được, và bị từ chối — khác hẳn crash. Một publish bị chặn mà không để lại bằng
chứng thì không phân biệt được với crash.

Thêm: **manifest không mang wall clock.** `created_at = as_of`. Rerun cùng
context ra manifest **giống nhau từng byte** — đó là thứ làm replay kiểm chứng
được. Wall clock chỉ nằm ở `audit.marketplace_batch_run`.

### 9.5 Một bug production, do test bắt được

| Bug | Vị trí | Hệ quả |
|---|---|---|
| Advisory `counter_invalid_transition_rate` tính cả counter mà sàn không hề công bố | `batch_layer/marketplace_quality.py` | Counter vắng mặt mang `MISSING_VALUE` ở mọi transition → một sàn không có `sold_count` hiện **100% invalid vĩnh viễn** |

Giá trị thiếu không phải transition xấu — nó là transition **không tồn tại**.
Mẫu số giờ lọc `last_value IS NOT NULL`. Phát hiện vì test "clean run phải pass
mọi check" đỏ trên fixture có counter null. Sửa trong `e741954`.

### 9.6 Ba chỗ lệch khỏi plan, đều có lý do

1. **WP1 tách thành 2 commit.** Plan bảo nối mart thứ 10 vào
   `build_marketplace_marts` ngay, nhưng `write_run_scoped_gold` và `stage()`
   còn đòi đúng 9 → suite đỏ giữa WP1 và WP6. Đã kéo phần *hợp đồng dataset thứ
   10* (`DATASET_COLUMNS` + cache DDL) vào WP1; WP6 giữ phần *gating*.
2. **WP6 và WP7 gộp một commit** (`089624a`). `publish()` nhận `QualityDecision`
   bắt buộc, không caller nào cấp được cho tới khi orchestration chạy gate.
   Tách ra thì commit trung gian có orchestration không chạy được.
3. **WP8 không dùng fixture tĩnh.** Test gọi thẳng `persist_fetch_result` của
   production để sinh artifact vào một dict, nên sidecar luôn đúng hình dạng
   crawler thật ghi thay vì một file chép tay sẽ trôi khỏi nó.

Và một quyết định schema ghi ở §2.1 của plan:
`audit.marketplace_quality_result` là **bảng mới**, không mở rộng
`audit.data_quality_result` như ghi chú Phase 6 dự kiến — bảng cũ do pipeline
legacy ghi, `checked_at` là `TIMESTAMP` naive trong khi mọi cột audit
marketplace là `TIMESTAMPTZ`, và PK không có chỗ cho severity/dataset.

### 9.7 Trạng thái test

**619 passed, 0 failed, 0 skipped** (14m42s) — toàn bộ suite, không loại file
nào. Đầu session: 519. Phase 7 thêm 100 test, Spark chạy thật.

Phủ **59/61** item của plan §17.

### 9.8 Còn nợ và một lỗi đặc tả

**Nợ cần Docker** (chưa chạy được trên máy này, `dockerDesktopLinuxEngine`
không kết nối):

- Phase 6 item **33–35, 43**: chạy thật `run_marketplace_warehouse` end-to-end
  với MinIO + PostgreSQL.
- Phase 7 item **56, 58**: chứng minh publish fail giữ nguyên *cả* cache cũ
  *lẫn* con trỏ manifest cũ; rerun cùng context ra manifest giống nhau từng
  byte end-to-end. (Byte-stability của manifest đã có unit test — item 33 của
  `test_marketplace_manifest.py`; thiếu là lần chạy thật.)

Chạy một lượt là đóng được cả sáu, giờ đã có gate và manifest tại chỗ.

**Lỗi đặc tả — đã ghi vào `PHASE_INDEX.md` §5 thay vì vá lén:** cả plan Phase 6
§16 lẫn Phase 7 §18 mô tả một "offline smoke với `--skip-postgres`". Smoke đó
**không chạy được**: `run_marketplace_warehouse` gọi `read_crawl_audit()` vô
điều kiện (`marketplace_warehouse.py:187`) và hàm này đọc
`audit.crawl_request_attempt` / `audit.crawl_run` qua JDBC, vì hai mart
`source_coverage_daily` và `crawl_reliability_daily` bắt buộc cần bằng chứng
audit. `--skip-postgres` chỉ bỏ qua publish. Phase 8 sở hữu Compose profile và
one-command smoke nên đường chạy offline thuộc về phase đó.

### 9.9 Việc tiếp theo

**Trước hết: chờ review PR #4**, merge vào `develop`.

**Phase 8 (Tuần 8)** — chưa bắt đầu. Theo Brief §21 và `PHASE_INDEX.md` §1:
P1-12 + nửa Kibana của P1-11.

- Compose profile cho crawler, sink, speed và batch
- One-command start / smoke / validate — **và sửa luôn lỗi đặc tả ở §9.8**
- Failure drill (`QUALITY_FAILED` giờ là trạng thái tạo ra được có chủ đích,
  drill phải dựng được nó rồi hồi phục)
- Kibana source-health và freshness dashboard
- Backup/export — phải giữ được con trỏ manifest, vì đó là định nghĩa duy nhất
  của "bản Gold tốt cuối cùng đang phục vụ"
- Cập nhật `docs/ARCHITECTURE.md` và `docs/DATA_MODEL.md`

`crawler/reparse.py` là bước verify read-only trong runbook hồi phục của
Phase 8.

**Chưa có `docs/PHASE_8_*.md`.** Theo `PHASE_INDEX.md` §4.3: đọc file đó lấy
đúng backlog ID, thấy thiếu plan thì **dừng lại và hỏi**, không tự viết.

### 9.10 Ghi chú môi trường

Không đổi so với §8.6. `pyspark` 4.0.4, Python 3.12.10, không có venv trong
repo. Một lần chạy full suite mất ~15 phút; riêng `test_marketplace_quality.py`
~12 phút vì mỗi case dựng cả 10 mart trên Spark thật.

---

## 10. Session 2026-10-01 — đóng nợ end-to-end, hai bug production mới

### 10.1 Mục tiêu

Đóng sáu item chỉ có thể kiểm bằng chạy thật: Phase 6 item 33–35, 43 và
Phase 7 item 56, 58. Tất cả đều cần PostgreSQL thật, không phải unit test.

### 10.2 Dựng môi trường — ba trở ngại, không cái nào nằm trong code

**Xung đột cổng với project khác.** Máy đang chạy `huvisoft-postgres-local`
(giữ cả 5432 **và** 5433), `invoice-minio-local` (9000/9001),
`invoice-redis-local` (6379). Mặc định của repo là Postgres 5433 và MinIO
9000, nên chạy thẳng sẽ **ghi DDL vào database của project khác**. Đã dựng
`docker-compose.override.yml` (untracked, đã thêm vào `.gitignore`) đổi sang
5434 / 9010 / 9011 / 6380, cộng `.env` (vốn gitignore) cho phía client.

Lưu ý khi làm lại: Compose **nối thêm** vào danh sách `ports` chứ không thay
thế, nên phải dùng tag `!override`, nếu không binding 5433 cũ vẫn còn và bind
vẫn fail.

**MinIO không pull được nữa.** Tag `RELEASE.2025-09-07T16-13-09Z` không còn
trên Docker Hub (`pull access denied`) lẫn quay.io (`401`). Dùng
`minio/minio:latest` + `minio/mc:latest` đã có sẵn trên máy, kèm
`pull_policy: never`.

**Spark không chạy được trên Windows host.** Thiếu `winutils.exe` /
`hadoop.dll`, mọi truy cập filesystem ném
`UnsatisfiedLinkError: NativeIO$Windows.access0` ngay ở `read.json`. Đây chính
là lý do item 33–35, 43 chưa bao giờ đóng được. Unit test không lộ vì chúng chỉ
dùng `createDataFrame` trong bộ nhớ, chưa từng chạm Hadoop FileSystem.

Không tải winutils từ repo bên thứ ba — binary native không rõ nguồn. Thay vào
đó chạy job trong container Linux `apache/spark:4.0.1` (Python 3.10, Java 17),
mount repo vào `/app`, mount JDBC jar vào `/opt/spark/jars`, join network
`ecommerce-lambda-architecture_bigdata`.

Bẫy nữa: Git Bash **dịch đường dẫn Unix thành đường Windows** trong `docker run
-e`, biến `PYTHONPATH=/app:...` thành `C:\Program Files (x86)\Git\app;...`.
Phải đặt `MSYS_NO_PATHCONV=1` và `MSYS2_ARG_CONV_EXCL='*'`.

### 10.3 Dữ liệu chạy thật

Seed bằng chính factory của project (`create_marketplace_offer` /
`create_offer_observation` / `create_observation_event`), nên `observation_id`
và hình dạng wire là của production: 4 offer × 12 ngày = 48 observation, 12
`crawl_run` + `crawl_request_attempt` với `parsed_count` khớp từng run.

Bốn offer có hình dạng giá khác nhau có chủ đích: một phẳng tuyệt đối rồi rơi
mạnh, một trôi đều, một phẳng suốt, một **có nhiễu thật rồi rơi mạnh**.

### 10.4 Sáu item, bằng chứng từng cái

| Item | Kiểm chứng | Kết quả |
|---|---|---|
| P6-33 | 10 mart key + schema tường minh | 10 dataset Parquet, count đúng |
| P6-34 | Gold run-scoped, không đè run khác | `runs/run_id=e2e-a` … `e2e-final` cùng tồn tại, mỗi cái đủ 10 dataset |
| P6-35 | Rerun cùng run_id ra cùng dòng | `dataset_counts` giống hệt giữa hai lần `e2e-c` |
| P6-43 | `--skip-postgres` vẫn ghi Gold | 10 dataset, **0 dòng** trong `marketplace_batch_run` |
| P7-56 | Publish fail giữ cache **và** con trỏ | `cache_version=e2e-e`, 48 dòng, `pointer=e2e-e`, run ghi `FAILED` |
| P7-58 | Rerun ra manifest giống từng byte | `IDENTICAL (2492 bytes)` |

Gate chặn publish, kiểm bằng cách làm lệch `parsed_count` một crawl run:
CLI in `{"failing_checks":["silver_parse_attempt_reconciliation"],...}` và exit
1; `marketplace_batch_run` ghi `QUALITY_FAILED` với `cache_published=f`,
`manifest_promoted=f`; `marketplace_quality_result` lưu `observed_value=1` kèm
mẫu **chỉ chứa định danh** `{"keys":["run-2026-09-05"]}`. Manifest của run bị
từ chối vẫn được ghi.

Detector anomaly bắn đúng trên offer có nhiễu: 50000 → 12000, median 50000,
MAD 1000, score −25.63 → `ANOMALOUS_LOW` / `MAD_SCORE_EXCEEDED`.

Offer phẳng tuyệt đối rơi 100000 → 20000 ra `INSUFFICIENT_DISPERSION`, **không**
bị gắn cờ — đúng quy tắc §6.3 nhánh 4. Đây là đánh đổi có chủ đích, nhưng hệ
quả thực tế đáng nhớ: **một offer giá bất động tuyệt đối sẽ không bao giờ sinh
anomaly, dù cú đổi giá đầu tiên lớn đến đâu.** Nếu muốn bắt ca đó thì cần một
nhánh dự phòng theo biến thiên tương đối, và đó là thay đổi quy tắc, không phải
sửa lỗi.

### 10.5 Hai bug production mới, đều do chạy thật phát hiện

| Bug | Vị trí | Hệ quả |
|---|---|---|
| `_with_marketplace()` chọn cột `marketplace_code` từ `audit.crawl_run` — bảng đó chỉ có `marketplace_id` | `batch_layer/marketplace_marts.py` | `UNRESOLVED_COLUMN`; **2/9 mart Phase 6** (`source_coverage_daily`, `crawl_reliability_daily`) chưa từng dựng được trên schema thật |
| Con trỏ manifest được promote **trước** khi publish cache | `batch_layer/marketplace_warehouse.py` | Publish fail → cache rollback đúng nhưng con trỏ đã nhảy sang run hỏng; bản Gold "đang phục vụ" là bản chưa từng publish |

Bug 1: fixture audit của Phase 6 khai cột theo cái code cần chứ không theo
`scripts/init_postgres.sql`, nên cả 43 test đều đi nhánh khác và mù hoàn toàn.
Đường đúng để lấy marketplace code là `attempts.task_id → crawl_frontier.
marketplace_code`; `crawl_run.marketplace_id` **không** dùng thay được vì nó là
`marketplace-tiki` còn observation là `tiki`, join sẽ rỗng và `parsed_count`
thành 0 một cách âm thầm.

Bug 2 bắt nguồn từ **plan tự mâu thuẫn**: §13 đòi publish fail phải giữ nguyên
cả cache lẫn con trỏ, §14 bước 11 lại bảo promote trước rồi mới publish. Code
làm theo §14. Đã sửa cả code lẫn §14.

Cả hai sửa theo đúng quy tắc repo: commit test trước (đỏ, tái hiện lỗi), commit
fix sau — `77c2650`/`d5eb226` và `57ed95e`/`a502586`.

### 10.6 Ba hạn chế môi trường, chưa sửa, thuộc Phase 8

1. **Spark không làm gì được với file trên Windows host** (thiếu winutils).
   Mọi lần chạy thật phải trong container Linux.
2. **`build_spark()` của marketplace job không gọi `spark_hadoop_options()`** —
   chỉ `warehouse_job.py` legacy gọi. Nên job này **không có credential S3A** và
   không đọc/ghi `s3a://` được dù MinIO đã chạy. Lần chạy này dùng lake file://.
   Nghĩa là đường s3a của marketplace batch vẫn **chưa được kiểm chứng lần nào**.
3. **`docker/spark-warehouse/Dockerfile` pin `apache/spark:3.5.1`**, mâu thuẫn
   với ràng buộc pyspark 4.x ở §6, và chỉ có entrypoint cho `warehouse_job.py`
   legacy — không chạy được marketplace job.

Hệ quả cho Phase 8: Compose profile và one-command smoke phải dựng đường chạy
Linux cho marketplace batch, và nếu muốn dùng MinIO thì phải nối
`spark_hadoop_options()` vào `build_spark()` trước.

Thêm một điểm đáng cân nhắc: sau bản sửa bug 2, bất biến "con trỏ không bao giờ
đi trước cache" đúng cho run thường, nhưng `--skip-postgres` vẫn đẩy được con
trỏ qua mặt cache. Đó là hành vi đã ghi trong plan §14 và là cờ do người vận
hành chủ động bật, nên giữ nguyên — nhưng runbook Phase 8 nên nói rõ.

### 10.7 Cách dựng lại môi trường này

```bash
# 1. Bật Docker Desktop, rồi:
docker compose up -d postgres-dw minio minio-init   # cần docker-compose.override.yml

# 2. Container chạy job (MSYS_NO_PATHCONV bắt buộc trên Git Bash)
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' docker run -d --name mp-e2e --user root \
  --network ecommerce-lambda-architecture_bigdata \
  -v "D:/code/data/ecommerce-lambda-architecture:/app" \
  -v "D:/code/data/ecommerce-lambda-architecture/.localjars/postgresql-42.7.5.jar:/opt/spark/jars/postgresql-42.7.5.jar:ro" \
  -e POSTGRES_HOST=postgres-dw -e POSTGRES_PORT=5432 \
  -e DATA_LAKE_PROFILE=local -e DATA_LAKE_LOCAL_ROOT=/app/data/lakehouse \
  -e PYTHONPATH="/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip" \
  -e PYSPARK_PYTHON=python3 -e PYSPARK_DRIVER_PYTHON=python3 \
  --entrypoint bash apache/spark:4.0.1 -lc "sleep infinity"
docker exec mp-e2e python3 -m pip install psycopg2-binary==2.9.10 python-dotenv==1.0.1

# 3. Chạy
MSYS_NO_PATHCONV=1 docker exec mp-e2e bash -c \
  'cd /app && python3 -m batch_layer.marketplace_warehouse --run-id <id> --as-of <iso>'
```

JDBC driver tải từ Maven Central vào `.localjars/` (untracked). Trên Windows
host, **không** dùng `PYSPARK_SUBMIT_ARGS` để nạp jar — nó làm chết Java
gateway; đặt jar thẳng vào `site-packages/pyspark/jars/` nếu cần chạy test
dùng JDBC ngoài container.

### 10.8 Review PR #4 — tám lỗi nữa, đều sửa trên cùng nhánh

Review toàn bộ diff của PR #4 tìm ra tám lỗi mà 624 test không bắt được. Mỗi
lỗi có một commit test tái hiện và một commit fix riêng.

| # | Lỗi | Sửa |
|---|---|---|
| 1 | Chạy lại một khung ngày cũ mà không có `--allow-backfill` thì publish cache trước, rồi con trỏ mới từ chối → cache phục vụ dữ liệu cũ hơn con trỏ, run vẫn `SUCCEEDED` | `promotion_refusal()` được hỏi **trước** khi stage; run bị giữ lại là `GOLD_WRITTEN`, reason `BACKFILL_REFUSED` |
| 2 | CLI reparse gọi `TikiCrawler()` không có `categories` → lỗi ở mọi lần chạy; không bao giờ đọc Silver nên luôn báo `IDENTICAL` | `fetch_robots=False` (fail closed); `silver_observation_path()` dùng chung với sink; đọc Silver theo key; thêm `--silver-dataset`, `--report-path` |
| 3 | Khóa chính `cache.marketplace_price_anomaly_daily` thiếu `currency` → đổi tiền tệ trong cùng ngày thì gate xanh nhưng publish vi phạm unique | Khóa gồm `currency`; migration `DROP CONSTRAINT IF EXISTS` / `ADD CONSTRAINT` idempotent |
| 4 | `previous_run_id` trỏ về chính run khi con trỏ đã là run đó → mất chuỗi manifest | Giữ predecessor mà con trỏ đã ghi |
| 5 | Session `partitionOverwriteMode=dynamic` → resume để lại partition Gold cũ | Writer đặt `partitionOverwriteMode=static` |
| 6 | Publish ghi `manifest_promoted=TRUE` trước khi con trỏ di chuyển | `mark_promotion()` ghi sau, theo kết quả promote |
| 7 | `--quality-only` để lại dòng audit `completed_at=NULL` | `mark_held(reason)` dùng chung với backfill |
| 8 | `--gold-root-uri` khác mặc định vẫn ghi manifest và promote con trỏ production | Root khác root phục vụ là scratch run: không manifest, không publish, không promote, reason `SCRATCH_GOLD_ROOT` |

Kiểm chứng trên PostgreSQL và Spark 4.0.1 thật (container `mp-e2e`):

```text
e2e-v1 | SUCCEEDED      | cache=t | promoted=t |                          ← run mới
e2e-v2 | QUALITY_FAILED | cache=f | promoted=f |                          ← gate chặn
e2e-v3 | GOLD_WRITTEN   | cache=f | promoted=f | held: QUALITY_ONLY
e2e-v4 | GOLD_WRITTEN   | cache=f | promoted=f | held: SCRATCH_GOLD_ROOT  ← không manifest nào ở production
e2e-v5 | GOLD_WRITTEN   | cache=f | promoted=f | held: BACKFILL_REFUSED   ← cache và con trỏ giữ e2e-v1
```

Migration khóa anomaly chạy hai lần trên database thật, kết thúc ở khóa mới.
Spark 4.0.1 xác nhận: ở chế độ dynamic, partition cũ còn lại; với `static`
trên writer, partition cũ bị xóa.

Suite: **655 passed, 0 failed, 0 skipped**.

Nợ còn lại, thuộc Phase 8: chưa có khóa giữa các batch run chạy đồng thời. Con
trỏ được đọc trước publish, nên một run khác có thể promote chen vào giữa.
Cần lock hoặc compare-and-swap con trỏ.

---

## 11. Session 2026-10-01 (chiều) — run đọc dữ liệu *tại* `as_of`

### 11.1 Vấn đề, phát hiện khi review lại toàn bộ Phase 7 sau merge

Plan §7.1 nói gate 8 "bỏ qua crawl run nằm ngoài **read window**", nhưng không
có code nào dựng read window cả. `run_marketplace_warehouse` đọc **toàn bộ**
Silver và toàn bộ audit crawl. Hệ quả:

1. **Backfill không bao giờ qua gate khi Silver vẫn đang lớn lên.** Gate 6
   (`observed_at <= as_of + 300s`, MANDATORY) thấy mọi dòng mới hơn `as_of`.
   Lần chạy `e2e-v5` ở §10.8 qua được chỉ vì dữ liệu seed cố định.
2. **Replay không phải ảnh chụp tại một thời điểm.** Manifest giống từng byte
   (P7-58) chỉ đúng khi Silver đứng yên giữa hai lần chạy.
3. **Một crawl run lệch số đếm chặn publish mãi mãi.** Gate 8 đối soát mọi
   crawl run có trong Silver, mà Silver thì luôn được đọc hết, nên chỉ cần một
   observation rơi vào DLQ hoặc sink còn đang trễ là mọi batch về sau đều đỏ.

### 11.2 Sửa

| Thay đổi | Ở đâu |
|---|---|
| `observations_as_of()`: giữ `observed_at <= as_of` (bao gồm biên), cắt **trước** dedup | `batch_layer/marketplace_warehouse.py` |
| `crawl_audit_as_of()`: attempt có `completed_at <= as_of`, crawl run có `started_at <= as_of` | như trên |
| Gate 6 so `observed_at` với `least(fetched_at, produced_at) + tolerance` của chính dòng đó | `batch_layer/marketplace_quality.py` |
| Gate 8 chỉ đối soát run đã **ổn định** (`completed_at <= as_of − settle`) và **còn trong lookback** (`>= as_of − lookback`); run đang `RUNNING` thì chờ; dòng Silver không có run audit thì xếp theo `observed_at` cuối | như trên |
| `MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS=900`, `..._LOOKBACK_SECONDS=172800` | `config/settings.py`, context |
| `quality-rules.v1` → **`quality-rules.v2`**, vì gate 6 và 8 đã đổi nghĩa | `config/settings.py` |

Plan Phase 7 đã sửa theo: §5, §7.1, §13, §14 bước 2–3, §17 item 21 và 24.

**Vì sao gate 6 phải đổi.** Khi đã cắt ở `as_of` thì không dòng nào còn vượt
`as_of`, và so với `as_of` không bao giờ bắn được nữa. Điều còn đáng bắt là
một observation tự nhận thời điểm muộn hơn chính response sinh ra nó hoặc
message chở nó: đó là lỗi đồng hồ hoặc lỗi parse.

**Đánh đổi của lookback, ghi rõ để khỏi đảo ngược nhầm:**

- Lookback phải lớn hơn khoảng cách giữa hai batch cộng settle. Nếu không, một
  crawl run có thể ổn định rồi trôi ra khỏi cửa sổ giữa hai batch mà chưa
  được đối soát lần nào.
- Lookback cũng là thời gian tối đa một chỗ lệch chặn publish. Sau đó run lệch
  không còn bị phán lại, nhưng kết quả FAIL của các batch trước vẫn nằm trong
  `audit.marketplace_quality_result`.
- Gate 8 vẫn **không** phát hiện crawl run vắng mặt hoàn toàn khỏi Silver
  (vẫn là left join, theo item 24). Muốn bắt ca đó thì phải đổi item 24.

### 11.3 Kiểm chứng trên PostgreSQL + Spark 4.0.1 thật (container `mp-e2e`)

Silver seed có 48 observation, từ 01/9 đến 12/9. Backfill tại `as_of=2026-09-08`:

```text
e2e-w1-old | code develop cũ, root scratch | QUALITY_FAILED  observed_at_within_future_tolerance = 20 dòng
e2e-w2-new | code mới, root scratch        | GOLD_WRITTEN    PASS, silver_rows=28 (4 offer × 7 ngày)
e2e-w3     | --quality-only, rồi thêm 1 dòng Silver ngày 20/9, rồi --resume → manifest IDENTICAL (2501 bytes)
e2e-w3     | --resume --allow-backfill     | SUCCEEDED       cache=t, promoted=t
e2e-w4     | as_of 2026-09-14              | SUCCEEDED       silver_rows=48 (dòng 20/9 bị cắt), pointer=e2e-w4, previous=e2e-w3
```

Dòng Silver giả ngày 20/9 đã xóa sau khi kiểm.

Suite: **674 passed, 0 failed, 0 skipped**.


---

## 12. Session 2026-10-01 (tối) — Phase 8 WP1: crawl service

### 12.1 Đã làm

Nhánh `phase-8-wp1-crawl-service`, cắt từ `develop` sau khi PR #5 và #6 merge.

| Thay đổi | Ở đâu |
|---|---|
| `python -m crawler.service`: vòng lặp quanh `CrawlWorker.run_once`, chờ trên stop signal, SIGTERM xong cả cycle rồi mới dừng | `crawler/service.py` |
| `publishing_executor`: publish từng observation theo thứ tự, đợi ack; lỗi ở vị trí `k` → `ObservationPublishError(acknowledged=k)` | như trên |
| `PUBLISH_ERROR`: retryable, không mở circuit nguồn; attempt ghi `parsed_count = số đã ack` | `crawler/scheduling.py`, `crawler/worker.py`, `crawler/contracts.py` |
| Migration hai CHECK constraint error-kind, idempotent | `scripts/init_postgres.sql` |
| `python -m crawler.seed_frontier`: lần chạy đầu của mỗi target neo ở 1970-01-01, nên seed lại là no-op | `crawler/seed_frontier.py` |
| `TIKI_LISTING_URL`; robots.txt đọc từ host thật sự được fetch | `config/settings.py`, `crawler/base.py`, `crawler/sites/tiki.py` |
| Connection factory đóng connection sau mỗi lần dùng | `crawler/service.py` |

Plan Phase 8 sửa theo ba điều WP1 phát hiện (commit `a60b98c`):

- bỏ `TIKI_ROBOTS_URL`;
- dừng sau khi xong cả cycle, không dừng giữa batch đã lease;
- neo task khi seed.

### 12.2 Ba bug production, đều chỉ lộ ra khi chạy thật lần đầu

| Bug | Vị trí | Hệ quả |
|---|---|---|
| `create_marketplace_producer()` **và** `create_change_producer()` truyền `enable_idempotence=True`, mà `kafka-python-ng 2.2.3` không có tham số đó | `data_ingestion/marketplace_producer.py`, `marketplace_change_producer.py` | Chưa factory nào từng tạo được producer: crawler không publish được, mọi micro-batch speed marketplace sẽ chết ở change producer |
| `lease_due()` `RETURNING task_id, …` không qualify, trong khi UPDATE join CTE `due` cũng có `task_id` | `crawler/frontier.py` | `AmbiguousColumn`: worker Phase 3 **chưa từng lease được task nào** trên Postgres thật |
| `with connection_factory() as conn` trên connection psycopg2 trần chỉ commit, không đóng | cách dùng ở `frontier.py`, `audit.py` | Service chạy lâu sẽ rò một connection mỗi lần gọi. Sửa bằng factory đóng connection (`postgres_connection_factory`); bản thân repository không đổi |

Hai bug đầu lọt qua vì mọi test producer và frontier chỉ dùng fake: fake producer nhận mọi tham số, fake cursor nhận mọi chuỗi SQL. Test mới:

- kiểm option của producer bằng chính bảng `DEFAULT_CONFIG` của client, không cần broker;
- kiểm cột trong `RETURNING` đã được qualify.

Hai bug này theo đúng quy tắc repo, mỗi bug một cặp commit test rồi fix: `5a81baf`/`680fc97` và `e8a1f1a`/`13509b0`. Bug connection lộ ra khi viết entrypoint, nên factory và test của nó nằm luôn trong commit feature `a593cd7`.

Bỏ `enable_idempotence` cũng buộc giảm `max_in_flight_requests_per_connection` xuống 1. Producer không idempotent mà để 5 request in-flight thì retry có thể đảo thứ tự message của một offer. Delivery giờ được ghi rõ là **at-least-once**, đúng với điều §2 đã chốt từ trước. Bản sao bị trùng được ID tất định hấp thụ ở downstream.

### 12.3 Kiểm chứng trên Postgres + Kafka thật

Dùng `postgres-dw` và `kafka` của Compose. Nguồn dữ liệu là một HTTP server tạm phục vụ fixture Tiki đã đóng băng; không gọi Tiki thật.

```text
init_postgres.sql chạy 2 lần             exit 0 cả hai; cả hai constraint chứa PUBLISH_ERROR
seed 1846 × 2 trang, seed lại             2 task mới, rồi 0
crawl.service --max-cycles 1              leased=2 succeeded=2; 2 attempt PARTIAL parsed=2; Kafka +4; Bronze body+metadata
Kafka dừng sau khi producer đã kết nối    attempt FAILED, http 200, PUBLISH_ERROR, parsed=0, raw_uri có; task RETRY_WAIT;
                                          crawl_source_state.consecutive_failures = 0
Kafka bật lại, 1 cycle                    attempt 2 PARTIAL parsed=2; task SUCCEEDED, lần kế READY
Σ parsed_count mọi attempt = 6 = tổng offset Kafka
```

Dòng cuối là bất biến mà gate 8 của Phase 7 dựa vào: số observation được ghi là đã parse luôn bằng số observation thật sự vào Kafka.

`kafka-python-ng` đã được cài vào Python của host. Gói này có trong `requirements.txt` nhưng trước đó chưa cài.

### 12.4 Trạng thái test

**711 passed, 0 failed, 0 skipped** trên HEAD của WP1 (trước WP1: 674). Số này
gồm hai lượt cộng lại: `test_marketplace_quality.py` chạy riêng (42 passed,
19m36s) và phần còn lại của suite (669 passed, 4m04s). Lượt full suite liền
một mạch trước đó bị treo trong file quality, sau khoảng 12 test Spark; chạy
riêng thì không treo lại. Nghi do môi trường Spark trên Windows host chứ
không do code, nhưng **chưa xác minh được nguyên nhân**. Nếu lặp lại, chạy
file quality riêng.

---

## 13. Session 2026-10-02 — Phase 8 WP2: Silver sink và speed services

### 13.1 Đã làm

Nhánh `phase-8-wp2-sink-speed-services`, cắt từ `develop` sau khi PR #7 merge.

| Thay đổi | Ở đâu |
|---|---|
| `python -m data_ingestion.marketplace_silver_service`: commit thủ công từng offset, **chỉ sau khi** record đã xử lý xong; record lỗi được retry tại đúng offset đó với backoff gấp đôi có trần; khi bỏ một batch thì tua lại mọi partition mà `poll()` đã đi qua | `data_ingestion/marketplace_silver_service.py` |
| `python -m speed_layer.marketplace_speed_service`: một query Structured Streaming có checkpoint; SIGTERM → `query.stop()`; bản ghi invalid được union vào output để audit đếm được | `speed_layer/marketplace_speed_service.py` |
| `MARKETPLACE_SPEED_CHECKPOINT_ROOT`; mỗi micro-batch dùng connection factory có đóng | `speed_layer/marketplace_speed_layer.py` |
| Connection factory và stop signal chuyển sang `common/` để các service dùng chung | `common/postgres.py`, `common/lifecycle.py` |

Lệch plan: trigger dùng `MARKETPLACE_STREAM_TRIGGER` đã có sẵn, không thêm `MARKETPLACE_SPEED_TRIGGER_SECONDS`.

### 13.2 Bảy bug production, mỗi bug một cặp test → fix

Ba bug đầu tìm thấy khi viết service; bốn bug sau chỉ lộ ra khi speed query
chạy thật lần đầu (§13.4).

| Bug | Vị trí | Hệ quả |
|---|---|---|
| Lỗi ghi Silver của một observation **hợp lệ** nằm chung `except` với lỗi decode/validate | `data_ingestion/marketplace_silver_sink.py` | MinIO sập → observation tốt bị đẩy vào DLQ như `CONTRACT_VALIDATION`, không bao giờ vào Silver (plan §6.2) |
| `F.to_json("decoded")` bỏ các field null; `_process_group` parse lại bằng wire contract nghiêm ngặt, đòi đủ mọi key | `speed_layer/marketplace_speed_layer.py` | Mọi observation có `brand`/`seller_id` null (thường gặp với Tiki) làm micro-batch chết |
| `SPARK_KAFKA_PACKAGE` vẫn là `_2.12:3.5.1` | `config/settings.py` | Không nạp được trên Spark 4 (Scala 2.13): speed query chết ngay khi khởi động |
| Hàm stateful coi tham số là **một** pandas DataFrame và trả về một DataFrame; `applyInPandasWithState` truyền vào và đòi lại một **iterator** | `speed_layer/marketplace_speed_layer.py` | Mọi micro-batch chết: `'generator' object has no attribute 'to_dict'`. Cùng chỗ: timeout chỉ được đặt khi có STATE mới, nên offer mà lần gọi cuối chỉ có bản trùng/đến muộn sẽ không bao giờ thành `OFFER_STALE` |
| `NEW_OFFER` đặt cả offer vào `current_value`, còn change khác đặt scalar; một field ES không thể vừa là object vừa là scalar | `speed_layer/marketplace_sinks.py` | `NEW_OFFER` đầu tiên khóa mapping thành object → lần đổi giá đầu tiên bị từ chối, query chết |
| Audit speed khóa theo `(query_name, batch_id)` và bỏ qua batch `SUCCEEDED`; checkpoint mới đánh số batch lại từ 0 | `speed_layer/marketplace_sinks.py`, `scripts/init_postgres.sql` | Replay với checkpoint mới **mất sạch dữ liệu**: batch 0 (16 observation) bị bỏ qua, offset vẫn được commit. Thủ tục dựng lại ES ở plan §11.4 không chạy được |
| `spark.sql.shuffle.partitions` để mặc định 200 | `speed_layer/marketplace_speed_service.py` | 200+ task cho vài chục record mỗi micro-batch; con số này bị khóa vào checkpoint ngay lần chạy đầu |

Bản sửa audit dùng `query_id`, lấy từ local property `sql.streaming.queryId`
mà Spark gắn vào mỗi micro-batch. Đã kiểm trên Spark 4.0.1 rằng giá trị này
giữ nguyên qua restart với cùng checkpoint và đổi khi dùng checkpoint khác.
Bảng audit có thêm cột `query_id` trong khóa chính, qua migration idempotent;
các dòng cũ mang giá trị `''`.

Bản sửa projection ES lưu giá trị dạng object thành chuỗi JSON chuẩn tắc.
Event Kafka giữ nguyên hình dạng.

Bug thứ hai lọt qua vì test decode của Phase 5 chỉ kiểm cột, chưa bao giờ đưa dòng đã decode vào xử lý tiếp.

### 13.3 Kiểm chứng Silver sink trên Kafka + MinIO thật

```text
6 message WP1 để lại, lake local          6 SILVER, 6 file, offset 6, lag 0
8 message mới, MinIO bị DỪNG              lỗi HTTPConnectionPool, không commit, DLQ = 0
MinIO bật lại                             8 SILVER, 8 object trong ecommerce-silver, offset 14, lag 0, DLQ = 0
```

**Một lần chạy đầu bị loại.** Host lúc đó chưa cài gói `minio`, nên mọi lần
"thất bại" thực ra là `No module named 'minio'` chứ không phải MinIO sập. Lần
đó chỉ chứng minh lỗi ghi được retry và không vào DLQ. Đã cài `minio>=7.2.15`
(có trong `requirements.txt`) rồi làm lại; bảng trên là lần làm lại.

Client `minio` tự retry nội bộ trước khi trả lỗi. Vì vậy trong 40 giây MinIO
sập chỉ thấy một lần thất bại, và backoff của service cộng thêm vào đó.

### 13.4 Speed service — kiểm chứng trên Kafka + ES + Redis + Postgres thật

Môi trường: chạy trong container `apache/spark:4.0.1` (`mp-e2e`), Kafka
`kafka:19092`, Redis, Postgres. Elasticsearch là một container tạm
`es-verify` dùng bản **8.18.1** (lý do ở dưới); client `elasticsearch==8.18.0`.

```text
lần 1                      chết: applyInPandasWithState ('generator'...)       → bug 4
lần 2, 14 observation      batch 0 SUCCEEDED: 14 in, 2 NEW_OFFER, ES/Redis 2 offer
kill -9, chạy lại          tiếp từ batch 2, input 0, change topic không tăng   → checkpoint đúng
giá giảm 199000 → 99500    batch chết: ES từ chối current_value scalar           → bug 5
replay, checkpoint mới     batch 0 bị audit bỏ qua, ES không được dựng lại       → bug 6
sau cả ba bản sửa          batch 0 SUCCEEDED: 16 in, 4 change
                           NEW_OFFER ×2, PRICE_CHANGED 199000→99500, LARGE_PRICE_DROP
                           ES: 4 change, 2 offer; Redis: 2 offer, 4 change gần đây
SIGTERM                    dừng trong ≤ 24 giây; batch đang chạy bị hủy, không lỗi
```

Change topic có 8 message cho 4 change khác nhau: 2 từ lần chạy đầu, 2 từ
batch chết ở bug 5 (sink publish Kafka **trước** khi ghi ES), và 4 từ lần
replay. Đây là at-least-once, đúng với cam kết; `event_id` tất định nên
downstream gộp được, và ES đã gộp.

**Sự cố môi trường trong lúc kiểm.** Ổ `C:` hết sạch dung lượng (0 GB), mà
`docker_data.vhdx` nằm trên đó, nên Docker engine sập khi đang pull
Elasticsearch. Đã dọn theo yêu cầu của chủ dự án:

- cache npm, pip, Gradle;
- `node_modules` của các project khác (1,3 GB trên `C:`, 4,5 GB trên `D:`);
- image Docker không còn container nào dùng, cùng build cache 7,7 GB;
- nén vhdx từ 19 GB xuống 15,1 GB.

`C:` còn trống khoảng 10 GB. Volume dữ liệu của project khác không bị đụng
tới.

Image `elasticsearch:8.18.0` bị pull **đúng lúc ổ đầy**. Bản giải nén của nó
có `/bin/tini`, `docker-entrypoint.sh`, `cacerts`, ... dài 0 byte, và vẫn hỏng
như vậy sau `rmi` + pull lại + restart engine. Containerd dùng lại snapshot
cũ dù đã tải blob mới (`Downloaded newer image`). Bản 8.18.1 giải nén bình
thường. Compose vẫn pin 8.18.0; việc pin version thuộc WP4, và trên máy này
phải đổi tag hoặc dọn snapshot thì mới chạy được 8.18.0.

### 13.5 Trạng thái test

**749 passed, 0 failed, 0 skipped** (trước WP2: 711). Chạy thành hai lượt:
phần còn lại của suite (707 passed, 3m51s) và `test_marketplace_quality.py`
riêng (42 passed, 18m42s).

### 13.6 Việc tiếp theo (resume ở đây)

1. **Review và merge PR #8** (WP2) vào `develop`.
2. **WP3** (plan Phase 8 §6.4–6.6, test 14–21):
   - batch scheduler dạng loop, với `as_of` và `run_id` tất định;
   - advisory lock để không chạy chồng; compare-and-swap con trỏ manifest
     (`PROMOTION_CONFLICT`);
   - `build_spark()` gọi `spark_hadoop_options()` để chạy được `s3a://`;
   - `validate_settings()` từ chối lookback ≤ interval + settle.
   Nhánh mới cắt từ `develop` sau khi PR #8 merge.
3. **WP4** (image + Compose) phải xử lý image `elasticsearch:8.18.0` hỏng
   trên máy này (§13.4): đổi pin sang một bản 8.18.x giải nén được, kèm
   Kibana cùng minor, hoặc dọn snapshot containerd. Plan §5.3 đòi pin version
   có lý do; ghi lý do vào Dockerfile/Compose.

**Môi trường đang để lại:**

- container `postgres-dw`, `minio`, `kafka`, `redis`, `mp-e2e` đang chạy;
  `es-verify` đã xoá. Kafka giữ 16 observation, 8 change, DLQ rỗng.
- Host Python đã cài thêm `kafka-python-ng` và `minio`; `mp-e2e` đã cài
  pandas, pyarrow, redis, elasticsearch, kafka-python-ng, minio. Cả hai nhóm
  đều nằm trong `requirements.txt`.
- Trước khi pull image lớn hay chạy suite dài, kiểm dung lượng trống `C:`
  (`df -h /c`).

---

## 14. Session 2026-10-02 (chiều) — merge WP2, Phase 8 WP3: batch scheduler

### 14.1 PR #8 trước khi merge

Review PR #8 tìm thấy một bug thật: mỗi micro-batch speed mở mới Kafka
producer, client ES và client Redis, nhưng chỉ đóng khi batch lỗi. Với trigger
30 giây, mỗi ngày rò khoảng 2.880 producer, mỗi cái kèm một network thread.
Bug có từ Phase 5, nhưng chỉ thành bug thật khi WP2 biến speed layer thành
service chạy lâu. Test `2e04195`, fix `19d1b82`, rồi merge (`4d3b4f2`).

Còn nợ, chưa sửa: nếu bước publish vào DLQ lỗi mãi với một record (ví dụ
record gần giới hạn 1 MB của Kafka), Silver sink đứng ở **mọi** partition.
Để lại cho drill WP6–WP7.

### 14.2 Đã làm

Nhánh `phase-8-wp3-batch-scheduler`, cắt từ `develop` sau khi PR #8 merge.

| Thay đổi | Ở đâu |
|---|---|
| `python -m batch_layer.marketplace_scheduler [--max-ticks N]`: `as_of = floor(now − lag, interval)` theo epoch UTC, `run_id = mp-YYYYMMDDTHHMMZ`; `SUCCEEDED` → skip, không có → chạy, status khác → `resume=True`; không bao giờ truyền `allow_backfill`; lỗi của một cửa sổ được log, vòng lặp chạy tiếp | `batch_layer/marketplace_scheduler.py` |
| `exclusive_batch`: `pg_try_advisory_lock` trên connection riêng, commit ngay để không idle-in-transaction suốt run, unlock trong `finally` | `batch_layer/marketplace_lock.py` |
| `run_marketplace_warehouse(context, *, lock=None, **options)` lấy lock **trước** khi dựng Spark hay ghi audit; CLI in `ALREADY_RUNNING`, exit 75 | `batch_layer/marketplace_warehouse.py` |
| `promote_manifest(..., expected_current_run_id)` bắt buộc; con trỏ đã đổi → `PROMOTION_CONFLICT`, không ghi gì, kể cả run manifest | `batch_layer/marketplace_manifest.py` |
| `build_spark()` áp `spark_hadoop_options()` qua `spark.hadoop.*` | `batch_layer/marketplace_warehouse.py` |
| `MARKETPLACE_BATCH_{INTERVAL,AS_OF_LAG}_SECONDS`, `MARKETPLACE_BATCH_LOCK_KEY`; từ chối lookback ≤ interval + settle | `config/settings.py` |

Plan sửa hai chỗ trước khi viết code (commit `6918f97`):

- **§6.4:** scheduler thức tại boundary **+ lag**. Thức đúng boundary thì
  `now − lag` vẫn rơi vào cửa sổ cũ, nên mỗi cửa sổ trễ cả một interval.
- **§6.6:** `apache/spark:4.0.1` không có `hadoop-aws`. Chỉ có option thì
  `s3a://` vẫn không chạy được.

Lệch plan: lock là tham số `lock` (callable trả context manager) để test
offline inject được. Scheduler tự query `audit.marketplace_batch_run`, vì
`marketplace_postgres.py` không nằm trong danh sách file được sửa (§4).

### 14.3 Kiểm chứng trên Postgres + MinIO thật (container `mp-e2e`)

`s3a://` cần `hadoop-aws-3.4.1.jar` và `bundle-2.24.6.jar` (AWS SDK v2, bản
mà Hadoop 3.4.1 khai báo). Cả hai đã tải vào `.localjars/` (gitignore, nằm
trên `D:`), nạp qua `PYSPARK_SUBMIT_ARGS=--jars ...`.

```text
giữ lock 820801 từ process khác      CLI: ALREADY_RUNNING, exit 75; tick: ALREADY_RUNNING
                                     → 0 audit row cho cả hai run
kill -9 process giữ lock             pg_locks advisory = 0: Postgres tự nhả
tick, interval 1 ngày, profile minio mp-20261002T0000Z SUCCEEDED, silver_rows 0, 13 gold rows,
                                     Gold ghi qua s3a://ecommerce-gold, con trỏ PROMOTED
tick, interval 1 giờ                 mp-20261002T0700Z SUCCEEDED, silver_rows 8 đọc qua s3a,
                                     con trỏ previous_run_id = mp-20261002T0000Z
đặt status = FAILED, tick lại        resumed=true, SUCCEEDED, ALREADY_CURRENT
tick lại lần nữa                     SKIPPED_SUCCEEDED
SIGTERM khi đang chờ                 dừng ngay, exit 0
```

`silver_rows 0` ở tick đầu là đúng: cả 8 observation trong MinIO được tạo
sau 00:00Z, nên bị cắt bởi `as_of`. Tick thứ hai mới chứng minh đọc dòng
thật qua `s3a://`.

Hành vi cần biết: một cửa sổ `FAILED` (crash) chỉ được chạy lại khi scheduler
restart trong cùng interval. Tick kế tiếp đã sang cửa sổ mới. Muốn chạy lại
cửa sổ cũ thì dùng CLI với `--resume` (và `--allow-backfill` nếu nó cũ hơn
con trỏ).

### 14.3b Review PR #9 — hai bug, mỗi bug một cặp test → fix

| Bug | Hệ quả | Sửa |
|---|---|---|
| `tick()` tra status run **ngoài** `try` | Postgres restart đúng lúc tick chạy làm process scheduler chết; mọi cửa sổ sau đó chờ restart tay | tra status trong `try`, lỗi thành outcome `FAILED` |
| CAS con trỏ chỉ chạy lúc promote, **sau** khi cache đã publish | Lock nằm trên connection idle suốt run Spark, có thể rớt. Nếu run khác promote trong lúc đó thì cache phục vụ run này, con trỏ phục vụ run kia, audit ghi `SUCCEEDED` và scheduler skip cửa sổ mãi | đọc lại con trỏ ngay trước khi publish; lệch → `GOLD_WRITTEN` + `PROMOTION_CONFLICT`, resume được. CAS lúc promote vẫn giữ, chỉ còn vài mili giây publish không được che |

### 14.4 Trạng thái test

**793 passed, 0 failed, 0 skipped** (trước WP3: 749). Chạy thành hai lượt:
phần còn lại của suite (751 passed, 3m50s) và `test_marketplace_quality.py`
riêng (42 passed, 20m01s, chạy trước hai bản sửa review; file đó không gọi
code nào hai bản sửa đụng tới).

### 14.5 Việc tiếp theo (resume ở đây)

1. Review và merge PR WP3.
2. **WP4** (image + Compose): `docker/spark-marketplace/Dockerfile` phải nhúng
   `hadoop-aws-3.4.1` + `bundle-2.24.6` và JDBC Postgres; xử lý image
   `elasticsearch:8.18.0` hỏng trên máy này (§13.4).
3. Ổ `C:` còn khoảng 6,5 GB. Đồ tải lớn đặt trên `D:` (chủ dự án đã đồng ý).

---

## 15. Session 2026-10-02 (tối) — merge WP3, Phase 8 WP4: image và Compose profiles

### 15.1 PR #9 trước khi merge

Review tìm ra hai bug; cả hai đã sửa trước khi merge (`7748f22`). Chi tiết ở
§14.3b.

### 15.2 Đã làm

Nhánh `phase-8-wp4-images-compose`, cắt từ `develop` sau khi PR #9 merge.

| Thay đổi | Ở đâu |
|---|---|
| Image `ecommerce/marketplace-python:1` (`python:3.12.11-slim`, 235 MB) cho crawl-worker và silver-sink | `docker/marketplace-python/Dockerfile` |
| Image `ecommerce/spark-marketplace:4.0.1`, chạy dưới user `spark`. Jar có pin version và ghi nguồn: Kafka connector 4.0.1, kafka-clients 3.9.1, commons-pool2 2.12.0, hadoop-aws 3.4.1, AWS SDK bundle 2.24.6, postgresql 42.7.5 | `docker/spark-marketplace/Dockerfile` |
| Pin chính xác các gói đã chạy thật | `requirements-marketplace.txt` |
| `docker compose up` chỉ chạy core; các profile `crawl`, `ingest`, `speed`, `batch` (kèm `batch-once`), `serve`, `legacy`; có healthcheck; mount source read-only; volume `speed_checkpoints` | `docker-compose.yml` |
| `kafka-init` tạo idempotent 3 topic đóng băng | như trên |
| Port host lấy từ `*_HOST_PORT` trong `.env` | `docker-compose.yml`, `.env.example` |
| Heartbeat mỗi vòng lặp cho crawl và silver; healthcheck chạy `python -m common.heartbeat --check` | `common/heartbeat.py`, hai service |
| `start_all.ps1` truyền `--profile legacy --profile serve` | `scripts/start_all.ps1` |
| Pin image và cách kiểm pull | `docs/RUNBOOK.md` |

### 15.3 Lệch plan, đều có lý do

- **MinIO không còn image nào pull được.** Mọi tag trên Docker Hub và quay.io,
  kể cả `latest`, đều trả `denied`. Theo §5.3, pin theo digest của bản đang
  cache trên máy, kèm `pull_policy: missing`. Runbook ghi cách `docker save`
  / `docker load` sang máy khác.
- **ES/Kibana lên 8.18.1.** Image 8.18.0 trên máy này bị hỏng (§13.4) và đã
  được xoá.
- **`warehouse-job` giữ profile `jobs`**, không chuyển sang `legacy`.
  `scripts/run_warehouse.ps1` gọi `--profile jobs`, mà file đó không được
  sửa. Nếu nằm dưới `legacy` thì `start_all.ps1` sẽ khởi động nó mỗi lần `up`.
- **Kibana nằm ở cả `serve` lẫn `legacy`**, vì `kibana-setup` của legacy phụ
  thuộc nó.
- **Healthcheck service Spark dùng `pgrep`**: image không có `curl`.
- **Thêm `.dockerignore`** (ngoài §4). Không có file này thì build context là
  cả repo, gồm `.git` và 535 MB jar trong `.localjars/`.
- **Chưa có service `ops`, `stub-source`, `ops-projector`,
  `kibana-marketplace-setup`.** Code của chúng thuộc WP5 và WP8; profile `ops`
  và `smoke` sẽ được thêm cùng code.
- ~~Kafka vẫn ghi log vào filesystem của container, không có volume, vì một
  volume mới sẽ thuộc root.~~ **Lập luận này sai, và đã gây sự cố ở WP5
  (§16.3).** Image có sẵn `/var/lib/kafka/data` thuộc `appuser`, nên volume
  mới thừa hưởng đúng quyền. Đã sửa ở WP5.

### 15.4 Kiểm chứng trên stack thật

Core dựng lại bằng `docker compose -f docker-compose.yml up -d` (bỏ qua file
override). Crawler trỏ vào một stub tạm phục vụ fixture Tiki
(`TIKI_LISTING_URL=http://tiki-stub:8000/...`), không gọi Tiki thật.

```text
core                         7 service; kafka-init exit 0 ("marketplace topics ready"), minio-init exit 0
                             ES 8.18.1 healthy; Kafka, Redis không bị tạo lại vì config thực tế không đổi
4 service profile            đều healthy sau khoảng 90 giây
crawl-worker                 cycle 1: leased 4, succeeded 4; các cycle sau idle
silver-sink                  ghi các offset mới thành SILVER
speed                        batch 0: 24 input, 5 change → ES: 5 change, 2 offer
batch-scheduler              mp-20261002T0000Z → SKIPPED_SUCCEEDED
stop cả 4 service            4 giây, cả 4 exit 0
start lại speed, gửi lại 1 observation
                             batch 5, cùng query_id → checkpoint trong volume được dùng lại
```

`.env` local của chủ dự án được thêm `POSTGRES_HOST_PORT=5434`,
`MINIO_API_HOST_PORT=9010`, `MINIO_CONSOLE_HOST_PORT=9011`,
`REDIS_HOST_PORT=6380`. File `docker-compose.override.yml` giờ thừa và có thể
xoá. Nếu còn, nó vẫn áp `!override` và `minio:latest` của nó.

### 15.4b Review PR #10: ba điểm, đều đã sửa

| Điểm | Kết quả kiểm | Sửa |
|---|---|---|
| Đổi `KAFKA_HOST_PORT` làm hỏng công cụ Kafka chạy *trong* container | Dựng broker thử với port host 9019. `kafka-topics.sh --bootstrap-server localhost:9092` **treo** (bị chuyển hướng sang `localhost:9019`). Healthcheck `kafka-broker-api-versions` thì vẫn pass, nên phần review nói về healthcheck không tái hiện được | `start_all.ps1`, `smoke_fullstack.ps1` và healthcheck dùng `kafka:19092` |
| `smoke_fullstack.ps1` vẫn chạy `up` không kèm profile, nên Kibana/Superset mà script kiểm không bao giờ được khởi động | đúng | truyền `--profile legacy --profile serve`. File này nằm ngoài §4, nhưng là caller bị chính WP4 làm hỏng |
| Silver sink chỉ beat mỗi lần poll; một poll backlog có tới 500 record vượt 180 giây | đúng | beat thêm sau mỗi record; test `a2e64ba` → fix `b297e39`. Đã kiểm lại `silver-sink` healthy trên stack |

### 15.5 Trạng thái test

Không tính `test_marketplace_quality.py`: **756 passed** (trước WP4: 751, có
thêm 5 test heartbeat). File quality không đụng tới code nào WP4 sửa (42
passed ở §14.4). Tổng: **798**.

### 15.6 Việc tiếp theo (resume ở đây)

1. Review và merge PR WP4.
2. **WP5:** `ops/stub_source.py` (thay stub tạm của §15.4), `ops validate`,
   `ops smoke`, `scripts/mp.ps1`; thêm profile `smoke` và `ops`.
3. Ổ `C:` còn khoảng 6,5 GB, image Spark marketplace nặng 3,6 GB. Lần build
   lại sau dùng được cache layer.

---

## 16. Session 2026-10-02 (đêm) — merge WP4, Phase 8 WP5: stub, validate, smoke

### 16.1 PR #10 trước khi merge

Review tìm ra ba điểm; cả ba đã sửa trước khi merge (`56380af`). Chi tiết ở
§15.4b.

### 16.2 Đã làm

Nhánh `phase-8-wp5-stub-ops-cli`, cắt từ `develop` sau khi PR #10 merge.

| Thay đổi | Ở đâu |
|---|---|
| Stub Tiki offline: robots.txt; trang listing dựng từ fixture thật đã đóng băng; listing ID riêng cho từng `(category, page)`; giá theo bảng cố định `(100, 95, 60, 100) %` đánh chỉ số bằng bộ đếm lượt phục vụ của từng trang; `POST /_stub/mode` với `429`/`500`/`timeout`/`drift` | `ops/stub_source.py` |
| `python -m ops`: `migrate`, `seed`, `seed-smoke`, `smoke-wait`, `wait-quiet`, `batch-plan`, `validate`, `status` | `ops/__main__.py` |
| 11 check §9.2, chỉ đọc, qua `Sources` có thể inject; một report JSON đã sắp xếp; exit 1 nếu có check fail | `ops/validate.py` |
| Các bước chờ của smoke | `ops/smoke.py` |
| `mp up/down/status/migrate/seed/smoke/validate/batch` | `scripts/mp.ps1` |
| Profile `ops` và `smoke`; `pyarrow` chuyển sang image Python | `docker-compose.yml`, `requirements-marketplace.txt` |
| Kafka ghi log lên volume `kafka_data` | `docker-compose.yml` (fix `0cabf20`) |

### 16.3 Smoke chạy thật: bốn lần, ba lần fail đều có lý do

| Lần | Kết quả | Nguyên nhân | Sửa |
|---|---|---|---|
| 1 | dừng tay | Speed chết với `Some data may have been lost`. Container Kafka đã bị tạo lại lúc sửa review WP4, log nằm trong container nên offset về 1, trong khi checkpoint speed vẫn nhớ offset 25. Thêm nữa, điều kiện "mỗi task thành công 2 lần" đếm theo task ID, nhưng mỗi lần crawl lại tạo ra **task mới** cho target đó | Kafka lên named volume (`0cabf20`; đã kiểm `--force-recreate` giữ nguyên offset); đếm theo target |
| 2 | 9/11 check | `kafka_to_silver_lag = 2`: crawler vẫn chạy lúc validate. `bronze_present`: §16.4 | smoke dừng crawler và chờ lag về 0 (`wait-quiet`); smoke chạy trên stack trắng |
| 3 | batch `QUALITY_FAILED` | `as_of = now − settle`, làm tròn xuống phút, ra 09:36:00, trong khi crawl đầu tiên chạy 09:36:11. Audit bị cắt hết, nên gate 8 SKIPPED, mà skip ở gate bắt buộc là fail | `batch-plan --after-last-crawl`: `as_of` = mốc phút đầu tiên ≥ crawl run cuối + settle, rồi chờ đồng hồ vượt qua |
| 4 | **SMOKE PASSED**, 309 giây, stack trắng (`COMPOSE_PROJECT_NAME=mp-smoke`) | | |

Lần 4: 12 attempt và Bronze đầy đủ; Silver lag 0; DLQ rỗng; ES 30 change
không trùng; 12 offer có trong Redis; batch `mp-20261002T0945Z` gồm 24 dòng
Silver, quality PASS, SUCCEEDED; con trỏ = cache; đủ 10 dataset Gold đúng số
dòng; đủ 17 kết quả quality; gate 8 khớp. **11/11 PASS.**

### 16.3b Review PR #11: bốn bug, đều đã sửa

| Bug | Hệ quả | Sửa |
|---|---|---|
| Task giả của smoke (category 9001–9003, ACTIVE) ở lại trong frontier thật sau khi smoke kết thúc | Một lần `mp up` sau đó, với `TIKI_LISTING_URL` thật, sẽ crawl **Tiki thật** cho các category bịa này mỗi giờ, và dữ liệu đó chảy vào Silver/Gold. Phá đúng cam kết "không chạm marketplace thật" | `park-smoke` chuyển chúng sang `DISABLED` mỗi khi smoke kết thúc, pass hay fail; `seed-smoke` bật lại cho lần sau |
| `mp.ps1` đặt biến môi trường cấp process cho smoke và `Set-Location` mà không khôi phục | Lệnh tiếp theo trong cùng shell thừa hưởng URL stub, cadence 1 phút và settle 60 giây | khôi phục trong `finally`; `Push-Location`/`Pop-Location`; script chỉ `exit` một lần |
| `validate -Json` copy report bất kể exit code | trả về report của lần chạy trước như thể của lần này | xoá report cũ trước; chỉ copy khi file mới tồn tại |
| `consumer_lag` cộng một danh sách partition rỗng thành 0 | topic không có metadata thì `kafka_to_silver_lag` pass, và `wait-quiet` trả về ngay | báo lỗi |

Test `e5a421f` → fix `8088af1` (Python) và `78f691b` (`mp.ps1`).

**Kiểm lại trên stack trắng:**
- smoke lần 5 gọi từ `C:\Users`: PASSED sau 326 giây. Sau đó thư mục vẫn là `C:\Users`, cả ba biến môi trường đều trống, và 6 task smoke đang chờ ở `DISABLED` (`parked_smoke: 6`).
- smoke lần 6 trên cùng stack: PASSED sau 339 giây, với `created: 0, reactivated: 6`, rồi lại `parked_smoke: 6`. Chạy lại trên stack cũ vẫn trung thực, vì các điều kiện chỉ đếm từ lúc smoke bắt đầu.

### 16.4 Phát hiện chưa sửa: lineage `file://` không khả chuyển

Các attempt WP1 chạy trên host với lake `local` ghi `raw_uri` là
`file:///D:/code/...`. Từ container không đọc được đường dẫn đó, nên
`bronze_present` fail trên stack dev, dù file vẫn nằm trên ổ D:. Check báo
đúng. Nguyên nhân là lineage được ghi dưới dạng đường dẫn tuyệt đối của host.
Muốn sửa phải ghi URI tương đối với gốc lake, mà việc đó đụng tới hợp đồng
Bronze của Phase 2, nên để ngoài Phase 8 và ghi vào runbook.

### 16.5 Lệch plan

- Fixture: `tests/fixtures/marketplace_raw/` mà §8 nhắc tới **không tồn
  tại**. Dùng fixture Tiki thật duy nhất có trong repo,
  `tests/fixtures/tiki_listing_sample.json`: 3 dòng, trong đó 1 dòng cố ý
  invalid. Mỗi trang ra 2 observation và 1 bản ghi bị từ chối.
- Test 31 nằm ở `tests/test_ops_stub_source.py` (plan không nêu tên file).
- `source_health_projected` chuyển sang WP8: index mà check này đọc chỉ có
  khi có ops projector.
- `redis_offer_state_present` chỉ đòi các offer còn trong TTL của Redis,
  tính từ offer mới nhất. Offer cũ hơn đã hết hạn một cách hợp lệ.
- Phần cần Docker (`up`, `down`, batch Spark) do `mp.ps1` chạy trên host,
  vì container `ops` không có Docker. Thêm các lệnh `seed-smoke`,
  `smoke-wait`, `wait-quiet`, `batch-plan` cho các bước của smoke.
- Smoke có vũ trụ riêng: category `9001`–`9003`, ACTIVE, cadence 1 phút,
  settle 60 giây. Điều kiện chỉ đếm từ lúc smoke bắt đầu, nên chạy lại trên
  stack cũ vẫn trung thực.
- `mp drill`, `mp backup`, `mp restore` sẽ có ở WP6–WP9.

### 16.6 Trạng thái test

Không tính `test_marketplace_quality.py`: **799 passed** (trước WP5: 756).
File quality không đụng tới code nào WP5 sửa (42). Tổng: **841**.

### 16.7 Việc tiếp theo (resume ở đây)

1. Review và merge PR WP5.
2. **WP6:** drill D1–D6 (`tests/drills/test_drills.py`, marker `drill`,
   `pytest.ini`), dùng `stub-source` cho các chế độ lỗi.
3. Stack dev đang chạy core. `speed_checkpoints` đã bị xoá vì topic Kafka
   được tạo lại (§16.3); bật lại `speed` sẽ dựng checkpoint mới.
4. Ổ `C:` còn khoảng 5 GB.

---

## 17. Session 2026-10-03 — Phase 8 WP6 (drill D1–D6): XONG

> **Trạng thái:** D1–D6 **đã chạy thật và pass hết** trên stack Compose sạch
> (`COMPOSE_PROJECT_NAME=mp-smoke`), sau một `mp smoke` pass. Sáu record bằng
> chứng nằm ở `data/ops/drills/`. Nhánh `phase-8-wp6-drills-d1-d6`, đã mở
> **PR #12** vào `develop` (đang chờ review).

### 17.1 Môi trường đã chuẩn hóa (máy mới)

| Hạng mục | Trước | Sau |
|---|---|---|
| Python host | 3.11.9, pyspark **3.5.1** | **3.12.13** (`uv python install 3.12`), `.venv` dựng lại, cài `requirements.txt` → **pyspark 4.0.4** |
| `.env` | bản tháng 7, còn `SPARK_KAFKA_PACKAGE=…_2.12:3.5.1` | copy từ `.env.example`; biến đó **đã bỏ**, mặc định trong `config/settings.py` là `…_2.13:4.0.1` |
| Docker | chưa chạy | Docker Desktop 28.3.2; hai image MinIO đã pin **có sẵn** trên máy này (`minio/minio@sha256:14cea493…`, `minio/mc@sha256:a7fe349e…`), không phải `docker load` từ máy khác |
| Image khác | thiếu | pull `apache/spark:4.0.1`, `python:3.12.11-slim`, ES/Kibana `8.18.1`; build `ecommerce/spark-marketplace:4.0.1` (3,6 GB) và `ecommerce/marketplace-python:1` |

Hai điều khác với §16/§17.3 của máy cũ:

- **Spark chạy được trên host này *trong bộ nhớ*** (Java 21 + pyspark 4.0.4):
  `createDataFrame` → `collect` round-trip OK, nên những test trước đây skip
  vì `requires_spark` nay chạy thật, suite mặc định lâu hơn (3–5 phút) nhưng
  phủ nhiều hơn. **Filesystem thì vẫn không**: ghi parquet ra ổ Windows vẫn
  ném `UnsatisfiedLinkError: NativeIO$Windows.access0` vì thiếu `winutils.exe`
  (kiểm lại 2026-10-03, xem `PHASE_INDEX.md` §5b mục 1). Mọi job Spark có I/O
  vẫn phải chạy trong container.
- `docker compose build` nhiều service cùng dùng một image name thì báo
  `image "…": already exists` và **vẫn thoát 0**; phải build riêng
  (`compose build speed`) mới ra `ecommerce/spark-marketplace:4.0.1`.

### 17.2 Bug production tìm được: không service dài hạn nào có `restart:`

Đúng như dự đoán ở bản §17.2 cũ. `foreachBatch` ném lỗi → `awaitTermination`
ném → `main()` thoát → container nằm im. D4 đòi "batch SUCCEEDED sau khi
khôi phục" nên chỉ có thể timeout.

| Bước | Commit |
|---|---|
| Test đọc `docker-compose.yml`, đòi `restart: unless-stopped` cho 5 service dài hạn và `restart: "no"` cho 2 service one-shot — fail đúng 5/8 vì đúng lý do | `aca5050` `tests/test_compose_services.py` |
| Fix: thêm `restart: unless-stopped` cho crawl-worker, silver-sink, speed, batch-scheduler, stub-source | `7231b8b` |

`unless-stopped` chứ không `always`: service nào drill chủ động `stop` thì
phải nằm yên. Hệ quả: D6 phải `docker update --restart no` trước khi kill
worker A, nếu không Docker dựng A dậy ngay và B không bao giờ nhận lease.

### 17.3 Siết D4 và D5 (yêu cầu của session này)

**D4 — đúng cùng một batch.** Bản cũ chờ "một batch FAILED" rồi "một batch
SUCCEEDED có row"; hai cái đó không bắt buộc là một, nên một query bỏ batch
hỏng rồi đi tiếp vẫn pass. Bản mới bám khóa audit `(query_id, batch_id)`:
FAILED → **retry dưới `started_at` mới trong lúc sink vẫn chết** → SUCCEEDED.
Thêm kiểm tra Redis: `rt:changes:recent` không có member trùng, và không
change nào trong Redis mà Elasticsearch không có.

**D5 — kill thật giữa micro-batch, xóa volume đúng project.** Chi tiết ở
§17.5; tóm tắt: dựng backlog Kafka để batch đủ dài mà kill vào giữa, chứng
minh bằng audit row kẹt `RUNNING`, rồi đòi đúng batch đó chạy lại thành
công. Volume lọc theo `label=com.docker.compose.project` và **fail nếu
không tìm thấy volume nào** — bản cũ lọc `name=speed_checkpoints` trên toàn
máy (máy này còn project `invoice-gateway`) và im lặng không xóa gì nếu
trượt, biến "replay" thành no-op.

Commit: `bf3de65` (siết D4 + D5 + D6 `stays_dead`, kèm test offline).

### 17.4 Kết quả D1–D6 (thật, 2026-10-03)

Stack: project `mp-smoke`, `mp smoke` **PASSED** trước khi bắt đầu.
Mỗi drill tự `restore` về `validate` xanh; cả sáu record đều `"passed": true`.

| Drill | Thời gian | Invariant đã chứng minh |
|---|---|---|
| D1 | 1m43s | 429/500/timeout → `RATE_LIMITED`/`SERVER_ERROR`/`TRANSIENT_NETWORK`; `Retry-After` được tôn trọng (due 14:29:09 sau khi xong 14:29:08); circuit mở đúng ngưỡng 5; stub về `ok` thì mọi target xong, **không task nào FAILED khi còn lượt** |
| D2 | 4m59s | Kafka dừng giữa crawl → `PUBLISH_ERROR`, `parsed_count = 0` = số đã ack, raw vẫn trong Bronze; Kafka lên lại, batch `mp-20261003T1435Z` **SUCCEEDED** → check 8 đối soát sạch |
| D3 | 1m43s | MinIO dừng → sink **không commit** (lag 12 → 12 sau 60 s), **DLQ không đổi** (0 → 0 → 0); MinIO lên lại, Silver đủ 12/12 quan sát đã ack |
| D4 | 3m12s | ES: batch 36 của query `396ae430…` FAILED 12:57:31 → retry 12:58:04 (vẫn RUNNING, ES còn chết) → SUCCEEDED 12:58:23. Redis: batch 38, cùng hình. ES 131 doc / 131 id, Redis 131/131, không orphan |
| D5 | 21m01s | Kill **trong** batch 8 (backlog 152 quan sát): audit row kẹt `RUNNING`, `completed_at` null; container 14:02:21 → 14:22:52; **đúng batch 8** chạy lại SUCCEEDED với 156 row / 184 change; ES 811 doc / 811 id, **0 event mất**. Replay: xóa `mp-smoke_speed_checkpoints`, `MARKETPLACE_STREAM_CHECKPOINT_VERSION=drill-d5`, đọc lại từ `earliest` → **811 → 811, 0 mới, 0 thiếu** |
| D6 | 1m02s | Giết worker A khi đang giữ 6 lease → B **không** lấy lease nào trước khi hết hạn (6/6 vẫn thuộc A); hết hạn thì B nhận và hoàn thành đủ 6; không task nào chạy song song |

D4 và D5 chạy lại sau khi sửa (§17.5). D1–D3 được **chạy lại liên tiếp** trên
bản code cuối cùng để chắc không rò state giữa các drill: cả ba pass.

### 17.5 Hai bug trong chính drill, tìm được khi chạy thật

Cả hai là bug của harness, **không phải** của production code — ghi lại vì
mỗi cái đều suýt bị đọc nhầm thành bug sản phẩm.

**(a) D4 đọc Elasticsearch trước khi nó refresh.** Lần chạy đầu của D4 bản
siết đi qua *mọi* assertion của plan rồi fail ở chỗ "mỗi change trong
`rt:changes:recent` phải có document": 109 member / 99 document, thiếu đúng
10 = `change_rows` của batch đó. Sink ghi ES **trước** Redis nên phép so là
đúng — nhưng Elasticsearch là near-real-time: bulk đã commit vẫn chưa
searchable cho tới lần refresh sau (mặc định 1 s), còn drill đọc ngay lúc
batch được audit SUCCEEDED. Đọc lại một phút sau: 109 và 109, 0 orphan.
`es_changes_covering` chờ có giới hạn cho tới khi mọi id Redis giữ đã
searchable. Commit `cb61184`.

**(b) D5 không kill được gì, rồi chụp ảnh quá sớm.**

1. Cách đầu là cho watcher chạy **trong** container và gọi `os.kill(1, 9)`.
   Bị **bỏ qua im lặng**: PID 1 *chính là* driver, và Linux không giao tín
   hiệu default-action cho init của một PID namespace khi tín hiệu đến từ
   trong chính namespace đó. Đo trên stack: watch 90 s, mọi micro-batch đều
   khớp, `RestartCount` không đổi.
2. Kill từ host thì không trúng cửa sổ: audit row chỉ mở trong lúc ghi sink
   — đo được **10 ms** (batch rỗng) và **28–90 ms** (một chu kỳ crawl) —
   trong khi `docker kill` mất **~220 ms** mới tới nơi.
   → Drill **tự tạo cửa sổ**: dừng query, để crawler dồn backlog Kafka, rồi
   kill vào đúng batch xả backlog (không có `maxOffsetsPerTrigger` nên cả
   backlog vào một batch). 60 quan sát đo được 268 ms; sàn đặt ở 150 quan
   sát. Poll giữ một connection (2 ms/lượt thay vì 20 ms). Trúng hay không
   **không được giả định**: batch phải còn `RUNNING` sau 15 s, trạng thái
   terminal ở đó là drill fail. Commit `db89914`, và `257f227` vì
   `postgres_connection_factory()` trả context manager chứ không phải
   connection.
3. Lần sau variant 1 pass nhưng replay báo **19 doc mới, 0 thiếu**. Không
   phải bất định: hai lần replay mà lần chạy đó tình cờ tạo ra (hai
   `checkpoint_version` khác nhau) **khớp nhau tuyệt đối** — 540 input row,
   617 change — còn query live mới tới 528/598. Ảnh "before" chụp quá sớm:
   `quiet()` chờ lag của **consumer group Silver**, mà speed query giữ
   offset trong checkpoint riêng nên lag đó không nhìn thấy; 12 quan sát
   của lần crawl cuối chưa được đọc. `wait_speed_drained` chờ theo audit:
   khi không micro-batch nào mang row lâu hơn một trigger thì không còn gì
   để mang. Commit `9e18cf2`.

### 17.6 Trạng thái test (Python 3.12.13, pyspark 4.0.4)

- Suite mặc định (`-m "not drill"`, bỏ `test_marketplace_quality.py`):
  **826 pass, 6 deselected**, 4m41s.
- `tests/test_marketplace_quality.py` riêng: **42 pass**, 16m19s.
- **Tổng 868.** Trước WP6 là 841 trên máy cũ; chênh lệch gồm 8 test
  `tests/test_compose_services.py`, 12 test harness mới trong
  `tests/test_ops_validate.py`, và những test Spark trước đây skip nay chạy
  thật trên host này.
- Sáu drill vẫn nằm ngoài suite mặc định (marker `drill`).

### 17.7 Việc tiếp theo (resume ở đây)

1. Review và merge **PR #12** (WP6) vào `develop`.
2. **WP7:** D7–D10 (drift, quality failure, publish failure, concurrent
   batch), plan §10. Không bắt đầu trước khi WP6 merge.
3. WP8 (Kibana), WP9 (backup/restore, D11), WP10 (tài liệu tổng thể
   ARCHITECTURE/DATA_MODEL/RUNBOOK) vẫn chưa làm.
4. Ổ `C:` còn khoảng 150 GB trên máy này.

### 17.8 Dựng môi trường trên máy khác

- `git checkout phase-8-wp6-drills-d1-d6`.
- `.env` không có trong git: copy `.env.example` thành `.env`. Nếu port bị
  chiếm thì đổi các `*_HOST_PORT`, và để `POSTGRES_PORT`, `MINIO_ENDPOINT`,
  `REDIS_PORT` khớp với chúng. **Đừng** mang lại
  `SPARK_KAFKA_PACKAGE=…_2.12:3.5.1`.
- Python 3.12 + `pip install -r requirements.txt` (pyspark 4.0.4).
- **MinIO:** Compose pin `minio/minio` và `minio/mc` theo digest, mà hai
  image này **không pull được nữa** (`docs/RUNBOOK.md`, mục Image pins).
  Máy không có thì chép sang bằng `docker save` / `docker load`.
- Image của project: build **từng service một** cho nhánh Spark (§17.1),
  hoặc `mp up --build`.
- Chạy drill: `$env:COMPOSE_PROJECT_NAME = "mp-smoke"`, `mp smoke`, rồi
  `mp drill d1` … `d6`. D5 mất khoảng 20 phút vì phải dồn backlog.

---

## 18. Session 2026-10-03 (tiếp) — Phase 8 WP7 (drill D7–D10): XONG

> **Trạng thái:** D7–D10 **đã chạy thật và pass hết**, rồi **chạy lại liên tiếp
> cả bốn** trên cùng stack để bắt rò state — cũng pass hết. Mười record bằng
> chứng (D1–D10) nằm ở `data/ops/drills/`. Nhánh
> `phase-8-wp7-drills-d7-d10`, rẽ từ `develop` sau khi PR #12 merge. Chưa mở PR.

### 18.1 Tồn đọng đã đóng trước khi bắt đầu

| Việc | Kết quả |
|---|---|
| PR #12 (WP6) | Merge vào `develop` bằng merge commit `54e7e80`; `develop` giờ chứa toàn bộ WP6 |
| Trailer `Co-Authored-By: Claude` | Bỏ khỏi 12 commit WP6 (`git filter-branch --msg-filter`), force-push; PR #12 còn đúng tên tác giả. ~137 commit cũ đã merge trong `develop`/`master` **cố ý để nguyên** (viết lại sẽ phải force-push nhánh chung và làm hỏng liên kết của PR #1–#11) |
| 8 nhánh phase-7/phase-8 đã merge | Xoá cả local lẫn remote. Giữ `phase-3-4-scheduler-kafka-silver` và `phase-5-6-speed-gold` làm lịch sử theo `PHASE_INDEX.md` §3b |
| `PHASE_INDEX.md` §5b | Kiểm lại cả ba hạn chế: mục 2 (s3a chưa kiểm chứng) **đã xong**, mục 1 (winutils) **vẫn còn** — đo lại và vẫn ném `UnsatisfiedLinkError`, mục 3 là legacy có chủ ý. Ghi chú "Spark chạy được trên host" ở §17.1 nói quá, đã sửa thành "chỉ trong bộ nhớ" |
| `RUNBOOK.md` | Lệnh khôi phục Kafka hardcode tên volume của project khác; nay hỏi Docker |

### 18.2 Kết quả D7–D10 (thật, 2026-10-03)

Stack `mp-smoke`, nối tiếp WP6, baseline `validate` xanh trước mỗi drill.

| Drill | Thời gian | Invariant đã chứng minh |
|---|---|---|
| D7 | 0m53s | Stub mode `drift` → attempt `PARSE_ERROR`; raw **vẫn trong Bronze**; task terminal (không còn schedulable); **circuit không nhúc nhích** (`[0, null]` → `[0, null]`). `crawler.reparse` trên đúng artifact đó: exit 1, `{"PARSE_FAILED": 1}`, và **không ghi gì**: Kafka end offset `{0:192, 1:322, 2:256}`, Silver 770 object, 404 attempt, pointer và cache version — tất cả giống hệt trước/sau |
| D8 | 2m54s | Chèn **một** attempt đã settle (`attempt_id=417`, `parsed_count=7`) vào trong lookback → run `mp-20261003T1555Z` **QUALITY_FAILED** trên `silver_parse_attempt_reconciliation`, `cache_published=false`, **17 quality result vẫn được lưu**; pointer/cache/12 dòng cache **y nguyên**. Xoá đúng row đó → resume cùng run → **SUCCEEDED**, pointer **mới** nhảy sang `mp-20261003T1555Z` |
| D9 | 2m47s | CHECK constraint `drill_d9_reject_one_cache_row` (`NOT VALID`) từ chối đúng một offer → run `mp-20261003T1603Z` **FAILED tại publish**, error nêu đúng tên constraint, `cache_published=false`; pointer, cache version và cả 12 dòng đang phục vụ **nguyên vẹn** (publish truncate rồi refill trong một transaction, nên refusal kéo cả truncate theo). Drop constraint trong `finally` → resume → **SUCCEEDED**, pointer và cache cùng trỏ run mới |
| D10 | 2m35s | Hai batch chạy gần như đồng thời, **khác run_id có chủ ý**: `mp-20261003T1607Z` exit **75** với `{"lock_key": 820801, "status": "ALREADY_RUNNING"}`, `mp-20261003T1607Z-d10` exit 0. Đối chiếu audit: run bị từ chối **không có dòng nào** trong `audit.marketplace_batch_run` (`loser_audit_row: null`); `marketplace_cache_version` đúng **1 dòng**; pointer và cache cùng trỏ run thắng |

**Chạy lại liên tiếp D7 → D8 → D9 → D10** trên cùng stack: cả bốn pass
(record hiện tại là của lượt này, 16:08–16:17). Sau cùng: không còn row D8
nào mang marker, không còn constraint D9, `validate` xanh.

### 18.3 Vì sao D10 dùng hai run_id khác nhau

Plan §10 viết "start two `mp batch` at once". Nếu cả hai cùng `run_id` thì
"run bị từ chối không ghi audit row" **không kiểm được**: chỉ có một dòng và
nó là của run thắng, nên thiếu dòng của run thua trông y hệt. Dùng hai id
(`<planned>` và `<planned>-d10`, cùng `as_of`) làm điều kiện đó kiểm được
thẳng: id bị từ chối phải **không có dòng nào**. Đây là deviation có chủ ý so
với cách đọc hẹp nhất của plan, và nó làm assertion mạnh hơn chứ không yếu
đi. Hệ quả: pointer có thể mang hậu tố `-d10`; không check nào của `validate`
phân tích run_id nên vô hại.

### 18.4 Một bug trong harness, tìm được khi chạy thật

Không có bug production nào trong WP7.

**D9 chết trước khi tới inject:** `ProgrammingError: no results to fetch`.
`Stack.query` luôn gọi `fetchall`, mà `ALTER TABLE` không có result set, nên
cả `ADD CONSTRAINT` lẫn lệnh đảo của nó đều hỏng. Thêm `Stack.execute` chạy
statement không fetch; test offline nay fail nếu DDL quay lại đi qua
`query()`. Commit `8ce545f`.

### 18.5 Trạng thái test

- Suite mặc định (`-m "not drill"`, bỏ `test_marketplace_quality.py`):
  **834 pass, 10 deselected**, 2m17s.
- `tests/test_marketplace_quality.py` riêng: **42 pass**.
- **Tổng 876.** Sau WP6 là 868; chênh lệch là 9 test offline mới cho harness
  WP7 trừ đi 1 test `test_the_drills_cover_d1_to_d6` bị thay bằng bản D1–D10.
- `tests/drills/test_drills.py` parametrize **D1–D10**, marker `drill` vẫn bị
  loại khỏi suite mặc định (10 deselected).

### 18.6 Việc tiếp theo (resume ở đây)

1. Mở PR WP7 vào `develop`, review, merge.
2. **WP8:** Kibana — index template, `ops/es_projector.py`, saved object
   `.ndjson`, test 27–30 (plan §11).
3. **WP9:** backup/restore và D11 (plan §12), test 24–26.
4. **WP10:** viết lại ARCHITECTURE/DATA_MODEL và hoàn thiện RUNBOOK (plan §13).

## 19. Session 2026-10-04 — Phase 8 WP8 (Kibana): XONG

> **Trạng thái:** index template, projector và hai dashboard **đã cài và chạy
> thật** trên stack `mp-smoke` đang sống, không phải chỉ viết ra. Thủ tục
> §11.4 (dựng lại hai index `*-v1` map động) cũng đã chạy một lần có chủ ý.
> Nhánh `phase-8-wp8-kibana`, rẽ từ `develop` (54e7e80) theo đúng luật không
> xếp chồng nhánh ở `PHASE_INDEX.md` §3b.

### 19.1 Những gì được thêm

| File | Việc |
|---|---|
| `display/kibana/marketplace_index_templates.py` | Sáu composable index template. Tiền là `scaled_float` scaling 1000000 (đúng sáu chữ số thập phân như `decimal(38,6)`), định danh là `keyword`, bốn index của projector là `dynamic: strict` |
| `ops/es_projector.py` | Loop service (profile `ops`). Bốn nguồn → bốn index, `_id` suy ra từ khóa của chính dòng dữ liệu, cửa sổ overlap, `--once`, `--rebuild` |
| `display/kibana/marketplace_dashboards.py` | **Sinh** saved object thay vì viết tay JSON. 10 object: 6 data view, 2 saved search, 2 dashboard (5 + 10 panel) |
| `display/kibana/saved_objects/marketplace_dashboards.ndjson` | Bản export đã commit; test fail nếu nó lệch với builder |
| `display/kibana/setup_marketplace_kibana.py` | Cài template → tạo index rỗng → xoá vỏ Phase 5 → import `.ndjson` với `overwrite=true` |
| `display/kibana/create_marketplace_speed_dashboard.py` | Còn lại một lời gọi mỏng vào importer trên (plan §11.3) |
| `docker-compose.yml` | Thêm `es-projector` (profile `ops`) và `kibana-marketplace-setup` (profile `serve`) |

### 19.2 Chạy thật trên stack `mp-smoke` (2026-10-04)

| Bước | Kết quả |
|---|---|
| `kibana-marketplace-setup` | 6 template cài xong, `marketplace-dlq-v1` được tạo rỗng, **10 saved object import thành công**. Chạy lại lần hai: y hệt (idempotent) |
| `es-projector --once --rebuild` | `source_health 1, crawl_attempts 464, speed_batches 599, dlq 0`. Ba index strict được tạo **từ template**: `dynamic=strict`, 10/18/17 field |
| `_id` thật | source-health `tiki`; attempt `466`; batch `marketplace-speed-v1:0c9e79a1-…:493` — đúng khóa chính ba thành phần |
| Loop service | Healthy. Mỗi 30 s một pass; pass nào cũng đọc lại ~30 speed batch trong cửa sổ overlap mà **tổng document không phồng lên** (599 → 609 đúng bằng số batch mới), tức ghi đè idempotent |
| Đường DLQ | Bắn **một** record không phải JSON vào `marketplace.observations.v1` → silver-sink `QUARANTINE` (stage `DECODE`) → projector index nó với `_id = dlq_…`. `payload_text` **không** được project. Pass sau `dlq: 0`, count vẫn 1 → offset đã commit *sau* khi index |
| Mười agg đúng bằng mười panel | Đều ra số thật: error_kind `{PARSE_ERROR 12, SERVER_ERROR 4, TRANSIENT_NETWORK 4, PUBLISH_ERROR 3, …}`, http `{200: 454, 500: 4, 429: 2}`, fetch latency p50/p95 = 2540/2996 ms, parsed/rejected = 878/442, micro-batch duration p50/p95 = 10/51 ms, freshness tiki = 8285 s |
| `mp validate` | **11/11 PASS** sau tất cả những việc trên |

### 19.3 Thủ tục §11.4 — dựng lại hai index `*-v1`, chạy một lần có chủ ý

`marketplace-changes-v1` và `marketplace-offers-current-v1` có từ Phase 5,
map động, nên `change_type` và `availability` là `text`. Hai panel dựng trên
chúng **không chạy được**: `Fielddata is disabled`. Template chỉ áp cho index
tạo *sau* nó, nên phải dựng lại.

Stop `speed` → xoá hai index → `MARKETPLACE_STREAM_CHECKPOINT_VERSION=v2` →
start `speed`.

**Trước:** 1001 change, 12 offer, `change_type` là `text`.
**Sau replay:** 1001 change, 12 offer — *đúng cùng bộ document, không thừa
không thiếu*, nhờ `_id` tất định. `change_type`/`availability` nay là
`keyword`; `current_value` là keyword kèm sub-field `numeric`
(`scaled_float`, `ignore_malformed`), nên `avg(current_value.numeric)` trên
`PRICE_CHANGED` ra 7.313.138 VND, còn giá trị JSON của `NEW_OFFER` bị bỏ qua
chứ không làm hỏng document.

Đây chính là biến thể replay của drill D5, chạy một lần có chủ ý, và nó chạy
được là nhờ bản sửa WP2 (batch khóa theo `(query_name, query_id, batch_id)`).

### 19.4 Hai chỗ lệch so với plan, đều có chủ ý

1. **`_id` của speed batch.** Plan §11.2 ghi `query_name:batch_id`. Khóa chính
   của bảng audit là `(query_name, query_id, batch_id)` từ WP2, vì batch id
   quay về 0 mỗi khi đổi checkpoint. Bỏ `query_id` khỏi `_id` thì batch 0 của
   lượt replay sẽ **ghi đè** batch 0 của lượt cũ — đúng con bug WP2 đã sửa,
   dựng lại ở tầng trên. `_id` ở đây bám đúng khóa chính. (Thủ tục §19.3 bên
   trên là bằng chứng sống: nếu dùng khóa hai thành phần thì 493 batch của
   query mới đã xoá sổ lịch sử của query cũ.)
2. **`payload_text` không được project.** Plan không nói rõ. Trường này giữ
   tới 1 MiB thân response thô; thân response thuộc về Bronze và topic DLQ,
   không thuộc một index vận hành (Brief §20, và cũng là luật các bảng audit
   đang theo). Document vẫn mang đủ toạ độ để tìm lại nó.

### 19.5 Một thứ phải đo mới biết: saved object cần dấu version

Import `.ndjson` lần đầu trả **500**, log Kibana: `Error trying to transform
document: Cannot read properties of undefined (reading 'layers')`. Nguyên
nhân: object không mang `typeMigrationVersion`, nên Kibana kéo nó qua **mọi**
migration từ 7.x, trong đó có migration Lens tiền-8.2 đi tìm
`datasourceStates.indexpattern` trong khi 8.x dùng `formBased`.

Không đoán số: tạo một object mỗi loại bằng chính API của Kibana 8.18.1 rồi
export ra đọc ngược. `coreMigrationVersion` `8.8.0`;
`typeMigrationVersion` `index-pattern` `8.0.0`, `search` `10.5.0`,
`dashboard` `10.2.0`. Đóng dấu xong thì import 10/10.

Hệ quả: bộ số này gắn với Kibana 8.18.1 mà `docker-compose.yml` đang pin.
Đổi pin thì phải đo lại — comment trong `marketplace_dashboards.py` nói rõ.

### 19.6 Trạng thái test

- Suite mặc định (`-m "not drill"`, bỏ `test_marketplace_quality.py`):
  **899 pass, 6 deselected**, 2m36s.
- `tests/test_marketplace_quality.py` riêng: **42 pass**, 12m03s.
- **Tổng 941.** Trên `develop` là 868; chênh 73 = 30 test
  `tests/test_ops_projector.py` + 41 test `tests/test_kibana_marketplace.py`
  + 2 test compose mới (`es-projector`, `kibana-marketplace-setup`).
- Drill vẫn bị loại khỏi suite mặc định; nhánh này rẽ từ `develop` nên mới có
  D1–D6 (6 deselected), D7–D10 về cùng PR WP7.

### 19.7 Việc tiếp theo (resume ở đây)

1. Merge PR WP7 vào `develop`, rồi rebase nhánh này lên `develop` và mở PR WP8.
2. **WP9:** backup/restore và D11 (plan §12), test 24–26.
3. **WP10:** viết lại ARCHITECTURE/DATA_MODEL và hoàn thiện RUNBOOK (plan §13).
4. `.env` trên máy này nay có `MARKETPLACE_STREAM_CHECKPOINT_VERSION=v2` —
   đừng hạ về `v1`, checkpoint cũ còn đó và sẽ tiếp tục từ offset cũ.

## 20. Session 2026-10-04 — Phase 8 WP9 (backup/restore): phần §12 XONG, D11 chờ WP7

> **Trạng thái:** `mp backup` và `mp restore` **đã chạy thật end-to-end**:
> backup 1832 file từ stack `mp-smoke`, restore vào project `mp-restore` mới
> tinh với volume trắng, cả bốn check của plan §12.3 pass. **D11 chưa làm** —
> nó phải thêm vào `ops/drills.py`, mà file đó trên `develop` mới có D1–D6;
> bản D1–D10 nằm ở nhánh WP7 chưa merge. Luật §3b cấm xếp chồng nhánh, nên
> D11 làm ngay sau khi PR WP7 vào `develop`.
> Nhánh `phase-8-wp9-backup-restore`, rẽ từ `develop` (54e7e80).

### 20.1 Những gì được thêm

| File | Việc |
|---|---|
| `ops/backup.py` | Toàn bộ §12: `backup()`, `restore()`, ba restore check, `LiveLake`/`LivePostgres`, manifest `marketplace-backup.v1` |
| `ops/__main__.py` | `ops backup`, `ops restore --from`, `ops backups`, và `ops validate --restored` |
| `scripts/mp.ps1` | `mp backup`, `mp restore -BackupId … -Project …` |
| `crawler/reparse.py` | `RawArtifactRef.from_uri()` — nghịch đảo của `body_path`, để restore check dựng lại ref từ `raw_uri` trong audit |
| `docker/marketplace-python/Dockerfile` | `postgresql-client-18` từ PGDG |

### 20.2 Vì sao pg_dump phải nằm trong image ops

Backup giữ **batch advisory lock** suốt thời gian chạy — không phải để đóng
băng Bronze/Silver (hai zone chỉ append, bản copy sớm vài giây là cũ hơn chứ
không rách), mà để không batch nào promote con trỏ mới giữa lúc đọc
`current.json` và lúc dump cache phải khớp với nó. Lock thuộc về session đã
lấy nó, nên dump **không thể** tách sang container thứ hai như cách mp.ps1
vẫn lái job Spark.

Server pin `postgres:18.3`; pg_dump từ chối server mới hơn chính nó, mà
Debian trixie chỉ có client 17. Thêm repo PGDG, pin `postgresql-client-18`
(đo được: 18.6). Đổi một pin thì phải đổi pin kia.

### 20.3 Chạy thật (2026-10-04)

| Bước | Kết quả |
|---|---|
| `ops backup` trên `mp-smoke` | 1832 file, 13 MB: gold 31, bronze 920, silver 879, postgres 2 (audit 172 KB, cache 38 KB). `pointer_run_id` = `cache_run_id` = `mp-20261003T1617Z-d10` |
| `mp restore` vào `mp-restore` (volume trắng) | verify **1832/1832** sha256 trước khi ghi; `pg_restore` audit rồi cache; 1829 lake object; con trỏ ghi **sau cùng** |
| Check 1 pointer == cache | PASS, cả hai `mp-20261003T1617Z-d10` |
| Check 2 datasets | PASS, cả **10** dataset khớp đúng row count trong manifest |
| Check 3 reparse | PASS, **5/5 IDENTICAL** |
| Check 4 quality-only batch | `quality_status: PASS`, `mandatory_failure_count: 0`, 878 silver row, `manifest_promoted: false` (`QUALITY_ONLY` — không đụng con trỏ). Dataset count trùng khít con trỏ |
| `ops validate --restored` | passed, nới đúng ba check, không có check lạ |
| Dọn | `mp-restore` xoá kèm volume; `mp-smoke` dựng lại, `validate` **11/11 PASS** |

### 20.4 Ba bug tìm được, đều do chạy thật

Không có bug production nào. Cả ba đều nằm trong code WP9 vừa viết, và cả ba
đều **chỉ lộ ra khi chạy**, không unit test nào ban đầu bắt được.

1. **Restore check lấy mẫu sai tập.** Lượt restore đầu tiên FAIL:
   `NEW_OBSERVATIONS` trên 1 trong 5 artifact. Tra ra attempt 380,
   `FAILED`/`PUBLISH_ERROR`, `parsed_count = 0` — đúng sản phẩm **có chủ ý**
   của drill D2: fetch và parse xong, raw vào Bronze, Kafka chết nên không gì
   vào Silver. Reparse nó ra `NEW_OBSERVATIONS` là **đúng**. Sửa: chỉ lấy mẫu
   `error_kind IS NULL AND parsed_count > 0`. `parsed_count` là số đã
   acknowledge, nên publish *một phần* cũng để lại observation Silver không
   bao giờ có; audit không phân biệt được bằng số, nhưng publish một phần
   luôn là `FAILED` có `error_kind`, và cái đó thì phân biệt được. Test chốt
   hai trường hợp đó viết **trước** bản sửa.
2. **Hai ký tự backspace trong `mp.ps1`.** Một lần sửa file bằng script đã
   đọc `\b` trong `"data\backups"` thành escape. PowerShell parse sạch rồi đi
   tìm một đường dẫn không tồn tại, nên lỗi hiện ra muộn mấy phút dưới dạng
   *"RESTORE FAILED: the quality gate refused the restored Silver"* — hoàn
   toàn không phải chuyện đã xảy ra. Sửa ba việc: bỏ hẳn việc đọc manifest từ
   host (lấy cửa sổ batch từ chính output của `ops restore`), sửa câu báo lỗi
   để nó nói đúng bước nào hỏng, và thêm một test chặn ký tự điều khiển trong
   `mp.ps1`.
3. **`validate` trên stack vừa restore không thể xanh hết.** Đúng ba check
   fail, và chỉ ba: `es_changes_unique`, `redis_offer_state_present` (ES và
   Redis không được backup), và `kafka_to_silver_lag` — broker mới tinh chưa
   từng có consumer group Silver nên hỏi offset của nó ném
   `GroupCoordinatorNotAvailableError`. Cả ba đúng bằng hàng "không backup"
   của plan §12.1. Thêm `ops validate --restored` nới **đúng ba cái đó** và
   fail ở cái thứ tư; danh sách được một test chốt cứng.

### 20.5 Một chỗ lệch so với plan

Plan §12.2 bước 5 đặt việc đối chiếu hai run_id ở bước ghi manifest, tức là
**sau** khi đã copy xong. Ở đây nó chạy ngay sau khi lấy lock, **trước** khi
copy byte nào: một backup không thể restore vào trạng thái `validate` chấp
nhận thì không đáng tốn bytes, và lock làm cho "ở bước 1" và "ở bước 5" là
cùng một giá trị. Assertion không yếu đi, chỉ fail sớm hơn.

### 20.6 D11 còn thiếu, và vì sao

D11 ("`mp backup`, rồi `mp restore` vào một Compose project mới; mọi check
§12.3 pass") phải vào `ops/drills.py` và `tests/drills/test_drills.py`.
Trên `develop` hai file đó mới có D1–D6; bản D1–D10 nằm trên
`phase-8-wp7-drills-d7-d10` chưa merge. Rẽ nhánh WP9 từ WP7 là xếp chồng,
`PHASE_INDEX.md` §3b cấm. Nên: merge PR WP7 → rebase nhánh này lên `develop`
→ thêm D11 vào đúng registry D1–D10.

Phần việc D11 sẽ phải làm đã chạy tay trọn vẹn ở §20.3, nên D11 chỉ còn là
gói nó vào harness drill (baseline xanh trước, khôi phục stack sau, ghi
record JSON).

**Lưu ý khi viết D11:** nó phải hạ stack chính xuống rồi dựng project thứ hai
— `container_name` là global. Đây là drill duy nhất làm thế, nên nó phải chạy
**cuối cùng** trong `drill all`, và phải dựng lại stack chính trước khi thoát.

### 20.7 Trạng thái test

- Suite mặc định (`-m "not drill"`, bỏ `test_marketplace_quality.py`):
  **871 pass, 6 deselected**, 2m17s.
- `tests/test_marketplace_quality.py` riêng: **42 pass**.
- **Tổng 913.** Trên `develop` là 868; chênh 45 = 42 test
  `tests/test_ops_backup.py` + 3 test `RawArtifactRef.from_uri`.

### 20.8 Việc tiếp theo (resume ở đây)

1. Merge PR WP7, rồi PR WP8, rồi rebase nhánh này và mở PR WP9.
2. **D11** ngay sau khi WP7 vào `develop` (§20.6).
3. **WP10:** viết lại ARCHITECTURE/DATA_MODEL và hoàn thiện RUNBOOK (plan §13).
4. `RawArtifactRef.from_uri` ở đây trùng chức năng với helper tách `raw_uri`
   mà WP7 đặt trong `ops/drills.py`. Khi rebase thì cho drills dùng bản trong
   `crawler/reparse.py` và xoá bản kia — một chỗ duy nhất biết layout Bronze.

## 21. Session 2026-10-04 (tiếp) — Phase 8 WP10 (tài liệu): XONG

> **Trạng thái:** `ARCHITECTURE.md` và `DATA_MODEL.md` **viết lại hoàn toàn**
> quanh pipeline marketplace, `RUNBOOK.md` thêm những mục còn thiếu, và ba
> việc dọn mà plan §13 chỉ đích danh. Chỉ sửa tài liệu — không một dòng code,
> suite đúng bằng `develop`. Nhánh `phase-8-wp10-docs`, rẽ từ `develop`
> (54e7e80).

### 21.1 Những gì được viết

| File | Việc |
|---|---|
| `docs/ARCHITECTURE.md` | Viết lại: luồng dữ liệu, 10 profile và service, cắt `as_of`, quality gate + con trỏ manifest, **bảng ranh giới hỏng** (mỗi component mất dependency thì sao, restart sửa gì, drill nào chứng minh), vai trò từng kho, pattern, vận hành, và một mục cuối nói rõ Phase 8 **không** chứng minh điều gì. Kaggle còn lại một mục "legacy demo" |
| `docs/DATA_MODEL.md` | Viết lại: ba contract đóng băng (observation / change / DLQ), layout bốn zone, manifest + con trỏ, **mười** bảng `cache.marketplace_*` kèm ba chỗ dễ đọc nhầm, toàn bộ bảng `audit.*`, sáu index Elasticsearch với luật `_id`, bốn khoá Redis, và mô hình Kaggle dồn vào §9 |
| `docs/RUNBOOK.md` | Thêm: seeding (kèm cảnh báo park task của smoke), **kiểm tra vận hành hằng ngày** ánh xạ Brief §20 sang 11 check của `validate`, backfill, và hai ràng buộc hay cắn (`--skip-postgres`, lookback vs interval) |
| `docs/PHASE_6_…md` §16, `docs/PHASE_7_…md` §18 | Thay câu "offline smoke với `--skip-postgres`" bằng ghi chú **Superseded by Phase 8** chỉ sang `mp smoke`, kèm lý do smoke đó không chạy được |
| `docs/PHASE_INDEX.md` | §1 thêm ranh giới **D5** (Phase 8 làm profile *vận hành*; Phase 9 giữ đóng gói *demo*); §5 đổi đoạn "Phase 4/5 chưa từng chạy như dịch vụ" thành phần nối đã xong ở WP1–WP3 (**D1**) |
| `docs/PROGRESS.md` §5 | Ghi **đảo ngược quyết định Airflow** (**D2**): không Airflow, thay bằng scheduler vòng lặp + advisory lock, kèm lý do và hệ quả kiểm được. Dòng Phase 6 trong bảng §3 cũng sửa theo |

### 21.2 Những gì cố ý KHÔNG viết ở đây

Plan §13 liệt kê cho RUNBOOK cả "recovery steps của từng drill", "recreating
the ES indices" và "backup and restore". Ba mục đó **đã có**, trên nhánh
khác, nên viết lại ở đây chỉ tạo conflict:

- D1–D6 ở WP6 (đã trong `develop`), D7–D10 ở nhánh WP7;
- dựng lại index `*-v1` ở nhánh WP8;
- backup/restore ở nhánh WP9.

Vì vậy **`RUNBOOK.md` chỉ trọn vẹn sau khi WP7, WP8 và WP9 merge.** Mục mới
của WP10 được chèn ngay dưới phần `mp`, cách xa chỗ WP8/WP9 chèn (ngay trên
"Image pins"), để ba nhánh không giẫm lên nhau.

### 21.3 Vài chỗ phải tra lại mới dám viết

Tài liệu mô tả "hệ thống như đã dựng", nên mọi con số và tên trường đều đọc
ngược từ code chứ không chép từ plan. Ba chỗ plan và code lệch nhau:

1. **Envelope của observation** không phải `{schema_version, crawl_run_id,
   produced_at, payload}` như trí nhớ, mà có 10 trường, trong đó bốn trường
   bị ràng buộc phải khớp payload (`event_id` = `observation_id`,
   `occurred_at` = `observed_at`, `crawl_run_id` và `raw_uri` cũng vậy).
   Đã viết đúng, và nói rõ vì sao ràng buộc đó tồn tại.
2. **17 quality rule** = 13 mandatory + 4 advisory (đếm trong
   `config/quality_rules.py`), không phải "12 là gate" như bản nháp đầu.
3. **`OfferObservation`** còn `discount_amount`, `discount_percent`,
   `promotion` mà bảng cũ bỏ sót; và nó mang `raw_uri`/`raw_sha256`/
   `adapter_version`/`crawl_run_id` chứ không mang `raw_artifact_id`.

### 21.4 Trạng thái test

- Suite mặc định: **826 pass, 6 deselected**, 2m07s.
- `tests/test_marketplace_quality.py`: **42 pass**.
- **Tổng 868** — đúng bằng `develop`, vì nhánh này không đụng code.

### 21.5 Việc tiếp theo (resume ở đây)

1. Merge theo thứ tự **WP7 → WP8 → WP9 → WP10**. Mỗi lần merge sau sẽ cần gỡ
   một conflict nhỏ ở `PROGRESS.md`/`PHASE_INDEX.md` (bốn nhánh cùng ghi vào
   §5 và bảng trạng thái), và WP9/WP10 thêm một chỗ ở `RUNBOOK.md`.
2. **D11** ngay sau khi WP7 vào `develop` (§20.6) — đây là việc duy nhất còn
   thiếu của Phase 8 so với plan.
3. Gộp `RawArtifactRef.from_uri` với helper tách `raw_uri` của WP7 (§20.8).
4. Sau đó Phase 8 đạt đủ Definition of Done (plan §17), và mở được PR
   `develop` → `master` để thầy hướng dẫn duyệt.

## 22. Session 2026-10-04 (tiếp) — merge WP7–WP10 vào `develop`

> **Trạng thái:** cả bốn nhánh đã vào `develop`. Phase 8 còn **đúng drill
> D11**. Nhánh WP7–WP10 giữ nguyên trên remote làm lịch sử; không xoá vội,
> vì chúng là bằng chứng cho bốn PR chưa bao giờ tồn tại.

### 22.1 Merge tại chỗ, không qua PR

Máy này **không có `gh` CLI**, nên không mở được PR. Bốn nhánh được merge
bằng `git merge --no-ff` ngay trên `develop` rồi push. Kết quả history giống
hệt merge qua PR, chỉ khác là không có số PR; commit merge nói thẳng điều đó
thay vì bịa `Merge PR #13`.

| # | Merge commit | Nhánh |
|---|---|---|
| 1 | `253982d` | `phase-8-wp7-drills-d7-d10` |
| 2 | `2c3200e` | `phase-8-wp8-kibana` |
| 3 | `6e2fef8` | `phase-8-wp9-backup-restore` |
| 4 | `17ba23a` | `phase-8-wp10-docs` |

Đúng thứ tự WP7 → WP8 → WP9 → WP10, vì WP9 và WP10 đều tham chiếu tới thứ
WP7/WP8 mang vào.

### 22.2 Conflict và cách gỡ

Bốn nhánh rẽ từ cùng một `develop` (54e7e80) nên không nhánh nào thấy diff
của nhánh khác — đúng ý luật §3b. Giá phải trả là conflict ở ba file tài
liệu, tất cả đều là **"cả hai bên cùng thêm"**, không bên nào sửa chữ của
bên kia:

| File | Conflict | Gỡ bằng |
|---|---|---|
| `PROGRESS.md` | 3 lần, mỗi nhánh append một §ở cuối file | Giữ **cả hai**, theo thứ tự §18 → §19 → §20 → §21 |
| `PHASE_INDEX.md` | 6 lần, đều ở dòng Phase 8 trong bảng §5 và đoạn số liệu suite | Giữ bản **mới nhất**, rồi viết lại một lần cuối ở §22.3 |
| `RUNBOOK.md` | 2 lần ở merge WP9 | Mục Kibana (WP8) và mục Backup (WP9) **giữ cả hai**; riêng ô `ops` trong bảng profile thì ghép tay, vì hai nhánh cùng sửa đúng một ô |

Một lỗi nhỏ do merge: ba chỗ nối §18/§19, §19/§20, §20/§21 mất dòng trống
trước heading, nên markdown không render heading. Đã sửa trong commit dọn.

### 22.3 Trạng thái sau merge

- Suite mặc định: **952 pass, 10 deselected**, 2m41s. Mười deselected là
  drill D1–D10 (marker `drill`, cần stack đang chạy).
- `tests/test_marketplace_quality.py`: **42 pass**.
- **Tổng 994.** Phép cộng khớp: 826 (`develop` cũ) + 8 (WP7) + 73 (WP8)
  + 45 (WP9) + 0 (WP10, chỉ tài liệu) = 952.
- `git diff --check` sạch.

### 22.4 Việc tiếp theo (resume ở đây)

1. **Drill D11** — việc duy nhất Phase 8 còn thiếu so với plan (§10 bảng
   drill, §12.3). Nay đã hết bị chặn: `ops/drills.py` trong `develop` đã có
   đủ harness D1–D10. Toàn bộ việc D11 phải chứng minh đã chạy tay ở §20.3;
   còn lại là gói vào harness. **Lưu ý:** D11 phải hạ stack chính xuống rồi
   dựng project thứ hai (`container_name` là global), nên nó chạy **cuối
   cùng** trong `drill all` và phải dựng lại stack chính trước khi thoát.
2. Gộp `RawArtifactRef.from_uri` (WP9, `crawler/reparse.py`) với helper tách
   `raw_uri` của WP7 trong `ops/drills.py` — một chỗ duy nhất biết layout
   Bronze.
3. Xong hai việc đó thì Phase 8 đạt Definition of Done (plan §17) và mở được
   PR `develop` → `master` để thầy hướng dẫn duyệt.

## 23. Session 2026-10-04 (tiếp) — drill D11: XONG. Phase 8 đóng.

> **Trạng thái:** D11 (backup/restore) **đã chạy thật và pass ngay lượt đầu**
> trên stack `mp-smoke`. Đây là việc cuối cùng của Phase 8 so với plan. Nhánh
> `phase-8-d11-backup-restore-drill`, rẽ từ `develop` sau khi WP7–WP10 merge.

### 23.1 Gộp helper trùng trước đã

`ops/drills.py` có `_raw_artifact_ref()` tự tách `raw_uri` (WP7), còn
`crawler/reparse.py` có `RawArtifactRef.from_uri()` (WP9) — hai chỗ cùng biết
layout Bronze. Nay chỉ còn một: `_raw_artifact_ref` là lớp vỏ mỏng gọi
`from_uri`, tồn tại để đổi `ValueError` thành `DrillFailed`. `Stack.reparse`
nhận thẳng `RawArtifactRef` thay vì dict. Module **ghi** layout cũng là module
**đọc** nó.

### 23.2 D11 khác mọi drill khác ở chỗ nào

Mười drill trước đều tiêm một sự cố *vào stack đang chạy*. D11 hỏi một câu
khác: **bằng chứng có sống sót khi mất luôn cái máy không?** Nên nó là drill
duy nhất **hạ cả stack xuống** — `container_name` là global, hai project của
cùng file Compose không thể cùng up. Hệ quả thiết kế:

- nó nằm **cuối** registry `DRILLS`, vì `drill all` chạy theo thứ tự đó; đặt
  giữa thì mọi drill sau nó sẽ bắt đầu từ một stack nguội. Một test offline
  chốt cứng `list(DRILLS)[-1] == "d11"`;
- `finally` của nó phải xoá project `<project>-restore` **kèm volume** trước
  khi trả quyền, nếu không `restore()` của harness không dựng lại được stack
  thật — hai project sẽ tranh nhau cùng bộ tên container.

Hai điều nó chứng minh mà một exit code 0 không chứng minh được:

1. **Volume của project đang chạy không hề là đầu vào của restore.** Drill
   chụp danh sách volume trước và sau, và fail nếu khác. Đây là nghĩa thực tế
   của luật "không bao giờ restore đè lên stack đang chạy".
2. **Con trỏ sau restore phục vụ đúng version mà backup đã ghi tên**, chứ
   không phải một version nào đó tự nó mạch lạc. Một stack restore nhầm bản
   vẫn pass mọi check nội bộ.

### 23.3 Kết quả thật (2026-10-04)

| Bước | Quan sát |
|---|---|
| baseline | `validate` xanh, không có vết mutation nào sót |
| backup | `bk-20261004T044256Z`, **1832 file** — gold 31, bronze 920, silver 879, postgres 2. `pointer_run_id` = `cache_run_id` = `mp-20261003T1617Z-d10`, và khớp cả với cái đang được phục vụ |
| restore vào `mp-smoke-restore` | exit 0; **cả ba** check §12.3 PASS |
| quality-only batch | `mp-20261003T1617Z-d10-d11`: `quality_status PASS`, 0 mandatory failure, 878 silver row, `manifest_promoted false` — không đụng con trỏ |
| `validate --restored` | passed; nới **đúng ba** (`es_changes_unique`, `kafka_to_silver_lag`, `redis_offer_state_present`), `unexpected` rỗng |
| con trỏ sau restore | `mp-20261003T1617Z-d10` — đúng bản backup ghi tên |
| volume project chạy | 7 volume, `intact: true` |
| dọn | project restore xoá kèm volume; harness dựng lại stack thật, `validate` xanh |

**Pass ngay lượt đầu**, không phải sửa gì — vì toàn bộ đường đi đã được chạy
tay ở WP9 §20.3, D11 chỉ gói nó lại.

**Chạy `drill d1` ngay sau D11**: pass. Đó là bằng chứng cho câu "D11 dựng
lại stack trước khi thoát" — không chỉ `validate` xanh, mà stack còn chạy
được drill tiếp theo.

### 23.4 Một trục trặc môi trường, không phải bug

Giữa chừng Docker Desktop tắt (`open //./pipe/dockerDesktopLinuxEngine: The
system cannot find the file specified`). Khởi động lại, `compose up -d`, chờ
healthcheck, `validate` 11/11 xanh — không mất dữ liệu, vì tất cả nằm trong
named volume. Không liên quan tới code.

### 23.5 Trạng thái test

- Suite mặc định: **964 pass, 11 deselected** (11 = D1–D11), 2m25s.
- `tests/test_marketplace_quality.py`: **42 pass**.
- **Tổng 1006.** Chênh so với `develop` (994) là 12 test offline mới cho D11:
  1 chốt thứ tự registry, 9 chốt từng điều kiện fail của drill, 2 cho bộ đọc
  JSON của tool container.

### 23.6 Phase 8 đã đóng

Đối chiếu Definition of Done (plan §17): mười một drill đều chạy thật và để
lại `validate` xanh; bốn dịch vụ chạy không người trông; batch chạy qua
`s3a://`; `mp smoke` và `mp validate` pass; Kibana có hai dashboard có dữ
liệu, mỗi panel ghi đúng thứ nó đo; một backup restore được vào project mới
với con trỏ và cache khớp nhau và reparse `IDENTICAL`;
ARCHITECTURE/DATA_MODEL/RUNBOOK mô tả hệ thống như đã dựng; suite mặc định
xanh và offline.

### 23.7 Việc tiếp theo

1. Mở PR `develop` → `master` để thầy hướng dẫn duyệt (`PHASE_INDEX.md` §3b:
   `master` chỉ nhận PR từ `develop`, và chỉ sau khi thầy duyệt).
2. Xoá các nhánh đã merge nếu muốn (convention §18.1); hiện vẫn còn
   `phase-8-wp7…` → `phase-8-wp10-docs` trên remote.
3. **Phase 9** (Tuần 9, P2-01…P2-07): đánh giá và feature freeze. Nó thừa kế
   từ Phase 8 các record drill làm bằng chứng tin cậy, `mp smoke` làm nền cho
   benchmark, stub source làm bộ sinh tải *có nhãn là fixture phát lại*, và
   các index của projector làm chuỗi thời gian vận hành.

## 24. Session 2026-10-04 (tiếp) — Phase 9: plan, và WP0 thu thập thật

Plan: `docs/PHASE_9_EVALUATION_FEATURE_FREEZE_IMPLEMENTATION_PLAN.md`, user
duyệt cả sáu quyết định D1–D6 ngày 2026-10-04. Nhánh `phase-9-evaluation-plan`.

### 24.1 Vì sao WP0 đi trước mọi thứ

Khảo sát lúc viết plan: `audit.crawl_request_attempt` của stack đang chạy chỉ
có 476 attempt trong hai ngày, toàn bộ là smoke/drill gọi stub. **Chưa từng có
thu thập liên tục trên Tiki thật**, trong khi Brief §23 đòi tối thiểu 30 ngày.
Ngày nào chưa thu là mất hẳn ngày đó.

### 24.2 Stack `mp-live`

- Bắt đầu: **2026-10-04 10:16:08 UTC**. Ngày 30 sớm nhất: **2026-11-03**.
- Tập theo dõi đóng băng trong `env/live.env`: category `1846, 8322, 1882,
  1520, 931`, mỗi cái 3 trang, tier `ACTIVE` (60 phút), delay 2 s.
  Tức 15 request/giờ, khoảng 600 offer.
- Chu kỳ đầu: 15/15 `SUCCEEDED`, 600 parsed, 0 rejected, latency p50
  3,674 ms, 2,365,145 byte raw; speed layer 600 change (`NEW_OFFER`).
  Frontier đã tự xếp chu kỳ sau lúc 11:16 UTC.
- Project `mp-smoke` đã `down` (giữ volume) để nhường tên container. Volume
  của `mp-live` là volume mới, không lẫn traffic stub.
- **Quy tắc D1:** không smoke, drill, benchmark hay demo nào chạy trong
  `mp-live`. Cho tới khi WP1 (tiền tố `container_name`) xong, máy này
  **không chạy smoke/drill**, vì chúng cần dừng `mp-live`.

Mỗi lệnh `mp.ps1`/`docker compose` cho stack này phải nạp `env/live.env`
trước (WP1 sẽ thêm `-Env`):

```powershell
Get-Content env/live.env | Where-Object { $_ -match '^[A-Z_]+=' } |
  ForEach-Object { $k, $v = $_ -split '=', 2; Set-Item "env:$k" $v }
.\scripts\mp.ps1 status
.\scripts\mp.ps1 validate -Json data/ops/live/validate-<date>.json   # mỗi ngày
```

### 24.3 Một lỗ hổng tìm được khi bật stack: core không tự khởi động lại

Năm service core (`kafka`, `minio`, `redis`, `postgres-dw`,
`elasticsearch`) không có restart policy, còn các service ứng dụng thì có.
Sau khi máy hoặc Docker khởi động lại, worker lên lại nhưng không có gì để nối
vào, và việc thu thập dừng mà không để lại dấu vết. Đã thêm
`restart: unless-stopped` (commit test `a51aaa0`, commit fix `97460f8`). Drill
chỉ `stop` core, mà `unless-stopped` tôn trọng `stop`, nên không drill nào
đổi hành vi.

### 24.4 Hai cài đặt máy host, nằm ngoài repo

Kiểm 2026-10-04:

- Windows đặt **sleep sau 30 phút** khi cắm điện (`STANDBYIDLE` AC = 1800 s).
  Máy ngủ thì không crawl.
- Docker Desktop **`AutoStart: false`**. Sau khi máy khởi động lại, không có
  gì chạy cho tới khi có người mở Docker.

Cả hai cần user tự đổi. Khoảng trống do chúng gây ra sẽ hiện trong báo cáo
P2-02 như mọi khoảng trống khác, và không được làm sạch đi.

### 24.5 Việc tiếp theo

1. Ngày mai (2026-10-05): kiểm batch theo lịch đầu tiên `SUCCEEDED`, con trỏ
   có giá trị, `validate` pass. Đó là tiêu chí nghiệm thu WP0 (plan §4).
2. WP1: tiền tố `container_name`, `env/bench.env`, `env/demo.env`,
   `mp.ps1 -Env`, chặn smoke/drill/demo trong `mp-live`.

## 25. Session 2026-10-04 (tiếp) — Phase 9 WP1: stack cô lập chạy cạnh `mp-live`

### 25.1 Những gì được thêm

- `container_name: ${MP_CONTAINER_PREFIX:-}<service>` cho cả 21 service. Mặc
  định rỗng, nên tên cũ và stack `mp-live` không đổi.
- `env/bench.env` (`mp-bench`, `bench-`) và `env/demo.env` (`mp-demo`,
  `demo-`). Mỗi file có bộ port riêng, không trùng nhau, và đặt cả địa chỉ
  client phía host mà drill dùng.
- `mp.ps1 -EnvFile <file>`: nạp biến cho đúng một lệnh, rồi trả lại môi
  trường cũ của shell gọi.
- `smoke`, `drill` và `down -Volumes` từ chối `mp-live` với exit 2.
  `ops.drills` có thêm `container()` và `refuse_live()`.
- `tests/test_compose_isolation.py`: 27 test offline.
- RUNBOOK có thêm mục "Four stacks on one machine".

### 25.2 Ba chỗ chỉ lộ ra khi chạy thật

1. **Chặn theo tên project là không đủ.** Không có `-EnvFile` thì tên không
   tiền tố *chính là* container của `mp-live`, và drill kết nối qua port 5433
   của nó. Vì vậy lớp chặn đọc nhãn `com.docker.compose.project` của container
   `kafka` mà lệnh sẽ chạm vào. `ops.drills` kiểm lại trước baseline, trước
   drill và trước bước restore trong `finally`.
2. **PowerShell 5.1 làm mất dấu `"` bên trong tham số truyền cho chương trình
   ngoài.** Template `{{index .Config.Labels "com.docker.compose.project"}}`
   bị hỏng, nên lần thử đầu lớp chặn trong `mp.ps1` để lọt. Drill vẫn bị chặn,
   nhưng là nhờ lớp Python. Đã đổi sang đọc nhãn dạng JSON. Đây cũng là nguyên
   nhân lỗi `function "com" not defined` gặp ở WP0.
3. Nếu env file chỉ đổi `*_HOST_PORT`, drill trên host vẫn truy vấn PostgreSQL
   của `mp-live`. Vì vậy mỗi file đặt cả `POSTGRES_PORT`,
   `KAFKA_BOOTSTRAP_SERVERS`, `MINIO_ENDPOINT`, `REDIS_PORT` và `ES_HOST`.

### 25.3 Nghiệm thu (plan §5), chạy thật 2026-10-04

- `mp -EnvFile env/live.env smoke`, `mp smoke` không env, `mp drill d1` không
  env, `mp -EnvFile env/live.env down -Volumes`: cả bốn exit 2 và không chạm
  vào gì.
- `mp -EnvFile env/bench.env smoke`: **SMOKE PASSED**, `validate` 11/11, batch
  `mp-20261004T1205Z` `SUCCEEDED`, trong khi `mp-live` vẫn chạy.
- `StartedAt` của 10 container `mp-live` trước và sau giống hệt nhau
  (`data/ops/live/startedat-{before,after}-wp1.txt`). Audit live không có
  attempt nào của task smoke. Chu kỳ live lúc 12:17 vẫn chạy đúng lịch:
  45/45 attempt, 1.800 parsed.
- Sau đó `mp-bench` được `down --volumes`.

### 25.4 Trạng thái test

Suite mặc định: **1038 pass**, 11 deselected (D1–D11), 16m28s. Chạy chậm vì
`mp-live` dùng chung CPU (lúc máy rảnh là 2m25s). Tăng 27 so với 1011.

### 25.5 Việc tiếp theo

1. 2026-10-05: nghiệm thu WP0, tức batch theo lịch đầu tiên của `mp-live`.
2. WP2: `audit.marketplace_stream_progress`, ba cột latency,
   `audit.storage_snapshot`.

## 26. Session 2026-10-04 (chiều) — Phase 9 WP2–WP8: viết xong và chạy thật

User đi vắng khoảng 2 tiếng và bảo làm tiếp. Mỗi WP đều có test offline và
một lần chạy thật trên stack cô lập cạnh `mp-live`.

| WP | Commit | Chạy thật |
|---|---|---|
| WP2 progress, latency, snapshot | `b6360de`, test `1b9c233` + fix `99bd092` | bench smoke; rồi triển khai lên `mp-live` (migrate, restart `speed`, tạo lại `es-projector`) |
| WP3 stub ID + load generator | test `fe6656f` + fix `78e0b15`, `ba4578b` | qua bench ingest |
| WP4 benchmark runner | `475902e` | `bench ingest --repeat 1 --sizes 10000` |
| WP5 reliability + freshness | `b0360eb` | `evaluate reliability\|freshness` trên `mp-live` |
| WP6 storage growth | `5c5f93f` | trên `mp-live` bị từ chối đúng (mới 1 ngày snapshot) |
| WP7 demo | `1886457` | `demo --without-serve --auto --snapshot`: 7/7 bước PASS, cả D3 và D8 |
| WP8 evidence | `e12a45c` | `evidence --skip-tests`: còn MISSING đúng những mục chưa có |

### 26.1 Sáu chỗ chỉ lộ ra khi chạy thật

1. **Spark phát idle progress mang batchId của batch sắp chạy.** Khi lưu
   event đó, recorder chiếm chỗ batch thật, và số liệu thật bị primary key từ
   chối. Tìm được trên `mp-live` (batch 238). Đã sửa: chỉ lưu event có
   `durationMs.addBatch`. Test và fix ở hai commit riêng. Dòng sai đã xoá.
2. **Kafka 4 bỏ `DescribeLogDirs` v0/v1 (KIP-896).** kafka-python không đo
   được log dir nữa. Đã đổi sang mount read-only volume `kafka_data` và cộng
   dung lượng thư mục partition.
3. **File index của Kafka là sparse, tạo trước 10 MiB.** Đo theo kích thước
   logic, `__consumer_offsets` hiện thành 1 GB, trong khi thực dùng 400 KB.
   Đã đổi sang đo theo `st_blocks`.
4. **`reltuples` bằng 0 khi bảng chưa ANALYZE.** Postgres chuyển sang dùng
   `n_live_tup`.
5. **Occurrence seed có `scheduled_for = 1970-01-01` (có chủ ý).** Đo độ trễ
   lịch theo cột này cho ra 1,79 tỷ giây. Đã loại occurrence seed, và thêm
   phép đo khoảng cách giữa hai lần crawl thành công của cùng một trang.
6. **Latency đo từ `produced_at`, không từ Kafka timestamp.** Giữ
   `kafka_timestamp` qua operator có state sẽ làm đổi schema output mà
   checkpoint `v2` của `mp-live` đang dùng. Hai mốc này trùng nhau tới vài ms
   (CreateTime). Plan §6.1 đã ghi lại thay đổi này.

### 26.2 Số đo đầu tiên trên `mp-live` (cửa sổ 0,13 ngày, chưa là kết quả)

- Crawl: 60/60 attempt thành công. Latency p50 3.060 ms, p95 3.910 ms.
  Khoảng cách giữa hai lần crawl một trang có p50 3.624 s, 100 % trong 110 %
  cadence. Độ trễ lịch p50 21 s, tối đa 32 s.
- Từ crawl tới Kafka (`produced_at − fetched_at`): p50 29 ms, p95 445 ms.
- Latency speed (tới khi sink ghi xong): p50 8–24 s, p95 tới khoảng 30 s.
  Con số này chủ yếu là trigger 30 s.
- Bench ingest 10.000 (replayed_fixture, 1 lần, máy đang bận): Silver xả
  khoảng **100 record/s**. Sink ghi mỗi observation một object JSON lên MinIO,
  có thể là nút thắt. Cần `--repeat 3` lúc máy rảnh mới thành kết quả.

### 26.3 Batch theo lịch đầu tiên của `mp-live`

`mp-20261004T0000Z` chạy lúc 10:21 cho cửa sổ trước khi thu thập bắt đầu.
Silver có 0 dòng, nên `silver_parse_attempt_reconciliation` (MANDATORY) bị
SKIPPED, và theo thiết kế Phase 7 điều đó tính là fail. Kết quả
`QUALITY_FAILED`, không publish. Đúng hành vi. Run có dữ liệu đầu tiên là
`mp-20261005T0000Z`, sau 00:30 UTC ngày 2026-10-05.

### 26.4 Trạng thái test

Suite mặc định trên `e12a45c`: **1140 pass**, 11 deselected (D1–D11),
12m30s, chạy khi `mp-live` đang hoạt động. Tăng 102 so với 1038 sau WP1.

### 26.5 Còn mở

1. **`docs/SOURCE_FEASIBILITY.md` có bản nháp** (`0d8dad1`). File chỉ tổng
   hợp số đã đo ở §4, §4a và audit `mp-live`, và **chờ thầy duyệt**.
2. **`bench all --repeat 3` đang chạy** từ 2026-10-04 khoảng 14:15 UTC, ước
   tính 7 giờ. Log ở `data/ops/bench-all.log`, kết quả ở `data/ops/bench/`.
   Trước đó đã chạy trial 1 lần cho từng kịch bản (lưu ở
   `data/ops/bench-trial/`, không tính là kết quả). Trial tìm ra một bug:
   trang stub luôn `PARTIAL`, mà kịch bản crawl chỉ chờ `SUCCEEDED` (test
   `effea85`, fix `1f40de2`). Kịch bản batch trên dữ liệu phát lại luôn
   `QUALITY_FAILED`, vì crawl run giả không có trong audit nên rule đối soát
   fail. Thời gian chạy batch vẫn là số đo hợp lệ, nhưng báo cáo phải ghi rõ
   điều này.
3. **Demo đầy đủ (có Kibana và Superset)** mới chỉ chạy `--without-serve`, vì
   RAM dùng chung với `mp-live`. Cần chạy khi có người theo dõi.
4. **Integration run (plan §12.2) và tag `feature-freeze-w9`**: chờ user.
5. Ngày 2026-11-03 mới đủ 30 ngày thu thập. Sau đó chạy lại `evaluate` và
   `evidence`.

### 26.6 Việc tiếp theo (cũ, nay resume ở §28.5)

**Đang chạy, không phụ thuộc session Claude nào:**

- `mp-live`: thu thập Tiki thật từ 2026-10-04 10:16 UTC. Kiểm bằng
  `.\scripts\mp.ps1 -EnvFile env/live.env status`.
- `bench all --repeat 3`: khởi động lại lúc 2026-10-04 khoảng 14:50 UTC, dưới
  dạng tiến trình PowerShell tách rời (`Start-Process`, ẩn cửa sổ). Lý do:
  tác vụ nền của session sẽ chết khi đóng Claude Code. Ước tính xong khoảng
  22:00 UTC.
  - Tiến độ: `Select-String '"bench_run"|bench_failed|exit=' data\opsench-all.log`.
  - Xong khi log có dòng `exit=0 finished=...`.
  - Kết quả ở `data/ops/bench/*.json`; bảng xem bằng
    `.\scripts\mp.ps1 -EnvFile env/bench.env bench report`.
  - Dừng giữa chừng: kill các process có `ops.bench` trong command line, rồi
    `docker compose -f docker-compose.yml -p mp-bench --profile "*" down --volumes`
    (đặt `$env:MP_CONTAINER_PREFIX="bench-"` trước).

**Thứ tự việc tiếp theo:**

1. **2026-10-05, sau 00:30 UTC:** nghiệm thu WP0.
   - Batch `mp-20261005T0000Z` phải `SUCCEEDED`, con trỏ có giá trị, và
     `mp -EnvFile env/live.env validate` pass.
   - Nếu batch `QUALITY_FAILED`, đọc `audit.marketplace_quality_result`
     trước, đừng sửa gì vội.
   - Từ ngày này `evaluate storage` cũng chạy được (đủ 2 ngày snapshot).
2. **Khi benchmark xong:**
   - Kiểm `bench report`, và ghi các số chính cùng điều kiện đo vào một mục
     PROGRESS mới. Mỗi file có `live_stack_running_containers = 10`, tức là đo
     khi `mp-live` chạy cùng.
   - Chạy `mp -EnvFile env/live.env evidence`. Mục còn MISSING khi đó chỉ nên
     là những gì chưa tới hạn.
3. **Chờ user hoặc thầy:**
   - Duyệt `docs/SOURCE_FEASIBILITY.md` (bản nháp).
   - Chạy demo đầy đủ có Kibana/Superset khi có người theo dõi RAM.
   - Integration run (plan §12.2), rồi tag `feature-freeze-w9`.
   - Merge `phase-9-evaluation-plan` vào `develop`, rồi mở PR `develop` →
     `master`. Nội dung PR đã soạn sẵn ở `data/ops/pr-develop-to-master.md`
     (gitignored, chỉ có trên máy này). Nó viết trước Phase 9, nên phải bổ
     sung Phase 9 trước khi mở PR.
4. **2026-11-03:** đủ 30 ngày thu thập. Chạy lại `evaluate` và `evidence`.

## 27. 2026-10-05 (job tự động, sáng sớm) — `bench all --repeat 3` đã chạy xong

Job chạy không người trông lúc 2026-10-04 22:43 UTC. Không sửa code, không
đụng `mp-live`.

### 27.1 Run kết thúc ra sao

- Bắt đầu 2026-10-04 14:48 UTC. File kết quả cuối (`batch-n-200000`) ghi lúc
  khoảng 21:12 UTC. Tổng khoảng **6 giờ 25 phút**.
- Đủ **36/36 lượt** (4 kịch bản × 3 biến thể × 3 lần), 11 file trong
  `data/ops/bench/`. Không có `bench_failed`.
- **Log không có dòng `exit=`.** Lúc kiểm, không còn process `ops.bench`, và
  không còn container `bench-*` nào, nên stack `mp-bench` đã tự xuống. Dòng
  `exit=` do lệnh bọc ngoài ghi, sau khi `mp.ps1` trả về. `mp.ps1` kết thúc bằng
  `exit $ExitCode`, nên nhiều khả năng lệnh bọc bị thoát cùng và không chạy tới
  dòng đó. Python chỉ trả mã khác 0 khi có `bench_failed`, nên coi như exit 0.
  Không có sleep/restart máy quanh lúc đó (System event log). Lần sau nên bọc
  `mp.ps1` trong `powershell -File` riêng, rồi ghi `exit=` ở process cha.
- Mọi file đều có `live_stack_running_containers = 10`, tức là **đo khi
  `mp-live` chạy cùng**. `git_dirty = false`. Crawl đo ở `faace6c`, các kịch bản
  khác ở `f8f32c2`. Giữa hai commit chỉ khác `docs/PROGRESS.md`. Image
  `marketplace-python` có digest khác nhau giữa các kịch bản (build lại mỗi
  project), code bên trong như nhau.
- Máy: Windows 11, Docker Desktop 28.3.2 (WSL2), VM 8 CPU, 16 GB. Mọi số là
  `dataset = replayed_fixture` (stub source), không phải số đo trên Tiki.

### 27.2 Số chính (trung vị của 3 lần, kèm min–max)

Crawl, 120 request, 3.240 dòng parse, mỗi lần:

| delay | thời gian (s) | request/s | parse/s | latency p50 (ms) | p95 (ms) | failed |
|---|---:|---:|---:|---:|---:|---:|
| 2 s | 365 (358–372) | 0,329 | 8,88 | 2.527 (2.480–2.589) | 3.025 | 0 |
| 0 s | 128 (122–134) | 0,939 | 25,4 | 562 (487–619) | 1.036 | 0 |

120/120 là `PARTIAL`, đúng thiết kế của trang stub (§26.5).

Ingest (load generator → Kafka → Silver):

| n | produce (rec/s) | Silver xả (rec/s) | tổng (s) | Silver latency p50 (s) | p95 (s) |
|---|---:|---:|---:|---:|---:|
| 10.000 | 483 | 80,0 (78,2–80,6) | 125 | 51 | 100 |
| 50.000 | 493 | 94,2 (93,5–95,2) | 531 | 209 | 403 |
| 200.000 | 537 | 116 (104–128) | 1.724 (1.567–1.917) | 659 | 1.290 |

Silver xả chậm hơn produce khoảng 4–6 lần, và latency tăng gần tuyến tính theo
n: record xếp hàng sau sink. Khớp với nhận xét §26.2 (sink ghi từng object lên
MinIO là nút thắt). Mức 200.000 dao động nhiều nhất (±10 % quanh trung vị).

Speed (300 s mỗi mức):

| rate yêu cầu | rate thực phát | xử lý (row/s) | kịp? | batch p50 (s) | latency batch p50 (s) | latency max (s) |
|---|---:|---:|---|---:|---:|---:|
| 50/s | 50 | 50,2 | có, xả 26 s | 4,3 | 19,3 | 35,2 |
| 200/s | 200 | 200,6 | có, xả 20 s | 13,5 | 28,6 | 48,1 |
| 1000/s | **548** | 432 (387–445) | không | 43,5 | 66,6 | 164 |

Ở mức 1000/s, generator chỉ phát được khoảng 548/s (cùng trần với produce của
ingest), nên mức này thực chất là "khoảng 550/s". Ngưỡng chịu được của speed
nằm giữa 200 và 550 row/s trên máy này. Báo cáo phải ghi rõ trần của generator.

Batch (trên dữ liệu phát lại):

| n Silver | thời gian (s) | Silver row/s | Gold rows |
|---|---:|---:|---:|
| 10.000 | 37,9 (37,8–38,4) | 264 | 8.011 |
| 50.000 | 105,8 (105,6–105,9) | 473 | 8.011 |
| 200.000 | 374,3 (373,0–374,8) | 534 | 8.011 |

Cả 9 lượt là `QUALITY_FAILED`, đúng như dự kiến: crawl run giả không có trong
audit nên rule đối soát fail. Thời gian chạy vẫn là số đo hợp lệ. Batch ổn định
nhất trong 4 kịch bản (chênh dưới 2 %). Gold luôn 8.011 dòng vì universe của
fixture cố định.

### 27.3 Một phát hiện trên `mp-live` trong lúc kiểm

Trang `{"page":3,"target":"1882"}` **không được crawl lại từ 2026-10-04
15:18 UTC**. Từ 16:00 UTC mỗi giờ chỉ còn 14/15 trang.

- Attempt duy nhất bị `FAILED`, `error_kind = UNKNOWN`, thông điệp
  `raw artifact fetched_at must lie between started_at and completed_at`
  (`crawler/contracts.py:397`). Tức là hai mốc thời gian đọc từ hai đồng hồ
  lệch nhau.
- Lúc đó benchmark vừa chuyển từ crawl sang ingest (15:17 UTC), máy đang tải
  nặng. Có thể đồng hồ WSL bị chỉnh lùi. Chưa xác minh.
- Frontier: `status = FAILED`, `attempts = 1`/`max_attempts = 5`, và không
  có occurrence tiếp theo. Trang này đã rơi khỏi lịch hẳn.
- 187 attempt khác đều `SUCCEEDED` (p50 2.901 ms, p95 3.713 ms).

Chưa sửa gì (job tự động không được sửa code hay dữ liệu live). Cần user quyết:
lỗi này nên được retry hay không, và có đưa trang về lịch không. Nếu để vậy,
30 ngày thu thập sẽ thiếu 1/15 trang từ ngày đầu.

### 27.4 Việc tiếp theo

Vẫn theo §26.6, trừ mục "khi benchmark xong" đã làm phần `bench report` và
mục này. Còn lại: chạy `mp -EnvFile env/live.env evidence`, nghiệm thu batch
`mp-20261005T0000Z` (job 07:47 sáng nay), và xử lý §27.3.

## 28. 2026-10-05 07:47 (job tự động) — nghiệm thu WP0: chưa đạt, scheduler bỏ qua cửa sổ

Job chạy không người trông lúc 00:47 UTC. Không sửa code, không đụng dữ liệu
`mp-live`. Báo cáo đầy đủ ở nhánh `daily-reports`, `reports/2026-10-05.md`.

### 28.1 Batch `mp-20261005T0000Z` không chạy

`audit.marketplace_batch_run` chỉ có `mp-20261004T0000Z`. Log
`batch-scheduler` có ba tick, cả ba đều `as_of = 2026-10-04`, đều
`QUALITY_FAILED` (`silver_parse_attempt_reconciliation` SKIPPED vì cửa sổ có 0
dòng Silver, như §26.3):

| tick in ra lúc (UTC) | ghi chú |
|---|---|
| 2026-10-04 10:22:02 | tick lúc khởi động |
| 2026-10-05 00:29:30 | thức sớm khoảng 50 s so với mốc 00:30:00 |
| 2026-10-05 00:30:20 | `started_at` 00:30:00,05, xong 00:30:19,59 |

Nguyên nhân, suy từ `batch_layer/marketplace_scheduler.py`:

1. `run_scheduler` ngủ bằng `stop.wait(timeout)`, tức là theo đồng hồ
   monotonic. Sau 14 giờ ngủ, nó thức khi wall clock còn trước 00:30, nên
   `window_as_of` vẫn ra cửa sổ 10-04, và nó resume run cũ.
2. Lần chờ 30 s tiếp theo thức sớm vài ms. `as_of` vẫn là 10-04, trong khi
   `started_at` ghi ngay sau đó đã là 00:30:00,05.
3. Tick đó xong sau mốc. `next_wake(now)` trả về mốc của cửa sổ *sau* cửa sổ
   hiện tại (2026-10-06 00:30), vì nó giả định cửa sổ hiện tại vừa chạy. Cửa sổ
   10-05 bị bỏ qua.

Scheduler vẫn sống và đang ngủ tới 2026-10-06 00:30. Test hiện có dùng clock
giả luôn đúng mốc, nên không bắt được. Hướng sửa (chưa làm): sau khi thức, ngủ
tiếp tới khi wall clock qua mốc, và tính mốc kế tiếp từ `as_of` vừa chạy, kèm
test với clock thức sớm. Lỗi crawl §27.3 cũng dính tới đồng hồ, có thể chung gốc
(đồng hồ VM WSL2), chưa xác minh.

### 28.2 Nghiệm thu WP0: chưa đạt

| điều kiện (§26.6 bước 1) | kết quả |
|---|---|
| batch `mp-20261005T0000Z` SUCCEEDED | không, batch không chạy |
| con trỏ có giá trị | không, `null` |
| `validate` pass | không, 7 PASS / 4 FAIL |

Bốn check fail đều vì chưa có con trỏ (`pointer_gold_exists`,
`pointer_matches_cache`, `quality_results_complete`,
`silver_reconciles_with_audit`). Phần thu thập của WP0 thì đạt: 10/10 container
healthy, 216 attempt, 215 `SUCCEEDED`, 8.600 dòng parse, latency p50 2.908 ms,
p95 3.724 ms, khoảng cách crawl một trang p50 3.623,6 s (100 % trong 110 %
cadence).

### 28.3 Evaluate và evidence

- `evaluate reliability|freshness|storage` đều chạy được (cửa sổ 0,588 ngày).
  Storage nay có 2 snapshot: tăng khoảng 64,4 MB/ngày, 10.754 byte/observation,
  dự báo ngày 30 khoảng 2,03 GB. Freshness theo offer còn rỗng vì chưa có Gold
  publish. Crawl → Kafka p50 29,8 ms, p95 116,8 ms.
- `evidence`: bundle `data/ops/evidence/20261005T004946Z` (commit `bd98dd8`,
  cây sạch). Suite chạy kèm: **1141 pass**, 0 fail, 741 s, khi `mp-live` đang
  chạy. 15 PRESENT, 1 MISSING, 3 MANUAL. So với bundle 2026-10-04 13:37 UTC,
  thêm PRESENT các mục 1 (bản nháp SOURCE_FEASIBILITY), 11, 12 (benchmark) và
  13 (storage).
- Mục MISSING duy nhất: **15, ví dụ lịch sử/thay đổi/bất thường giá**. Mục này
  cần Gold đã publish từ dữ liệu thật, nên đang bị chặn bởi §28.1, không phải
  chỉ "chưa tới hạn". Ba mục MANUAL (ảnh Superset/Kibana, phần hạn chế, video
  demo) là việc tay.

### 28.4 Máy

Ổ C: còn 177,7 GB. VM Docker 16,3 GB RAM, 10 container `mp-live` dùng khoảng
5,8 GiB. Đồng hồ container và host lệch dưới 0,1 s lúc kiểm.

User đã lên lịch `mp-shutdown` lúc 08:50 giờ máy hôm nay: dừng `mp-live`
(giữ volume), tắt máy, và sẽ chuyển thu thập sang host khác.

### 28.5 Việc tiếp theo (resume ở đây)

1. **Sửa scheduler** (§28.1), có test, rồi triển khai lên stack thu thập.
2. Quyết định có chạy bù cửa sổ 2026-10-05 không (chạy tay `mp -EnvFile
   env/live.env batch` với as_of đó, hoặc restart `batch-scheduler`). Chỉ làm
   được khi stack chạy lại.
3. Trang `1882` p3 (§27.3).
4. Trên host mới: kiểm NTP, rồi nghiệm thu WP0 lại: một batch `SUCCEEDED` trên
   dữ liệu thật, con trỏ có giá trị, `validate` pass. Sau đó chạy lại
   `evidence`, mục 15 phải thành PRESENT.
5. Các mục chờ user hoặc thầy ở §26.6 bước 3 vẫn giữ nguyên. Mốc 30 ngày
   (2026-11-03) tính từ ngày thu thập bắt đầu, nên sẽ dời nếu thu thập bị gián
   đoạn khi chuyển host.
