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
| 6 | Orchestration (Airflow) | ⏳ Chưa bắt đầu | |
| 7 | Serving API (FastAPI) | ⏳ Chưa bắt đầu | |
| 8 | Dashboards + thesis polish | ⏳ Chưa bắt đầu | |

Legend: ✅ xong · 🔶 đang làm/dở · ⏳ chưa bắt đầu · ⏸️ tạm hoãn

## 4. Chi tiết Phase 1 (crawler) — phase đang làm

### Đã xong (commit `727bcf6`, đã push lên `origin/master`)

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
- **Orchestration**: Airflow 1 container LocalExecutor, tái dùng `postgres-dw`
  làm metadata DB — không Celery cluster.
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
- Commit crawler layer: `727bcf6` (branch `master`, đã push)

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
commit gốc chung (`4de5ac1`), nên phiên làm việc sau rẽ từ đó không nhìn thấy
và tự viết plan mới. Hai bộ tên biến, hai bộ tên module cho cùng một việc.

Bài học: **plan là hợp đồng, phải nằm ở nhánh chung.** Một hợp đồng chỉ một
nhánh nhìn thấy thì không phải hợp đồng.

### 8.2 Mô hình nhánh mới (chốt trong session này)

```text
phase-<n>-<tên>  --PR-->  develop  --PR (chỉ khi thầy hướng dẫn duyệt)-->  master
```

- `master`: **không nhận commit trực tiếp nữa**, kể cả docs. Chỉ nhận PR từ
  `develop` sau khi thầy duyệt. Hiện đang ở `f4c6dcb`, đúng bản gốc.
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
master              f4c6dcb   bản gốc, chờ thầy duyệt
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
develop                            6f746f0  docs: adopt the phase 7 plan
phase-7-quality-anomaly-replay     f8a13d5  10 commit, PR #4 -> develop (MERGED 2026-09-30)
```

Plan nằm ở `docs/PHASE_7_QUALITY_ANOMALY_REPLAY_IMPLEMENTATION_PLAN.md`
(1334 dòng), **commit vào `develop` trước khi viết dòng code nào** — đúng
§4.2, quy tắc thêm sau sự cố 2026-09.

PR #4: https://github.com/Duy-Duc82/ecommerce-lambda-architecture/pull/4
20 file, +3601/−43. Đã merge vào `develop` (`4d14354`) ngày 2026-09-30.

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
mọi check" đỏ trên fixture có counter null. Sửa trong `338092e`.

### 9.6 Ba chỗ lệch khỏi plan, đều có lý do

1. **WP1 tách thành 2 commit.** Plan bảo nối mart thứ 10 vào
   `build_marketplace_marts` ngay, nhưng `write_run_scoped_gold` và `stage()`
   còn đòi đúng 9 → suite đỏ giữa WP1 và WP6. Đã kéo phần *hợp đồng dataset thứ
   10* (`DATASET_COLUMNS` + cache DDL) vào WP1; WP6 giữ phần *gating*.
2. **WP6 và WP7 gộp một commit** (`f8ba430`). `publish()` nhận `QualityDecision`
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
fix sau — `939d2a6`/`9a7d28b` và `2944391`/`0405e58`.

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

Plan Phase 8 sửa theo ba điều WP1 phát hiện (commit `03b820f`):

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

Hai bug này theo đúng quy tắc repo, mỗi bug một cặp commit test rồi fix: `4205ee6`/`254190e` và `1d66b2b`/`6ababaa`. Bug connection lộ ra khi viết entrypoint, nên factory và test của nó nằm luôn trong commit feature `7adf9a6`.

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
