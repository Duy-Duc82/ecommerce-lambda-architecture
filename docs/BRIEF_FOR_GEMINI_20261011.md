# Brief: Project context for literature and data-source research

> Purpose: give a research assistant (Gemini, Perplexity, etc.) enough context to find **relevant papers/surveys** and **data sources**.
> Date: 2026-10-11. Deadline for the advisor report: about 2026-10-18.

## 1. What the project is

A graduation thesis (Data Engineering) building a **multi-site price intelligence and anomaly detection platform** for **Vietnamese e-commerce**.

- Crawl public product and price data from many Vietnamese e-commerce sites, on a repeating schedule (hourly or daily).
- Store it, process it with a Lambda architecture, run ML, and show dashboards.
- **Data constraint from the advisor:** crawl web pages directly (HTML, browser rendering). Do **not** rely on private or internal APIs.

## 2. System architecture (already built)

```
Crawlers (Python: urllib + HTML parsing, Playwright for JS pages)
   │  raw HTML/JSON saved first (Bronze, raw-first; can re-parse later)
   ▼
Kafka topic `ecommerce_price_snapshots`
   ├─► Speed layer: Spark Structured Streaming → real-time price-change rules → Elasticsearch/Kibana
   └─► Batch layer: Spark → MinIO (S3A) Delta Lake warehouse (Bronze/Silver/Gold)
            → star schema (fact_price_snapshot, dim_site, dim_product, dim_date…)
            → data marts + ML jobs → Postgres (serving cache) → Superset dashboards
Orchestration: scheduler / Airflow. Data-quality checks and outlier rates shown on dashboards.
```

- **ML already implemented:** a rolling **MAD (median absolute deviation)** price-anomaly detector with an IQR fallback, computed per product per day in Spark.
- **ML libraries available:** darts (forecasting), pyod (anomaly detection), PyTorch, scikit-learn, Spark MLlib.

## 3. Data the crawler produces (one row = one product observed at one point in time)

| Field | Meaning |
|---|---|
| snapshot_time | When it was crawled |
| site | Source site (tiki, lazada, cellphones, …) |
| product_id | Site-specific ID (not comparable across sites) |
| product_name | Raw title (Vietnamese, noisy: "Điện thoại Samsung Galaxy A27 5G 6GB/128GB – Chính hãng") |
| category_path | Category hierarchy (ragged, differs per site) |
| brand | Brand |
| price | Current selling price (VND) |
| list_price | "Original" price before discount |
| rating, review_count | Rating and number of reviews |
| seller_name | Shop/seller (marketplaces have many sellers per product) |
| in_stock | Availability |
| url | Product URL |

Some sites also expose a **"sold count"** (Tiki "đã bán") and **merchant offer lists**, from a price-comparison site.

**Expected properties of the data:**
- Panel time series: thousands to tens of thousands of products × many snapshots per day, across 5–10 sites.
- Noisy, multilingual (mostly Vietnamese) text.
- Heavy-tailed prices.
- Fake or "bait" listings (very low prices, accessories listed under phone categories).
- Frequent promotions; possibly inflated list prices ("fake discounts").
- No user behavior data (views, carts, purchases are not visible to a crawler).

## 4. Research request A — papers and surveys

Find **surveys first**, then key papers (2018–2026 preferred, classic ones allowed), for ML problems that can be solved with the data in section 3.

| # | Problem | Example search directions |
|---|---|---|
| 1 | Price anomaly / outlier detection on price time series | time-series anomaly detection surveys; e-commerce pricing-error detection |
| 2 | Price forecasting / dynamic pricing analysis | e-commerce price forecasting, panel/global forecasting models, dynamic pricing surveys |
| 3 | Product matching / entity resolution across shops | product matching (e.g. Ditto, WDC Products benchmark), multilingual/Vietnamese title matching |
| 4 | Fraudulent / fake listing detection in marketplaces | marketplace fraud, counterfeit/scam listing detection |
| 5 | Fake discount / reference-price manipulation | inflated reference prices, deceptive discounts |
| 6 | Demand estimation from public signals (sold count, reviews, rank) | sales-rank → demand, review-count proxies |
| 7 | Product categorization from titles | product title classification, hierarchical categorization |
| 8 | Price dispersion and competition across online retailers | economics/IS papers on online price dispersion and price comparison sites |

For each paper, return: **title, authors, year, venue, verified link (DOI/arXiv), problem, method, dataset used, and how it applies to our data**. Do not invent citations: only list papers with a working link.

## 5. Research request B — data sources (optional cross-check)

We need sources that are:
- easy to crawl from HTML;
- stable for continuous scheduled crawling (no captcha or anti-bot walls);
- allowed by robots.txt;
- carry prices that change often;
- ideally cover the same products across multiple shops (for matching and comparison).

**Already measured (2026-10-09/11):**
- **Blocked:** Shopee (anti-bot/login wall), TikTok Shop (captcha), Long Châu / Fahasa / Guardian (HTTP 403).
- **Crawlable from HTML:** Tiki, Thế Giới Di Động, Điện Máy Xanh, CellphoneS, FPT Shop, Nguyễn Kim, Phong Vũ, GearVN, Di Động Việt, Điện Máy Chợ Lớn, Chợ Tốt, Websosanh.vn (price comparison).
- **Needs a browser:** Lazada, Bách Hóa Xanh, Hasaki.

Useful additions:
- other Vietnamese price-comparison or aggregator sites;
- public datasets of Vietnamese or Southeast Asian e-commerce prices;
- public product-matching benchmarks.
