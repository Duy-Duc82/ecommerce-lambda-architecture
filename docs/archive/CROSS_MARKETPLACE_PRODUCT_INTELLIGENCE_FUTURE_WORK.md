# Bản đề xuất future work — Cross-Marketplace Product Intelligence

> Tài liệu này là Brief pivot ban đầu. Sau khi khóa scope DATN 3 tháng, hướng
> cross-marketplace product matching/comparison được chuyển thành future work.
> Kế hoạch đang có hiệu lực nằm tại file Brief ở repository root.

## 1. Bối cảnh project hiện tại

Repository hiện tại: `ecommerce-lambda-architecture`.

Project đang là một nền tảng phân tích hành vi e-commerce theo Lambda Architecture.

Nguồn dữ liệu hiện tại là Kaggle historical clickstream gồm khoảng:

- 109,950,743 events
- ~14.7 GB
- 61 ngày dữ liệu
- Các event chính:
  - `view`
  - `cart`
  - `purchase`
- Có:
  - `user_id`
  - `user_session`
  - `product_id`
  - `category`
  - `brand`
  - `price`
  - timestamp

Điểm mạnh của dataset cũ là nó cung cấp một event log khá đầy đủ về hành vi người dùng trong một khoảng thời gian liên tục, cho phép phân tích funnel, session, conversion và revenue.

Kiến trúc hiện tại gồm:

- Kafka
- Spark Structured Streaming
- Spark batch processing
- MinIO Bronze / Silver / Gold
- PostgreSQL BI cache + audit
- Elasticsearch
- Redis
- Superset
- Kibana
- ML forecasting + anomaly detection
- Data quality gate
- Idempotency
- Quarantine invalid records
- Audit pipeline
- Automated tests

Hiện tại ingestion chủ yếu là replay CSV historical vào Kafka.

---

# 2. Vấn đề của hướng hiện tại

## 2.1 Data acquisition chưa phản ánh hệ thống thực tế

Pipeline processing tương đối hoàn chỉnh nhưng source vẫn là dataset được tải sẵn.

Điều này khiến realtime hiện tại chủ yếu là:

`historical data -> replay -> Kafka`

chứ chưa phải:

`real external system -> continuous acquisition -> Kafka`

Đối với đồ án tốt nghiệp, mong muốn là dữ liệu phải được hệ thống tự thu thập từ các nguồn thực tế thay vì tải dataset có sẵn hoặc sử dụng API cung cấp dataset.

---

## 2.2 Ý tưởng thay Kaggle bằng crawler Shopee không tương thích với behavioral analytics

Dataset Kaggle hiện tại có grain:

`(user, session, product, action, timestamp)`

Crawler public Shopee không thể lấy được các private behavioral events như:

- ai xem sản phẩm
- ai add-to-cart
- ai purchase
- user session
- conversion path

Crawler chỉ có thể quan sát public state của marketplace tại một thời điểm.

Ví dụ:

`product + marketplace + seller + timestamp -> price, sold_count, rating, review_count,...`

Do đó không thể tái tạo chính xác:

`view -> cart -> purchase`

và không nên cố invent behavioral events từ các counter public.

Ví dụ:

`sold_count: 1000 -> 1020`

chỉ cho biết counter tăng 20 trong khoảng thời gian quan sát, không cung cấp 20 purchase events có timestamp/user/session.

---

# 3. Quyết định pivot

Không tiếp tục cố duy trì bài toán:

**E-commerce Behavioral Analytics**

Thay vào đó pivot project thành:

# Cross-Marketplace E-commerce Product Intelligence Platform

Mục tiêu mới:

Thu thập dữ liệu sản phẩm liên tục từ nhiều marketplace, chuẩn hóa các nguồn khác nhau, xác định những listing thuộc cùng một sản phẩm, phân tích biến động theo thời gian và cung cấp một hệ thống hỗ trợ người dùng so sánh/xếp hạng các offer.

Tên đề tài có thể theo hướng:

**Xây dựng nền tảng dữ liệu lớn thu thập, đồng nhất và phân tích sản phẩm đa sàn thương mại điện tử**

hoặc:

**A Big Data Platform for Cross-Marketplace Product Matching and Temporal Market Analysis**

---

# 4. Câu hỏi chính của hệ thống mới

Thay vì hỏi:

> Người dùng hành xử như thế nào trong một cửa hàng e-commerce?

Hệ thống mới hướng tới:

> Cùng một sản phẩm được bán và phản ứng như thế nào trên nhiều marketplace theo thời gian?

Ví dụ với:

`iPhone 17 256GB`

hệ thống cần có khả năng tổng hợp:

- Shopee
- TikTok Shop
- Lazada
- các marketplace khác nếu khả thi

và trả lời:

- Sàn nào đang có giá thấp nhất?
- Giá hiện tại cao/thấp hơn market median bao nhiêu?
- Giá đã biến động thế nào trong 7/30/90 ngày?
- Offer nào có mức giá bất thường?
- Listing nào đang có sales velocity tốt hơn?
- Review của người mua tích cực/tiêu cực như thế nào?
- Người mua thường khen/chê aspect nào?
- Seller nào có các public reputation signals tốt hơn?
- Offer nào có balance tốt nhất giữa giá, độ phổ biến và review?
- Ranking các offer thay đổi thế nào theo tiêu chí người dùng?

---

# 5. Thay đổi quan trọng về data semantics

Không ép data crawler vào canonical event contract cũ.

Cần xây domain model mới.

## 5.1 Canonical Product

Đại diện cho một sản phẩm vật lý/logical chung giữa các marketplace.

Ví dụ:

`Apple iPhone 17`

Fields dự kiến:

- canonical_product_id
- brand
- model
- name
- canonical_category
- specification

---

## 5.2 Canonical Variant

Cần phân biệt variant vì giá chỉ có ý nghĩa khi so cùng variant.

Ví dụ:

`iPhone 17 / 256GB / Black`

Fields:

- variant_id
- canonical_product_id
- storage
- color
- size
- model_code
- các attributes đặc thù category

---

## 5.3 Marketplace Offer

Một listing/offer cụ thể trên một marketplace.

Ví dụ:

`iPhone 17 256GB của Shop A trên Shopee`

Fields:

- offer_id
- marketplace
- platform_listing_id
- seller_id
- canonical_variant_id
- source_url
- first_seen_at
- last_seen_at
- product_match_confidence

---

## 5.4 Offer Observation

Snapshot của offer tại thời điểm crawl.

Fields có thể gồm:

- offer_id
- observed_at
- listed_price
- current_price
- discount
- sold_count nếu public
- rating
- rating_count
- review_count
- stock/availability nếu public
- ranking_position nếu có
- promotion information nếu có
- raw source reference

Grain:

`1 row = state của một marketplace offer tại thời điểm T`

---

## 5.5 Review

Nếu có thể thu thập hợp lệ:

- review_id
- offer_id
- marketplace
- rating
- text
- created_at
- seller_reply
- relevant metadata

Không giả định các marketplace có cùng semantics review.

---

# 6. Bài toán nghiên cứu quan trọng nhất: Product Entity Resolution

Crawler chỉ là acquisition.

Một bài toán cốt lõi hơn là xác định:

`Shopee listing A`

`TikTok listing B`

`Lazada listing C`

có phải cùng một canonical product/variant hay không.

Ví dụ:

- `Tai nghe Sony WH1000XM5 chính hãng`
- `SONY WH-1000XM5 Wireless Headphones`
- `Tai nghe Bluetooth Sony WH-1000XM5`

phải được map về:

`Sony WH-1000XM5`

Đây có thể trở thành research contribution chính của đồ án.

---

# 7. Hướng giải Product Entity Resolution

## Phase 1: Rule-based

Dùng:

- normalized brand
- model number
- title similarity
- exact/fuzzy specification matching
- color
- storage
- size
- category

Ưu tiên category có model/SKU rõ ràng.

---

## Phase 2: ML/NLP

Có thể sử dụng:

- text embedding
- fuzzy matching
- attribute similarity
- sentence similarity
- classification/pair matching model

Score ví dụ:

`match_score = brand_score + model_score + title_score + attribute_score`

Không cần image matching ở MVP.

Image embedding có thể để extension sau.

---

# 8. Category nên chọn cho MVP

Không bắt đầu với fashion vì entity resolution rất khó.

Nên ưu tiên electronics/consumer electronics vì có:

- brand rõ
- model number
- specification
- variant rõ

Ví dụ:

- smartphone
- headphone
- mouse
- SSD
- laptop
- smart appliance

Ví dụ canonical products:

- iPhone 17
- Sony WH-1000XM5
- Logitech MX Master 3S
- Samsung 990 Pro
- Xiaomi devices

---

# 9. Multi-source acquisition layer mới

Kiến trúc không nên bind crawler trực tiếp với downstream.

Đề xuất adapter abstraction:

`MarketplaceAdapter`

có interface logic như:

- discover_products()
- fetch_listing()
- fetch_product()
- fetch_reviews()
- parse_listing()
- parse_product()
- parse_reviews()

Implementations:

- ShopeeAdapter
- TikTokShopAdapter
- LazadaAdapter
- future adapters

Mục tiêu là marketplace-specific logic chỉ nằm ở acquisition/parsing layer.

Downstream sử dụng canonical schemas.

---

# 10. Kiến trúc tổng thể đề xuất

High-level:

External marketplaces

→ Marketplace adapters

→ Crawl Scheduler / Frontier

→ Crawl Workers

→ Raw data

→ MinIO Bronze

→ Parser / Normalization

→ Kafka canonical observation topics

→ Speed Layer

→ Batch Layer

→ Entity Resolution

→ Temporal Analytics

→ Review Intelligence

→ Ranking Engine

→ Serving Database / Search Index

→ Backend API

→ Product Intelligence Web Application

---

# 11. Không bỏ hệ thống hiện tại nếu không cần thiết

Ưu tiên reuse:

- Kafka
- Spark
- MinIO
- PostgreSQL
- Elasticsearch
- Redis
- monitoring/testing patterns hiện có

Không thêm Flink, Kubernetes, Iceberg,... chỉ để tăng số công nghệ nếu chưa có requirement rõ ràng.

Kiến trúc hiện tại đã đủ nhiều thành phần.

Ưu tiên giải quyết domain/data problems trước.

---

# 12. Vai trò mới của Lambda Architecture

## Speed layer

Có thể xử lý các change events gần realtime:

- PRICE_CHANGED
- LARGE_PRICE_DROP
- SOLD_COUNT_CHANGED
- SALES_VELOCITY_SPIKE
- REVIEW_COUNT_SPIKE
- RATING_CHANGED
- OFFER_DISAPPEARED
- NEW_OFFER
- STOCK_STATUS_CHANGED nếu quan sát được

Speed layer trả lời:

> Điều gì vừa xảy ra?

---

## Batch layer

Batch xử lý các bài toán cần độ chính xác hoặc computation cao hơn:

- entity resolution
- canonical product building
- deduplication
- historical recomputation
- market statistics
- product/offer aggregation
- sentiment analysis
- scoring
- model training

Batch layer trả lời:

> Xu hướng dài hạn và kết luận chính xác hơn là gì?

---

# 13. Raw data phải được giữ lại

Crawler response không được parse rồi bỏ.

Data flow:

`fetch -> raw Bronze -> parser -> normalized Silver`

Lợi ích:

- parser bug có thể reprocess
- schema source thay đổi có thể reparsing
- debugging
- lineage
- reproducibility

Mỗi normalized record cần trace được về raw observation/source URL.

---

# 14. Adaptive crawling

Không crawl tất cả listings với cùng frequency.

Phân loại:

HOT:
- giá biến động mạnh
- sales velocity cao
- campaign
- nhiều reviews mới
- ranking thay đổi

WARM:
- có activity trung bình

COLD:
- ít biến động

Ví dụ:

- HOT: 30 min
- WARM: 4h
- COLD: 24h

Các con số chỉ là initial configuration, phải benchmark thực tế.

Extension:

`next_crawl_time = f(change_probability, freshness_requirement, crawl_budget)`

---

# 15. Không cần crawl toàn marketplace

Xây một monitored product universe.

Ví dụ MVP:

- 100–500 canonical products
- 1–2 categories
- 2 marketplaces

Scale dần:

- 500
- 5,000
- 20,000 products

Big Data đến từ:

`products × offers × marketplaces × observations × time`

không cần crawl toàn Shopee.

---

# 16. Temporal Market Intelligence

Các derived metrics cần xem xét.

## Price history

Theo dõi:

- current price
- original price
- discount
- price change
- market median
- min/max
- volatility

---

## Fair/Reference Price

Không dùng đơn giản mean.

Nên xem xét robust statistics:

- median
- MAD
- IQR
- percentile

Ví dụ:

`market_median_price`

và:

`price_difference_percent`

Không khẳng định "giá đúng/sai".

Có thể nói:

- below market median
- above market median
- unusually cheap
- unusually expensive
- price anomaly

---

# 17. Sales proxy

Nếu marketplace public `sold_count`, không coi đó là transaction log.

Derive:

`observed_sales_delta = sold_count(t2) - sold_count(t1)`

và:

`sales_velocity = observed_sales_delta / elapsed_time`

Phải sử dụng naming trung thực:

- observed sales delta
- sales velocity proxy
- popularity proxy

Không gọi là exact sales nếu source không đảm bảo.

---

# 18. Review Intelligence

Không chỉ tính:

`average_rating`

Cần:

- review count
- rating distribution
- review velocity
- sentiment distribution
- sample size
- confidence

---

## Aspect-Based Sentiment Analysis

Phân biệt product aspects:

- battery
- camera
- display
- performance
- quality
- price

và seller/transaction aspects:

- delivery
- packaging
- seller service
- warranty
- authenticity-related mentions

Ví dụ:

Product sentiment:
- Camera: 91% positive
- Battery: 63% positive

Seller experience:
- Delivery: 72% positive
- Packaging: 80% positive

Không collapse toàn bộ review thành một positive/negative label duy nhất nếu có thể.

---

# 19. Ranking Engine

Output cuối cùng không chỉ là BI dashboard.

Xây một user-facing Product Intelligence / Decision Support System.

Người dùng search:

`iPhone 17 256GB`

Hệ thống:

1. Resolve canonical product.
2. Lấy các offers cùng variant.
3. Phân tích.
4. Xếp hạng.
5. Giải thích.
6. Cho link về listing gốc.

---

## Các signal ranking

Có thể gồm:

- PriceScore
- SellerReputationScore
- ReviewScore
- PopularityScore
- PriceStabilityScore
- FreshnessConfidence
- DataConfidence

Ví dụ:

`OfferScore = weighted combination of observable signals`

Không gọi trực tiếp là `Trustworthiness = 93%`.

Nên gọi:

- Offer Score
- Seller Reputation Score
- Purchase Confidence Score

và ghi rõ đây là derived score dựa trên public observable signals.

---

# 20. Explainable Ranking

Không chỉ trả:

`Score = 92`

Phải giải thích:

- giá thấp hơn market median 4%
- official store nếu source xác nhận
- sales velocity gần đây cao
- 91% review sentiment tích cực
- seller rating tốt
- dữ liệu được cập nhật gần đây
- có cảnh báo nếu review sample quá nhỏ

User-facing explanation quan trọng hơn raw score.

---

# 21. Multiple ranking strategies

Không phải user nào cũng có cùng utility function.

Có thể cung cấp:

- Recommended
- Cheapest
- Most Popular
- Best Reviewed
- Official Store
- Best Value

Extension:

cho user thay đổi weights giữa:

- price
- review
- seller reputation
- popularity

---

# 22. Confidence / uncertainty

Ví dụ:

Offer A:
- 86% positive
- 1,042 reviews

Offer B:
- 100% positive
- 5 reviews

Không được rank B cao chỉ dựa trên percentage.

Ranking cần xem xét:

- sample size
- statistical uncertainty
- data freshness
- entity match confidence
- missing attributes

Có thể xây:

`final_score = offer_score × confidence`

hoặc cách calibrated hơn.

---

# 23. Data freshness là first-class metric

Mỗi observation cần timestamp.

UI có thể hiển thị:

- Price updated 10 minutes ago
- Reviews updated 4 hours ago

Nếu crawl source lỗi lâu:

`STALE DATA`

Freshness ảnh hưởng confidence/ranking.

Điều này kết nối trực tiếp crawler reliability với chất lượng output cuối.

---

# 24. Product cuối cùng

BI vẫn giữ nhưng chỉ dành cho:

- Data Engineer
- Admin
- Research
- system monitoring
- model evaluation
- data coverage

End-user application riêng dùng cho:

- search
- compare
- product analysis
- ranking
- historical price
- review insight
- marketplace link

UI concept:

`iPhone 17 256GB`

Summary:
- market median price
- lowest offer
- 7/30 day price trend
- overall sentiment
- main positive aspects
- main negative aspects

Ranking:

1. Shopee Offer A
2. TikTok Offer B
3. Lazada Offer C

Mỗi result có:

- price
- relative market price
- rating/reviews
- sentiment
- popularity/sales velocity proxy
- seller signal
- final score
- explanation
- data freshness
- source link

---

# 25. Core thesis contributions

Nên tập trung vào 5 bài toán:

## Core 1 — Multi-Marketplace Data Acquisition

- continuous crawling
- scheduler
- retries
- raw preservation
- data lineage
- schema drift
- data quality
- crawl observability

## Core 2 — Cross-Marketplace Product Entity Resolution

- product normalization
- variant matching
- cross-source product matching
- confidence scoring

Đây có thể là research core mạnh nhất.

## Core 3 — Temporal Market Intelligence

- price history
- price volatility
- price anomaly
- observed sales velocity
- rating/review evolution

## Core 4 — Review Intelligence

- sentiment analysis
- aspect-based sentiment analysis
- sample confidence

## Core 5 — Explainable Multi-Criteria Offer Ranking

- price
- popularity
- seller signals
- review sentiment
- freshness
- confidence
- explainability

---

# 26. Những thứ KHÔNG nên đưa vào core scope ban đầu

Để tránh scope explosion:

- recommendation system cá nhân hóa
- fraud detection
- scam detection
- fake review detection
- causal price elasticity
- demand forecasting
- image-based entity matching
- arbitrary number of marketplaces
- Kubernetes migration
- Flink migration
- Iceberg migration

Có thể ghi là future work.

---

# 27. MVP đề xuất

MVP ban đầu nên nhỏ nhưng end-to-end.

## Sources

Ưu tiên 2 marketplace.

Ví dụ:

- Shopee
- TikTok Shop

Nếu một nguồn quá khó hoặc không phù hợp thì abstraction phải cho phép thay bằng marketplace khác.

---

## Category

Chỉ 1 category dễ entity resolution.

Ví dụ:

`smartphones` hoặc `consumer electronics`.

---

## Product universe

100–500 canonical products.

---

## Data

Ưu tiên:

- product/listing metadata
- seller metadata public
- price
- discount
- rating
- rating count
- sold count nếu có
- reviews nếu khả thi
- source URL
- timestamps

---

## Intelligence MVP

Phải có:

1. Cross-marketplace product matching.
2. Historical price.
3. Market price comparison.
4. Basic sales/popularity proxy.
5. Review sentiment.
6. Explainable offer ranking.
7. Product detail/search API.
8. Minimal user-facing web page.

---

# 28. Các câu hỏi nghiên cứu có thể dùng trong thesis

Primary question:

> Làm thế nào tích hợp dữ liệu không đồng nhất và biến động theo thời gian từ nhiều sàn thương mại điện tử để cung cấp phép so sánh và xếp hạng các lựa chọn mua hàng có thể giải thích?

Subquestions:

1. Làm thế nào nhận dạng cùng một sản phẩm/variant giữa các marketplace khác nhau?
2. Làm thế nào chuẩn hóa price, rating, sold counter và review có semantics khác nhau?
3. Làm thế nào thu thập dữ liệu đủ fresh với crawl budget hữu hạn?
4. Làm thế nào xây ranking đa tiêu chí khi dữ liệu có uncertainty và missing values?
5. Làm thế nào giải thích ranking cho end user?
6. Temporal observations từ public marketplace có thể cung cấp những demand/price signals nào mà không invent unavailable facts?

---

# 29. Một số nguyên tắc thiết kế bắt buộc

1. Không invent data mà source không cung cấp.
2. Phân biệt observed facts và derived metrics.
3. Mọi derived metric phải trace được về source observations.
4. Không gọi proxy thành ground truth.
5. Không đánh đồng sold counter giữa các marketplace nếu semantics khác nhau.
6. Không so price nếu chưa canonicalize đúng product + variant.
7. Không đánh giá sentiment nếu sample quá nhỏ mà không thể hiện confidence.
8. Raw source data phải replay/reprocess được.
9. Marketplace-specific logic không được leak vào downstream domain.
10. Hệ thống phải graceful degradation khi một source tạm thời unavailable.

---

# 30. Yêu cầu Codex lập plan

Hãy đọc toàn bộ repository hiện tại trước khi đề xuất refactor.

Không được assume README phản ánh hoàn toàn code.

Cần inspect:

- repository structure
- canonical schema hiện tại
- producer
- Kafka topics
- Spark streaming code
- warehouse job
- Bronze/Silver/Gold schemas
- PostgreSQL cache
- Elasticsearch usage
- Redis usage
- ML module
- serving layer
- tests
- Docker Compose
- configuration

Sau đó tạo một **implementation plan sơ bộ**, chưa code ngay.

Plan cần bao gồm:

### A. Current-state assessment

- Thành phần nào giữ lại.
- Thành phần nào refactor.
- Thành phần nào deprecated.
- Thành phần nào cần xây mới.

### B. Proposed target architecture

Vẽ architecture/data flow mới rõ ràng.

### C. New domain model

Đề xuất schemas cho:

- CanonicalProduct
- CanonicalVariant
- Marketplace
- Seller
- MarketplaceOffer
- OfferObservation
- Review
- EntityMatch
- derived market metrics

### D. Kafka topic design

Đề xuất các topics, event schemas và responsibilities.

Không tạo quá nhiều topics nếu chưa cần thiết.

### E. Data lake model

Thiết kế Bronze / Silver / Gold mới.

### F. Crawl subsystem

Thiết kế:

- adapter abstraction
- scheduler
- frontier/task queue
- worker
- retry
- dedup
- freshness
- raw storage
- metrics

Không tập trung vào anti-bot bypass.

### G. Entity Resolution

Đề xuất roadmap rule-based -> ML-assisted.

Phải có cách evaluate matching accuracy.

### H. Analytics

Đề xuất cách tính:

- price history
- market median
- price anomaly
- sales velocity proxy
- review sentiment
- aspect sentiment

### I. Ranking

Đề xuất v1 scoring model có explainability và confidence.

### J. Serving layer

Đề xuất API/backend architecture cho:

- product search
- product detail
- offer comparison
- historical trends
- ranking
- external marketplace links

### K. BI/Observability

Phân biệt rõ:

- operational dashboards
- research/BI dashboards
- end-user application

### L. Migration strategy

Không rewrite toàn bộ repository.

Đưa ra từng phase migration từ behavioral analytics architecture sang cross-marketplace intelligence architecture.

### M. MVP

Đưa ra MVP có thể hoàn thành trước, ưu tiên 1 category + 2 marketplaces + 100–500 products.

### N. Testing strategy

Bao gồm:

- parser tests
- canonicalization tests
- entity matching tests
- pipeline tests
- idempotency
- data quality
- ranking tests
- source adapter contract tests

### O. Risks

Phân tích:

- source instability
- missing fields
- marketplace semantics differences
- crawl rate constraints
- data freshness
- entity matching errors
- review availability
- sparse observations
- scope explosion
- ML complexity

### P. Milestones

Chia implementation thành các phase nhỏ, mỗi phase phải tạo ra một vertical slice chạy được.

Ưu tiên:

`real acquisition -> canonical data -> storage -> analysis -> serving`

trước khi thêm ML phức tạp.

---

# 31. Expected planning philosophy

Không over-engineer.

Không thay technology chỉ vì technology mới hơn.

Reuse platform hiện tại khi hợp lý.

Ưu tiên correctness của:

- data semantics
- entity resolution
- provenance
- temporal observations
- explainability

hơn số lượng công nghệ.

Mục tiêu cuối cùng là biến project từ:

`Historical dataset -> Big Data analytics dashboard`

thành:

`Real multi-source acquisition -> cross-market data integration -> product intelligence -> explainable decision-support application`

Hãy đưa ra plan ở mức đủ cụ thể để sau đó có thể chia thành GitHub issues/tasks và implement theo từng phase.
