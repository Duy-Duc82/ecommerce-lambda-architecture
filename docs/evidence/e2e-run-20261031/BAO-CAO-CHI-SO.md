# Báo cáo chỉ số vận hành pipeline, 31/10/2026

Stack `mp-refactor` (nhánh `phase-10/star-on-refactor`) chạy liên tục từ 02:30 UTC (09:30 VN). Báo cáo gồm số đo thực tế của từng tầng, giới hạn biên (cấu hình, năng lực pipeline, giới hạn của trang web) và kết quả các thí nghiệm gây lỗi có chủ đích. Số liệu thô và truy vấn nằm ở `raw/`. Mục 1–8 tính đến **13:15 UTC (20:15 VN)**; mục 9 là số chốt của cả đợt chạy (đến 17:24 UTC).

## 0. Môi trường và cách đo

| Mục | Giá trị |
|---|---|
| Máy | Windows 11, RAM 31 GB; Docker Desktop 28.3.2, VM Linux giới hạn 14 GB (`.wslconfig`), 8 vCPU |
| Nguồn | Tiki (HTML danh mục, HTTP) từ 02:30 UTC; Lazada (Chromium, 1 tab) từ 08:55 UTC |
| Cấu hình crawl | `CRAWL_REQUEST_DELAY_SECONDS=2.0`, jitter 0–1 s, mỗi host một throttle; Lazada thêm sàn 5 s/trang theo spec của nguồn; tier ACTIVE crawl lại mỗi 60 phút, SWEEP mỗi ngày |
| Speed | Spark Structured Streaming, hai truy vấn nối tiếp (normalizer `records → observations`, rồi truy vấn có trạng thái ghi ES/Redis/Kafka). Trigger 30 s đến 09:03:49 UTC, sau đó 5 s |
| Batch | Airflow `marketplace_batch` mỗi giờ, chờ cửa sổ khép 20 phút, 8 task |
| Nguồn số liệu | bảng `audit.*` (Postgres), CSDL Airflow, Prometheus, Loki, Elasticsearch, offset Kafka |

Lưu ý về đo crawl:
- `latency_ms` trong `audit.crawl_request_attempt` là **tổng thời gian một lượt**, gồm cả thời gian chờ throttle, tải trang, ghi Bronze, parse và đẩy Kafka. Đây không phải độ trễ mạng.
- `started_at` được ghi trước khi chờ throttle. Vì vậy khoảng cách giữa hai `started_at` có lúc nhỏ hơn 2 s, nhưng hai lần gửi HTTP thực tế tới cùng một host luôn cách nhau ít nhất 2 s.
- Thời gian phản hồi thuần của Tiki đo trong đợt khảo sát nguồn (12/10) là 0,55–0,67 s.

## 1. Crawl

### 1.1 Số đo thực tế (02:30 → 13:15 UTC)

| Chỉ số | Tiki | Lazada |
|---|---|---|
| Lượt tải | 4.478 | 64 |
| Thành công | 4.449 (99,35%) | 63 (98,44%) |
| Request/giây, trung bình các phút có crawl | **0,325** | **0,063** |
| Request/giây, p50 / p95 / tối đa theo phút | 0,350 / 0,367 / 0,383 | 0,050 / 0,100 / 0,100 |
| Request/phút tối đa | 23 | 6 |
| Khoảng cách giữa hai request, p50 / p95 | 2,52 s / 7,68 s | 10,52 s / 21,15 s |
| Thời gian một lượt, p50 / p95 / p99 / max | 2,42 / 3,20 / 3,74 / 39,9 s | 5,40 / 9,27 / 32,8 / 37,2 s |
| Dung lượng trang trung bình | 410 KB | 2,38 MB |
| Bản ghi trung bình / tối đa mỗi trang | 25,7 / 55 | 40 / 40 |
| Tổng bản ghi parse được | 114.928 | 2.560 |
| Trang rỗng (chỉ có page-context) | 447 | 0 |
| Lượt phải thử lại | 9 | 1 |

Theo thời gian (Tiki, tính theo giờ UTC):

| Giờ | Lượt tải | Lỗi | Bản ghi | Raw (MB) |
|---|---|---|---|---|
| 02 (từ 02:30) | 475 | 4 | 12.620 | 191,9 |
| 03 | 1.255 | 19 | 32.556 | 522,1 |
| 04 | 1.261 | 0 | 30.906 | 521,5 |
| 05 | 1.265 | 0 | 33.908 | 552,8 |
| 06 | 42 | 1 | 601 | 14,5 |
| 07–12 | 25 mỗi giờ | 0–5 | ~600 mỗi giờ | ~10,7 mỗi giờ |

Từ 02:30 đến khoảng 06:00 là lượt quét SWEEP (4.227 danh mục lấy từ sitemap), chạy liên tục khoảng 1.260 trang/giờ. Sau đó các danh mục SWEEP hẹn quét lại sau một ngày, nên mỗi giờ chỉ còn 25 trang ACTIVE của Tiki và 15 trang của Lazada.

### 1.2 Giới hạn biên của tốc độ crawl

| Mức | Tiki | Lazada | Cơ sở |
|---|---|---|---|
| Đang chạy (thực tế, các phút có crawl) | 0,325 req/s (tối đa 0,383) | 0,063 req/s (tối đa 0,1) | §1.1 |
| Trần theo cấu hình lịch sự | 0,40 req/s (khoảng cách trung bình 2 + 0,5 s jitter); 0,5 req/s nếu bỏ jitter | 0,2 req/s (sàn 5 s/trang) | `Throttle`, spec nguồn |
| Benchmark với stub, delay 2 s (31/10, 3 lần, code hiện tại) | 0,362 req/s, 14,5 dòng/s, p50 2,37 s, 0 lỗi | — | `raw/bench-crawl-delay-2s.json` |
| **Năng lực pipeline khi không có delay** (stub, 04/10, code Phase 9) | **0,939 req/s**, 25,4 dòng/s, p50 562 ms | — | `C:\code\data\data\ops\bench`, PROGRESS §27 |
| Giới hạn của trang web | Trang danh mục: 100/100 OK ở 1 req/s (mức cao nhất đã thử, chưa tìm thấy ngưỡng chặn). Trang sản phẩm: bị chặn trong khoảng 12–20 req/phút, phục hồi sau 6–15 phút | Khoảng 8 s/trang qua trình duyệt; chưa thử bậc tốc độ | khảo sát nguồn 11–12/10, §3.13 |
| Không dùng được | Shopee chặn ở mọi mức (slider captcha); Sendo không truy cập được | | khảo sát nguồn |
| **Ngưỡng chặn thực tế của Lazada (31/10)** | — | Bị chặn sau 76 trang trong khoảng 5 giờ (trung bình 0,004 req/s; từng đợt 0,1 req/s) | §1.3 |

Kết luận về tốc độ crawl:
- **Tiki**: tốc độ đang chạy bằng khoảng 90% trần cấu hình (0,36 trên 0,40). Benchmark với stub ở cùng delay cho cùng con số (0,362), nên giới hạn hiện nay là **delay lịch sự, không phải trang web hay pipeline**.
- Nếu bỏ delay, một worker đạt khoảng 0,94 req/s; lúc đó giới hạn là thời gian xử lý mỗi trang, khoảng 1,06 s. Đây là giới hạn biên trong điều kiện lý tưởng của một worker.
- Trang danh mục Tiki đã chịu được 1 req/s khi khảo sát, nhưng không nên vượt mức đã đo.
- **Lazada**: bị giới hạn bởi sàn 5 s/trang cộng thời gian render trang (p50 5,4 s), nên khoảng cách giữa hai trang khoảng 10,5 s, tức 0,095 req/s.
- **Sức chứa theo ngày** ở cấu hình hiện tại: Tiki khoảng 31.000 trang/ngày (khoảng 800.000 dòng listing); Lazada khoảng 8.200 trang/ngày nếu frontier đủ lớn. Hiện frontier Lazada chỉ có 15 trang ACTIVE.
- **Benchmark delay 0 trên code hiện tại chưa chạy xong**: tiến trình bị Claude Code dừng lúc 09:30 UTC vì máy host gần hết RAM (stack benchmark chạy song song với stack chính). Số 0,939 req/s là của code ngày 04/10.

### 1.3 Lỗi crawl và cách hệ thống xử lý

| Loại | Số lượt | Ý nghĩa | Xử lý |
|---|---|---|---|
| `PARSE_ERROR` "no products", Tiki | 24 | 23 lượt là danh mục rỗng thật (`paging.total = 0`) trước bản sửa lúc 03:53. 1 lượt là `c21298`: Tiki báo 284 sản phẩm nhưng HTML không có sản phẩm nào (danh mục `global`, render phía client) | Danh mục rỗng giờ là trang hợp lệ có 0 dòng (447 trang). Trang có `paging` mâu thuẫn vẫn tính là lỗi |
| `PUBLISH_ERROR` | 4 Tiki + 1 Lazada | Do thí nghiệm E3 (dừng Kafka) | Cả 5 được thử lại thành công sau 1 phút 45 giây đến 2 phút 36 giây |
| `STORAGE_ERROR` | 1 | Do thí nghiệm E5 (dừng MinIO) | Thử lại thành công sau 58 s |
| `RATE_LIMITED` / HTTP 4xx–5xx | **0** | — | — |
| **`BLOCKED`, Lazada (sự cố thật, 14:06 UTC)** | 5 | Lazada trả về trang chặn/challenge ở **trang thứ 76 trong ngày**: sau khoảng 5 giờ crawl, mỗi giờ 12–15 trang, mỗi trang cách nhau khoảng 10 s. Trong chu kỳ đó có 11 trang thành công trước khi bị chặn | Thử lại với backoff +40 s, +1 phút 40, +2 phút 40, +5 phút 12; sau 5 lượt bị chặn liên tiếp, **circuit breaker mở 15 phút** (14:16:59–14:31:59), cảnh báo bật lúc 14:20 và tự tắt lúc 14:35. Trang chặn không được lưu thành dữ liệu (`raw_bytes` = 0). Tiki không bị ảnh hưởng. Sau đó worker Lazada được dừng đến hết đợt chạy để không tiếp tục thăm dò |
| `duplicate_listing_on_page` (records DLQ) | 41 message tính cả các ngày trước, 26 trong hôm nay | Tiki hiển thị cùng một sản phẩm hai lần trên một trang | Quarantine theo thiết kế |

## 2. Speed (Kafka → Spark → Elasticsearch, Redis, Kafka `changes`)

| Chỉ số | Trigger 30 s (02:30–09:03) | Trigger 5 s (từ 09:41) |
|---|---|---|
| **Độ trễ đầu-cuối p50** (crawler đẩy vào Kafka → mọi sink đã ghi) | **46,2 s** | **9,1 s** (7,5–9,2 s qua các giờ) |
| Độ trễ p95 (trung vị các batch) | 59,4 s | 9,4 s (p95 của p95: 16,1 s) |
| Độ trễ xấu nhất trong vận hành bình thường | 61,5 s | 24,2 s |

Cả ngày, cả hai giai đoạn:

| Chỉ số | Giá trị |
|---|---|
| Micro-batch | 3.451, thất bại 0 |
| Dòng vào / dòng thay đổi | 110.690 / 156.297 |
| Dòng lỗi định dạng / đến trễ | 0 / 0 |
| Dòng trùng | 54, đều do phát lại sau crash trong E2, không áp dụng hai lần |
| Thời gian một micro-batch có dữ liệu | gom kết quả khoảng 1,35 s; ghi Kafka 66 ms, ES 57 ms (p95 131), Redis 20 ms |
| Thông lượng | vào tối đa 117,6 dòng/s; xử lý tối đa 713,7 dòng/s (lúc xả hàng đợi sau khi khởi động lại) |
| Lag so với Kafka | lớn nhất 0 offset |
| Bộ nhớ state | tối đa 183 MB |
| CPU / RAM của container speed | trung bình 0,77 core (tối đa 2,28); RAM tối đa 4,0 GB |

**Giới hạn biên của speed** (số benchmark trước đây, máy dev):
- Thử tải: theo kịp 200 dòng/s (batch p50 13,5 s ở trigger 30 s). Ở mức 1.000 dòng/s, bộ sinh tải chỉ đạt 548 dòng/s và speed xử lý 432 dòng/s, nên không theo kịp.
- Mô hình chi phí sau khi tối ưu: 1,46 s + 1,96 ms/dòng (R² 0,96), bão hoà ở khoảng **440 dòng/s**. Mức này tương đương khoảng 17 trang Tiki mỗi giây, **gấp khoảng 48 lần tốc độ crawl hiện tại**, nên speed không phải điểm nghẽn.
- Ngâm tải 15 phút ở 10 dòng/s với trigger 2 s: độ trễ khoảng 2,2 s và ổn định. Đây là mức độ trễ thấp nhất đã đo được.

Nguồn: `C:\code\data-refactor\docs\PROGRESS.md` §29–31 và `C:\code\data\docs\PROGRESS.md` §27.

**Vì sao trigger 30 s cho độ trễ 46 s**: speed có hai hop nối tiếp, mỗi hop chờ trung bình nửa chu kỳ trigger rồi xử lý. Giảm trigger xuống 5 s làm độ trễ giảm 5 lần, đổi lại CPU trung bình của speed tăng 18% (0,70 → 0,83 core, so 4 giờ trước và sau khi đổi; lượng dữ liệu vào hai giai đoạn khác nhau nên đây chỉ là số tham khảo).

**Trường hợp biên**: bản ghi cũ từ 25/10 vẫn còn trong Kafka (thời gian lưu giữ chưa hết) và được xử lý khi speed chạy lại, cho độ trễ đo được 5,5 ngày. Đây là đặc tính của Kafka: không mất dữ liệu khi tầng xử lý dừng lâu, nhưng chỉ số độ trễ cần lọc trường hợp này.

## 3. Batch (Airflow, mỗi giờ)

| Run | Kết quả | Silver | Gold | Phút |
|---|---|---|---|---|
| 02:00 | PROMOTED (sau khi sửa lỗi) | 38.808 | 262.665 | 24,3 |
| 03:00 | FAILED: layout Silver cũ, đã sửa bằng reprocess | – | – | 6,1 |
| reprocess 03:28 | PROMOTED | 65.840 | 448.601 | 8,2 |
| 04:00 → 11:00 | 8 run, đều PROMOTED, quality PASS | 81.543 → 146.092 | 543.580 → 797.466 | 6,7–8,7 |
| 12:00 | QUALITY_FAILED rồi PROMOTED sau khi sửa (mục 5, lỗi B1) | 147.342 | 798.007 | – |

Thời gian từng task (p50, run thành công):

| Task | p50 | Max | CPU tối đa | RAM tối đa |
|---|---|---|---|---|
| dimensions | 117 s | 334 s | 4,6 core | 4,7 GB |
| silver | 100 s | 114 s | 4,7 core | 4,0 GB |
| facts | 63 s | 102 s | 3,6 core | 4,6 GB |
| quality | 53 s | 106 s | 2,4 core | 4,1 GB |
| marts | 43 s | 77 s | 2,6 core | 3,7 GB |
| publish | 30 s | 36 s | 1,0 core | 2,7 GB |
| promote | 2 s | 2,5 s | — | — |

- Mỗi run có khoảng 147.000 dòng Silver và 798.000 dòng Gold; chạy hết khoảng 6,7 phút, tức khoảng **356 dòng Silver/giây**. Benchmark trước đây đo 534 dòng/s ở 200.000 dòng.
- Độ trễ lập lịch: task đầu tiên bắt đầu sau p50 0,7 s; chờ hàng đợi trung bình dưới 1 s.
- **Độ tươi BI**: Gold cho mốc `HH:00` có trên Superset lúc khoảng `HH:27` (chờ cửa sổ khép 20 phút cộng 7 phút xử lý). Vì vậy dữ liệu trên Superset cũ hơn thực tế từ 27 phút đến 1 giờ 27 phút.
- Quality của run mới nhất: 16/16 luật bắt buộc PASS. Luật advisory `price_anomaly_evaluation_coverage = 0` vì phát hiện giá bất thường chưa có đủ số ngày lịch sử làm baseline.
- Cache BI (run 11:00): 75.455 offer, 3.908 người bán, 105.329 dòng lịch sử giá theo ngày. 13/13 chart Superset đều có dữ liệu.
- Maintenance (prune, backup, storage snapshot): 4,4 phút; riêng backup 259 s.

## 4. Thí nghiệm gây lỗi (kiến trúc hiện tại)

Mỗi thí nghiệm gây một lỗi, đo phục hồi, rồi để hệ thống tự xử lý. Nhật ký chi tiết ở `raw/experiments.log`.

| # | Lỗi gây ra | Phát hiện | Phục hồi | Mất / trùng dữ liệu |
|---|---|---|---|---|
| E1a | `docker kill` speed | Healthcheck thấy container `Exited (137)` trong vòng 5 phút | **Không tự khởi động lại**: Docker coi `docker kill` là dừng thủ công. Khởi động lại bằng tay thì 12,2 s sau có micro-batch thành công | 0 |
| E1b | `kill -9` JVM của Spark bên trong speed | — | Restart policy khởi động lại container sau 0,6 s; truy vấn chạy lại sau 7 s; checkpoint sạch (offset = commit) | 0 |
| E4 | `kill -9` worker crawl Tiki khi đang giữ 4 lease | — | Container khởi động lại sau 0,85 s. Bốn task được crawl lại **5 phút 11 giây đến 5 phút 17 giây** sau crash, đúng khi lease 300 s hết hạn | 0 mất, 0 trùng (25 lượt cho 25 task trong giờ đó) |
| E3 | Dừng Kafka 90 s khi đang crawl | "Nhiều dòng log lỗi" bật sau 3 phút (157 dòng lỗi từ speed) | Kafka healthy 7 s sau khi bật. 5 lượt `PUBLISH_ERROR` được thử lại sau 1 phút 45 giây đến 2 phút 36 giây. Speed crash 3 lần rồi tự phục hồi | 0 mất. Kafka nhận thêm 40 message gửi lại (at-least-once); batch khử trùng, luật `observation_id_unique` PASS. **Lộ ra lỗi B1** (mục 5) |
| E2 | Dừng Elasticsearch 94 s | "Container khởi động lại liên tục" và "Nhiều dòng log lỗi" bật sau 4 phút, tự hết sau 10 và 31 phút | Crawl không bị ảnh hưởng (37/37 thành công). Speed crash 4 lần rồi tự phục hồi | 0 mất. 54 dòng phát lại được nhận ra là trùng và không áp dụng hai lần; độ trễ tối đa 120 s |
| E5 | Dừng MinIO 91 s | — | 1 lượt `STORAGE_ERROR` (client S3 tự thử lại khoảng 40 s, nên worker chững lại thay vì lỗi hàng loạt), crawl lại thành công sau 58 s. Speed không bị ảnh hưởng | 0 mất; độ trễ tối đa 26 s |

Tham chiếu thêm, bộ drill D1–D11 (03–04/10, kiến trúc trước refactor, đều PASS):
- 429 kèm `Retry-After` được tôn trọng; 500 được thử lại.
- Kafka, MinIO, Elasticsearch, Redis bị dừng đều phục hồi.
- Speed bị kill giữa batch không mất bản ghi nào (811/811).
- Lease của worker chết được nhận lại 6/6.
- Parser lệch format giữ lại raw và không ghi gì.
- Quality fail chặn publish; ghi cache lỗi thì rollback.
- Hai batch chạy cùng lúc bị khoá.
- Backup/restore toàn vẹn.

Nguồn: `C:\code\data\data\ops\drills\d*.json`.

## 5. Lỗi tìm thấy và đã sửa trong ngày

| # | Lỗi | Phát hiện bằng | Sửa |
|---|---|---|---|
| A1 | Task Airflow chạy code cũ đóng sẵn trong image (không có stage `dimensions`) | batch 02:00 lỗi | Build image r4 / python:3 / airflow r3; stack dev mount working tree |
| A2 | Đọc Silver dạng Delta bị lỗi vì gắn schema khai báo sẵn | batch | Chỉ gắn schema khi đọc Parquet |
| A3 | Layout Silver lệch giữa code cũ và mới | batch 03:00 | Rebuild bằng `marketplace_reprocess`; thêm kiểm tra `SilverLayoutMismatch` |
| A4 | Lệnh `ops prune-raw` chưa có trong CLI | maintenance | Thêm lệnh và test kiểm tra mọi lệnh DAG gọi đều tồn tại |
| A5 | Backup không có thư mục ghi được | maintenance | Bind `AIRFLOW_REPORTS_MOUNT` |
| A6 | Listing MinIO trả về object đánh dấu thư mục rỗng mà Delta tạo | backup | `S3ObjectStore` bỏ qua key kết thúc bằng `/` |
| A7 | Danh mục Tiki rỗng thật bị tính là lỗi parse | 116 task `FAILED` | Trang có 0 dòng là hợp lệ |
| **B1** | **Lượt crawl lỗi publish ghi `parsed_count` = số message Kafka đã xác nhận, trong khi Silver đọc file Bronze** | **E3 → batch 12:00 `QUALITY_FAILED`** | `parsed_count` = số dòng đã lưu xuống Bronze; sửa 5 dòng audit; chạy lại quality → PROMOTED sau 5 phút. Nếu không sửa, mọi batch bị giữ đến khi các run lỗi ra khỏi cửa sổ đối chiếu (tới khoảng 1 ngày). Trong lúc đó quality gate vẫn giữ BI ở bản đúng |
| B2 | Cảnh báo "Crawl ngừng" báo nhầm khi crawler rảnh giữa hai chu kỳ | cảnh báo lúc 06:23 | Chỉ báo khi có task `READY` quá hạn hơn 10 phút, xét theo từng marketplace |
| B3 | Cảnh báo "Speed không có micro-batch" báo nhầm khi không có dữ liệu mới | healthcheck lúc 09:48 | Chỉ báo khi Kafka có bản ghi mới trong 15 phút qua |
| B4 | `healthcheck.sh` hardcode stack cũ; gọi qua `localhost` → `::1` thất bại ngắt quãng | vận hành | Nhận `env/<stack>.env`, dùng `127.0.0.1`, có timeout |
| B5 | Dòng thăm dò glob của Spark và lỗi SMTP của Grafana bị gắn `level=error` | Loki | Alloy gắn `level=expected` |
| B6 | **Mọi biểu đồ Lens trên cả 3 dashboard Kibana đều trống**: panel kẹt ở trạng thái đang tải, không báo lỗi; chỉ panel bảng hiển thị | người dùng xem Kibana; tái hiện bằng Chromium headless (log console và network) | Tham chiếu data view của layer phải nằm trong `attributes.references` của panel Lens, không chỉ ở cấp dashboard. Sửa `_lens_panel`, sinh lại saved objects, import lại; thêm test. Cả 3 dashboard đã hiển thị đúng |

Toàn bộ thay đổi chưa commit; danh sách file trong `git status` của worktree `C:\code\data-refactor`.

## 6. Giám sát

| Chỉ số (11 giờ) | Giá trị |
|---|---|
| Endpoint lên | Grafana, Superset 100%; Loki 99,92%; Airflow, Elasticsearch 99,69%; Kibana, MinIO 99,76%. Phần thiếu là do các thí nghiệm và lúc tạo lại container |
| Log trong Loki theo layer | orchestration 43.075, speed 20.683, storage 7.276, serving 4.326, batch 4.321, observability 3.755, crawl 1.876, ops 228, ingest 20 |
| Series trong Prometheus | khoảng 12.700 |
| Cảnh báo đúng | batch lỗi, quality bắt buộc FAIL, DAG lỗi, container khởi động lại liên tục, nhiều dòng log lỗi: đều bật trong 0–4 phút sau sự cố và tự hết |
| Cảnh báo sai | 2 (B2, B3), đều đã sửa |
| Chưa phát hiện kịp | Speed chết hẳn khi không có dữ liệu mới (E1a): luật mới chỉ báo khi có dữ liệu chờ xử lý. Endpoint Kafka không được probe nên E3 chỉ bị phát hiện qua log |

## 7. Tài nguyên

| Chỉ số | Giá trị |
|---|---|
| RAM của VM Docker | trung bình 59,5%, tối đa **93,5%** (lúc batch chạy). Ngưỡng cảnh báo 90% cần kéo dài 10 phút nên không bật |
| CPU của VM | trung bình 27%, tối đa 88% (lúc batch) |
| RAM tối đa theo service | batch 4,0–4,7 GB mỗi stage; speed 4,0 GB; Elasticsearch 2,1 GB; MinIO 1,5 GB; Kafka 1,1 GB; Airflow scheduler 1,1 GB; Kibana 0,66 GB; worker Lazada 0,66 GB; worker Tiki 0,06 GB |
| Máy host | Hai lần thiếu RAM. (1) 09:30, khi chạy thêm stack benchmark: RAM trống còn 2,3–2,9 GB, Claude Code dừng benchmark. (2) 14:28, chỉ với stack chính: available 1,4 GB, commit 47,5 / 52,2 GB. Commit theo tiến trình: VM Docker 14,0 GB (đúng mức trần), Chrome 5,6 GB, SignalRGB 5,0 GB, Claude Code 3,0 GB, Zalo 2,0 GB, các ứng dụng khác khoảng 4 GB. Xả page cache trong VM (4,3 GB → 0,8 GB) và dừng worker Lazada. **Một máy 32 GB chỉ chạy được một stack đầy đủ, và cần đóng các ứng dụng nặng khác** |
| Khối lượng dữ liệu | Kafka `records` 117.249 message, `changes` 156.532 trong 11 giờ. Elasticsearch: `changes` 188.500 tài liệu (200 MB), `offers-current` 75.900 (74 MB). Bronze raw 385 MB / records 74 MB (đo lúc 08:30) |

## 8. Đề xuất

1. **Worker khởi động lại nên nhận lại ngay lease của chính nó** (E4 mất 5 phút chờ lease hết hạn). Chỉ an toàn khi mỗi worker ID chỉ có một bản chạy.
2. **Speed nên thử lại có backoff khi sink tạm lỗi**, thay vì crash và để Docker khởi động lại (E2, E3). Dữ liệu vẫn đúng nhờ checkpoint, nhưng sẽ bớt nhiễu log và cảnh báo.
3. **Probe Kafka trong Alloy** và thêm cảnh báo "container speed/crawl không chạy", để bắt được trường hợp E1a.
4. **Đặt trigger 5 s làm mặc định** cho stack demo (độ trễ 9 s thay vì 46 s).
5. **Chạy lại benchmark crawl ở delay 0 trên code hiện tại** khi máy không chạy stack khác, để cập nhật năng lực pipeline (số hiện có là của ngày 04/10).
6. **Mở rộng frontier Lazada**: hiện chỉ có 15 trang mỗi giờ, quá ít để đánh giá thống kê cho nguồn này.

## 9. Cập nhật cuối (chốt lúc 17:24 UTC, 00:24 VN ngày 1/11)

Toàn bộ đợt chạy: 02:30 → 17:24 UTC, tức **14 giờ 54 phút**. Số thô: `raw/metrics-final.txt`, `raw/airflow-tasks-final.txt`, `raw/speed-trigger5s-final.txt`.

| Chỉ số | Tiki | Lazada |
|---|---|---|
| Lượt tải / thành công | 4.578 / 4.549 (99,37%) | 80 / 74 (92,50%, gồm 5 lượt `BLOCKED`) |
| Request/giây khi bận: trung bình / p50 / tối đa | 0,311 / 0,350 / 0,383 | 0,056 / 0,050 / 0,100 |
| Thời gian một lượt p50 / p95 | 2,41 / 3,20 s | 5,35 / 8,81 s |
| Bản ghi parse được | 117.332 | 3.000 |

| Speed | Giá trị |
|---|---|
| Micro-batch | 6.429, thất bại 0 |
| Dòng vào / dòng thay đổi | 113.530 / 156.542 |
| Trùng / đến trễ / lỗi định dạng | 54 (phát lại trong E2) / 0 / 0 |
| Độ trễ đầu-cuối, trigger 5 s (09:41–17:24, 149 batch có dữ liệu) | p50 **8,8 s**, p95 9,7 s, p95 của p95 15,3 s; tối đa 120 s (trong E2) |
| Độ trễ đầu-cuối, trigger 30 s | p50 46,2 s, p95 59,4 s |

| Batch | Giá trị |
|---|---|
| Run theo giờ | 16 run từ 02:00 đến 17:00: 14 PROMOTED ngay, 1 FAILED (03:00, layout Silver cũ), 1 QUALITY_FAILED rồi PROMOTED sau khi sửa (12:00) |
| Reprocess | 1 lần (03:28), PROMOTED |
| Run cuối `mp-20261031T1700Z` | PROMOTED, PASS; Silver 151.742, Gold 798.946 |
| Thời gian mỗi run | 6,7–8,9 phút khi máy đủ RAM; **10,3 và 13,8 phút ở hai run cuối**, trùng giai đoạn máy thiếu RAM (dữ liệu chỉ tăng khoảng 600 dòng Silver mỗi giờ) |
| Cache BI cuối (Superset) | 75.911 offer, 4.078 người bán, 106.234 dòng lịch sử giá theo ngày, 5.624 dòng giá theo danh mục |
| Elasticsearch cuối | `changes` 188.745 tài liệu (200 MB), `offers-current` 75.911 (76 MB), `crawl-attempts` 6.141, `speed-batches` 6.562 |

Trạng thái khi chốt:
- crawl, speed và es-projector đã dừng;
- DAG batch, discovery và reprocess đã tắt;
- đã chạy backup cuối;
- toàn bộ stack được dừng bằng `docker compose stop` (giữ volume).

Mở lại để xem: `sh docs/evidence/e2e-run-20261031/xem-lai-bi.sh`.

### Tổng hợp trường hợp biên quan sát được trong ngày

| Trường hợp | Loại | Kết quả |
|---|---|---|
| Danh mục Tiki rỗng (`paging.total = 0`) | thật | 447 trang hợp lệ có 0 dòng, sau bản sửa A7 |
| Trang Tiki báo có hàng nhưng HTML rỗng (`c21298`) | thật | `PARSE_ERROR`, giữ đúng là lỗi |
| Sản phẩm lặp trên cùng một trang Tiki | thật | 26 bản ghi vào DLQ `duplicate_listing_on_page` |
| Lazada chặn crawler sau 76 trang | thật | backoff, rồi circuit breaker 15 phút; cảnh báo bật và tắt đúng |
| Bản ghi Kafka cũ 5,5 ngày được xử lý khi speed chạy lại | thật | không mất dữ liệu; độ trễ đo được 5,5 ngày |
| Máy host thiếu RAM (2 lần) | thật | tác vụ nền bị dừng; batch chậm 1,5–2 lần |
| Gọi `localhost` → `::1` thất bại ngắt quãng | thật | đổi sang `127.0.0.1` |
| Crash speed / crawl worker; dừng Kafka / Elasticsearch / MinIO | thí nghiệm E1–E5 | tự phục hồi, 0 mất dữ liệu (mục 4) |
| Kafka gửi lại (at-least-once) và batch phát lại | thí nghiệm | khử trùng đúng: luật `observation_id_unique` PASS, 54 dòng trùng không áp dụng hai lần |
| Lượt crawl lỗi publish làm lệch phép đối chiếu | lộ ra từ E3 | lỗi B1; quality gate giữ BI ở bản đúng; đã sửa |
