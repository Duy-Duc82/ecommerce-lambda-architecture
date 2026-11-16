# Báo cáo hệ thống: Marketplace Price Intelligence (Lambda, ELT đa nguồn)

> Đúng với: commit `9c73660` trên nhánh `docs/system-report`, tức `feat/discount-credibility` merge thêm `phase-11/databricks` · Cập nhật: 2026-11-16 · Nguồn chân lý: `src/ecomdata/`, `compose/`, `databricks/`, `scripts/init_postgres.sql`, `docs/adr/`

Bộ tài liệu này mô tả hệ thống **như code đang chạy**, không theo kế hoạch. Mỗi khẳng định đều trỏ tới file trong code. Khi tài liệu cũ (`docs/ARCHITECTURE.md`, `docs/DATA_MODEL.md`) nói khác code, bộ này theo code và ghi chỗ lệch vào [05-sai-lech-va-rui-ro.md](05-sai-lech-va-rui-ro.md).

## Đọc theo thứ tự nào

| # | File | Trả lời câu hỏi | Spec hình chính |
|---|---|---|---|
| 1 | [01-tong-quan.md](01-tong-quan.md) | Hệ thống làm gì, gồm những tầng nào, chạy ở đâu? | Toàn cảnh Lambda; tầng module; triển khai local và hybrid Databricks |
| 2 | [02-luong-du-lieu.md](02-luong-du-lieu.md) | Một byte từ trang Tiki biến đổi thế nào qua từng tầng cho tới dashboard? | Luồng chi tiết; 9 chặng biến đổi; schema từng chặng; truy ngược lineage |
| 3 | [03-xu-ly-trong-tang.md](03-xu-ly-trong-tang.md) | Bên trong mỗi tầng xử lý những gì, theo luật nào? | Frontier; speed có trạng thái; Silver; star; mart; quality; serving |
| 4 | [04-pipeline.md](04-pipeline.md) | Có những pipeline nào, lịch chạy, đầu vào/ra, cách chạy lại khi lỗi? | Danh mục pipeline; sequence thu thập; state machine batch; DAG Airflow; job Databricks |
| 5 | [05-sai-lech-va-rui-ro.md](05-sai-lech-va-rui-ro.md) | Tài liệu cũ lệch code ở đâu, code có rủi ro gì? | (bảng) |

Tài liệu tham chiếu sâu hơn, không chép lại ở đây:

- Danh sách cột đầy đủ của star schema: `docs/model/star-schema.dbml` (sinh ra `.mmd`, `.puml`, `.sql`, PNG).
- Lý do của từng quyết định: `docs/adr/0001` đến `0009`.
- Vận hành, sự cố: `docs/RUNBOOK.md`; giám sát: `docs/OBSERVABILITY.md`; Databricks: `docs/DATABRICKS_READINESS.md`.
- Spec từng hình cho báo cáo (bảng node, cạnh, caption): nhánh `docs/diagram-specs`, thư mục `docs/diagrams/`.

## Quy ước sơ đồ

Bộ tài liệu không nhúng hình vẽ sẵn. Mỗi hình được viết thành một **spec để vẽ** bằng công cụ bất kỳ (draw.io, Excalidraw, Lucidchart, Figma…). Một spec gồm:

1. Tiêu đề `Hình X.Y — <tên>` và một dòng **Loại hình** (flowchart trái → phải, sequence, state, ER, Gantt…) kèm **ý chính** người xem phải thấy.
2. Bảng **Khung**: các nhóm cần vẽ thành khung bao, màu tầng của khung (nếu hình có nhóm).
3. Bảng **Node**: số thứ tự, nhãn ghi trên hình, hình dạng, màu tầng.
4. Bảng **Cạnh**: từ node nào tới node nào, nhãn trên cạnh, kiểu nét. Sequence diagram thay bằng bảng thông điệp theo thứ tự; state diagram thay bằng bảng trạng thái và bảng chuyển trạng thái.
5. Caption in nghiêng `*Hình X.Y. …*` để đặt dưới hình trong báo cáo.

Vẽ đủ node và cạnh trong bảng là đủ ý. Bố cục, kích thước và icon tuỳ người vẽ.

**Màu theo tầng** (giống nhau ở mọi hình):

| Tầng | Màu | Gồm |
|---|---|---|
| Nguồn ngoài | xám `#E5E7EB` | Tiki, Lazada |
| Thu thập | xanh dương `#DBEAFE` | crawl-worker, frontier |
| Lưu trữ lake | vàng `#FEF3C7` | Bronze, Silver, Gold, `dw` |
| Batch | cam `#FFEDD5` | stage Spark |
| Speed | tím `#EDE9FE` | Spark Structured Streaming |
| Kafka | đỏ nhạt `#FEE2E2` | topic |
| Phục vụ | xanh lá `#DCFCE7` | PostgreSQL `cache`, Elasticsearch, Redis, Superset, Kibana |
| Điều phối, giám sát | xanh ngọc `#CCFBF1` | Airflow, Grafana, Databricks Jobs |

**Hình dạng:** hình trụ là nơi lưu dữ liệu; lục giác là topic Kafka; chữ nhật bo góc là tiến trình; hình thoi là quyết định; chữ nhật thường là hệ thống ngoài hoặc công cụ người dùng (Superset, Kibana).

**Nét:** nét liền là dữ liệu chảy; nét đứt là điều khiển (lập lịch, lease, đọc trạng thái); nét đậm là đường chính của hình.

**Tên:** giữ nguyên tên trong code (`offer_observations_v2`, `marketplace.records.v1`); giải thích viết tiếng Việt.

**Số liệu:** mỗi con số đo được đều ghi ngày đo và trỏ tới `docs/evidence/`. Số không có nguồn thì không ghi.

## Từ vựng

| Thuật ngữ | Nghĩa trong hệ thống này |
|---|---|
| offer | một listing của một sàn: (`source_code`, `platform_listing_id`) → `offer_id` |
| observation (sighting) | một lần crawler thấy một offer trên một trang, ở một thời điểm |
| record | một dòng nguồn nguyên vẹn (envelope `marketplace-record.v1`), chưa chuẩn hoá |
| run | một lượt batch, khoá bởi `run_id` (`mp-YYYYMMDDTHHMMZ`) và `as_of` |
| `as_of` | mốc thời gian dữ liệu mà run được phép thấy; mart và quality cắt tại đây |
| star / `dw` | 8 bảng dimension, bridge và 6 bảng fact (Delta) dùng chung cho mọi run |
| mart | bảng phục vụ theo run (12 bảng), bản được promote được sao vào PostgreSQL `cache.*` |
| counter | số mà sàn hiển thị (đã bán, số đánh giá). Thay đổi của counter **không phải** đơn hàng |
