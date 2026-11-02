# Bộ sơ đồ cho báo cáo

> Đúng với: commit `f82f5b5` (nhánh `phase-10/star-on-refactor`) · Cập nhật: 2026-11-02 · Nguồn chân lý: `src/ecomdata/`, `compose/`, `docs/adr/`

Thư mục này lên sẵn nội dung cho từng sơ đồ của báo cáo và bộ doc (brief "Bộ tài liệu cho ecommerce-lambda-architecture", 31/10/2026). Mỗi file là một sơ đồ. Mở file ra là vẽ được ngay, không cần đọc lại code.

## Mỗi file có gì

1. **Dùng ở đâu**: doc tham chiếu và chương báo cáo đặt hình.
2. **Câu hỏi sơ đồ trả lời**: một câu. Hình nào không trả lời được câu đó là vẽ sai.
3. **Loại và bố cục**: flowchart, sequence, state, ER…; hướng vẽ và cách chia nhóm.
4. **Thành phần**: mỗi node một dòng, gồm id, nhãn hiển thị, nhóm, ghi chú.
5. **Quan hệ**: mỗi cạnh một dòng, gồm từ, tới, nhãn và kiểu nét.
6. **Chú thích dưới hình**: câu caption dùng trong báo cáo.
7. **Bản nháp Mermaid**: render được ngay. Cần hình đẹp hơn thì vẽ lại bằng draw.io hoặc Excalidraw theo đúng bảng thành phần và quan hệ.
8. **Kiểm chứng**: các file code đã dùng để lên nội dung. Code đổi thì kiểm lại đúng các file này.

## Danh sách

| # | File | Sơ đồ | Doc đích | Loại |
|---|---|---|---|---|
| 01 | [01-overview.md](01-overview.md) | Toàn hệ thống trong một hình | README, chương 1 | flowchart LR |
| 02 | [02-data-flow.md](02-data-flow.md) | Luồng dữ liệu chi tiết | ARCHITECTURE §2 | flowchart LR |
| 03 | [03-module-layers.md](03-module-layers.md) | Các tầng module và luật import | ARCHITECTURE §1 | flowchart TB |
| 04 | [04-deployment.md](04-deployment.md) | Container, image, profile compose | ARCHITECTURE §7 | flowchart (nhóm theo profile) |
| 05 | [05-storage-roles.md](05-storage-roles.md) | Lake là bản lưu, Postgres là cache | ARCHITECTURE, ADR-0008 | flowchart LR |
| 06 | [06-failure-boundaries.md](06-failure-boundaries.md) | Ranh giới lỗi: hỏng chỗ nào thì mất gì | ARCHITECTURE, RUNBOOK | flowchart + bảng |
| 07 | [07-acquisition-pipeline.md](07-acquisition-pipeline.md) | Một lượt thu thập một trang (raw first) | ARCHITECTURE §3, ADR-0001 | sequence |
| 08 | [08-frontier-lifecycle.md](08-frontier-lifecycle.md) | Vòng đời task crawl và circuit breaker | ARCHITECTURE, RUNBOOK | stateDiagram |
| 09 | [09-contracts-topics.md](09-contracts-topics.md) | Hợp đồng dữ liệu, topic Kafka, DLQ | CONTRACTS | flowchart LR |
| 10 | [10-medallion-lake-layout.md](10-medallion-lake-layout.md) | Các zone Medallion và layout thư mục lake | DATA_MODEL | cây thư mục / flowchart TB |
| 11 | [11-star-schema.md](11-star-schema.md) | Star schema `dw` | DATA_MODEL, chương mô hình dữ liệu | ER |
| 12 | [12-manifest-pointer.md](12-manifest-pointer.md) | Manifest của run và pointer `current.json` | DATA_MODEL, ADR-0005 | flowchart + khối JSON |
| 13 | [13-batch-state-machine.md](13-batch-state-machine.md) | Bảy stage và state machine của run | ADR-0005, chương batch | stateDiagram |
| 14 | [14-day-window-certified-star.md](14-day-window-certified-star.md) | Cửa sổ ngày, star đã chứng nhận, RESTORE | ADR-0007 | sequence |
| 15 | [15-quality-gate.md](15-quality-gate.md) | Cổng chất lượng | QUALITY | flowchart TB (cây quyết định) |
| 16 | [16-lineage-traceback.md](16-lineage-traceback.md) | Truy ngược một dòng mart về byte gốc; reparse | LINEAGE | flowchart RL |
| 17 | [17-speed-layer.md](17-speed-layer.md) | Speed: chuẩn hoá, phát hiện thay đổi, sink | ARCHITECTURE, chương speed | flowchart LR |
| 18 | [18-airflow-dags.md](18-airflow-dags.md) | Bốn DAG Airflow | ADR-0002, chương điều phối | flowchart LR mỗi DAG |
| 19 | [19-observability.md](19-observability.md) | Giám sát: Alloy, Prometheus, Loki, Grafana | OBSERVABILITY | flowchart LR |
| 20 | [20-runbook-symptoms.md](20-runbook-symptoms.md) | Sự cố theo triệu chứng | RUNBOOK | flowchart TB (cây quyết định) |

## Quy ước vẽ chung

- **Màu theo tầng**, giữ giống nhau ở mọi hình:

  | Tầng | Màu nền gợi ý | Gồm |
  |---|---|---|
  | Nguồn ngoài | xám `#E5E7EB` | Tiki, Lazada |
  | Thu thập | xanh dương `#DBEAFE` | crawl-worker, frontier |
  | Lưu trữ lake | vàng `#FEF3C7` | Bronze, Silver, Gold, `dw` |
  | Batch | cam `#FFEDD5` | stage Spark |
  | Speed | tím `#EDE9FE` | Spark Structured Streaming |
  | Kafka | đỏ nhạt `#FEE2E2` | topic |
  | Phục vụ | xanh lá `#DCFCE7` | PostgreSQL cache, ES, Redis, Superset, Kibana |
  | Điều phối, giám sát | xanh ngọc `#CCFBF1` | Airflow, Grafana |

- **Hình dạng node**:
  - hình trụ: nơi lưu dữ liệu (bucket, bảng, index);
  - hình lục giác: topic Kafka;
  - chữ nhật bo góc: tiến trình (container, stage);
  - hình thoi: quyết định.
- **Nét cạnh**:
  - nét liền: dữ liệu chảy;
  - nét đứt: điều khiển (lập lịch, đọc trạng thái);
  - nét đậm: đường chính của luồng mà hình đang kể.
- **Tên**: giữ nguyên tên trong code (`offer_observations_v2`, `marketplace.records.v1`). Nhãn giải thích viết tiếng Việt.
- **Số liệu**: số trên hình (số dòng, thời gian) phải ghi ngày đo và trỏ tới `docs/evidence/`. Không có nguồn thì không ghi.
- **Một ý mỗi hình**: chi tiết nào không phục vụ câu hỏi của hình thì bỏ, để hình khác gánh.

## Sơ đồ có sẵn, không vẽ lại

- Star schema đầy đủ cột: `docs/model/star-schema.dbml` là nguồn; `.mmd`, `.puml`, `.sql`, `marketplace_star_schema.png` sinh từ nó. File 11 chỉ lên bản rút gọn cho báo cáo.
