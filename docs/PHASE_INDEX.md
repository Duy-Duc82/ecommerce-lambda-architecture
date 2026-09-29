# Phase index — nguồn chân lý dùng chung

> Parent plan: `Brief để xây dựng plan sơ bộ cho việc cải tiến ecommerce-lambda-architecture.md`

File này là **nguồn chân lý duy nhất** cho việc chia phase. Nó nằm trên `master`
để mọi nhánh rẽ ra đều thấy. Mọi plan phase phải suy ra từ Brief §21 (kế hoạch
12 tuần) và §22 (prioritized backlog), không được tự định nghĩa lại.

## 1. Ánh xạ Tuần ↔ Phase ↔ Backlog

Cột "Backlog" trích nguyên từ Brief §22. Một phase **không được** lấn sang
backlog ID của phase khác.

| Phase | Tuần | Tên (theo Brief §21) | Backlog (Brief §22) |
|---|---|---|---|
| 1 | 1 | Feasibility và scope freeze | P0-01 |
| 2 | 2 | Raw-first crawler vertical slice | P0-02 … P0-07, P0-11, P0-12 |
| 3 | 3 | Scheduler, retry và source thứ hai | P0-08, P0-09, P0-10 |
| 4 | 4 | Canonical Kafka và Silver | P1-01 … P1-04 |
| 5 | 5 | Speed layer | **P1-05, P1-06** |
| 6 | 6 | Batch temporal warehouse | **P1-07, P1-10**, Superset draft (P1-11) |
| 7 | 7 | Quality, anomaly và replay | P1-08, P1-09 |
| 8 | 8 | Reliability và operations | P1-12, Kibana/dashboard (P1-11) |
| 9 | 9 | Evaluation và feature freeze | P2-01 … P2-07 |
| — | 10 | Buffer hoặc optional comparison | P3-01 … P3-04 (chỉ khi P0–P2 đạt DoD) |
| — | 11 | Report review | — |
| — | 12 | Finalization | — |

### Ranh giới dễ nhầm

- **P1-08 Data quality gates là Tuần 7**, không phải Tuần 5.
- **P1-09 Gold publish manifest là Tuần 7** (Brief §21 Tuần 7 liệt kê rõ).
  Tuần 6 chỉ có *PostgreSQL cache DDL/publish*, là P1-10.
- **P1-12 Recovery and failure tests là Tuần 8**, không phải Tuần 6.
- **P1-07 Temporal Gold marts là Tuần 6**, không được kéo lên Tuần 5.

## 2. Vertical slice bắt buộc mỗi phase

Trích Brief §21. Phase chỉ được coi là xong khi slice chạy end-to-end.

| Phase | Vertical slice |
|---|---|
| 2 | `live source -> raw Bronze -> parsed observation` |
| 3 | scheduled continuous collection restart-safe |
| 4 | `crawl -> Bronze -> Kafka -> Silver` |
| 5 | `observation -> change event -> ES/Redis -> Kibana` |
| 6 | `Silver -> Gold -> PostgreSQL -> Superset` |

## 3. Hợp đồng đóng băng (không phase nào được định nghĩa lại)

Các mục dưới đây do Brief sở hữu. Plan phase chỉ được *tham chiếu*, không được
đặt tên khác.

- **Kafka topics** (Brief §11): `marketplace.observations.v1`,
  `marketplace.observations.v1.dlq`, `marketplace.changes.v1`.
- **Change event types** (Brief §10) — đúng 7, không thêm bớt: `NEW_OFFER`,
  `PRICE_CHANGED`, `LARGE_PRICE_DROP`, `RATING_CHANGED`, `COUNTER_CHANGED`,
  `AVAILABILITY_CHANGED`, `OFFER_STALE`.
- **Kafka key**: observations theo `(marketplace, platform_listing_id)`;
  changes theo offer ID.
- **`observation_id`** derive từ marketplace, listing ID, observed time, raw hash.
- **Daily price mart** (Brief §15) mang first/last/min/max price và
  observation count.
- **Counter delta** (Brief §15): không clamp âm về zero; gắn
  `counter_reset_or_invalid`.

## 4. Quy tắc tránh tái diễn việc chia hai đường

Sự cố 2026-09: hai bản triển khai Phase 5/6 độc lập cùng tồn tại vì plan
Phase 5/6 chỉ nằm ở ngọn một nhánh feature, không có ở commit gốc chung.

1. **Mọi `docs/PHASE_*.md` phải nằm trên `master`.** Plan là hợp đồng, không
   phải sản phẩm của phase.
2. **Commit plan trước khi viết code**, không phải commit cuối của nhánh.
3. **Trước khi mở phase mới**, đọc file này để lấy đúng backlog ID; nếu thấy
   thiếu plan, dừng lại và hỏi — không tự viết plan mới.
4. Đổi ranh giới phase phải sửa file này trước, theo Brief §27 change-control.

## 5. Trạng thái hiện tại (2026-09-29)

| Phase | Trạng thái | Nhánh |
|---|---|---|
| 1 | có code | `phase-1-marketplace-foundation` |
| 2 | có code | `phase-3-4-scheduler-kafka-silver` (commit `4de5ac1`) |
| 3 | **chưa có code** | — |
| 4 | một phần (topics, DLQ, Silver sink) | `phase-3-4-scheduler-kafka-silver` |
| 5 | code có, **test thiếu** (10/38) | `phase-3-4-scheduler-kafka-silver` |
| 6 | code có, **test thiếu** (10/47) | `phase-3-4-scheduler-kafka-silver` |

Phase 5/6 đã chốt lấy bản `phase-3-4-scheduler-kafka-silver` làm nền vì scope
khớp Brief §22. Nhánh `phase-5-6-speed-gold` giữ lại làm tham chiếu test, không
phát triển tiếp. Việc còn lại là bổ sung test theo mục 13 (Phase 5) và mục 15
(Phase 6) của hai plan tương ứng.
