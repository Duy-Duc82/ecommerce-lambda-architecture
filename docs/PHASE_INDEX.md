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

## 3b. Mô hình nhánh (chốt 2026-09-29)

```text
phase-<n>-<tên>  --PR-->  develop  --PR (chỉ khi thầy hướng dẫn duyệt)-->  master
```

- **`master`**: chỉ nhận PR từ `develop`, và chỉ sau khi thầy hướng dẫn duyệt.
  Không commit thẳng vào đây nữa, kể cả docs.
- **`develop`**: nhánh tích hợp. Mọi nhánh phase PR vào đây. Đây là nơi chạy
  full suite trước khi trình duyệt.
- **`phase-<n>-*`**: mỗi phase một nhánh, **luôn rẽ từ `develop` mới nhất**,
  commit nhỏ và revert được riêng lẻ. Giữ nguyên nguyên tắc cũ: production fix
  nằm ở commit riêng, tách khỏi test đã phát hiện ra nó.

**Không xếp chồng nhánh.** Một nhánh phase không được rẽ từ nhánh phase khác.
Nếu phase sau cần code của phase trước, chờ PR của phase trước merge vào
`develop` rồi mới cắt nhánh mới từ `develop`. Xếp chồng làm PR thứ hai hiện
luôn diff của PR thứ nhất, không review được, và buộc phải merge theo đúng thứ
tự.

Các nhánh cũ (`phase-1-marketplace-foundation`, `phase-3-4-scheduler-kafka-silver`,
`phase-5-6-speed-gold`) có trước mô hình này; giữ làm lịch sử, không phát triển
tiếp. Công việc của chúng đã nằm trong `develop`.

## 4. Quy tắc tránh tái diễn việc chia hai đường

Sự cố 2026-09: hai bản triển khai Phase 5/6 độc lập cùng tồn tại vì plan
Phase 5/6 chỉ nằm ở ngọn một nhánh feature, không có ở commit gốc chung.

1. **Mọi `docs/PHASE_*.md` phải nằm trên `master`.** Plan là hợp đồng, không
   phải sản phẩm của phase.
2. **Commit plan trước khi viết code**, không phải commit cuối của nhánh.
3. **Trước khi mở phase mới**, đọc file này để lấy đúng backlog ID; nếu thấy
   thiếu plan, dừng lại và hỏi — không tự viết plan mới.
4. Đổi ranh giới phase phải sửa file này trước, theo Brief §27 change-control.

## 5. Trạng thái hiện tại (2026-10-01)

| Phase | Tuần | Trạng thái | Test |
|---|---|---|---|
| 1 | 1 | ✅ trong `develop` | |
| 2 | 2 | ✅ trong `develop` | |
| 3 | 3 | ✅ trong `develop` | 113 (đủ 30/30 item) |
| 4 | 4 | ✅ trong `develop` | |
| 5 | 5 | ✅ trong `develop` | đủ 38/38 item |
| 6 | 6 | ✅ trong `develop` | **47/47 item** |
| 7 | 7 | ✅ trong `develop` (PR #4); bản sửa cắt `as_of` trên `phase-7-asof-cutoff` | **61/61 item** |
| 8 | 8 | 🔵 plan trong `develop` (PR #6); WP1 trên `phase-8-wp1-crawl-service` | P1-12, Kibana (P1-11) |
| 9 | 9 | ⏳ chưa bắt đầu | P2-* |

**Phase 1–7 đã xong** và nằm trong `develop`. Phase 7 (P1-08 quality gates,
P1-09 gold publish manifest) merge qua PR #4 ngày 2026-09-30. Bản sửa sau
merge — run đọc Silver và audit *tại* `as_of`, gate 8 có cửa sổ đối soát — nằm
trên `phase-7-asof-cutoff`, chi tiết ở `PROGRESS.md` §11.
Suite: **674 pass, 0 fail, 0 skip** (trước Phase 7 là 519).
`master` vẫn chưa nhận phase nào: chờ thầy hướng dẫn duyệt `develop`.

**Nợ cũ đã đóng bằng chạy thật (2026-10-01).** Bốn item Phase 6 (33–35, 43) và
hai item Phase 7 (56, 58) đều được kiểm bằng `run_marketplace_warehouse` chạy
end-to-end trên PostgreSQL thật với 48 observation seed từ chính factory của
project. Chi tiết bằng chứng ở `PROGRESS.md` §10.

Lần chạy đó phát hiện **hai bug production** mà không unit test nào lộ ra, cả
hai đã sửa trên nhánh Phase 7 (commit test riêng, commit fix riêng):

- `_with_marketplace()` chọn cột `marketplace_code` từ `audit.crawl_run`, bảng
  đó chỉ có `marketplace_id`. Hai trong chín mart Phase 6
  (`source_coverage_daily`, `crawl_reliability_daily`) chưa từng dựng được trên
  schema thật. Fixture audit của Phase 6 khai cột theo cái code cần chứ không
  theo `scripts/init_postgres.sql`, nên cả 43 test đều mù.
- Con trỏ manifest được promote **trước** khi publish cache. Publish fail thì
  cache rollback đúng nhưng con trỏ đã nhảy sang run hỏng. Nguyên nhân gốc là
  plan Phase 7 tự mâu thuẫn — §13 đòi giữ con trỏ, §14 bước 11 bảo promote
  trước; §14 đã được sửa.

## 5b. Ba hạn chế môi trường, để lại cho Phase 8

Phát hiện khi chạy thật, chưa sửa, đều nằm ngoài phạm vi Phase 7:

1. **Spark không truy cập được filesystem trên Windows host** — thiếu
   `winutils.exe`/`hadoop.dll`, ném `UnsatisfiedLinkError:
   NativeIO$Windows.access0`. Mọi lần chạy thật phải trong container Linux.
   Đây là lý do item 33–35 và 43 treo từ Phase 6; unit test không lộ vì chúng
   chỉ dùng `createDataFrame` trong bộ nhớ.
2. **`build_spark()` của marketplace job không gọi `spark_hadoop_options()`** —
   chỉ `warehouse_job.py` legacy gọi. Job này do đó không có credential S3A và
   **đường `s3a://` chưa từng được kiểm chứng lần nào**. Lần chạy 2026-10-01
   dùng lake `file://`.
3. **`docker/spark-warehouse/Dockerfile` pin `apache/spark:3.5.1`** — mâu thuẫn
   với ràng buộc pyspark 4.x ở §6, và chỉ có entrypoint cho `warehouse_job.py`
   legacy.

Hệ quả: Compose profile và one-command smoke của Phase 8 phải dựng đường chạy
Linux cho marketplace batch, và muốn dùng MinIO thì phải nối
`spark_hadoop_options()` vào `build_spark()` trước.

Điều này cũng giải thích **"offline smoke với `--skip-postgres`"** mà plan
Phase 6 §16 và Phase 7 §18 mô tả: smoke đó không chạy được, vì
`read_crawl_audit()` được gọi vô điều kiện (hai mart coverage/reliability bắt
buộc cần bằng chứng audit qua JDBC) còn `--skip-postgres` chỉ bỏ qua publish.
Sửa chỗ này thuộc Phase 8.

Các nhánh cũ `phase-1-marketplace-foundation`, `phase-3-4-scheduler-kafka-silver`,
`phase-5-6-speed-gold` có trước mô hình `develop`; giữ làm lịch sử, công việc
của chúng đã nằm trong `develop`.

## 6. Ràng buộc môi trường

`pyspark` phải là **4.x**. pyspark 3.5.x chỉ hỗ trợ Python 3.8-3.11 và Java
8/11/17; trên Python 3.12 + Java 21 thì JVM chạy nhưng python worker chết ngay,
nên mọi thao tác trả dữ liệu về Python đều hỏng và không test được DataFrame.
Spark 4 bật ANSI mode mặc định — đã kiểm tra, không làm lệch mart nào.

Test dùng Spark phải set `spark.sql.session.timeZone=UTC` cho khớp
`build_spark()`, và nhớ rằng `collect()` trả timestamp naive theo local zone
của driver.
