# Đánh giá hệ thống và kế hoạch ổn định hoá (13/11/2026)

- **Mục tiêu người dùng đặt ra:** không cần hào nhoáng, nhưng phải chạy được, chạy mượt, đáp ứng yêu cầu đã chốt, và thiết kế có căn cứ.
- **Căn cứ đối chiếu:** Definition of Done của brief (`docs/archive/ORIGINAL_BRIEF.md` §23), hướng dẫn của giảng viên 24/10 (Airflow, ELT, khối lượng dữ liệu), kế hoạch 03/11 (`docs/archive/PLAN_POLISH_AND_ML.md`), ba lượt chạy thật có bằng chứng (25/10, 31/10, 07/11 trong `docs/evidence/`), CI trên GitHub Actions.
- **Nhánh của đợt này:** `fix/stabilise-20261113` (trên `chore/repo-cleanup`, PR #19). Mọi sửa đổi đều có test và dẫn về một mục trong nhật ký lỗi 07/11.

## 1. Kết luận ngắn

Hệ thống đã đủ về **kiến trúc và tính đúng**: pipeline crawl → Bronze → Kafka → speed/batch → star schema Delta → quality gate → Postgres/Superset và Kibana chạy trọn vẹn trong ba lượt thật; thí nghiệm gây lỗi E1–E5 (31/10) không mất dữ liệu; 1.500+ test, CI xanh. Thứ còn thiếu không phải tính năng mà là:

1. **Dữ liệu.** Mới có 3 ngày crawl (25/10, 31/10, 07/11). DoD đòi 30 ngày liên tục; ML (giảm giá ảo, dự báo nhu cầu) và luật anomaly (7 ngày trong 14 ngày) đều cần lịch sử. Đây là thứ duy nhất **không bù lại được bằng cách làm thêm code**. Demo cuối tháng 12: nếu bật thu thập trước 15/11 thì tới 15/12 có 30 ngày.
2. **Vận hành trơn tru khi bật lại.** Ba trong sáu sự cố ngày 07/11 là do tri thức vận hành nằm trong đầu người chứ không trong công cụ (DAG bị pause, kích tay cần logical date, phải chép audit sang lake). Đợt này chuyển chúng vào code và runbook.
3. **Xử lý lỗi ở cấp dòng trong speed.** Một event sai hợp đồng làm chết cả lớp speed 45 phút (07/11). Đã sửa và bổ sung DLQ để không mất bằng chứng.

## 2. Hiện trạng so với yêu cầu

| Yêu cầu (DoD §23 / giảng viên) | Trạng thái | Bằng chứng |
|---|---|---|
| Thu thập liên tục ≥ 30 ngày | **Chưa đạt** (3 ngày rời rạc; stack tắt từ 07/11) | `docs/evidence/run-20261107/NHAT-KY-LOI.md` mục 4 |
| Raw lưu trước parse, có checksum/provenance | Đạt | Bronze/raw v2, `raw_sha256`, `raw_uri` trong mọi observation |
| Silver trace về raw, reparse không trùng | Đạt | luật `observation_id_unique`, drill reparse |
| Scheduler/retry chạy lại sau restart | Đạt, có điểm yếu | frontier Postgres + lease; worker khởi động lại chờ lease hết hạn 5 phút (31/10 §8.1) |
| Kafka contract có phiên bản | Đạt | `marketplace-record.v1`, `marketplace-observation.v1`, `marketplace-changes.v1`, DLQ v1 |
| Speed phát hiện thay đổi, restart từ checkpoint | Đạt | 6.429 micro-batch 31/10, 0 thất bại; p50 8,8 s với trigger 5 s |
| ES/Redis idempotent | Đạt | id xác định; test 29–38 của sink |
| Batch: current state, history, freshness, coverage, reliability | Đạt, vượt yêu cầu | star schema 7 dim + 6 fact Delta, 10 mart; MERGE theo cửa sổ ngày |
| Quality fail chặn publish; Postgres giữ bản tốt cuối | Đạt | run 11:00 ngày 07/11 QUALITY_FAILED, Superset vẫn giữ bản 31/10 |
| Superset và Kibana đúng ngữ nghĩa | Đạt về số liệu; chưa làm đẹp (P4 của kế hoạch 03/11) | dashboard sinh bằng code |
| Đánh giá hiệu năng, độ tươi, tin cậy, tăng trưởng lưu trữ | Đạt cho 1 ngày | `docs/evidence/e2e-run-20261031/BAO-CAO-CHI-SO.md`; chưa có số liệu nhiều tuần |
| Test và smoke chạy trong môi trường có tài liệu | Đạt | CI: unit mỗi push, Spark 4 shard khi PR vào develop/master |
| Khởi động và demo lại theo runbook | Đạt sau đợt này | `docs/RUNBOOK.md` mục "Bringing a stack back" |
| Airflow (giảng viên 24/10) | Đạt | 4 DAG; ADR-0002 |
| ELT giữ mọi trường (giảng viên 24/10) | Đạt | Bronze/records JSONL đầy đủ trường; `<source>_listing_wide` |
| Bài toán ML (kế hoạch 03/11) | **Chưa bắt đầu**, bị chặn bởi dữ liệu | – |

## 3. Đánh giá thiết kế

**Điểm mạnh, giữ nguyên.**

- Quyết định được ghi thành ADR (8 bản), luật phụ thuộc giữa tầng được `import-linter` và test kiến trúc kiểm tự động. Domain thuần, không I/O, nên luật thay đổi và luật counter test được không cần Spark.
- Batch là máy trạng thái có stage rõ, `publish` trước `promote`, lock advisory, manifest có phiên bản Delta, cửa sổ ngày và backfill phải chủ động (`--allow-backfill`). Đây là phần "khoa học" nhất của hệ thống: chạy lại cho cùng kết quả, không ghi đè lịch sử ngoài ý muốn.
- Quality gate tách bắt buộc/cảnh báo, mọi kết quả ghi cùng rule version và run id; luật bắt buộc bị SKIPPED tính là FAIL (không có Gold nào lên BI khi chưa kiểm).
- Mọi id xác định từ nội dung (offer, observation, change, dlq), nên at-least-once của Kafka và replay micro-batch không tạo trùng.
- Đánh giá dựa trên bằng chứng đo được (throughput, p50/p95, thí nghiệm gây lỗi), không dựa trên ảnh chụp dashboard.

**Điểm yếu có bằng chứng, và cách xử lý.**

| # | Điểm yếu | Bằng chứng | Xử lý |
|---|---|---|---|
| 1 | Speed coi lỗi một dòng là lỗi cả micro-batch; offset không commit nên restart đọc lại đúng dòng đó mãi | 07/11 mục 5, Kibana đứng 45 phút | **Đã sửa** (commit `d27a370`): dòng sai hợp đồng thành INVALID, dòng còn lại vẫn xử lý |
| 2 | Normalizer để lọt discount âm dù hợp đồng domain cấm | 07/11 mục 5 | **Đã sửa** (cùng commit): `_bounded()` cho discount và percent, dùng chung speed và Silver |
| 3 | Dòng INVALID chỉ còn là một con số; topic DLQ observations khai báo mà không ai ghi | 07/11 mục 5 "việc nên làm" | **Đã sửa**: speed ghi DLQ record v1 có payload, id xác định theo (topic, partition, offset, stage); projector sẵn có đưa vào Kibana |
| 4 | Tham số `COUNTER_MAX_GAP` (24 h) bằng đúng cadence SWEEP (1 ngày) nên mọi bước chuyển của tier SWEEP đều `GAP_TOO_LONG` theo cấu trúc, không phải theo dữ liệu | 07/11 mục 4: 51–57% không hợp lệ, hơn 99% là GAP_TOO_LONG | **Đã sửa**: mặc định 48 h (chịu được một lần bỏ sót sweep), settings từ chối gap < 1,5 × cadence chậm nhất; ghi vào DATA_MODEL |
| 5 | Tri thức vận hành không nằm trong công cụ: DAG giữ cờ pause qua restart; kích tay cần logical date; audit cũ phải chép sang lake | 07/11 mục 1–3 | **Đã sửa**: template có fallback `dag_run.run_after`; `mp.ps1 dags list/pause/unpause`; checklist trong RUNBOOK |
| 6 | Speed crash khi sink tạm lỗi (ES, Kafka) thay vì thử lại có backoff; Docker khởi động lại container | 31/10 E2, E3 | Chưa sửa. Dữ liệu vẫn đúng nhờ checkpoint; cái mất là log nhiễu và cảnh báo giả. Đề xuất P2 |
| 7 | Worker crawl khởi động lại chờ lease cũ hết hạn (5 phút) | 31/10 E4 | Chưa sửa. Đề xuất P2: worker nhận lại lease của chính worker id |
| 8 | Lazada: bị chặn sau 76 trang, frontier chỉ 15 trang/giờ | 31/10 §9 | Giữ `CRAWL_SOURCES=tiki` cho thu thập dài hạn; Lazada chỉ là nguồn minh hoạ đa nguồn |
| 9 | Nợ code đã biết: `build_offer_change_daily` chưa dùng `domain/change_rules.py`; `ops/drills.py` 72 KB; 11 lỗi parse Lazada chưa rõ | kế hoạch 03/11 P2 | Chưa làm; không ảnh hưởng tính đúng |
| 10 | Máy chạy stack là laptop dùng chung: thiếu RAM làm batch chậm 1,5–2 lần, giám sát nền bị dừng | 31/10 §7, 07/11 mục 6 | Chạy tối thiểu (mục 5.1); test Spark trên CI, không trên máy |

## 4. Việc đã làm trong đợt này

Năm commit trên `fix/stabilise-20261113`. Trên máy đã chạy xanh: ruff, lint-imports, và các test đơn vị của những module bị chạm (sink speed, orchestration, settings, context, projector, validate, kiến trúc). Test Spark để CI chạy khi mở PR vào `develop`.

| Commit | Nội dung | Test |
|---|---|---|
| `d27a370` | Hai bản sửa ngày 07/11 (discount ngoài hợp đồng → null; dòng sai hợp đồng không giết speed) và nhật ký lỗi | `test_normalize.py`, `test_marketplace_speed_layer.py` |
| DLQ | Speed ghi dòng INVALID vào `marketplace.observations.v1.dlq` kèm payload | 4 test mới trong `test_marketplace_speed_sinks.py` |
| Airflow | Kích tay không cần `--logical-date`; run theo lịch không đổi | render với `StrictUndefined` như Airflow |
| Counter gap | Mặc định 48 h; settings từ chối gap mâu thuẫn với cadence | `tests/unit/settings/test_counter_gap.py` |
| Ops | `mp.ps1 dags`, checklist bật lại stack | – (PowerShell) |

## 5. Việc còn lại, theo thứ tự

### 5.1 P0, ngay hôm nay: bật thu thập dài hạn và để yên

Không làm gì khác trước việc này. Cấu hình tối thiểu để laptop chịu được nhiều tuần: crawl Tiki + Kafka + MinIO + Postgres + Airflow (batch mỗi giờ). Speed, Kibana, Superset và bộ giám sát chỉ bật khi cần xem hoặc demo (giảng viên 24/10: "speed chỉ chạy lúc test").

```powershell
# Windows: tắt sleep; Docker Desktop: Start when you log in.
cd C:\code\data-refactor
.\scripts\mp.ps1 -EnvFile env/refactor.env up -With crawl,orchestration,ops
.\scripts\mp.ps1 -EnvFile env/refactor.env dags unpause
docker exec rf-crawl-worker python -m ecomdata.ops audit-export      # một lần, idempotent
.\scripts\mp.ps1 -EnvFile env/refactor.env validate
```

Kiểm tra hằng ngày (2 phút): `mp.ps1 -EnvFile env/refactor.env status` và `validate`; mỗi tuần `backup`. Khi cần BI: `up -With speed,serve`. Ổ C: còn 178 GB (13/11); ngày 31/10 Bronze tăng 385 MB raw + 74 MB records trong 6 giờ crawl Tiki, tức khoảng 1,5–2 GB/ngày, nên 6 tuần cần dưới 100 GB kể cả Silver, Gold và Kafka.

Chấp nhận khi: `validate` PASS mỗi ngày, `counter_invalid_transition_rate` xuống dưới 50% sau sweep thứ hai, `price_anomaly_evaluation_coverage` > 0 sau 7 ngày.

### 5.2 P1, tuần này: merge

1. Mở PR `fix/stabilise-20261113` → `develop` (xếp sau PR #19). CI chạy 4 shard Spark khi PR vào develop.
2. Merge #19 rồi PR này. Sau merge, image Spark/Airflow không cần build lại vì stack dev mount worktree (`AIRFLOW_SOURCE_MOUNT`), nhưng **speed và crawl-worker đang chạy phải restart** để nạp code mới: `mp.ps1 -EnvFile env/refactor.env up -With crawl,speed` (compose recreate).

### 5.3 P2, 2–3 ngày: độ bền vận hành (điểm yếu 6, 7, 9)

- `BatchWriter`: thử lại có giới hạn với backoff khi sink ném lỗi mạng (ES/Redis/Kafka), ghi audit `FAILED` rồi mới ném. Chấp nhận: drill E2/E3 không tạo restart container.
- Worker: khi khởi động, nhận lại ngay các task đang `LEASED` bởi chính `worker_id` của nó. Chấp nhận: drill E4 không còn khoảng chờ 5 phút.
- Nợ code: `build_offer_change_daily` gọi `change_rules`; tách `drills.py`.

### 5.4 P3: bài toán phân tích, đã chọn và cài đặt (ADR-0009, nhánh `feat/discount-credibility`)

Bài toán: **độ tin cậy của khuyến mãi** (giảm giá ảo). Mức giảm hiển thị của mỗi offer-ngày được so với lịch sử giá của chính offer đó: giá tham chiếu là giá thấp nhất 30 ngày trước (quy tắc Omnibus), trần 50 % theo Nghị định 81/2018. Năm luật có lý do đọc được từ cột của mart; Isolation Forest (scikit-learn) chỉ xếp hạng để xem tay. Hai mart mới `discount_credibility_daily` và `discount_daily` dựng trong stage `marts`, qua cổng chất lượng và publish như mười mart cũ. Chạy thử trên Gold thật 31/10: `docs/evidence/discount-credibility-20261113/`. Dự báo nhu cầu để lại cho đến khi có ≥ 3 tuần dữ liệu liên tục (ADR-0009 nêu lý do).

Việc còn lại của P3: gán nhãn tay ~100 offer-ngày top để đo precision@k khi có ≥ 7 ngày dữ liệu; dựng chart Superset theo spec `serving/superset/marketplace_promotion_dashboard.py`.

### 5.5 P4: dashboard

Giữ dashboard sinh bằng code; chỉ thống nhất màu và bố cục bốn trang Superset theo kế hoạch 03/11 §4. Không thêm công cụ mới.

## 6. Rủi ro và quyết định cần người dùng

| Rủi ro | Kiểm soát |
|---|---|
| Laptop ngủ, Docker không tự bật, stack chết như `mp-live` tháng 10 | Tắt sleep, Docker tự khởi động, kiểm tra mỗi sáng bằng `status` (có thể gắn vào báo cáo 07:47 hằng ngày) |
| Tiki chặn hoặc đổi schema | raw-first + fixtures; luật drift trong Silver báo; adapter version |
| Không kịp 30 ngày trước demo | Bật ngay; nếu demo trước 15/12, báo cáo trung thực số ngày thực có, replay chỉ dùng cho benchmark |
| Merge thứ tự PR #19 rồi PR này | Nhánh này tạo từ #19 nên không xung đột |

Quyết định cần chốt: (1) bật thu thập theo mục 5.1 ngay; (2) có bật speed liên tục hay chỉ khi demo; (3) có làm P2 trước P3 hay bỏ P2.
