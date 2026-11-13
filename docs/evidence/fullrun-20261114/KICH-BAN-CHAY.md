# Kịch bản chạy toàn luồng 10 giờ có đánh giá ML (để giao cho agent tự chạy)

- **Mục tiêu:** chạy toàn bộ pipeline (crawl → Kafka → speed → ES/Redis/Kibana; Airflow batch mỗi giờ → star → marts gồm hai mart ML của ADR-0009 → quality → Postgres → Superset) khoảng 10 giờ, kiểm tra từng lớp mỗi giờ, rồi chốt kết quả theo `CHIEN-LUOC-DANH-GIA-ML.md`.
- **Worktree:** `C:\code\data-refactor`, nhánh `feat/discount-credibility` (PR #21). Stack `mp-refactor`, container `rf-*`. Mọi lệnh chạy từ gốc worktree.
- **Không làm:** không gán nhãn (mục 6), không sửa ngưỡng trong `danh_gia_ml.py` sau khi đã thấy số, không chạy full pytest trên máy (CPU 99 %, xem CI), không `down -Volumes`, không bật crawler Lazada (bị chặn 31/10), không push lên `develop`/`master`.
- **Commit:** chỉ dưới tên người dùng, không thêm dòng `Co-Authored-By` (quy ước của repo này).

## 0. Trạng thái lúc giao (14/11, 01:40 giờ VN)

Docker Desktop đang chạy; stack đang bật một phần: crawl-worker (Tiki), speed, Kafka, MinIO, Postgres, ES, es-projector, Airflow, Loki/Prometheus/Grafana/Alloy. **Kibana và Superset đã tắt (exit 255).** Image Spark `ecommerce/spark-marketplace:4.0.1-r4` là bản cũ, **chưa có scikit-learn**. Bảng `cache.marketplace_discount_credibility_daily` và `cache.marketplace_discount_daily` **chưa tồn tại** (chưa `migrate`). Có một run batch `mp-20261113T1800Z` ở trạng thái `GOLD_WRITTEN` do Airflow tự chạy bằng code mới mount từ worktree; nó sẽ trượt ở `publish` vì thiếu bảng cache, đó là lý do bước 1 pause DAG trước.

## 1. Chuẩn bị (khoảng 20 phút)

```powershell
cd C:\code\data-refactor
# 1.1 Dừng batch trước khi đổi bảng và image.
.\scripts\mp.ps1 -EnvFile env/refactor.env dags pause
# 1.2 Bảng cache mới (idempotent).
.\scripts\mp.ps1 -EnvFile env/refactor.env migrate
# 1.3 Build lại image (Spark có scikit-learn) và bật đủ các lớp, Kibana và Superset.
#     Build mất 5-15 phút; chạy nền và chờ xong, không ngắt giữa chừng.
.\scripts\mp.ps1 -EnvFile env/refactor.env up -With crawl,speed,orchestration,serve,ops
# 1.4 Kiểm tra scikit-learn có trong image.
docker run --rm ecommerce/spark-marketplace:4.0.1-r4 python3 -c "import sklearn; print(sklearn.__version__)"
# 1.5 Audit crawl cũ sang lake (ADR-0008), một lần, idempotent.
docker exec rf-crawl-worker python -m ecomdata.ops audit-export
# 1.6 Bật lại batch và discovery (maintenance và reprocess để pause).
.\scripts\mp.ps1 -EnvFile env/refactor.env dags unpause
.\scripts\mp.ps1 -EnvFile env/refactor.env dags
```

Run batch `mp-20261113T1800Z` kẹt từ trước: nếu sau bước 1.6 nó vẫn chặn run mới (`max_active_runs=1`), đánh dấu failed trong Airflow UI (`http://localhost:8384`) hoặc REST `PATCH /api/v2/dags/marketplace_batch/dagRuns/<id>` với `{"state":"failed"}`, ghi lý do vào `NHAT-KY.md`.

**Mốc giờ 0** là lúc bước 1.6 xong; ghi giờ đó (UTC và giờ VN) vào đầu `NHAT-KY.md`.

## 2. Mỗi giờ (giờ 1 đến giờ 10)

```sh
sh docs/evidence/fullrun-20261114/theo-doi.sh
```

Script ghi thêm vào `theo-doi.log` và chụp verdict ML vào `snapshots/<run_id>.csv` (dùng để đo độ ổn định). Sau mỗi lần chạy:

- dòng `PROBLEM` nào cũng phải được xử lý theo mục 5 và ghi vào `NHAT-KY.md` (giờ, hiện tượng, nguyên nhân gốc, cách sửa, kiểm chứng), theo mẫu `docs/evidence/run-20261107/NHAT-KY-LOI.md`;
- từ giờ 3, chạy thêm `python docs/evidence/fullrun-20261114/danh_gia_ml.py report` để thấy tiêu chí nào đang CHUA_DU/FAIL sớm.

Python: `/c/code/data/.venv/Scripts/python` (có psycopg2, minio, requests). Máy dùng chung và RAM chật: không chạy vòng lặp nền dài; kiểm tra theo lượt.

## 3. Kiểm tra từng lớp một lần trong giờ đầu

| Lớp | Kiểm tra | Đạt |
|---|---|---|
| Crawl | `theo-doi.sh` mục crawl | lượt tải tăng, không `RATE_LIMITED`/`CLIENT_ERROR` |
| Speed | `theo-doi.sh` mục speed; Kibana `http://localhost:5604` có thay đổi giá mới | `failed=0`; p95 giảm dần sau khi đuổi kịp phần tồn Kafka |
| DLQ speed (PR #20) | `observations DLQ messages` | dòng INVALID nào cũng có bản ghi DLQ; ES index `marketplace-dlq-*` có tài liệu tương ứng |
| Batch | `audit.marketplace_batch_run` | run thứ hai trở đi PROMOTED, quality PASS |
| ML | `danh_gia_ml.py report` | bảng cache có dòng; `ml_method = ISOLATION_FOREST` xuất hiện; dòng log `discount_ml_fitted` trong Loki |
| Serving | Superset `http://localhost:8086` (admin/admin) mở được bảng `marketplace_discount_credibility_daily`; Grafana `http://localhost:3004` không cảnh báo đỏ | |

## 4. Chốt ở giờ 10

```sh
python docs/evidence/fullrun-20261114/danh_gia_ml.py snapshot
python docs/evidence/fullrun-20261114/danh_gia_ml.py report          # -> bao-cao-ml.json
python docs/evidence/fullrun-20261114/danh_gia_ml.py export-labels   # -> nhan-tay.csv, nhan-tay-lich-su-gia.csv (để dành, mục 6)
```

Rồi viết `KET-QUA.md` (tiếng Việt, cùng thư mục):

1. Thời gian chạy thực, các lớp đã bật, số liệu tổng: lượt crawl, bản ghi, micro-batch speed, run batch theo trạng thái.
2. Bảng mọi tiêu chí của `CHIEN-LUOC-DANH-GIA-ML.md` mục 1-3 với số đo, ngưỡng, PASS/FAIL/CHUA_DU, lấy từ `bao-cao-ml.json`. Không đổi ngưỡng; tiêu chí FAIL thì ghi nguyên nhân.
3. Ghi chú hiệu năng: mốc thời gian stage `marts` lấy từ run đầu của chính lượt này (không có mốc cũ có ML), nên đây là đo ổn định, không phải so trước/sau.
4. Mục "Lỗi gặp trong lượt chạy" tóm tắt `NHAT-KY.md`.

Sau đó:

```powershell
.\scripts\mp.ps1 -EnvFile env/refactor.env backup
.\scripts\mp.ps1 -EnvFile env/refactor.env dags pause
docker compose -p mp-refactor --profile "*" stop    # giữ volume
```

Commit thư mục `docs/evidence/fullrun-20261114/` (trừ `theo-doi.log` nếu quá 1 MB) và mọi bản sửa code đã làm trong lượt chạy (mỗi bản sửa một commit riêng, có test), push lên `feat/discount-credibility`; CI của PR #21 sẽ chạy lại.

## 5. Xử lý sự cố thường gặp

| Hiện tượng | Xử lý |
|---|---|
| Không run batch nào trong 90 phút | `dags` xem có bị pause; run kẹt thì đánh dấu failed (mục 1) |
| Run QUALITY_FAILED vì `silver_parse_attempt_reconciliation` SKIPPED | chạy lại `audit-export` (bước 1.5), chờ run sau |
| Advisory `counter_invalid_transition_rate` FAIL | dự kiến: dữ liệu đứt quãng từ 07/11 (`GAP_TOO_LONG`); ghi lại, không sửa ngưỡng |
| `ml_method` luôn NONE | image chưa có scikit-learn (bước 1.4) hoặc ít hơn 50 dòng giảm giá; xem dòng `discount_ml_fitted` |
| Speed khởi động lại liên tục | `docker logs rf-speed --tail 100`; một dòng sai hợp đồng giờ phải thành INVALID + DLQ, nếu vẫn crash thì đó là lỗi mới, ghi nhật ký và sửa có test |
| Crawl `RATE_LIMITED` hoặc `CLIENT_ERROR` 403 kéo dài | dừng `rf-crawl-worker`, ghi nhật ký, tiếp tục các lớp khác với dữ liệu đã có |
| RAM host trống dưới 3 GB | dừng Kibana trước, rồi Superset; batch chậm là chấp nhận được |
| Container `Exited (255)` | `docker start <tên>`; nếu lặp lại, xem log và ghi nhật ký |

## 6. Gán nhãn đánh giá mô hình: để dành, bàn lại sau

Isolation Forest học không giám sát nên huấn luyện không cần nhãn. Nhưng để đo nó *đúng* đến đâu (precision@k) thì phải có người xác nhận trên một mẫu nhỏ; số đo không nhãn ở mục 3 của chiến lược chỉ cho biết mô hình chạy và nhất quán. Người dùng và Fable sẽ bàn lại **cách gán nhãn** (bộ nhãn, ai gán, gán mù hay không, cỡ mẫu, xử lý trường hợp đổi biến thể) trước khi gán.

Lượt chạy này chỉ **giữ lại dữ liệu** cho việc đó, và agent không gán nhãn:

| File | Nội dung | Giữ ở đâu |
|---|---|---|
| `nhan-tay.csv` | 3 danh sách × 30 dòng của ngày mới nhất: top `rule_score`, top `ml_rank_percent` trong `CREDIBLE`/`INSUFFICIENT_HISTORY`, baseline `ANOMALOUS_LOW`; có tiêu đề, URL, giá, giá tham chiếu, lý do, hạng; cột `label`, `note` để trống | commit cùng thư mục |
| `nhan-tay-lich-su-gia.csv` | lịch sử giá theo ngày của mọi offer ứng viên, đủ để gán nhãn khi stack đã tắt | commit cùng thư mục |
| `snapshots/*.csv` | verdict mỗi giờ (độ ổn định, và để so khi đổi luật) | commit cùng thư mục |
| backup cuối lượt (`data/ops/backups/bk-*`) | lake + cache đầy đủ, để xuất lại danh sách khác nếu cách gán nhãn đổi cỡ mẫu hoặc ngày | trên máy, không commit |

Đề xuất ban đầu để bàn (chưa chốt): bốn nhãn A/B/C/N theo mục 7 nhật ký 07/11, precision@30 = (A + B) / 30, so ba danh sách với nhau. `danh_gia_ml.py score-labels` đã tính theo đề xuất này; nếu cách gán đổi thì sửa hàm đó.
