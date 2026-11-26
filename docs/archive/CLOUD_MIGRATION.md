# Kế hoạch mở rộng lưu trữ lên cloud

## 0. Chẩn đoán: crash không đến từ tầng lưu trữ

Trước khi lên kế hoạch, ba dữ kiện đo được trên chính repo này:

| Dữ kiện | Bằng chứng |
|---|---|
| Dự án hiện không dùng HDFS | `grep -rn "hdfs\|namenode\|datanode"` → 0 kết quả ngoài `.venv`; `docker-compose.yml` có 12 service, không có namenode |
| Spark worker bị cấu hình 2 GB | `docker-compose.yml` cũ: `SPARK_WORKER_MEMORY: 2g`, `SPARK_WORKER_CORES: 2` |
| Dữ liệu đầu vào 5–9 GB mỗi file | `data/data_kaggle/2019-Nov.csv` 8589 MB, `2019-Oct.csv` 5406 MB |
| Phần cứng dư sức | 31.1 GB RAM (trống 15.3 GB), 8 nhân, 181 GB đĩa trống |

**Kết luận: worker 2 GB không thể giữ nổi một shuffle stage của file 8.6 GB.** Triệu chứng là OOM và heartbeat timeout — nhìn giống hạ tầng sập, nhưng nguyên nhân là một con số trong config. Đây nhiều khả năng cũng là lý do cụm HDFS trước đây "crash không hoàn thành được": không phải HDFS yếu, mà là toàn bộ stack bị bóp bộ nhớ trên cùng một chiếc laptop.

**Hệ quả cho kế hoạch: chuyển dữ liệu lên cloud KHÔNG sửa được lỗi này.** Nếu executor vẫn 2 GB, đọc từ S3 chỉ chậm hơn đọc từ đĩa vì thêm chặng mạng. Hai vấn đề phải tách rời và xử lý theo đúng thứ tự.

---

## 1. "Chuẩn big data phải dùng HDFS" — tiền đề này đã lỗi thời

Đây là điểm cần làm rõ vì nó quyết định cả chương kiến trúc của báo cáo.

HDFS ra đời năm 2006, khi mạng trong datacenter **chậm hơn** đĩa cục bộ. Vì vậy Hadoop được thiết kế quanh nguyên tắc *data locality*: mang phép tính đến chỗ dữ liệu, vì chuyển dữ liệu đi quá đắt. Ngày nay mạng datacenter đạt 10–100 Gbps, **nhanh hơn đĩa** — tiền đề gốc của HDFS không còn đúng.

Toàn ngành đã dịch chuyển theo:

- EMR, Databricks, Snowflake, BigQuery, Synapse: đều đặt trên object storage, không phải HDFS
- Netflix, Uber, Airbnb: đã rời HDFS sang S3
- Hadoop 3 bổ sung S3A committer **chính là để chạy tốt trên object store**
- Iceberg / Delta Lake / Hudi ra đời để cung cấp thứ HDFS có sẵn (transaction, schema evolution) trên nền object store

Đặc điểm định nghĩa kiến trúc dữ liệu hiện đại là **tách lưu trữ khỏi tính toán**. HDFS ghép chặt hai tầng: muốn thêm dung lượng phải thêm node compute, và cụm phải luôn chạy kể cả khi không xử lý gì.

**Vậy nên MinIO không phải giải pháp chữa cháy — nó là kiến trúc đúng, và S3 chính là bản cloud của nó.** Việc đổi từ HDFS sang MinIO là một nâng cấp, không phải một bước lùi.

### Khác biệt kỹ thuật cần nắm (phần ghi điểm khi bảo vệ)

| | HDFS | Object store (MinIO / S3) |
|---|---|---|
| `rename` | nguyên tử, O(1) | copy + delete, O(n) → **ảnh hưởng commit protocol** |
| Data locality | có | không (bù bằng băng thông) |
| Nhất quán | strong | strong (S3 từ 12/2020) |
| Mở rộng dung lượng | phải thêm node | độc lập với compute |
| Liệt kê nhiều file nhỏ | nhanh | chậm → cần nén file |
| Chi phí khi nhàn rỗi | cụm vẫn chạy | chỉ trả tiền dung lượng |

Dòng `fs.s3a.committer.name = directory` trong `config/storage.py` chính là để xử lý vấn đề `rename` ở hàng đầu tiên: committer mặc định của Spark dựa vào rename nguyên tử, thứ object store không có.

### Nếu hội đồng hỏi "sao không dùng HDFS?"

> Vì kiến trúc tách lưu trữ khỏi tính toán. HDFS ghép chặt hai tầng và đòi cụm luôn hoạt động. S3A là giao diện Hadoop tiêu chuẩn nên mã xử lý không đổi một dòng, và đây đúng là kiến trúc mà EMR và Databricks đang dùng. MinIO cho phép phát triển cục bộ với cùng giao thức, rồi chuyển sang S3 chỉ bằng đổi cấu hình.

---

## 2. Trạng thái: đã triển khai

### 2.1 Sửa nguyên nhân crash (miễn phí, không cần cloud)

`docker-compose.yml` — worker giờ nhận cấu hình theo máy, mặc định hợp lý:

```yaml
SPARK_WORKER_CORES:  ${SPARK_WORKER_CORES:-4}
SPARK_WORKER_MEMORY: ${SPARK_WORKER_MEMORY:-8g}
```

`batch_layer/warehouse_job.py:build_spark` — nâng worker thôi chưa đủ, executor phải được lệnh mới đòi phần bộ nhớ đó:

```python
.config("spark.executor.memory",            os.getenv("SPARK_EXECUTOR_MEMORY", "6g"))
.config("spark.driver.memory",              os.getenv("SPARK_DRIVER_MEMORY", "4g"))
.config("spark.sql.files.maxPartitionBytes", os.getenv("SPARK_MAX_PARTITION_BYTES", "67108864"))
.config("spark.sql.adaptive.enabled",       "true")
```

Chia split 64 MB thay vì 128 MB mặc định: đổi chi phí lập lịch lấy khoảng trống bộ nhớ, phù hợp khi mỗi dòng khá rộng.

### 2.2 Tầng lưu trữ portable (`config/storage.py`)

Trước đây `build_spark` hardcode hai thiết lập **chỉ đúng với MinIO**:

```python
hadoop.set("fs.s3a.path.style.access", "true")       # AWS S3 cần false
hadoop.set("fs.s3a.connection.ssl.enabled", "false") # mọi endpoint cloud cần true
```

Nghĩa là tính "portable" của S3A chỉ tồn tại trên lý thuyết — job này không thể chạy với bucket cloud thật. Giờ cả hai đến từ profile đang kích hoạt:

| Profile | Endpoint | TLS | path-style | Ghi chú |
|---|---|:--:|:--:|---|
| `local` | — | — | — | parquet dưới `DATA_LAKE_LOCAL_ROOT`, dùng cho test |
| `minio` | `MINIO_ENDPOINT` | ✗ | ✓ | stack docker hiện tại |
| `s3` | SDK tự phân giải theo region | ✓ | ✗ | AWS S3 |
| `r2` | `DATA_LAKE_ENDPOINT` | ✓ | ✓ | Cloudflare R2 |
| `b2` | `DATA_LAKE_ENDPOINT` | ✓ | ✓ | Backblaze B2 |

Chuyển nhà cung cấp = đổi một biến môi trường:

```bash
DATA_LAKE_PROFILE=s3
DATA_LAKE_REGION=ap-southeast-1
DATA_LAKE_ACCESS_KEY=...
DATA_LAKE_SECRET_KEY=...
```

`DATA_LAKE_MODE` cũ vẫn được tôn trọng (`s3a` → profile `minio`) nên docker-compose và `scripts/*.ps1` không phải sửa.

Region mặc định `ap-southeast-1` (Singapore): đi us-east-1 từ Việt Nam thêm khoảng 200 ms mỗi request, con số này chi phối khi tải nhiều object nhỏ.

Đã kiểm chứng: `tests/test_storage.py` (8 test) khẳng định đúng hai thiết lập từng bị hardcode phải **đảo ngược** giữa profile MinIO và profile cloud — một hồi quy sẽ không thể âm thầm lọt qua.

---

## 3. Kế hoạch còn lại

### Giai đoạn 1 — Xác nhận pipeline chạy hết local (không tốn tiền)

1. `docker compose up -d`, chạy `warehouse-job` trên `2019-Oct.sample.csv` (270 MB) → xác nhận thông suốt
2. Chạy lại trên `2019-Oct.csv` đầy đủ (5.4 GB) với cấu hình bộ nhớ mới
3. Ghi lại thời gian chạy và mức bộ nhớ đỉnh — **đây là số liệu đối chứng cho báo cáo**

Nếu bước 2 chạy xong, giả thuyết "crash do config" được chứng minh, và việc lên cloud trở thành lựa chọn kiến trúc chứ không phải giải pháp cấp cứu.

### Giai đoạn 2 — Đưa lưu trữ lên cloud

Chỉ đẩy **silver + gold** lên. Bronze là CSV thô 14 GB, giữ ở máy: nó tái tạo được từ nguồn Kaggle và không có lý do gì trả tiền lưu trữ.

Ước lượng dung lượng: 14 GB CSV → Parquet + snappy nén khoảng 3–5 lần → **silver + gold cỡ 3–4 GB**.

| Nhà cung cấp | Miễn phí | Phí sau đó | Egress | Đánh giá |
|---|---|---|---|---|
| **Cloudflare R2** | 10 GB | $0.015/GB/tháng | **$0** | Vừa khít, không phí egress — tốt nhất cho đồ án |
| Backblaze B2 | 10 GB | $0.006/GB/tháng | 3× dung lượng lưu/tháng miễn phí | Rẻ nhất |
| AWS S3 | 5 GB (12 tháng) | $0.023/GB/tháng | $0.09/GB | Tên tuổi có sức nặng khi bảo vệ; egress là chỗ đau |

Với 3–4 GB, chi phí thực tế ở cả ba đều gần như bằng 0. Yếu tố quyết định là **egress** — mỗi lần Spark local đọc lại toàn bộ gold từ cloud là một lần tính phí ra. R2 miễn phí khoản này.

### ✅ ĐÃ CHỐT (2026-08-17): **AWS S3, region `ap-southeast-1`**

Quyết định của user. Đánh đổi đã biết và cách xử lý:

- **Egress KHÔNG phải chỗ đau ở quy mô này** (đã kiểm lại 2026-08-17): AWS cho
  **100 GB/tháng data transfer out miễn phí vĩnh viễn**, gộp chung mọi service &
  region — không phải quyền lợi 12 tháng. Gold 3–4 GB ⇒ đọc full **~25 lần/tháng
  vẫn $0**. Chỉ vượt mức đó mới tính $0.09/GB. (Ghi chú cũ ở file này từng coi
  egress là điểm đau chính — sai, vì bỏ sót hạn mức 100 GB.)
- **Chi phí thật lại nằm ở số lượng object, không phải dung lượng**: PUT
  ~$0.005/1.000 request. Spark ghi Parquet thành hàng nghìn file nhỏ mỗi lần
  chạy ⇒ 30 lần chạy/tháng × 2.000 file = 60.000 PUT ≈ **$0.30**, đắt gấp 3 lần
  tiền lưu trữ. Cách giảm: `coalesce`/`repartition` trước khi ghi (file to hơn,
  ít object hơn) và đọc silver **incremental** theo watermark (Phase 4) thay vì
  full-reprocess.
- Bronze CSV 14 GB **không đẩy lên** — vừa tốn tiền lưu vừa tốn PUT, mà tái tạo
  được từ nguồn Kaggle.
- Bù lại: tên tuổi S3 + region Singapore là câu trả lời gọn khi hội đồng hỏi
  "sản phẩm này chạy được trên cloud thật chứ?", và `fs.s3a.*` là cấu hình
  Hadoop chuẩn nên mọi tài liệu tham khảo đều khớp.

Cấu hình cần đặt trong `.env` (không sửa dòng code nào):

```
DATA_LAKE_PROFILE=s3
DATA_LAKE_REGION=ap-southeast-1
DATA_LAKE_ACCESS_KEY=<AWS access key id>
DATA_LAKE_SECRET_KEY=<AWS secret access key>
```

`config/storage.py` để `endpoint=None` cho profile `s3` → SDK tự phân giải
`s3.ap-southeast-1.amazonaws.com`, `path_style_access=False` (S3 dùng
virtual-hosted style, khác MinIO), TLS bật.

### Free tier đã hết thì dùng S3 thế nào (2026-08-17)

Free tier hết **không** chặn việc dùng S3; nó chỉ chuyển sang trả tiền theo dùng.
Ở quy mô đồ án này, hoá đơn thực tế **dưới $1/tháng**:

| Khoản | Đơn giá (`ap-southeast-1`, tham khảo) | Dùng thực tế | Tiền/tháng |
|---|---|---|---|
| Lưu trữ S3 Standard | ~$0.025/GB | silver+gold 4 GB | **~$0.10** |
| PUT/COPY/POST/LIST | ~$0.005/1.000 | ~60.000 PUT (30 lần chạy) | **~$0.30** |
| GET | ~$0.0004/1.000 | vài chục nghìn | **~$0.02** |
| Egress ra Internet | $0.09/GB **sau 100 GB free/tháng** | 4 GB × vài lần đọc | **$0** |
| | | | **≈ $0.4/tháng** |

Cộng VAT 10% (AWS xuất hoá đơn cho khách VN) vẫn ~$0.45. Cần thẻ Visa/Mastercard
thanh toán quốc tế; AWS tạm giữ $1 để xác minh.

**Việc phải làm, theo đúng thứ tự:**

1. **Xác định account thuộc chế độ nào** (Billing and Cost Management → *Free tier*):
   - Account tạo **trước 15/07/2025**: free tier 12 tháng cũ. Hết hạn = tự động
     pay-as-you-go, **không cần làm gì thêm** — và các mục "always free" (trong
     đó có 100 GB egress/tháng) vẫn còn.
   - Account tạo **sau 15/07/2025**: chế độ mới, $200 credit / 6 tháng. Khi credit
     hết hoặc quá 6 tháng, **Free plan tự đóng** ⇒ phải **chuyển sang Paid plan**
     (Billing → Account → *Upgrade to Paid plan*) mới dùng tiếp được, nếu không
     tài nguyên bị giới hạn/đóng.
2. **Chặn hoá đơn bất ngờ trước khi tạo bucket**: AWS Budgets → budget $1 và $5
   (2 budget đầu miễn phí) + bật *Receive Billing Alerts*. Đây là bước quan trọng
   nhất với account sinh viên.
3. **Tạo 2 bucket** `ecommerce-silver`, `ecommerce-gold` ở `ap-southeast-1`:
   Block Public Access **bật**, **Versioning TẮT** (job ghi đè Parquet mỗi lần
   chạy — bật versioning là nhân bản dung lượng vô ích), default encryption SSE-S3.
4. **Lifecycle rule bắt buộc: *Abort incomplete multipart uploads* sau 1 ngày.**
   S3A committer bỏ dở upload khi job chết → phần đã upload **vẫn bị tính tiền
   nhưng không thấy trong danh sách object**. Đây là khoản phí âm thầm kinh điển.
5. **IAM user riêng** (không dùng root key), policy giới hạn đúng 2 bucket:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
         "Resource": ["arn:aws:s3:::ecommerce-silver", "arn:aws:s3:::ecommerce-gold"]
       },
       {
         "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                    "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
         "Resource": ["arn:aws:s3:::ecommerce-silver/*", "arn:aws:s3:::ecommerce-gold/*"]
       }
     ]
   }
   ```

   `AbortMultipartUpload` + `ListMultipartUploadParts` là bắt buộc cho S3A —
   thiếu là job chết giữa lúc commit.
6. Đặt 4 biến `DATA_LAKE_*` ở trên vào `.env`, chạy `warehouse_job` — **không sửa
   dòng code nào**.
7. Đối chiếu số dòng gold giữa local và cloud để xác nhận toàn vẹn.

**Chiến lược tiết kiệm nhất mà vẫn "chạy trên cloud thật" khi bảo vệ:** giữ
MinIO cho phát triển hằng ngày (đang chạy tốt, $0), chỉ bật
`DATA_LAKE_PROFILE=s3` trong **giai đoạn demo/bảo vệ** rồi xoá bucket sau đó —
tổng chi phí cỡ vài chục cent. Nếu không muốn dùng thẻ: profile `r2` đã có sẵn
trong `config/storage.py` (Cloudflare R2 free 10 GB, egress $0, đổi 1 biến môi
trường) — nhưng khi đó mất cái tên "AWS S3" trong báo cáo.

*Đơn giá trên là tham khảo, đối chiếu lại tại https://aws.amazon.com/s3/pricing/
(chọn region Asia Pacific — Singapore) trước khi ghi vào báo cáo.*

### Giai đoạn 3 — (Tùy chọn) đưa compute lên cloud

Chỉ làm nếu Giai đoạn 1 cho thấy laptop thật sự không đủ. Khi dữ liệu ở cloud mà compute ở nhà, mỗi lần chạy phải kéo toàn bộ dataset qua đường truyền Internet — thường chậm hơn chạy hoàn toàn local.

Lựa chọn theo chi phí tăng dần: Spark local (miễn phí) → một VM cloud cùng region với bucket → EMR Serverless / Dataproc Serverless (trả theo lần chạy) → Databricks.

---

## 4. Cần quyết định

1. ~~**Nhà cung cấp**: R2 hay S3?~~ → **đã chốt S3 `ap-southeast-1`** (2026-08-17), xem mục 3.
2. **Có chạy Giai đoạn 1 trước không?** Tôi khuyến nghị có — nếu 5.4 GB chạy trót lọt trên máy, toàn bộ nhánh cloud chuyển từ "bắt buộc" sang "để chứng minh kiến trúc", và tiết kiệm được nhiều tuần. **Vẫn chưa chạy được: Docker Desktop đang tắt (2026-08-17).**
