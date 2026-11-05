# Chuẩn bị cho Databricks

Ngày 2026-10-25, nhánh `phase-10/star-on-refactor`. Trạng thái: **đánh giá và các đường nối nhỏ đã làm; chưa chạy trên Databricks.**

Tài liệu này chưa biết Databricks sẽ xuất hiện theo dạng nào (gói miễn phí, tài khoản trường, Azure, AWS). Mọi chỗ phụ thuộc vào câu trả lời ghi là **cần xác nhận**. Các phát biểu về hành vi của Databricks là hiểu biết chung, chưa kiểm trên một workspace thật.

## 1. Cái gì đã di chuyển được

Kiến trúc sau refactor đã tách sẵn các điểm mà Databricks cần thay:

| Điểm | Hiện trạng | Vì sao đủ để chuyển |
|---|---|---|
| Nơi lake nằm | `StorageSettings` với profile `local`, `minio`, `s3`, `r2`, `b2`; mọi layer đọc URI `s3a://` | đổi endpoint và khoá, không đổi mã job |
| Tạo SparkSession | một chỗ: `infra/spark.py` (`SparkSessionFactory`) | một chỗ để nói "phiên do nền tảng cấp" |
| Đọc ghi bảng | một chỗ: `processing/batch/lake.py` (`SparkLakeIO`) | một chỗ để đổi Parquet sang Delta |
| Logic batch | stage chỉ nói chuyện qua Protocol (`LakeIO`, `AuditRepo`, `QualityRepo`, `CachePublisher`, `ObjectStore`) | thay adapter, không sửa stage |
| Batch không dùng Kafka | batch đọc Bronze/records (tệp), không đọc topic | chạy được ở nơi không với tới Kafka cục bộ |
| Điểm vào | CLI `python -m ecomdata.processing.batch run --stage ...` | một lệnh có tham số, đóng gói được thành task |

## 2. Cái gì không di chuyển được nguyên trạng

| Phụ thuộc | Chỗ trong mã | Vấn đề trên Databricks | Hướng xử lý |
|---|---|---|---|
| **PostgreSQL cục bộ** | `lake.read_crawl_audit` (JDBC), `repositories.py` (audit, quality, cache, advisory lock) | cluster trên cloud không với tới `localhost`; cổng Postgres của máy cá nhân không mở ra ngoài | chọn một trong ba, mục 3 |
| **Kafka cục bộ** | `processing/speed/*` | cùng lý do; speed là Structured Streaming, cần broker với tới được | giữ speed ở máy cục bộ; Databricks chỉ chạy batch |
| **MinIO** | profile `minio` | không với tới từ cloud | Bronze phải nằm ở S3, R2 hoặc B2 (đã có profile) hoặc ADLS; cần quyết định |
| **Cấu hình session** | `spark.driver.memory`, `spark.hadoop.fs.s3a.*`, `spark.jars.packages` | cluster có sẵn cấu hình; một số setting bị từ chối khi chạy serverless (**cần xác nhận**) | `SPARK_RUNTIME=databricks`: factory không đặt gì |
| **Docker** | Airflow chạy task bằng `DockerOperator` | không có Docker trên Databricks | task `DatabricksRunNowOperator` hoặc `DatabricksSubmitRunOperator` (provider `apache-airflow-providers-databricks`) |
| **Image Spark của dự án** | `docker/spark` | Databricks dùng runtime của họ; Python và thư viện khác | đóng gói thành wheel; entry point `ecomdata-batch` |
| **`applyInPandasWithState`** | speed | không chắc có trên mọi loại compute (**cần xác nhận**) | không đưa speed lên Databricks |

## 3. Kiến trúc đích đề xuất: tách "tính toán" và "phục vụ"

```
Máy cục bộ                                   Cloud
crawl-worker -> Bronze/raw + records ------> bucket S3/R2/B2 (profile s3|r2|b2)
frontier, audit (Postgres)                         |
                                                   v
export crawl audit -> Bronze/audit (Parquet) -> Databricks job: silver -> marts -> quality  (Delta)
                                                   |
Postgres cache.*  <-- publish (chạy cục bộ) <------+ Gold (Delta)
Superset, Kibana, speed (Kafka)                    audit và quality của run: bảng Delta
```

Ba chỗ cần xây, theo thứ tự:

1. **Xuất crawl audit ra lake.** Job cục bộ ghi `audit.crawl_request_attempt`, `crawl_run`, `crawl_frontier` vào `Bronze/audit/` dạng Parquet trước mỗi batch. `read_crawl_audit` đọc từ đó khi chạy ở Databricks. Giữ đường JDBC cho môi trường hiện tại.
2. **Audit, quality và khoá run lưu trong Delta** (adapter mới cho `AuditRepo`, `QualityRepo`). Khoá chạy chồng: `max_concurrent_runs = 1` của Databricks job, thay cho advisory lock của Postgres.
3. **Publish chạy cục bộ.** Stage `publish` và `promote` đọc Gold từ bucket (Spark cục bộ có jar Delta) rồi ghi `cache.*` vào Postgres cục bộ. Hai stage này đã tách riêng, nên Airflow chỉ cần gọi `silver`, `marts`, `quality` trên Databricks rồi `publish`, `promote` trong Docker.

Phương án khác, chưa chọn: đưa luôn Postgres lên dịch vụ ngoài (Neon, Supabase, RDS) để một Postgres duy nhất phục vụ mọi nơi. Đơn giản hơn về mã nhưng kéo frontier và audit của crawler ra khỏi máy; cần quyết định riêng.

## 4. Đã làm trong nhánh này

| Việc | Chỗ | Kiểm |
|---|---|---|
| `DATA_LAKE_TABLE_FORMAT=parquet\|delta` (mặc định `parquet`, nên không đổi gì) | `settings`, `SparkLakeIO` (`write_silver`, `read_silver`, `write_gold`, `read_gold`) | `tests/unit/processing/test_lake_format.py`, `tests/unit/infra/test_spark_factory.py` |
| Extension và catalog Delta khi chọn `delta` trên phiên do dự án tạo | `infra/spark.py` (`DELTA_SESSION_CONFIG`) | `test_spark_factory.py` |
| `SPARK_RUNTIME=databricks`: factory không đặt cấu hình nào, lấy phiên của nền tảng | `infra/spark.py` | `test_spark_factory.py` |
| Jar `delta-spark` và `delta-storage` 4.0.1 trong image Spark; tag mới `ecommerce/spark-marketplace:4.0.1-r3` | `docker/spark/Dockerfile` | smoke ghi và đọc Delta trong image (mục 5) |
| Sửa console script gãy `ecomdata-ops` (trỏ tới module không còn) và thêm `ecomdata-batch` | `pyproject.toml` | `tests/architecture/test_entry_points.py` |

## 5. Kiểm trong Docker

Chạy trong image `ecommerce/spark-marketplace:4.0.1-r3` (Spark 4.0.1, Delta 4.0.1), qua `SparkSessionFactory` và `SparkLakeIO` với `table_format="delta"`, trên hệ tệp cục bộ trong container (kết quả ở mục 7).

## 6. Cần người dùng quyết định

1. **Databricks xuất hiện theo dạng nào?** Gói miễn phí hay tài khoản trường/Azure/AWS? Quyết định ba điều: có dùng được cluster tuỳ chọn hay chỉ serverless, có được gọi ra internet và tới bucket của mình hay không, và có cần Unity Catalog hay không.
2. **Bronze nằm ở đâu?** Cần một bucket cloud (S3, R2, B2 hoặc ADLS) mà cả máy crawl lẫn Databricks với tới. Hiện Bronze nằm ở MinIO cục bộ.
3. **Chọn phương án mục 3** (tách tính toán/phục vụ) hay đưa Postgres lên dịch vụ ngoài.
4. **Vai trò của Databricks trong luận văn:** nơi chạy batch thật, hay chỉ một bản chạy thử để chứng minh pipeline di chuyển được. Cái sau rẻ hơn nhiều.

## 7. Kết quả smoke Delta trong image

Chạy lúc 2026-10-25, một bảng Silver chia partition theo `observed_date`:

| Bước | Kết quả |
|---|---|
| `write_silver` 3 dòng, 2 partition | `read_silver` trả 3 dòng |
| `write_silver` lại, chỉ partition 2026-10-02 (ghi đè partition động) | vẫn 3 dòng; partition 2026-10-01 còn nguyên, partition 2026-10-02 được thay |
| `MERGE INTO delta.\`...\`` cập nhật một dòng | dòng `a` đổi giá 1 thành 11, các dòng khác giữ nguyên |
| `DESCRIBE HISTORY` | 3 phiên bản |
| `VACUUM` | chạy, không lỗi |

Chỉ kiểm hệ tệp cục bộ. Delta trên S3A (MinIO) và trên bucket cloud chưa chạy: cần kiểm ở bước sau, vì S3A không có rename nguyên tử và Delta dùng cơ chế log riêng cho trường hợp đó.

## 8. Triển khai (nhánh `phase-11/databricks`, 2026-11-05)

Quyết định 2026-11-05: chọn hướng lai. Batch và speed chạy trên Databricks, đọc và ghi Delta trên S3. Crawler, frontier, Kafka, PostgreSQL `cache.*`, Superset, Elasticsearch và Redis giữ ở stack local. Crawler từ IP datacenter dễ bị chặn, và stream chạy 24/7 trên cloud thì tốn tiền.

```
Laptop: crawler -> Bronze (MinIO) --mc mirror--> S3 bronze
                                                    |
Databricks job ecomdata-marketplace-batch:  silver -> dimensions -> facts -> marts -> quality  (S3 silver, gold, Delta)
Databricks job ecomdata-marketplace-speed:  Bronze/records -> gold/speed/{observations, offer_state, price_changes, outputs, records_dlq, stream_progress}
```

### Mã

| Việc | Chỗ |
|---|---|
| `--skip-postgres` không lấy khoá advisory trong PostgreSQL; chống chạy chồng bằng `max_concurrent_runs: 1` | `processing/batch/bootstrap.py` |
| Speed trên lakehouse: cùng normaliser và cùng phép so trạng thái, nhưng nguồn là tệp Bronze (Auto Loader) và đích là bảng Delta. Ghi lặp không nhân đôi (`txnAppId`/`txnVersion`, MERGE) | `processing/speed/lakehouse.py`, console script `ecomdata-speed-lakehouse` |
| Trigger `availableNow` cho query có trạng thái | `processing/speed/service.py` (`start_query`) |
| Asset Bundle: wheel, hai job, cluster một node chế độ dedicated (DBR 17.3 LTS, Spark 4.0, Python 3.12) | `databricks.yml`, `databricks/resources/jobs.yml` |

Bundle dùng cluster classic, không chạy được trên Free Edition (chỉ có serverless). Cần một workspace trả phí hoặc dùng thử trên AWS.

### Người dùng chuẩn bị (một lần)

1. **Workspace Databricks** trên AWS, region `ap-southeast-1` nếu được chọn. Đặt AWS Budgets $1 và $5 trước.
2. **Ba bucket S3** cùng region, tên duy nhất toàn cầu, ví dụ `ecomdata-<tên>-bronze`, `-silver`, `-gold`.
3. **Một IAM user** chỉ có quyền `s3:GetObject`, `PutObject`, `DeleteObject`, `ListBucket` trên ba bucket đó. Tạo access key cho user này.
4. **Databricks CLI**: `winget install Databricks.DatabricksCLI`, rồi đăng nhập OAuth trong trình duyệt:
   ```powershell
   databricks auth login --host https://<workspace>.cloud.databricks.com
   ```
   Token nằm trong `~/.databrickscfg` và bộ nhớ đệm token của CLI. Không đưa token vào repo hay vào chat.
5. **Secret scope**, khoá nhập ở prompt, không nằm trong lịch sử lệnh:
   ```powershell
   databricks secrets create-scope ecomdata
   databricks secrets put-secret ecomdata aws_access_key
   databricks secrets put-secret ecomdata aws_secret_key
   ```

### Chạy

```powershell
# Bronze: một cửa sổ vài ngày từ MinIO lên S3 (records và crawl audit)
docker run --rm --network host minio/mc sh -c "mc alias set local http://localhost:<MINIO_API_HOST_PORT> <minio-user> <minio-pass> && mc alias set s3 https://s3.ap-southeast-1.amazonaws.com <key> <secret> && mc mirror local/ecommerce-bronze/marketplace s3/<bucket-bronze>/marketplace"

$env:BUNDLE_VAR_bucket_bronze = "<bucket-bronze>"; $env:BUNDLE_VAR_bucket_silver = "<bucket-silver>"; $env:BUNDLE_VAR_bucket_gold = "<bucket-gold>"
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev marketplace_batch --params as_of=2026-11-05T00:00:00Z
databricks bundle run -t dev marketplace_speed                                   # availableNow: xử lý những gì đã có rồi dừng
databricks bundle run -t dev marketplace_speed --params mode=--run-minutes=120   # cửa sổ demo chạy liên tục 2 giờ
```

Batch local trên cùng cửa sổ Bronze và cùng `--as-of` là bản đối chứng. So `dataset_counts`, `quality_status`, số dòng và version của các bảng `dw/`. Lưu kết quả vào `docs/evidence/databricks-<ngày>/`.

### Rủi ro chưa kiểm

- Wheel kéo theo dependency của nó (`pyarrow>=24`, `psycopg2-binary`, `elasticsearch`, ...). Nếu runtime có pyarrow cũ hơn, cài đè có thể làm hỏng các API pandas của Spark. Lần chạy đầu cần xem log cài thư viện của cluster.
- `ProcessingTimeTimeout` (offer bị coi là cũ sau `stale_after_seconds`) chỉ chạy khi query sống liên tục. Với `availableNow` các change STALE có thể không sinh ra.
- Delta trên S3A từ Databricks: dùng S3 commit service của nền tảng. Chưa kiểm đường `s3a://` cùng khoá trong `spark_conf` trên DBR 17.3.
