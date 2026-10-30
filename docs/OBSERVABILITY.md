# Giám sát hệ thống (observability)

Thiết kế ngày 30/10/2026. Áp dụng cho mọi stack (`mp-live`, `mp-refactor`, `mp-tikihtml`, `mp-bench`, `mp-demo`) qua `compose/docker-compose.observability.yml`.

## 1. Kiến trúc

```
  container của stack ─ log ─┐
  file log task Airflow ─────┤                 ┌──► Loki (log, 7 ngày) ─────────┐
  cgroup, /proc, /sys ───────┤                 │                                 ├──► Grafana
  Kafka, Postgres, Redis, ES ┼──► Grafana Alloy ┤                                 │   dashboard + cảnh báo
  bảng audit.* (pipeline) ───┤   (1 container)  └──► Prometheus (metrics, 7 ngày) ┘
  StatsD của Airflow ────────┤
  HTTP probe, MinIO /metrics ┘
```

| Thành phần | Vai trò | Image |
|---|---|---|
| Grafana Alloy | Bộ thu thập duy nhất: đọc log container và log task Airflow, chạy các exporter tích hợp, scrape, đẩy log sang Loki và metrics sang Prometheus (remote write) | `grafana/alloy:v1.10.0` |
| Prometheus | Chỉ lưu metrics (nhận remote write) và tính recording rule; giữ 7 ngày, tối đa 2 GB | `prom/prometheus:v3.2.1` |
| Loki | Chỉ lưu log; compactor xoá log quá 7 ngày | `grafana/loki:3.4.2` |
| Grafana | Dashboard và cảnh báo cho cả metrics lẫn log | `grafana/grafana:11.5.2` |

Alloy thay cho Promtail: Promtail đã hết hỗ trợ (đầu năm 2026), còn Alloy vừa gom log vừa thay cho tám exporter vốn phải chạy thành container riêng (node-exporter, cAdvisor, kafka-exporter, postgres-exporter, redis-exporter, elasticsearch-exporter, statsd-exporter, blackbox-exporter).

Grafana chỉ giám sát vận hành (hệ thống có khoẻ không). Câu hỏi nghiệp vụ (dữ liệu nói gì) vẫn thuộc Superset (Gold, star schema) và Kibana (speed layer trên Elasticsearch).

Không dùng: Tempo (pipeline batch/streaming, không có chuỗi request giữa các microservice), Mimir/Thanos (một máy), Alertmanager (Grafana alerting đủ dùng), Pyroscope.

## 2. Quy ước nhãn

Mọi service trong compose có nhãn Docker `ecomdata.layer`. Log và metrics cùng dùng bộ nhãn:

| Nhãn | Ý nghĩa | Nguồn |
|---|---|---|
| `project` | Compose project (`mp-refactor`, ...) | Alloy gắn cho mọi log và metric |
| `layer` | `crawl`, `ingest`, `speed`, `batch`, `orchestration`, `serving`, `storage`, `ops`, `observability` | nhãn `ecomdata.layer` của container; với log task Airflow thì suy từ DAG |
| `service` | Service compose; container task: `batch-<stage>`, `maintenance-<task>`, `discover-<source>` | nhãn compose hoặc `ecomdata.service` |
| `container` | Tên container | Docker |
| `level` (log) | `error`, `warn`, `info`; `expected` cho lỗi Postgres cố ý của batch | regex trên nội dung dòng log |
| `dag_id`, `task_id` (log task) | DAG và task Airflow | đường dẫn file log |
| `run_id`, `attempt` (log) | Lần chạy batch và lần thử | structured metadata của Loki, không phải nhãn (mỗi run một giá trị) |

Lọc log theo một lần chạy batch: `{layer="batch"} | run_id="mp-20261025T1400Z"`.

## 3. Nguồn metrics

| Nhóm | Thành phần trong Alloy | Metric chính |
|---|---|---|
| Máy | `prometheus.exporter.unix` | `node_cpu_*`, `node_memory_*`, `node_filesystem_*`, `node_pressure_*` (PSI) |
| Container | `prometheus.exporter.cadvisor` + recording rule | `container:cpu_usage_cores`, `container:memory_working_set_bytes`, `container:oom_events:increase10m`, ... |
| Kafka | `prometheus.exporter.kafka` | `kafka_topic_partition_current_offset`, `kafka_consumergroup_lag` |
| Postgres, Redis, ES | exporter tương ứng | `pg_*`, `redis_*`, `elasticsearch_*` |
| MinIO | scrape `/minio/v2/metrics/{cluster,bucket}` | `minio_bucket_usage_total_bytes`, ... |
| Endpoint | `prometheus.exporter.blackbox` | `probe_success{instance="kibana"}`, ... |
| Pipeline | `prometheus.exporter.postgres` + `observability/alloy/queries-warehouse.yaml` | `mp_crawl_5m_*`, `mp_crawl_frontier_*`, `mp_batch_*`, `mp_quality_*`, `mp_speed_*`, `mp_stream_5m_*`, `mp_storage_*` |
| Airflow (trạng thái) | `prometheus.exporter.postgres` + `queries-airflow.yaml` (database `airflow`) | `airflow_dag_last_success_timestamp`, `airflow_dag_paused`, `airflow_task_24h_task_instances`, `airflow_job_heartbeat_age_seconds` |
| Airflow (sự kiện) | `prometheus.exporter.statsd` + `statsd-airflow.yaml` | `airflow_ti_finish`, `airflow_task_duration_seconds`, `airflow_dagrun_duration_seconds`, `airflow_scheduler_heartbeat` |

### Metrics của pipeline lấy từ bảng audit

Pipeline vốn đã ghi mọi lần tải trang, micro-batch, batch run và luật quality vào schema `audit`. Exporter Postgres đọc lại các bảng đó mỗi phút, nên không phải sửa code ứng dụng để có metrics. Truy vấn có hậu tố `_5m` đếm những gì kết thúc trong 5 phút gần nhất; các truy vấn còn lại là trạng thái hiện tại. Riêng cột `crawl_request_attempt.completed_at` được thêm index để truy vấn cửa sổ không phải quét cả bảng.

### Lag của speed layer

Spark Structured Streaming giữ offset trong checkpoint, không commit vào consumer group, nên kafka-exporter không thấy lag của speed. Speed service ghi `offsets_behind_latest` (giá trị `maxOffsetsBehindLatest` lớn nhất trong các nguồn Kafka của micro-batch) vào `audit.marketplace_stream_progress`, và dashboard đọc từ đó.

### Container trên Docker Desktop

Docker Desktop dùng containerd image store. cAdvisor (cả bản tích hợp trong Alloy lẫn bản riêng v0.53) không đọc được layer của store này ("failed to identify the read-write layer ID") và bỏ qua mọi container. Vì vậy cAdvisor được trỏ vào một socket không tồn tại và đọc thẳng cgroup `/docker/<id>`. Alloy phát thêm `up{job="container_info"}` cho mỗi container của stack, mang nhãn tên, service và layer, rồi `observability/prometheus/rules.yml` ghép hai nguồn thành các chuỗi `container:*`. Nếu sau này chạy trên Linux với Docker thường, cách này vẫn đúng.

## 4. Log task Airflow

Container của `DockerOperator` không phải service compose, và bị xoá ngay khi chạy xong (`auto_remove=success`). Task ngắn có thể biến mất trước khi Alloy kịp phát hiện, nên Alloy không tail các container này. Thay vào đó:

- Airflow ghi log mỗi lần thử của task vào volume `airflow_logs` (`dag_id=…/run_id=…/task_id=…/attempt=N.log`). `DockerOperator` chép toàn bộ output của container vào file đó.
- Alloy đọc các file (`loki.source.file`), lấy `dag_id`, `task_id`, `run_id`, `attempt` từ đường dẫn và `level` từ JSON của Airflow hoặc từ dấu hiệu lỗi trong dòng log.
- Container task vẫn mang nhãn `ecomdata.project`, `ecomdata.layer`, `ecomdata.service` (`ecomdata.orchestration.tasks.task_labels`), nên CPU và RAM của từng stage batch vẫn hiện trên dashboard tài nguyên.

Volume `airflow_logs` cũng giữ log khi container Airflow được tạo lại; trước đây log nằm trong container và mất theo.

## 5. Dashboard

Sinh bằng `python observability/build_grafana_dashboard.py` (đừng sửa JSON bằng tay). Grafana: `http://localhost:${GRAFANA_HOST_PORT}`, trang chủ là dashboard tổng quan.

| Dashboard | Nội dung |
|---|---|
| `lambda-overview` | Endpoint lên/xuống, cảnh báo đang bật, RAM/CPU/đĩa máy, độ tươi dữ liệu speed/batch/cache, bản ghi crawl, lag speed, CPU/RAM/lỗi theo layer |
| `lambda-pipeline` | Crawl (bản ghi, lỗi theo loại, độ trễ, frontier, circuit breaker), Kafka (message/giây, lag, DLQ), speed (tốc độ vào và xử lý, lag, thời gian micro-batch), batch và quality, khối lượng dữ liệu |
| `lambda-resources` | Máy (CPU, RAM, PSI bộ nhớ, đĩa, I/O, mạng), container (CPU, RAM, RAM so với giới hạn, OOM, I/O), kho lưu trữ |
| `lambda-orchestration` | DAG (lần thành công gần nhất, pause, heartbeat), task 24 giờ, thời gian task và DAG run, độ trễ lập lịch, log task batch và log Airflow |
| `lambda-logs` | Log theo layer, lọc theo layer, service, `run_id` và regex |

Trên Docker Desktop, "máy" là VM Linux của Docker. Panel đĩa theo dõi ổ dữ liệu Docker (`/mnt/docker-desktop-disk`) và ổ C: của Windows (`/mnt/host/c`, nơi file đĩa ảo phình ra).

## 6. Cảnh báo

19 luật trong `observability/grafana/provisioning/alerting/rules.yaml` (sinh cùng script). Không có dữ liệu thì coi là bình thường, nên stack không chạy crawl hay speed sẽ không báo nhầm.

| Nhóm | Luật | Điều kiện |
|---|---|---|
| infrastructure | Endpoint không trả lời | probe thất bại 3 phút, với endpoint đã từng lên trong 6 giờ qua |
| | RAM máy gần đầy | > 90% trong 10 phút |
| | Đĩa gần đầy | > 85% trong 15 phút |
| | Container bị OOM kill | có OOM trong 10 phút |
| | Container khởi động lại liên tục | > 2 lần trong 30 phút |
| | Alloy không gửi metrics | không có metric của Alloy trong 5 phút (mọi cảnh báo khác đang mù) |
| pipeline | Crawl ngừng | không có lần tải nào trong 20 phút, dù crawl đã chạy trong 6 giờ qua |
| | Crawl bị chặn | > 20% lần tải là `RATE_LIMITED`/`BLOCKED` trong 10 phút |
| | Circuit breaker mở | worker tạm ngừng một nguồn |
| | Speed không có micro-batch thành công | > 10 phút (bỏ qua nếu speed đã dừng quá 6 giờ) |
| | Speed đi sau Kafka | > 5.000 offset trong 10 phút |
| | **Batch không chạy đúng khung giờ** | cửa sổ đã promote mới nhất cũ hơn 2,5 giờ. Đây là lỗi WP0 ngày 05/10; luật đọc bảng audit nên đúng cho cả Airflow lẫn scheduler vòng lặp cũ |
| | Batch run thất bại hoặc bị giữ | run mới nhất `FAILED`, `QUALITY_FAILED` hoặc `HELD` |
| | Luật quality bắt buộc FAIL | có luật `MANDATORY` fail |
| | DLQ tăng đột biến | > 100 message trong 30 phút |
| | Consumer group đi sau Kafka | lag > 10.000 trong 15 phút |
| | Nhiều dòng log lỗi | > 20 dòng `level=error` trong 10 phút ở một layer (Loki) |
| orchestration | DAG run gần nhất thất bại | lần thất bại gần nhất mới hơn lần thành công gần nhất |
| | Scheduler không heartbeat | > 120 giây |

Kênh nhận: không cấu hình gì thì cảnh báo chỉ hiện trong Grafana (mục Alerting). Muốn nhận qua Telegram, đặt `TELEGRAM_BOT_TOKEN` và `TELEGRAM_CHAT_ID` (trong shell hoặc file env không commit) rồi tạo lại container `grafana`; `observability/grafana/start.sh` sẽ thêm contact point và notification policy.

## 7. Tài nguyên

| Container | `mem_limit` | Đo trên `mp-refactor` 30/10 (chưa chạy crawl/speed) |
|---|---|---|
| alloy | 512 MB | 172 MB |
| prometheus | 768 MB | 59 MB |
| loki | 512 MB | 82 MB |
| grafana | 384 MB | 87 MB |

Tổng khoảng 400 MB lúc đo; Prometheus và Loki lớn dần theo dữ liệu trong 7 ngày giữ lại.

Cổng chỉ mở trên `127.0.0.1`. Alloy chạy `privileged`, `pid: host`, `cgroup: host` và mount `/` chỉ đọc để đọc cgroup và `/proc` của máy; chấp nhận được cho máy phát triển, không nên dùng nguyên như vậy trên máy dùng chung.

## 8. Vận hành

```sh
# thêm -f compose/docker-compose.observability.yml vào lệnh compose của stack
docker compose -p mp-refactor --env-file env/base.env --env-file env/ports.refactor.env --env-file env/refactor.env \
  -f compose/docker-compose.yml -f compose/docker-compose.observability.yml up -d loki prometheus alloy grafana
```

| Việc | Cách làm |
|---|---|
| Xem luồng dữ liệu trong Alloy | `http://localhost:${ALLOY_HOST_PORT}` (sơ đồ component, trạng thái từng cái) |
| Truy vấn metrics trực tiếp | `http://localhost:${PROMETHEUS_HOST_PORT}` |
| Sửa dashboard hoặc cảnh báo | sửa `observability/build_grafana_dashboard.py`, chạy lại, rồi `docker restart <prefix>grafana` |
| Database đã có từ trước | chạy `ops migrate` một lần để có cột `offsets_behind_latest` và index mới |
| Sau khi sửa `src/ecomdata/orchestration` | build lại image Airflow (tag mới) vì package được cài sẵn trong image |

Cổng theo stack (`env/ports.<stack>.env`):

| Stack | Grafana | Prometheus | Loki | Alloy |
|---|---|---|---|---|
| live (mặc định) | 3001 | 9090 | 3101 | 12345 |
| tiki-html | 3003 | 9093 | 3103 | 12353 |
| refactor | 3004 | 9094 | 3104 | 12354 |
| bench | 3005 | 9095 | 3105 | 12355 |
| demo | 3006 | 9096 | 3106 | 12356 |
