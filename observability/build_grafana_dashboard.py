"""Build the Grafana dashboards and alert rules of the stack (docs/OBSERVABILITY.md).

Writes observability/grafana/dashboards/*.json and
observability/grafana/provisioning/alerting/rules.yaml. Run it after changing a panel or a
rule, and commit the output: Grafana reads the files, not this script.

    python observability/build_grafana_dashboard.py

Dashboards: lambda-overview (home), lambda-pipeline, lambda-resources, lambda-orchestration,
lambda-logs. Each stack has its own Prometheus and Loki, so no query filters on project.
"""
from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).parent
DASHBOARDS = HERE / "grafana" / "dashboards"
ALERTING = HERE / "grafana" / "provisioning" / "alerting"
PROM = {"type": "prometheus", "uid": "prometheus"}
LOKI = {"type": "loki", "uid": "loki"}
LAYERS = [
    ("crawl", "Crawl (crawl-worker, discovery): tải trang, ghi Bronze, publish Kafka"),
    ("ingest", "Ingest (kafka-init): tạo topic Kafka"),
    ("speed", "Speed (Spark Structured Streaming): Kafka → ES / Redis"),
    ("batch", "Batch (task Airflow): Silver → star → Gold → quality → publish → promote"),
    ("orchestration", "Orchestration (Airflow scheduler, api-server, dag-processor)"),
    ("serving", "Serving (es-projector, Kibana, Superset)"),
    ("storage", "Storage (Kafka, MinIO, Postgres, Redis, Elasticsearch)"),
    ("ops", "Ops (backup, prune, snapshot)"),
]
LINKS = [{"title": "Dashboards", "type": "dashboards", "tags": ["lambda"], "asDropdown": False, "includeVars": False}]


class Board:
    """Panels laid out left to right, row by row, on Grafana's 24-column grid."""

    def __init__(self) -> None:
        self.panels: list[dict] = []
        self.x = self.y = self.row_h = 0

    def add(self, panel: dict, w: int, h: int) -> None:
        if self.x + w > 24:
            self.newline()
        panel.update(id=len(self.panels) + 1, gridPos={"h": h, "w": w, "x": self.x, "y": self.y})
        self.panels.append(panel)
        self.x += w
        self.row_h = max(self.row_h, h)

    def newline(self) -> None:
        self.y += self.row_h
        self.x = self.row_h = 0

    def row(self, title: str) -> None:
        self.newline()
        self.panels.append({"type": "row", "title": title, "id": len(self.panels) + 1, "collapsed": False,
                            "gridPos": {"h": 1, "w": 24, "x": 0, "y": self.y}, "panels": []})
        self.y += 1


def prom(expr: str, legend: str = "", instant: bool = False, ref: str = "A") -> dict:
    return {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend, "instant": instant,
            "range": not instant}


def loki(expr: str, legend: str = "", instant: bool = False, ref: str = "A") -> dict:
    target = {"refId": ref, "datasource": LOKI, "expr": expr, "legendFormat": legend}
    if instant:
        target["queryType"] = "instant"
    return target


def thresholds(*steps: tuple[str, float | None]) -> dict:
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for c, v in steps]}


GREEN_RED = thresholds(("green", None), ("red", 1))


def ts(title: str, targets: list[dict], unit: str = "short", stack: bool = False, bars: bool = False,
       desc: str = "") -> dict:
    ds = targets[0]["datasource"]
    return {"type": "timeseries", "title": title, "description": desc, "datasource": ds, "targets": targets,
            "fieldConfig": {"defaults": {"unit": unit, "custom": {
                "drawStyle": "bars" if bars else "line", "fillOpacity": 60 if bars else 10, "lineWidth": 1,
                "showPoints": "never", "stacking": {"mode": "normal" if stack else "none"}}}, "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}


def stat(title: str, targets: list[dict], unit: str = "short", steps: dict | None = None, desc: str = "",
         mappings: list | None = None, text: str = "auto", color: str = "background") -> dict:
    return {"type": "stat", "title": title, "description": desc, "datasource": targets[0]["datasource"],
            "targets": targets,
            "fieldConfig": {"defaults": {"unit": unit, "thresholds": steps or thresholds(("green", None)),
                                         "mappings": mappings or [], "noValue": "–"}, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": color, "textMode": text,
                        "graphMode": "none", "justifyMode": "auto"}}


def table(title: str, targets: list[dict], exclude: list[str] | None = None, desc: str = "") -> dict:
    hidden = {name: True for name in ["Time", "__name__", "job", "instance", "project", *(exclude or [])]}
    return {"type": "table", "title": title, "description": desc, "datasource": targets[0]["datasource"],
            "targets": [dict(t, format="table", instant=True, range=False) for t in targets],
            "transformations": [{"id": "merge"}, {"id": "organize", "options": {"excludeByName": hidden}}],
            "fieldConfig": {"defaults": {}, "overrides": []}, "options": {"showHeader": True}}


def logs(title: str, expr: str) -> dict:
    return {"type": "logs", "title": title, "datasource": LOKI, "targets": [loki(expr)],
            "options": {"showTime": True, "showLabels": True, "wrapLogMessage": True, "sortOrder": "Descending",
                        "enableLogDetails": True}}


UP_DOWN = [{"type": "value", "options": {"0": {"text": "DOWN", "color": "red"}, "1": {"text": "UP", "color": "green"}}}]


def dashboard(uid: str, title: str, board: Board, tags: list[str], variables: list | None = None,
              refresh: str = "30s", time_from: str = "now-3h") -> dict:
    return {"uid": uid, "title": title, "timezone": "Asia/Ho_Chi_Minh", "refresh": refresh,
            "time": {"from": time_from, "to": "now"}, "schemaVersion": 39, "tags": ["lambda", *tags],
            "links": LINKS, "templating": {"list": variables or []}, "panels": board.panels, "editable": True}


# Freshness: how old the newest data of each path is.
SPEED_AGE = "time() - max(mp_speed_last_last_success_timestamp)"
BATCH_AGE = "time() - max(mp_batch_promoted_last_as_of_timestamp > 0)"
CACHE_AGE = "time() - max(mp_serving_cache_published_timestamp)"
# container:* are Prometheus recording rules (observability/prometheus/rules.yml): cAdvisor's
# raw cgroup series joined with the container's name, service and layer.
CPU_BY = "sum by ({by}) (container:cpu_usage_cores)"
# The disks that fill up: "/" on a Linux host; on Docker Desktop, the VM's Docker data disk and
# Windows' C: (where that disk's VHDX file grows), not the VM's own small root.
DISK = 'mountpoint=~"/|/mnt/docker-desktop-disk|/mnt/host/c", fstype!~"tmpfs|rootfs"'
DISK_USED = f"(1 - node_filesystem_avail_bytes{{{DISK}}} / node_filesystem_size_bytes{{{DISK}}}) * 100"
MEM_BY = "sum by ({by}) (container:memory_working_set_bytes)"


def overview() -> dict:
    b = Board()
    b.row("Sức khoẻ")
    b.add(stat("Endpoint", [prom("probe_success", "{{instance}}", instant=True)], mappings=UP_DOWN, text="value_and_name",
               steps=thresholds(("red", None), ("green", 1)),
               desc="HTTP probe của Alloy tới từng web endpoint (blackbox)."), 12, 4)
    b.add({"type": "alertlist", "title": "Cảnh báo đang bật", "options": {
        "viewMode": "list", "groupMode": "default", "maxItems": 20, "sortOrder": 1, "dashboardAlerts": False,
        "alertName": "", "alertInstanceLabelFilter": "", "stateFilter": {"firing": True, "pending": True}}}, 12, 8)
    b.add(stat("RAM máy (VM Docker)", [prom("(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100",
                                            instant=True)], "percent",
               thresholds(("green", None), ("orange", 80), ("red", 90))), 4, 4)
    b.add(stat("CPU máy", [prom('(1 - avg(rate(node_cpu_seconds_total{mode="idle"}[2m]))) * 100', instant=True)],
               "percent", thresholds(("green", None), ("orange", 80), ("red", 95))), 4, 4)
    b.add(stat("Đĩa", [prom(f"max by (mountpoint) ({DISK_USED})", "{{mountpoint}}", instant=True)],
               "percent", thresholds(("green", None), ("orange", 75), ("red", 85)), text="value_and_name"), 4, 4)
    b.row("Độ tươi dữ liệu và luồng")
    b.add(stat("Speed: batch thành công gần nhất", [prom(SPEED_AGE, instant=True)], "s",
               thresholds(("green", None), ("orange", 120), ("red", 600))), 4, 4)
    b.add(stat("Batch: cửa sổ đã promote mới nhất", [prom(BATCH_AGE, instant=True)], "s",
               thresholds(("green", None), ("orange", 5400), ("red", 9000)),
               desc="Tuổi của as_of của run PROMOTED mới nhất. Hourly + 10 phút settle → bình thường < 1,5 giờ."), 4, 4)
    b.add(stat("Cache BI publish", [prom(CACHE_AGE, instant=True)], "s",
               thresholds(("green", None), ("orange", 5400), ("red", 9000))), 4, 4)
    b.add(stat("Crawl: bản ghi / 5 phút", [prom("sum(mp_crawl_5m_parsed_records) or vector(0)", instant=True)],
               steps=thresholds(("red", None), ("green", 1))), 4, 4)
    b.add(stat("Speed lag (offset)", [prom("max(mp_stream_5m_offsets_behind_latest)", instant=True)],
               steps=thresholds(("green", None), ("orange", 1000), ("red", 5000)),
               desc="Số offset Kafka mà query speed còn đi sau topic (Spark maxOffsetsBehindLatest)."), 4, 4)
    b.add(stat("Batch run mới nhất", [prom("max by (run_id, status) (mp_batch_latest_as_of_timestamp)", "{{status}}",
                                           instant=True)], text="name", color="none"), 4, 4)
    b.row("Tài nguyên theo layer")
    b.add(ts("CPU theo layer (core)", [prom(CPU_BY.format(by="layer"), "{{layer}}")], stack=True), 8, 8)
    b.add(ts("RAM theo layer", [prom(MEM_BY.format(by="layer"), "{{layer}}")], "bytes", stack=True), 8, 8)
    b.add(ts("Dòng log lỗi / 5 phút theo layer",
             [loki('sum by (layer) (count_over_time({level="error"}[5m]))', "{{layer}}")], bars=True, stack=True), 8, 8)
    return dashboard("lambda-overview", "Lambda — tổng quan", b, ["overview"])


def pipeline() -> dict:
    b = Board()
    b.row("Crawl")
    b.add(ts("Bản ghi parse được / 5 phút", [prom("sum by (marketplace) (mp_crawl_5m_parsed_records)", "{{marketplace}}")],
             bars=True, stack=True), 8, 8)
    b.add(ts("Lần tải / 5 phút theo kết quả", [prom("sum by (status) (mp_crawl_5m_attempts)", "{{status}}")],
             bars=True, stack=True), 8, 8)
    b.add(ts("Lỗi theo loại / 5 phút", [prom('sum by (marketplace, error_kind) (mp_crawl_5m_attempts{error_kind!="NONE"})',
                                              "{{marketplace}} {{error_kind}}")], bars=True, stack=True,
             desc="RATE_LIMITED và BLOCKED là dấu hiệu bị chặn; PARSE_ERROR gồm cả category rỗng."), 8, 8)
    b.add(ts("Độ trễ tải trang", [prom("max by (marketplace) (mp_crawl_latency_5m_p95_ms)", "p95 {{marketplace}}"),
                                  prom("max by (marketplace) (mp_crawl_latency_5m_avg_ms)", "avg {{marketplace}}", ref="B")],
             "ms"), 8, 8)
    b.add(ts("Byte thô ghi vào Bronze / 5 phút", [prom("sum by (marketplace) (mp_crawl_5m_raw_bytes)", "{{marketplace}}")],
             "bytes", bars=True, stack=True), 8, 8)
    b.add(ts("Frontier: task chờ theo trạng thái", [prom("sum by (marketplace, status) (mp_crawl_frontier_tasks)",
                                                         "{{marketplace}} {{status}}")]), 8, 8)
    b.add(stat("Task đến hạn lâu nhất đã chờ", [prom('max by (marketplace) (mp_crawl_frontier_oldest_due_seconds{status="READY"})',
                                                     "{{marketplace}}", instant=True)], "s",
               thresholds(("green", None), ("orange", 1800), ("red", 3600)), text="value_and_name"), 8, 4)
    b.add(stat("Circuit breaker", [prom("max by (marketplace) (mp_crawl_source_circuit_open)", "{{marketplace}}",
                                        instant=True)], text="value_and_name", steps=GREEN_RED,
               mappings=[{"type": "value", "options": {"0": {"text": "đóng"}, "1": {"text": "MỞ"}}}]), 8, 4)
    b.add(stat("Lần tải thành công gần nhất", [prom("time() - max by (marketplace) (mp_crawl_source_last_success_timestamp > 0)",
                                                    "{{marketplace}}", instant=True)], "s",
               thresholds(("green", None), ("orange", 900), ("red", 3600)), text="value_and_name"), 8, 4)
    b.row("Kafka")
    b.add(ts("Message vào / giây theo topic",
             [prom("sum by (topic) (rate(kafka_topic_partition_current_offset[5m]))", "{{topic}}")]), 8, 8)
    b.add(ts("Lag của consumer group", [prom("sum by (consumergroup, topic) (kafka_consumergroup_lag)",
                                              "{{consumergroup}} ← {{topic}}")],
             desc="Chỉ các consumer commit offset (es-projector). Lag của speed ở hàng Speed."), 8, 8)
    b.add(ts("DLQ: message mới / 30 phút",
             [prom('sum(increase(kafka_topic_partition_current_offset{topic="marketplace.records.v1.dlq"}[30m]))', "dlq")],
             desc="duplicate_listing_on_page là quarantine theo thiết kế; tăng đột biến mới đáng xem."), 8, 8)
    b.row("Speed (Spark Structured Streaming)")
    b.add(ts("Tốc độ vào và xử lý (dòng/giây)", [
        prom("max by (query_name) (mp_stream_5m_input_rows_per_second)", "vào {{query_name}}"),
        prom("max by (query_name) (mp_stream_5m_processed_rows_per_second)", "xử lý {{query_name}}", ref="B")],
        desc="Xử lý thấp hơn vào kéo dài nghĩa là speed không theo kịp."), 8, 8)
    b.add(ts("Lag: offset còn đi sau topic", [prom("max by (query_name) (mp_stream_5m_offsets_behind_latest)",
                                                   "{{query_name}}")]), 8, 8)
    b.add(ts("Thời gian micro-batch", [prom("max by (query_name) (mp_stream_5m_batch_duration_ms_avg)", "avg {{query_name}}"),
                                       prom("max by (query_name) (mp_stream_5m_batch_duration_ms_max)", "max {{query_name}}",
                                            ref="B")], "ms"), 8, 8)
    b.add(ts("Dòng / 5 phút", [prom(f"sum(mp_speed_5m_{c})", c, ref=chr(65 + i)) for i, c in enumerate(
        ["input_rows", "applied_rows", "invalid_rows", "duplicate_rows", "late_rows", "change_rows"])]), 12, 8)
    b.add(ts("Ghi ra serving / 5 phút", [prom("sum(mp_speed_5m_es_rows)", "Elasticsearch"),
                                         prom("sum(mp_speed_5m_redis_rows)", "Redis", ref="B")]), 12, 8)
    b.row("Batch và quality")
    b.add(table("Run mới nhất", [prom("max by (run_id, status) (mp_batch_latest_duration_seconds)"),
                                 prom("max by (run_id, status) (mp_batch_latest_silver_rows)", ref="B"),
                                 prom("max by (run_id, status) (mp_batch_latest_gold_rows)", ref="C"),
                                 prom("max by (run_id, status) (mp_batch_latest_mandatory_failures)", ref="D")]), 12, 5)
    b.add(stat("Run 24 giờ qua theo trạng thái", [prom("sum by (status) (mp_batch_runs_24h_runs)", "{{status}}",
                                                       instant=True)], text="value_and_name", color="value"), 12, 5)
    b.add(ts("Thời gian mỗi run", [prom("max by (run_id) (mp_batch_latest_duration_seconds)", "{{run_id}}")], "s",
             bars=True), 8, 8)
    b.add(ts("Silver và Gold rows của run", [prom("max(mp_batch_latest_silver_rows)", "silver"),
                                             prom("max(mp_batch_latest_gold_rows)", "gold", ref="B")]), 8, 8)
    b.add(ts("Tuổi dữ liệu batch và cache", [prom(BATCH_AGE, "batch (as_of đã promote)"),
                                             prom(CACHE_AGE, "cache BI", ref="B")], "s"), 8, 8)
    b.add(stat("Quality run mới nhất", [prom("sum by (severity, status) (mp_quality_latest_checks)",
                                             "{{severity}} {{status}}", instant=True)], text="value_and_name",
               color="value"), 12, 5)
    b.add(table("Luật quality đang FAIL", [prom("max by (run_id, check_name, dataset, severity) (mp_quality_failing_observed_value)")]),
          12, 5)
    b.row("Khối lượng dữ liệu")
    b.add(ts("Dung lượng theo kho (snapshot hằng ngày)", [prom("sum by (component) (mp_storage_bytes)", "{{component}}")],
             "bytes", stack=True), 8, 8)
    b.add(ts("MinIO theo bucket", [prom("max by (bucket) (minio_bucket_usage_total_bytes)", "{{bucket}}")], "bytes",
             stack=True), 8, 8)
    b.add(ts("Bản ghi vào hệ thống / giờ",
             [prom("sum(sum_over_time(mp_crawl_5m_parsed_records[1h])) / 5", "crawl (ước từ cửa sổ 5 phút)"),
              prom('sum(increase(kafka_topic_partition_current_offset{topic="marketplace.records.v1"}[1h]))',
                   "Kafka marketplace.records.v1", ref="B")],
             desc="Gauge 5 phút được lấy mẫu mỗi phút nên tổng 1 giờ chia 5."), 8, 8)
    return dashboard("lambda-pipeline", "Lambda — pipeline và dữ liệu", b, ["pipeline"])


def resources() -> dict:
    b = Board()
    b.row("Máy (VM Linux của Docker Desktop)")
    b.add(ts("CPU", [prom('(1 - avg(rate(node_cpu_seconds_total{mode="idle"}[2m]))) * 100', "CPU %")], "percent"), 8, 8)
    b.add(ts("RAM", [prom("node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes", "đang dùng"),
                     prom("node_memory_MemTotal_bytes", "tổng", ref="B")], "bytes"), 8, 8)
    b.add(ts("Áp lực bộ nhớ (PSI): giây chờ / giây", [
        prom("rate(node_pressure_memory_waiting_seconds_total[2m])", "some"),
        prom("rate(node_pressure_memory_stalled_seconds_total[2m])", "full", ref="B")], "s",
        desc="> 0 kéo dài nghĩa là tiến trình phải chờ bộ nhớ: dấu hiệu thiếu RAM trước khi OOM."), 8, 8)
    b.add(ts("Đĩa đã dùng", [prom(f"max by (mountpoint) ({DISK_USED})", "{{mountpoint}}")], "percent",
             desc="/mnt/host/c là ổ C: của Windows, nơi file đĩa ảo của Docker Desktop phình ra."), 8, 8)
    b.add(ts("Đĩa I/O", [prom("sum(rate(node_disk_read_bytes_total[2m]))", "đọc"),
                         prom("sum(rate(node_disk_written_bytes_total[2m]))", "ghi", ref="B")], "Bps"), 8, 8)
    b.add(ts("Mạng", [prom('sum(rate(node_network_receive_bytes_total{device!~"lo|veth.*|docker.*|br-.*"}[2m]))', "vào"),
                      prom('sum(rate(node_network_transmit_bytes_total{device!~"lo|veth.*|docker.*|br-.*"}[2m]))', "ra",
                           ref="B")], "Bps"), 8, 8)
    b.row("Container")
    b.add(ts("CPU theo container (core)", [prom(f'topk(10, {CPU_BY.format(by="container")})', "{{container}}")]), 12, 9)
    b.add(ts("RAM theo container", [prom(f'topk(10, {MEM_BY.format(by="container")})', "{{container}}")], "bytes"), 12, 9)
    b.add(ts("RAM so với giới hạn", [prom("container:memory_working_set_bytes / container:memory_limit_bytes * 100",
                                          "{{container}}")], "percent",
             desc="Chỉ các container có mem_limit (stack observability)."), 8, 8)
    b.add(ts("OOM kill / 10 phút", [prom("container:oom_events:increase10m > 0", "{{container}}")], bars=True), 8, 8)
    b.add(ts("Đĩa I/O theo container", [prom("topk(8, container:io_bytes:rate2m)", "{{container}}")], "Bps"), 8, 8)
    b.row("Kho lưu trữ")
    b.add(ts("Postgres: dung lượng database", [prom("max by (datname) (pg_database_size_bytes)", "{{datname}}")],
             "bytes"), 6, 8)
    b.add(ts("Elasticsearch: store", [prom("sum(elasticsearch_indices_store_size_bytes)", "store")], "bytes"), 6, 8)
    b.add(ts("Redis: bộ nhớ", [prom("max(redis_memory_used_bytes)", "used")], "bytes"), 6, 8)
    b.add(ts("Prometheus và Loki", [prom("max(prometheus_tsdb_storage_blocks_bytes)", "Prometheus TSDB"),
                                    prom('sum(rate(loki_distributor_bytes_received_total[5m]))', "Loki nhận (B/s)",
                                         ref="B")], "bytes"), 6, 8)
    return dashboard("lambda-resources", "Lambda — tài nguyên", b, ["resources"], time_from="now-6h")


def orchestration() -> dict:
    b = Board()
    b.row("DAG")
    b.add(stat("Lần thành công gần nhất", [prom("time() - max by (dag_id) (airflow_dag_last_success_timestamp > 0)",
                                                "{{dag_id}}", instant=True)], "s",
               thresholds(("green", None), ("orange", 5400), ("red", 9000)), text="value_and_name",
               desc="Chỉ marketplace_batch chạy hằng giờ; discovery hằng ngày, maintenance hằng tuần."), 12, 5)
    b.add(stat("DAG đang pause", [prom("max by (dag_id) (airflow_dag_paused)", "{{dag_id}}", instant=True)],
               text="value_and_name", steps=thresholds(("green", None), ("orange", 1)),
               mappings=[{"type": "value", "options": {"0": {"text": "chạy"}, "1": {"text": "PAUSE"}}}]), 6, 5)
    b.add(stat("Scheduler heartbeat", [prom("max by (job_type) (airflow_job_heartbeat_age_seconds)", "{{job_type}}",
                                            instant=True)], "s",
               thresholds(("green", None), ("orange", 60), ("red", 120)), text="value_and_name"), 6, 5)
    b.add(table("Task kết thúc trong 24 giờ", [prom("sum by (dag_id, task_id, state) (airflow_task_24h_task_instances)")]),
          12, 9)
    b.add(ts("Task kết thúc / giờ theo trạng thái", [prom('sum by (state) (increase(airflow_ti_finish{state!=""}[1h]))', "{{state}}")],
             bars=True, stack=True), 12, 9)
    b.add(ts("Thời gian trung bình của task (batch)", [prom(
        'sum by (task_id) (rate(airflow_task_duration_seconds_sum{dag_id="marketplace_batch"}[2h])) / '
        'sum by (task_id) (rate(airflow_task_duration_seconds_count{dag_id="marketplace_batch"}[2h]))', "{{task_id}}")],
        "s"), 12, 8)
    b.add(ts("Thời gian DAG run", [prom('max by (dag_id, state) (airflow_dagrun_duration_seconds{quantile="0.5"})',
                                        "{{dag_id}} {{state}}")], "s"), 12, 8)
    b.add(ts("Độ trễ lập lịch", [prom("max by (dag_id) (airflow_dagrun_schedule_delay_seconds{quantile=\"0.5\"})",
                                      "{{dag_id}}")], "s",
             desc="Từ lúc run lẽ ra bắt đầu đến lúc scheduler thực sự tạo nó."), 12, 8)
    b.add(ts("Executor và pool", [prom("max(airflow_executor_running_tasks)", "running"),
                                  prom('max(airflow_pool_slots{state="open_slots"})', "open slots", ref="B")]), 12, 8)
    b.row("Log")
    b.add(logs("Log task batch (container của DockerOperator)", '{layer="batch"} |~ "$search"'), 24, 10)
    b.add(logs("Log Airflow", '{layer="orchestration"} |~ "$search"'), 24, 10)
    search = {"name": "search", "type": "textbox", "label": "Tìm trong log (regex)", "query": "",
              "current": {"text": "", "value": ""}}
    return dashboard("lambda-orchestration", "Lambda — Airflow", b, ["airflow"], [search], time_from="now-24h")


def log_board() -> dict:
    b = Board()
    b.add(ts("Số dòng log / phút theo layer", [loki('sum by (layer) (count_over_time({layer=~"$layer"}[1m]))', "{{layer}}")],
             bars=True, stack=True), 12, 8)
    b.add(ts("Lỗi / 5 phút theo layer (level=error)",
             [loki('sum by (layer) (count_over_time({layer=~"$layer", level="error"}[5m]))', "{{layer}}")],
             bars=True, stack=True), 12, 8)
    b.add(stat("Lỗi trong khoảng thời gian đang chọn",
               [loki('sum by (layer) (count_over_time({layer=~"$layer", level="error"}[$__range]))', "{{layer}}",
                     instant=True)], steps=GREEN_RED, text="value_and_name"), 24, 4)
    b.add(logs("Chỉ dòng lỗi và cảnh báo, mới nhất trước",
               '{layer=~"$layer", service=~"$service", level=~"error|warn"} | run_id=~"$run_id"'), 24, 10)
    for layer, title in LAYERS:
        b.row(title)
        b.add(logs(f"Log {layer}", f'{{layer="{layer}", service=~"$service"}} | run_id=~"$run_id" |~ "$search"'), 24, 9)

    def label_var(name: str, stream: str) -> dict:
        return {"name": name, "type": "query", "datasource": LOKI, "definition": f"label_values({name})",
                "query": {"label": name, "type": 1, "stream": stream, "refId": "LokiVariableQueryEditor-VariableQuery"},
                "multi": True, "includeAll": True, "allValue": ".+", "current": {"text": "All", "value": "$__all"},
                "refresh": 2}

    variables = [
        label_var("layer", ""),
        label_var("service", '{layer=~"$layer"}'),
        {"name": "run_id", "type": "textbox", "label": "run_id (regex, ví dụ mp-20261025T1400Z)", "query": ".*",
         "current": {"text": ".*", "value": ".*"}},
        {"name": "search", "type": "textbox", "label": "Tìm trong log (regex)", "query": "",
         "current": {"text": "", "value": ""}},
    ]
    return dashboard("lambda-logs", "Lambda — log theo layer", b, ["logs"], variables, refresh="10s", time_from="now-1h")


# ---------------------------------------------------------------- alert rules

def rule(uid: str, title: str, expr: str, op: str, value: float, *, wait: str = "5m", severity: str = "warning",
         summary: str, ds: dict = PROM, window: int = 600) -> dict:
    """One Grafana alert rule: query A (instant), condition C = A <op> value. No series means OK."""
    query = {"refId": "A", "expr": expr, "instant": True, "range": False}
    if ds is LOKI:
        query["queryType"] = "instant"
    return {
        "uid": uid, "title": title, "condition": "C", "for": wait, "noDataState": "OK", "execErrState": "Error",
        "labels": {"severity": severity},
        "annotations": {"summary": summary, "value": "{{ $values.A }}"},
        "data": [
            {"refId": "A", "relativeTimeRange": {"from": window, "to": 0}, "datasourceUid": ds["uid"], "model": query},
            {"refId": "C", "datasourceUid": "__expr__", "model": {
                "refId": "C", "type": "threshold", "expression": "A",
                "conditions": [{"evaluator": {"type": op, "params": [value]}}]}},
        ],
    }


def alert_rules() -> dict:
    infra = [
        rule("mp-endpoint-down", "Endpoint không trả lời",
             "probe_success and on (instance) max_over_time(probe_success[6h]) == 1", "lt", 1, wait="3m",
             severity="critical", summary="{{ $labels.instance }} không trả lời HTTP (đã chạy trong 6 giờ qua)."),
        rule("mp-host-memory", "RAM máy gần đầy",
             "(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100", "gt", 90, wait="10m",
             summary="RAM của VM Docker dùng {{ $values.A }}%."),
        rule("mp-disk", "Đĩa gần đầy", f"max by (mountpoint) ({DISK_USED})", "gt", 85, wait="15m",
             summary="{{ $labels.mountpoint }} dùng {{ $values.A }}%."),
        rule("mp-oom", "Container bị OOM kill", "container:oom_events:increase10m",
             "gt", 0, wait="0s", severity="critical", summary="{{ $labels.container }} bị kernel giết vì hết bộ nhớ."),
        rule("mp-restart-loop", "Container khởi động lại liên tục",
             "changes(container:start_time_seconds[30m])", "gt", 2, wait="0s",
             summary="{{ $labels.container }} khởi động lại {{ $values.A }} lần trong 30 phút."),
        rule("mp-no-metrics", "Alloy không gửi metrics", "absent_over_time(alloy_build_info[5m])", "gt", 0, wait="0s",
             severity="critical", summary="Không có metrics từ Alloy trong 5 phút: mọi cảnh báo khác đang mù."),
    ]
    data = [
        rule("mp-crawl-stopped", "Crawl ngừng",
             "(sum(mp_crawl_5m_attempts) or vector(0)) and on () (max_over_time((sum(mp_crawl_5m_attempts))[6h:5m]) > 0)",
             "lt", 1, wait="15m", severity="critical",
             summary="Không có lần tải nào trong 20 phút, dù crawl đã chạy trong 6 giờ qua."),
        rule("mp-crawl-blocked", "Crawl bị chặn hoặc giới hạn tốc độ",
             'sum by (marketplace) (mp_crawl_5m_attempts{error_kind=~"RATE_LIMITED|BLOCKED"}) / '
             "sum by (marketplace) (mp_crawl_5m_attempts)", "gt", 0.2, wait="10m",
             summary="{{ $labels.marketplace }}: {{ $values.A }} phần lần tải bị RATE_LIMITED/BLOCKED."),
        rule("mp-circuit-open", "Circuit breaker của nguồn đang mở", "max by (marketplace) (mp_crawl_source_circuit_open)",
             "gt", 0, wait="0s", summary="{{ $labels.marketplace }}: worker tạm ngừng tải nguồn này."),
        rule("mp-speed-stale", "Speed không có micro-batch thành công",
             "time() - max by (query_name) (mp_speed_last_last_success_timestamp) < 21600", "gt", 600, wait="0s",
             severity="critical", summary="{{ $labels.query_name }}: batch thành công gần nhất cách {{ $values.A }} giây."),
        rule("mp-speed-lag", "Speed đi sau Kafka", "max by (query_name) (mp_stream_5m_offsets_behind_latest)", "gt", 5000,
             wait="10m", summary="{{ $labels.query_name }} còn {{ $values.A }} offset chưa đọc."),
        rule("mp-batch-stale", "Batch không chạy đúng khung giờ",
             "time() - max(mp_batch_promoted_last_as_of_timestamp > 0)", "gt", 9000, wait="0s", severity="critical",
             summary="Cửa sổ dữ liệu promote mới nhất đã cũ {{ $values.A }} giây (> 2,5 giờ): batch bỏ lỡ khung giờ."),
        rule("mp-batch-failed", "Batch run thất bại hoặc bị giữ",
             'count(mp_batch_latest_as_of_timestamp{status=~"FAILED|QUALITY_FAILED|HELD"})', "gt", 0, wait="0s",
             severity="critical", summary="Run batch mới nhất ở trạng thái FAILED, QUALITY_FAILED hoặc HELD."),
        rule("mp-quality-mandatory", "Luật quality bắt buộc FAIL",
             'sum(mp_quality_latest_checks{severity="MANDATORY", status="FAIL"})', "gt", 0, wait="0s",
             severity="critical", summary="{{ $values.A }} luật MANDATORY fail ở run được chấm gần nhất."),
        rule("mp-dlq-burst", "DLQ tăng đột biến",
             'sum(increase(kafka_topic_partition_current_offset{topic="marketplace.records.v1.dlq"}[30m]))', "gt", 100,
             wait="0s", summary="{{ $values.A }} message vào DLQ trong 30 phút."),
        rule("mp-consumer-lag", "Consumer group đi sau Kafka", "sum by (consumergroup) (kafka_consumergroup_lag)", "gt",
             10000, wait="15m", summary="{{ $labels.consumergroup }} lag {{ $values.A }} message."),
        rule("mp-error-burst", "Nhiều dòng log lỗi",
             'sum by (layer) (count_over_time({level="error", layer!="observability"}[10m]))', "gt", 20, wait="0s",
             ds=LOKI, summary="Layer {{ $labels.layer }}: {{ $values.A }} dòng lỗi trong 10 phút."),
    ]
    airflow = [
        rule("mp-dag-failed", "DAG run gần nhất thất bại",
             "max by (dag_id) (airflow_dag_last_failure_timestamp) > max by (dag_id) (airflow_dag_last_success_timestamp)",
             "gt", 0, wait="0s", severity="critical", summary="{{ $labels.dag_id }}: run kết thúc gần nhất là failed."),
        rule("mp-scheduler-heartbeat", "Airflow scheduler không heartbeat",
             'max(airflow_job_heartbeat_age_seconds{job_type="SchedulerJob"})', "gt", 120, wait="2m",
             severity="critical", summary="Scheduler im lặng {{ $values.A }} giây: không DAG nào được lập lịch."),
    ]

    def group(name: str, rules: list[dict]) -> dict:
        return {"orgId": 1, "name": name, "folder": "Lambda pipeline", "interval": "1m", "rules": rules}

    return {"apiVersion": 1, "groups": [group("infrastructure", infra), group("pipeline", data),
                                        group("orchestration", airflow)]}


def main() -> None:
    DASHBOARDS.mkdir(parents=True, exist_ok=True)
    ALERTING.mkdir(parents=True, exist_ok=True)
    for board in (overview(), pipeline(), resources(), orchestration(), log_board()):
        path = DASHBOARDS / f"{board['uid']}.json"
        path.write_text(json.dumps(board, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        print(path, len(board["panels"]))
    # JSON is YAML; Grafana's provisioning reads the file either way.
    rules = ALERTING / "rules.yaml"
    rules.write_text("# Generated by observability/build_grafana_dashboard.py; edit the script, not this file.\n"
                     + json.dumps(alert_rules(), ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(rules, sum(len(g["rules"]) for g in alert_rules()["groups"]))


if __name__ == "__main__":
    main()
