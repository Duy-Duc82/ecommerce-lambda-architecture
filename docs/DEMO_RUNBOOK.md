# Runbook: stack demo `mp-tikihtml` (full stack + giám sát log)

Stack dùng cho buổi demo với giáo viên: crawl HTML Tiki, Kafka, Silver, speed (Spark Structured Streaming), batch (Spark, mỗi giờ), Elasticsearch/Kibana, Postgres/Superset, Alloy/Prometheus/Loki/Grafana (thiết kế: `docs/OBSERVABILITY.md`). Số đo của lượt chạy 8 giờ ngày 24/10: `docs/evidence/fullstack-run-20261024/bao-cao-chay-thu.html` (nằm ở worktree `C:\code\data`, chưa commit).

## Địa chỉ

| Dịch vụ | URL | Đăng nhập |
|---|---|---|
| Kibana, speed layer | http://localhost:5602, gồm các dashboard "Demo — dữ liệu chảy liên tục qua pipeline", "Marketplace — realtime changes", "Marketplace — source health and freshness" | — |
| Superset, batch layer (Gold) | http://localhost:8089/superset/dashboard/marketplace-batch-gold/ | admin / admin |
| Grafana: tổng quan, pipeline, tài nguyên, Airflow, log | http://localhost:3003 (trang chủ là `lambda-overview`) | admin / `GRAFANA_ADMIN_PASSWORD` trong `env/base.env` (cũng xem được ở chế độ khách) |
| Prometheus, Alloy | http://localhost:9093, http://localhost:12353 | — |
| MinIO Console, dữ liệu gốc | http://localhost:9011 | minioadmin / minioadmin |

## Bật, dừng, kiểm tra

Chạy từ thư mục gốc của repo (Git Bash). Lệnh dùng `env/base.env` + `env/ports.tiki-html.env` + `env/tiki-html.env` + `compose/docker-compose.observability.yml` (qua `run-fullstack.sh`):

```sh
sh run-fullstack.sh up -d loki prometheus alloy grafana   # giám sát trước
sh run-fullstack.sh up -d                                  # toàn bộ stack
sh run-fullstack.sh ps
sh observability/healthcheck.sh                             # in dòng PROBLEM nếu có vấn đề
docker compose -p mp-tikihtml --profile "*" stop           # dừng, dữ liệu trong volume vẫn giữ
docker compose -p mp-tikihtml --profile "*" start          # bật lại đúng các container cũ
```

Chỉ lần đầu tạo stack mới cần migrate và seed danh mục:

```sh
set -a; . env/tiki-html.env; set +a
docker compose --profile ops run --rm ops python -m ops migrate
docker compose --profile ops run --rm ops python -m ops seed --marketplace tiki --tier ACTIVE \
  $(python -c "import json; print(' '.join('--category '+c for c in json.load(open('env/tiki_html_categories.json'))))")
```

## Cấu hình riêng của demo

`run-fullstack.sh` chỉ dùng cho demo, không thay đổi mặc định của repo:

- `CRAWL_REQUEST_DELAY_SECONDS=3.5`: crawl khoảng 10 trang/phút, đều theo thời gian.
- `MARKETPLACE_STREAM_TRIGGER=5 seconds`: dữ liệu vào Elasticsearch khoảng 3 giây sau khi crawl xong (trung vị).
- Batch mỗi giờ (`MARKETPLACE_BATCH_INTERVAL_SECONDS=3600`, lag 1200 s).
- `SPARK_DRIVER_MEMORY=3g` (trong `env/base.env`, đọc bởi `infra/spark.py`): với heap mặc định 1 GB, batch bị OOM ngày 24/10. Bản vá `stream.override.yml` đã gộp vào cấu hình chính.
- 300 danh mục: 60 danh mục trong `env/tiki_html_categories.json` và 240 danh mục chọn ngẫu nhiên từ sitemap (đã seed sẵn trong frontier của volume).

## Dựng lại dashboard

Cần Python có `requests` và `sqlalchemy`, ví dụ `pip install requests sqlalchemy psycopg2-binary`:

```sh
python observability/build_grafana_dashboard.py          # sinh lại 5 dashboard Grafana và luật cảnh báo
python observability/build_kibana_demo_dashboard.py http://localhost:5602
python observability/build_superset_batch_dashboard.py http://localhost:8089
python observability/run_report_metrics.py out.json      # đo số liệu cho báo cáo (sửa cửa sổ T0/T1 trong file)
```

## Lưu ý

- Stack này là bản demo. Phase 10 (`docs/PLAN_PHASE10_DIMENSIONAL_MODEL.md`) phải dùng project và port riêng, không đụng `mp-tikihtml`.
- Các container đang chạy hiện mount file từ một worktree tạm (scratchpad của session ngày 24/10). Lần tới chạy `sh run-fullstack.sh up -d` từ thư mục này, compose sẽ tạo lại container với file mount từ đây; dữ liệu trong volume vẫn giữ nguyên.
- Docker Desktop dùng `C:\Users\Admin\.wslconfig` (`memory=14GB`, `swap=8GB`, `autoMemoryReclaim=gradual`). Đóng các ứng dụng nặng (Epic Games launcher) trước khi demo.
