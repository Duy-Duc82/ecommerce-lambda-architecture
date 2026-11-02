#!/bin/sh
# Mở lại dữ liệu của lượt chạy 31/10/2026 để xem trên BI, không chạy pipeline.
# Chỉ bật các container phục vụ việc xem (cùng volume, dữ liệu đứng yên như lúc chốt):
#   Superset + Postgres (cache BI), Kibana + Elasticsearch (speed), Grafana + Prometheus + Loki (giám sát), MinIO (lake).
# Không bật crawl, speed, Airflow, Alloy: không có dữ liệu mới và máy nhẹ.
#
#   sh docs/evidence/e2e-run-20261031/xem-lai-bi.sh          bật
#   sh docs/evidence/e2e-run-20261031/xem-lai-bi.sh stop     tắt
set -e
VIEW="rf-postgres-dw rf-minio rf-elasticsearch rf-kibana rf-superset rf-prometheus rf-loki rf-grafana"
if [ "$1" = "stop" ]; then
  docker stop $VIEW
  exit 0
fi
docker start $VIEW
echo "Chờ Superset và Kibana sẵn sàng..."
for i in $(seq 1 60); do
  s=$(curl -s -m 5 -o /dev/null -w '%{http_code}' http://127.0.0.1:8086/health || true)
  k=$(curl -s -m 5 -o /dev/null -w '%{http_code}' http://127.0.0.1:5604/api/status || true)
  [ "$s" = 200 ] && [ "$k" = 200 ] && break
  sleep 5
done
cat <<'EOF'
Superset (BI, Gold):  http://localhost:8086/superset/dashboard/marketplace-batch-gold/   admin / admin
Kibana (speed):       http://localhost:5604/app/dashboards   (đặt khung thời gian: 31/10 02:00 -> 17:00 UTC)
Grafana (giám sát):   http://localhost:3004/d/lambda-overview  (khung: 31/10 09:00 -> 1/11 00:00 giờ VN)
Báo cáo chỉ số:       docs/evidence/e2e-run-20261031/BAO-CAO-CHI-SO.md
EOF
