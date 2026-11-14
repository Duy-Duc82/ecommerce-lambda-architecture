#!/bin/sh
# Một lượt kiểm tra stack mp-refactor trong lượt chạy toàn luồng 14/11. Gọi mỗi giờ:
#   sh docs/evidence/fullrun-20261114/theo-doi.sh
# Ghi thêm vào theo-doi.log (cùng thư mục) và in ra màn hình. Dòng bắt đầu bằng PROBLEM là
# việc cần xử lý; KICH-BAN-CHAY.md mục 5 nói xử lý thế nào.
cd "$(dirname "$0")/../../.." || exit 1
# Git Bash rewrites /opt/... in docker exec arguments into a Windows path.
export MSYS_NO_PATHCONV=1
HERE=docs/evidence/fullrun-20261114
LOG=$HERE/theo-doi.log
PY=${PY:-/c/code/data/.venv/Scripts/python}
export PYTHONIOENCODING=utf-8
PG=rf-postgres-dw
q() { docker exec "$PG" sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$1"' sh "$1"; }

{
  # 1. Container, crawl, speed, changes, DLQ (script có sẵn của dự án).
  sh observability/healthcheck.sh env/refactor.env

  # 2. Batch: các run trong 3 giờ gần nhất và luật chất lượng FAIL của run mới nhất.
  echo "-- batch (3h)"
  q "SELECT run_id || ' ' || status FROM audit.marketplace_batch_run
     WHERE started_at >= now() - interval '3 hours' ORDER BY started_at"
  latest=$(q "SELECT run_id FROM audit.marketplace_batch_run ORDER BY started_at DESC LIMIT 1")
  q "SELECT 'quality ' || severity || ' FAIL ' || check_name || ' observed=' || coalesce(observed_value::text, '-')
     FROM audit.marketplace_quality_result WHERE run_id = '$latest' AND status <> 'PASS'"
  q "SELECT 'PROBLEM batch: ' || run_id || ' ' || status FROM audit.marketplace_batch_run
     WHERE started_at >= now() - interval '3 hours' AND status IN ('FAILED','QUALITY_FAILED')"
  q "SELECT 'PROBLEM batch: no run started in the last 90 minutes'
     WHERE NOT EXISTS (SELECT 1 FROM audit.marketplace_batch_run WHERE started_at >= now() - interval '90 minutes')"

  # 3. Speed: micro-batch lỗi và dòng INVALID trong 1 giờ; INVALID phải có bản ghi DLQ (PR #20).
  echo "-- speed (1h)"
  q "SELECT 'batches=' || count(*) || ' failed=' || count(*) FILTER (WHERE status = 'FAILED')
            || ' input=' || coalesce(sum(input_rows), 0) || ' invalid=' || coalesce(sum(invalid_rows), 0)
            || ' changes=' || coalesce(sum(change_rows), 0) || ' p95_ms=' || coalesce(max(latency_p95_ms)::text, '-')
     FROM audit.marketplace_speed_batch WHERE started_at >= now() - interval '1 hour'"
  q "SELECT 'PROBLEM speed: ' || count(*) || ' failed micro-batches in 1h'
     FROM audit.marketplace_speed_batch WHERE started_at >= now() - interval '1 hour' AND status = 'FAILED'
     HAVING count(*) > 0"
  docker exec rf-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
    --topic marketplace.observations.v1.dlq 2>/dev/null | awk -F: '{s+=$3} END {print "observations DLQ messages (total)=" s+0}'

  # 4. Crawl: lỗi theo loại trong 1 giờ (RATE_LIMITED, CLIENT_ERROR tức bị chặn 403, ROBOTS_DENIED: dừng lại xem).
  echo "-- crawl (1h)"
  q "SELECT kind || '=' || n FROM (SELECT coalesce(error_kind, 'OK') AS kind, count(*) AS n
     FROM audit.crawl_request_attempt WHERE started_at >= now() - interval '1 hour' GROUP BY 1) t ORDER BY kind"
  q "SELECT 'PROBLEM crawl: ' || error_kind || ' x' || count(*) FROM audit.crawl_request_attempt
     WHERE started_at >= now() - interval '1 hour' AND error_kind IN ('RATE_LIMITED','CLIENT_ERROR','ROBOTS_DENIED') GROUP BY error_kind"

  # 5. ML: chụp verdict của cache đang phục vụ (để đo độ ổn định), đếm trạng thái ngày mới nhất.
  echo "-- ml"
  q "SELECT 'PROBLEM ml: cache.marketplace_discount_credibility_daily missing, run mp.ps1 migrate'
     WHERE to_regclass('cache.marketplace_discount_credibility_daily') IS NULL"
  "$PY" "$HERE/danh_gia_ml.py" snapshot
  q "SELECT credibility_status || '=' || count(*) FROM cache.marketplace_discount_credibility_daily
     WHERE observed_date = (SELECT max(observed_date) FROM cache.marketplace_discount_credibility_daily)
     GROUP BY credibility_status ORDER BY credibility_status"

  # 6. Tài nguyên host (máy dùng chung; dưới 3 GB trống là nguy cơ, xem 07/11 mục 6).
  echo "-- host"
  powershell.exe -NoProfile -Command \
    "\$o = Get-CimInstance Win32_OperatingSystem; 'free_ram_gb=' + [math]::Round(\$o.FreePhysicalMemory/1MB,1)" 2>/dev/null
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' 2>/dev/null | grep '^rf-' | sort -k2 -h | tail -5
} 2>&1 | tee -a "$LOG"
echo "== end $(date -u +%FT%TZ)" | tee -a "$LOG"
