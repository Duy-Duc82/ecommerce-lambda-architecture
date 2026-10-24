#!/bin/sh
# One health snapshot of the full stack; prints PROBLEM lines for anything that needs a fix.
ES=localhost:9201; LOKI=localhost:3101
echo "== $(date -u +%FT%TZ) (VN $(date -u -d "+7 hours" +%H:%M))"
docker ps -a --filter "label=com.docker.compose.project=mp-tikihtml" --format '{{.Names}} {{.Status}}' \
  | grep -vE "init|setup|ops-1|batch-once" | grep -vE 'Up .*\(healthy\)|Up [0-9]+ (seconds|minutes|hours)$|Up About' | sed 's/^/PROBLEM container: /'
cnt() { curl -s "$ES/$1/_count" -H 'Content-Type: application/json' -d "{\"query\":{\"range\":{\"$2\":{\"gte\":\"now-$3\"}}}}" | python -c 'import json,sys; print(json.load(sys.stdin).get("count",-1))'; }
c=$(cnt 'marketplace-crawl-attempts-*' completed_at 10m); s=$(cnt 'marketplace-speed-batches-*' started_at 3m); e=$(cnt 'marketplace-changes-*' detected_at 10m); d=$(cnt 'marketplace-dlq-*' failed_at 30m)
echo "crawl/10m=$c speedbatches/3m=$s changes/10m=$e dlq/30m=$d"
[ "$c" -lt 1 ] && echo "PROBLEM crawl: no crawl attempt in 10 min"
[ "$s" -lt 1 ] && echo "PROBLEM speed: no micro-batch in 3 min"
[ "$d" -gt 0 ] && echo "PROBLEM ingest: $d observations in the DLQ in 30 min"
docker exec th-postgres-dw psql -U admin -d data_warehouse -tAc "select run_id||' '||status||' q='||coalesce(quality_status,'-')||' gold='||coalesce(gold_rows::text,'-') from audit.marketplace_batch_run order by started_at desc limit 3" | sed 's/^/batch: /'
docker exec th-postgres-dw psql -U admin -d data_warehouse -tAc "select 'PROBLEM batch: '||run_id||' '||status||' '||coalesce(error_message,'') from audit.marketplace_batch_run where started_at > now()-interval '2 hours' and status not in ('SUCCEEDED','RUNNING')"
docker exec th-postgres-dw psql -U admin -d data_warehouse -tAc "select status, count(*) from audit.crawl_frontier group by 1" | tr '\n' ' '; echo
curl -s -G $LOKI/loki/api/v1/query --data-urlencode 'query=sum by (layer, service) (count_over_time({project="mp-tikihtml", level="error", layer!="observability"}[30m]))' \
  | python -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print("errors/30m", {x["metric"]["service"]: x["value"][1] for x in r})'
powershell -NoProfile -Command "\$o=Get-CimInstance Win32_OperatingSystem; 'host RAM free {0:N1} GB' -f (\$o.FreePhysicalMemory/1MB)"
docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' | grep -E 'speed|batch|elastic|kafka|superset' | tr '\n' ';'; echo
