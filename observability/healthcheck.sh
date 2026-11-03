#!/bin/sh
# One health snapshot of a full stack; prints PROBLEM lines for anything that needs a fix.
# A container that exited 0 was stopped on purpose (one-shot jobs, services left out of this run).
#   sh observability/healthcheck.sh env/refactor.env      (default: env/tiki-html.env)
# Loads env/base.env, env/ports.<stack>.env and the named file, like the compose commands.
cd "$(dirname "$0")/.." || exit 1
ENV_FILE=${1:-env/tiki-html.env}
STACK=$(basename "$ENV_FILE" .env)
set -a; . env/base.env; . "env/ports.$STACK.env"; . "$ENV_FILE"; set +a
PROJECT=$COMPOSE_PROJECT_NAME; PG=${MP_CONTAINER_PREFIX}postgres-dw
# 127.0.0.1, not localhost: Docker Desktop publishes on IPv4 only, and ::1 fails now and then.
ES=127.0.0.1:$ES_HOST_PORT; LOKI=127.0.0.1:$LOKI_HOST_PORT
psql() { docker exec "$PG" sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$1"' sh "$1"; }

echo "== $PROJECT $(date -u +%FT%TZ) (VN $(date -u -d "+7 hours" +%H:%M))"
docker ps -a --filter "label=com.docker.compose.project=$PROJECT" --format '{{.Names}} {{.Status}}' \
  | grep -vE 'Up .*\(healthy\)|Up [0-9]+ (seconds|minutes|hours)$|Up About|Up Less than|Exited \(0\)' | sed 's/^/PROBLEM container: /'
# -1 when Elasticsearch does not answer.
cnt() { curl -s -m 15 "$ES/$1/_count" -H 'Content-Type: application/json' -d "{\"query\":{\"range\":{\"$2\":{\"gte\":\"now-$3\"}}}}" \
  | python -c 'import json,sys
try: print(json.load(sys.stdin).get("count", -1))
except ValueError: print(-1)'; }
c=$(cnt 'marketplace-crawl-attempts-*' completed_at 10m); s=$(cnt 'marketplace-speed-batches-*' started_at 3m); e=$(cnt 'marketplace-changes-*' detected_at 10m); d=$(cnt 'marketplace-dlq-*' failed_at 30m)
echo "crawl/10m=$c speedbatches/3m=$s changes/10m=$e dlq/30m=$d"
[ "$c" -lt 0 ] && echo "PROBLEM serving: Elasticsearch at $ES does not answer"
# Idle between cadences is the design; only due work left waiting is a problem.
due=$(psql "select count(*) from audit.crawl_frontier where status='READY' and scheduled_for < now() - interval '10 minutes' and marketplace_code in (select distinct f.marketplace_code from audit.crawl_request_attempt a join audit.crawl_frontier f using (task_id) where a.completed_at > now() - interval '6 hours')")
[ "$c" -eq 0 ] && [ "${due:-0}" -gt 0 ] && echo "PROBLEM crawl: $due tasks overdue by 10+ min and no crawl attempt in 10 min"
# Spark runs no batch without new input; a stall is pages crawled with no batch after them.
c3=$(cnt 'marketplace-crawl-attempts-*' completed_at 3m)
[ "$s" -eq 0 ] && [ "$c3" -gt 0 ] && echo "PROBLEM speed: $c3 pages crawled in 3 min but no micro-batch"
[ "$d" -gt 0 ] && echo "PROBLEM ingest: $d observations in the DLQ in 30 min"
psql "select run_id||' '||status||' q='||coalesce(quality_status,'-')||' gold='||coalesce(gold_rows::text,'-') from audit.marketplace_batch_run order by started_at desc limit 3" | sed 's/^/batch: /'
psql "select 'PROBLEM batch: '||run_id||' '||status||' '||left(coalesce(error_message,''),200) from audit.marketplace_batch_run where started_at > now()-interval '2 hours' and status in ('FAILED','QUALITY_FAILED','HELD') and started_at > (select coalesce(max(started_at),'-infinity') from audit.marketplace_batch_run where status='PROMOTED')"
psql "select 'PROBLEM batch: newest promoted window is '||round(extract(epoch from now()-max(as_of))/3600,1)||' h old' from audit.marketplace_batch_run where status='PROMOTED' having now()-max(as_of) > interval '150 minutes'"
psql "select status, count(*) from audit.crawl_frontier group by 1" | tr '\n' ' '; echo
curl -s -m 15 -G "$LOKI/loki/api/v1/query" --data-urlencode "query=sum by (layer, service) (count_over_time({project=\"$PROJECT\", level=\"error\", layer!=\"observability\"}[30m]))" \
  | python -c 'import json,sys
try: r=json.load(sys.stdin)["data"]["result"]; print("errors/30m", {x["metric"].get("service"): x["value"][1] for x in r})
except (ValueError, KeyError): print("PROBLEM observability: Loki does not answer")'
powershell -NoProfile -Command "\$o=Get-CimInstance Win32_OperatingSystem; 'host RAM free {0:N1} GB' -f (\$o.FreePhysicalMemory/1MB)" 2>/dev/null
docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' | grep "^$MP_CONTAINER_PREFIX" | grep -E 'speed|batch|elastic|kafka|superset' | tr '\n' ';'; echo
