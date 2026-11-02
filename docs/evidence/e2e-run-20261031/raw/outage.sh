#!/bin/sh
# outage.sh NAME CONTAINER HHMM SECONDS : once Tiki crawling starts after HHMM, stop CONTAINER for SECONDS, then measure.
NAME=$1; C=$2; AT=$3; DUR=$4
S=/c/Users/Admin/AppData/Local/Temp/claude/C--code-data/91437fd0-5e51-4853-9b2f-9c6aa732ce4e/scratchpad
Q(){ docker exec -i rf-postgres-dw sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA' ; }
until [ "$(date -u +%H%M)" -ge "$AT" ]; do sleep 20; done
until [ "$(echo "select count(*) from audit.crawl_frontier where status='LEASED' and marketplace_code='tiki'" | Q)" -gt 0 ]; do sleep 2; done
R0=$(docker inspect rf-speed rf-crawl-worker rf-es-projector rf-crawl-worker-lazada --format '{{.Name}}={{.RestartCount}}' | tr '\n' ' ')
T0=$(date -u +%FT%T.%3NZ); docker stop $C >/dev/null; echo "$NAME stop $C at $T0 (restarts before: $R0)" | tee -a $S/experiments.log
sleep $DUR
T2=$(date -u +%FT%T.%3NZ); docker start $C >/dev/null
until [ "$(docker inspect $C --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}')" = healthy ] || [ "$(docker inspect $C --format '{{if .State.Health}}x{{else}}{{.State.Status}}{{end}}')" = running ]; do sleep 2; done
T3=$(date -u +%FT%T.%3NZ); echo "$NAME start $T2, healthy $T3" | tee -a $S/experiments.log
sleep 600
echo "select a.status, coalesce(a.error_kind,'-'), count(*), min(a.started_at), max(a.completed_at), left(min(a.error_message),140) from audit.crawl_request_attempt a where a.started_at between '$T0'::timestamptz - interval '1 minute' and '$T3'::timestamptz + interval '10 minutes' group by 1,2;" | Q | sed "s/^/$NAME attempts: /" | tee -a $S/experiments.log
echo "select '$NAME failed then ok', count(distinct a.task_id), max(b.completed_at - a.completed_at) from audit.crawl_request_attempt a join audit.crawl_request_attempt b on b.task_id=a.task_id and b.status='SUCCEEDED' and b.started_at>a.started_at where a.status='FAILED' and a.started_at > '$T0'::timestamptz - interval '1 minute';" | Q | tee -a $S/experiments.log
echo "select '$NAME speed', status, count(*), sum(input_rows), sum(es_rows), max(extract(epoch from completed_at-started_at)), min(started_at), max(completed_at), left(min(error_message),120) from audit.marketplace_speed_batch where started_at between '$T0'::timestamptz - interval '1 minute' and '$T3'::timestamptz + interval '10 minutes' group by 2;" | Q | tee -a $S/experiments.log
echo "select '$NAME speed latency after', max(latency_max_ms), round(percentile_cont(0.5) within group (order by latency_p50_ms)) from audit.marketplace_speed_batch where started_at between '$T0' and '$T3'::timestamptz + interval '10 minutes' and input_rows>0;" | Q | tee -a $S/experiments.log
echo "$NAME restarts after: $(docker inspect rf-speed rf-crawl-worker rf-es-projector rf-crawl-worker-lazada --format '{{.Name}}={{.RestartCount}}' | tr '\n' ' ')" | tee -a $S/experiments.log
