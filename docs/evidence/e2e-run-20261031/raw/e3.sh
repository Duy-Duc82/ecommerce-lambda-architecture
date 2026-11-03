#!/bin/sh
S=/c/Users/Admin/AppData/Local/Temp/claude/C--code-data/91437fd0-5e51-4853-9b2f-9c6aa732ce4e/scratchpad
Q(){ docker exec -i rf-postgres-dw sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA' ; }
off(){ MSYS_NO_PATHCONV=1 docker exec rf-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic marketplace.records.v1 2>/dev/null | awk -F: '{s+=$3} END {print s}'; }
until [ "$(date -u +%H%M)" -ge 1058 ]; do sleep 20; done
until [ "$(echo "select count(*) from audit.crawl_frontier where status='LEASED' and marketplace_code='tiki'" | Q)" -gt 0 ]; do sleep 2; done
O0=$(off); T0=$(date -u +%FT%T.%3NZ); docker stop rf-kafka >/dev/null; T1=$(date -u +%FT%T.%3NZ)
echo "E3 kafka stop requested $T0, stopped $T1, records offset before $O0" | tee -a $S/experiments.log
sleep 90
T2=$(date -u +%FT%T.%3NZ); docker start rf-kafka >/dev/null
until docker inspect rf-kafka --format '{{.State.Health.Status}}' | grep -q healthy; do sleep 2; done
T3=$(date -u +%FT%T.%3NZ); echo "E3 kafka start $T2, healthy $T3" | tee -a $S/experiments.log
sleep 600
echo "select a.status, coalesce(a.error_kind,'-'), count(*), min(a.started_at), max(a.completed_at), left(min(a.error_message),120) from audit.crawl_request_attempt a join audit.crawl_frontier f using (task_id) where a.started_at between '$T0'::timestamptz - interval '3 minutes' and '$T3'::timestamptz + interval '10 minutes' group by 1,2;" | Q | sed 's/^/E3 attempts: /' | tee -a $S/experiments.log
echo "select 'E3 tasks retried', count(distinct task_id) from audit.crawl_request_attempt where started_at > '$T0' and attempt_number>1;" | Q | tee -a $S/experiments.log
echo "select 'E3 parsed records published since T0-3m', coalesce(sum(parsed_count),0) from audit.crawl_request_attempt where started_at > '$T0'::timestamptz - interval '3 minutes' and status='SUCCEEDED';" | Q | tee -a $S/experiments.log
echo "E3 records offset after: $(off) (before $O0)" | tee -a $S/experiments.log
echo "select 'E3 speed', status, count(*), sum(input_rows), min(started_at), max(completed_at) from audit.marketplace_speed_batch where started_at between '$T0'::timestamptz - interval '1 minute' and '$T3'::timestamptz + interval '10 minutes' group by 2;" | Q | tee -a $S/experiments.log
docker inspect rf-speed rf-crawl-worker rf-es-projector rf-crawl-worker-lazada --format '{{.Name}} {{.State.Status}} restarts={{.RestartCount}}' | sed 's/^/E3 container: /' | tee -a $S/experiments.log
