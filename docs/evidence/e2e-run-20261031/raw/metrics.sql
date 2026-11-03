-- Window t0..t1 (UTC). Usage: psql -v t0=... -v t1=... -f metrics.sql
\pset footer off
\echo '### C1 crawl per marketplace: totals, rate, latency, size'
with a as (
  select f.marketplace_code m, a.* from audit.crawl_request_attempt a join audit.crawl_frontier f using (task_id)
  where a.completed_at >= :'t0' and a.completed_at < :'t1')
select m, count(*) attempts, count(*) filter (where status='SUCCEEDED') ok, count(*) filter (where status='PARTIAL') partial,
  count(*) filter (where status='FAILED') failed,
  round(100.0*count(*) filter (where status='SUCCEEDED')/count(*),2) ok_pct,
  round(count(*)/extract(epoch from (max(completed_at)-min(started_at)))::numeric,4) rps_span,
  round(percentile_cont(0.5) within group (order by latency_ms)) lat_p50, round(percentile_cont(0.95) within group (order by latency_ms)) lat_p95,
  round(percentile_cont(0.99) within group (order by latency_ms)) lat_p99, max(latency_ms) lat_max,
  round(avg(raw_bytes)/1024.0,1) kb_avg, round(avg(parsed_count),1) records_avg, sum(parsed_count) records, sum(rejected_count) rejected
from a group by m order by m;
\echo '### C2 crawl busy-minute rate per marketplace (minutes with >=1 attempt), requests/second'
with per_min as (
  select f.marketplace_code m, date_trunc('minute', a.started_at) t, count(*) n
  from audit.crawl_request_attempt a join audit.crawl_frontier f using (task_id)
  where a.started_at >= :'t0' and a.started_at < :'t1' group by 1,2)
select m, count(*) busy_minutes, round(avg(n)/60.0,4) rps_avg, round(((percentile_cont(0.5) within group (order by n))/60.0)::numeric,4) rps_p50,
  round(((percentile_cont(0.95) within group (order by n))/60.0)::numeric,4) rps_p95, round(max(n)/60.0,4) rps_max, max(n) max_per_min
from per_min group by m order by m;
\echo '### C3 gap between consecutive requests of one marketplace worker (start to start, seconds, gaps under 120 s)'
with s as (
  select f.marketplace_code m, a.crawl_run_id, a.started_at,
         extract(epoch from a.started_at - lag(a.started_at) over (partition by f.marketplace_code order by a.started_at)) gap
  from audit.crawl_request_attempt a join audit.crawl_frontier f using (task_id)
  where a.started_at >= :'t0' and a.started_at < :'t1')
select m, count(gap) gaps, round(min(gap)::numeric,2) min_s, round((percentile_cont(0.5) within group (order by gap))::numeric,2) p50_s,
  round((percentile_cont(0.95) within group (order by gap))::numeric,2) p95_s
from s where gap is not null and gap < 120 group by m order by m;
\echo '### C4 crawl errors per marketplace'
select f.marketplace_code m, a.status, coalesce(a.error_kind,'-') kind, a.http_status, count(*), left(min(a.error_message),90) sample
from audit.crawl_request_attempt a join audit.crawl_frontier f using (task_id)
where a.completed_at >= :'t0' and a.completed_at < :'t1' and a.status<>'SUCCEEDED' group by 1,2,3,4 order by 1,5 desc;
\echo '### C5 retries: attempts by attempt number'
select f.marketplace_code m, a.attempt_number, count(*) from audit.crawl_request_attempt a join audit.crawl_frontier f using (task_id)
where a.completed_at >= :'t0' and a.completed_at < :'t1' group by 1,2 order by 1,2;
\echo '### C6 empty pages (page-context only) and pages with rows'
select f.marketplace_code m, count(*) filter (where a.parsed_count<=1) empty_pages, count(*) filter (where a.parsed_count>1) pages_with_rows,
  max(a.parsed_count) max_records_page
from audit.crawl_request_attempt a join audit.crawl_frontier f using (task_id)
where a.completed_at >= :'t0' and a.completed_at < :'t1' and a.status='SUCCEEDED' group by 1;
\echo '### C7 frontier now'
select marketplace_code m, tier, status, count(*) from audit.crawl_frontier group by 1,2,3 order by 1,2,3;
\echo '### C8 source state (circuit breaker)'
select * from audit.crawl_source_state;
\echo '### S1 speed micro-batches'
select count(*) batches, count(*) filter (where status='SUCCEEDED') ok, count(*) filter (where status='FAILED') failed,
  sum(input_rows) input_rows, sum(invalid_rows) invalid, sum(duplicate_rows) duplicates, sum(late_rows) late, sum(change_rows) changes,
  sum(es_rows) es_rows, sum(redis_rows) redis_rows,
  round((percentile_cont(0.5) within group (order by extract(epoch from completed_at-started_at)))::numeric,3) dur_p50_s,
  round((percentile_cont(0.95) within group (order by extract(epoch from completed_at-started_at)))::numeric,3) dur_p95_s,
  round(max(extract(epoch from completed_at-started_at))::numeric,3) dur_max_s
from audit.marketplace_speed_batch where started_at >= :'t0' and started_at < :'t1';
\echo '### S2 speed end-to-end latency (crawler publish to all sinks written), batches with rows, ms'
select count(*) batches_with_rows, round(percentile_cont(0.5) within group (order by latency_p50_ms)) p50_of_p50,
  round(percentile_cont(0.5) within group (order by latency_p95_ms)) p50_of_p95, round(percentile_cont(0.95) within group (order by latency_p95_ms)) p95_of_p95,
  max(latency_max_ms) worst
from audit.marketplace_speed_batch where started_at >= :'t0' and started_at < :'t1' and input_rows>0 and latency_p50_ms is not null;
\echo '### S3 speed stages, batches with rows, ms'
select round(avg(stage_clients_ms)) clients_avg, round(avg(stage_collect_ms)) collect_avg, round(avg(stage_kafka_ms)) kafka_avg,
  round(avg(stage_es_ms)) es_avg, round(avg(stage_redis_ms)) redis_avg,
  round(percentile_cont(0.95) within group (order by stage_es_ms)) es_p95
from audit.marketplace_speed_batch where started_at >= :'t0' and started_at < :'t1' and input_rows>0;
\echo '### S4 Spark progress: throughput and lag'
select query_name, count(*) progresses, max(num_input_rows) max_rows_batch, round(max(input_rows_per_second)::numeric,1) max_in_rps,
  round(max(processed_rows_per_second)::numeric,1) max_proc_rps,
  round((percentile_cont(0.5) within group (order by batch_duration_ms))::numeric) dur_p50_ms, max(batch_duration_ms) dur_max_ms,
  max(offsets_behind_latest) max_behind, max(state_memory_bytes) max_state_bytes
from audit.marketplace_stream_progress where progress_at >= :'t0' and progress_at < :'t1' group by 1;
\echo '### S5 speed cost: batch duration against rows (least squares, batches with rows)'
select query_name, count(*) n, round(regr_intercept(batch_duration_ms, num_input_rows)::numeric,1) fixed_ms,
  round(regr_slope(batch_duration_ms, num_input_rows)::numeric,4) per_row_ms, round(regr_r2(batch_duration_ms, num_input_rows)::numeric,3) r2
from audit.marketplace_stream_progress where progress_at >= :'t0' and progress_at < :'t1' and num_input_rows>0 group by 1;
\echo '### B1 batch runs'
select run_id, status, quality_status, silver_rows, gold_rows, mandatory_failure_count,
  round(extract(epoch from completed_at-started_at)/60.0,2) minutes, round(silver_rows/nullif(extract(epoch from completed_at-started_at),0)::numeric,1) silver_rows_per_s
from audit.marketplace_batch_run where started_at >= :'t0' and started_at < :'t1' order by started_at;
\echo '### B2 quality results of the newest promoted run'
select check_name, severity, status, observed_value from audit.marketplace_quality_result
where run_id=(select run_id from audit.marketplace_batch_run where status='PROMOTED' order by started_at desc limit 1) order by severity, status, check_name;
\echo '### B3 BI cache'
select run_id, published_at, dataset_counts from audit.marketplace_cache_version;
