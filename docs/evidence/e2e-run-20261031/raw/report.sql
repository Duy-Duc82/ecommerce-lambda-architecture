\pset footer off
\echo '## crawl by hour (UTC)'
select date_trunc('hour', completed_at) h, count(*) attempts, count(*) filter (where status='SUCCEEDED') ok,
       count(*) filter (where status='FAILED') failed, sum(parsed_count) parsed, round(sum(raw_bytes)/1e6,1) raw_mb,
       round(percentile_cont(0.95) within group (order by latency_ms)) p95_ms
from audit.crawl_request_attempt where completed_at between '2026-10-31 02:30Z' and '2026-10-31 08:30Z' group by 1 order by 1;
\echo '## crawl errors'
select error_kind, count(*) from audit.crawl_request_attempt where completed_at between '2026-10-31 02:30Z' and '2026-10-31 08:30Z' and status<>'SUCCEEDED' group by 1;
\echo '## speed micro-batches'
select count(*) batches, count(*) filter (where status='SUCCEEDED') ok, sum(input_rows) input_rows,
       round(avg(extract(epoch from completed_at-started_at))::numeric,2) avg_s, round(max(extract(epoch from completed_at-started_at))::numeric,2) max_s
from audit.marketplace_speed_batch where started_at between '2026-10-31 02:30Z' and '2026-10-31 08:30Z';
\echo '## batch runs'
select run_id, status, quality_status, silver_rows, gold_rows, round(extract(epoch from completed_at-started_at)/60.0,1) minutes
from audit.marketplace_batch_run where started_at between '2026-10-31 02:30Z' and '2026-10-31 08:40Z' order by started_at;
\echo '## quality (latest run)'
select status, severity, count(*) from audit.marketplace_quality_result where run_id=(select run_id from audit.marketplace_batch_run where status='PROMOTED' order by started_at desc limit 1) group by 1,2;
\echo '## BI cache'
select run_id, published_at, dataset_counts from audit.marketplace_cache_version;
