\pset footer off
\echo '### A1 Airflow task durations (successful tries), seconds'
select dag_id, task_id, count(*) n, round(percentile_cont(0.5) within group (order by duration)::numeric,1) p50_s,
  round(max(duration)::numeric,1) max_s, round(avg(extract(epoch from start_date-queued_dttm))::numeric,1) queue_wait_avg_s
from task_instance where state='success' and start_date >= :'t0' and start_date < :'t1' group by 1,2 order by 1, p50_s desc;
\echo '### A2 Airflow task failures and retries'
select dag_id, task_id, state, max(try_number) max_try, count(*) from task_instance
where start_date >= :'t0' and start_date < :'t1' and (state<>'success' or try_number>1) group by 1,2,3 order by 1,2;
\echo '### A3 DAG runs: schedule delay and duration'
select dag_id, state, count(*) n, round(percentile_cont(0.5) within group (order by extract(epoch from end_date-start_date))::numeric/60,2) dur_p50_min,
  round(percentile_cont(0.5) within group (order by extract(epoch from start_date-coalesce(data_interval_end, run_after)))::numeric,1) start_delay_p50_s
from dag_run where start_date >= :'t0' and start_date < :'t1' group by 1,2 order by 1,2;
