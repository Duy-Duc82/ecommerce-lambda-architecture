\pset pager off

-- Row counts for the BI cache published by the batch pipeline.
-- (The star schema — dims + fact — lives in the MinIO gold zone, not here.)
SELECT 'funnel_daily' AS object_name, COUNT(*) AS row_count FROM cache.funnel_daily
UNION ALL
SELECT 'product_daily', COUNT(*) FROM cache.product_daily
UNION ALL
SELECT 'category_daily', COUNT(*) FROM cache.category_daily
UNION ALL
SELECT 'session_daily', COUNT(*) FROM cache.session_daily
UNION ALL
SELECT 'daily_revenue', COUNT(*) FROM cache.daily_revenue
UNION ALL
SELECT 'predictions', COUNT(*) FROM cache.predictions
UNION ALL
SELECT 'anomalies', COUNT(*) FROM cache.anomalies;

-- Recent pipeline runs.
SELECT run_id, status, source_path, started_at, completed_at,
       bronze_rows, silver_rows, rejected_rows, gold_rows, error_message
FROM audit.pipeline_run
ORDER BY started_at DESC
LIMIT 10;

-- Data-quality gate history.
SELECT run_id, check_name, status, observed_value, expectation, checked_at
FROM audit.data_quality_result
ORDER BY checked_at DESC, check_name
LIMIT 50;

-- Forecast models present (expect: lstm, nbeats).
SELECT model, COUNT(*) AS forecast_points, ROUND(AVG(predicted_revenue)::numeric, 2) AS avg_pred
FROM cache.predictions
GROUP BY model
ORDER BY model;
