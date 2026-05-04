-- Daily Health Status Trend
-- Shows the trend of pipeline health over the last 7 days based on error logs.

SELECT 
    DATE(timestamp) AS run_date,
    stage,
    COUNT(*) AS error_count
FROM delta.`/Volumes/workspace/techtalent_lakehouse/healthReport`
WHERE status = 'FAIL'
  AND timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY run_date, stage
ORDER BY run_date ASC;
