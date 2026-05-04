-- Recent System Crashes
-- Specifically looks for scraper_error or Exception metrics indicating a pipeline crash.

SELECT 
    timestamp,
    stage,
    source,
    metric_name AS error_type,
    message AS traceback
FROM delta.`/Volumes/workspace/techtalent_lakehouse/healthReport`
WHERE status = 'FAIL' 
  AND (metric_name LIKE 'scraper_error_%' OR metric_name NOT LIKE 'null_%' AND metric_name NOT LIKE 'duplicate_%')
ORDER BY timestamp DESC
LIMIT 50;
