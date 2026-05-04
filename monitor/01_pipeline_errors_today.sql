-- Pipeline Errors Today
-- Retrieves all failed records and system errors for the current day from the healthReport.

SELECT 
    timestamp,
    stage,
    source,
    metric_name,
    message
FROM delta.`/Volumes/workspace/techtalent_lakehouse/healthReport`
WHERE status = 'FAIL' 
  AND DATE(timestamp) = CURRENT_DATE()
ORDER BY timestamp DESC;
