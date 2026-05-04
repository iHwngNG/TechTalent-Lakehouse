-- Data Quality Summary
-- Counts the total number of errors (FAIL and WARNING) by stage and metric.

SELECT 
    stage,
    metric_name,
    status,
    COUNT(*) AS total_incidents
FROM delta.`/Volumes/workspace/techtalent_lakehouse/healthReport`
WHERE status IN ('FAIL', 'WARNING')
GROUP BY stage, metric_name, status
ORDER BY total_incidents DESC;
