-- Total Scraped Jobs
-- Calculates the total number of jobs ingested into the Silver layer (fact_jobs) by source and date.

SELECT 
    DATE(ingested_at) AS ingestion_date,
    source,
    COUNT(job_id) AS total_jobs_scraped
FROM workspace.techtalent_lakehouse.fact_jobs
GROUP BY ingestion_date, source
ORDER BY ingestion_date DESC;
