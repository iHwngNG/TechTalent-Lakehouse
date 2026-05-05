# TechTalent Lakehouse

TechTalent Lakehouse is a Databricks-based data engineering project that turns messy public IT job postings into a structured, monitorable talent dataset. The idea is simple: scrape hiring demand from platforms like ITviec and TopDev, move the raw data into a lakehouse, validate it, and publish a cleaner analytical layer that can support market research, workforce planning, and salary benchmarking.

This project was designed as a portfolio-grade example of how a modern data pipeline should behave in the real world. It is not just a scraper. It demonstrates incremental ingestion, Bronze-to-Silver transformation, data quality checks, error isolation, observability, and Databricks-native storage patterns.

## Why this project exists

Recruiters, talent teams, and workforce planners often need a live picture of the hiring market, but most public job data is fragmented, inconsistent, and hard to trust at scale. TechTalent Lakehouse addresses that problem by building a repeatable pipeline around public recruitment data.

At a business level, the project is meant to answer questions such as:

- Which skills are showing up most often in current IT hiring demand?
- Which companies are hiring most aggressively?
- What does the salary language in job posts look like across sources?
- How healthy is the data pipeline behind those insights?

## Tech stack

- `Python` for scraping, orchestration scripts, and validation logic
- `Playwright` for browser automation against dynamic job sites
- `crawl4ai` for listing-page crawling and HTML capture
- `BeautifulSoup` for parsing job listing and detail pages
- `Databricks` as the execution and storage platform
- `Delta Lake` for curated storage and upsertable warehouse tables
- `PySpark Structured Streaming` with Auto Loader patterns for incremental Bronze-to-Silver processing
- `Databricks Volumes` for raw landing data, monitoring output, and Silver storage
- `Databricks SQL` for operational monitoring queries

## What the project does today

The current implementation focuses on the core of the platform:

- Scraping IT job postings from `ITviec` and `TopDev`
- Saving raw job data into Databricks Volumes as JSONL micro-batches
- Running Bronze-level validation before persisting each batch
- Streaming raw files into a Silver Delta table with Auto Loader semantics
- Deduplicating and upserting jobs into a curated `fact_jobs` table
- Logging quality metrics and runtime failures into a shared monitoring table
- Supplying SQL queries for pipeline health and data-quality monitoring

## Architecture at a glance

The project follows a Medallion-style pattern:

1. `Source websites`
   ITviec and TopDev are scraped with Playwright-based browser automation plus anti-bot handling.
2. `Bronze`
   Raw records are written to Databricks Volumes in append-only JSONL files.
3. `Silver`
   Databricks Auto Loader reads new files incrementally, normalizes fields, deduplicates records, and upserts them into Delta.
4. `Monitoring`
   Validation results and runtime errors are written into a health-report Delta table that can be queried from Databricks SQL.

In this repo, the most important code paths are:

- [scrapers/base_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/base_scraper.py)
- [scrapers/itviec_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/itviec_scraper.py)
- [scrapers/topdev_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/topdev_scraper.py)
- [src/transformation/bronzeToSilver/01_transform_jobs.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/src/transformation/bronzeToSilver/01_transform_jobs.py)
- [src/validators/common/quality_report.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/src/validators/common/quality_report.py)

## What the output looks like

The main output of this project is not a single CSV or report. It is a working pipeline plus a small data warehouse layer inside Databricks.

The pipeline output has three parts.

First, there is the raw landing layer in Databricks Volumes:

- `/Volumes/workspace/techtalent_lakehouse/raws/itviec`
- `/Volumes/workspace/techtalent_lakehouse/raws/topdev`

Each scraper writes append-only JSONL micro-batches there. Those files preserve the original structured scrape output before Silver curation.

Second, there is the curated warehouse table in the Silver layer:

- `workspace.techtalent_lakehouse.fact_jobs`

This is the main warehouse table produced by the current codebase. It is stored as Delta and built through incremental Bronze-to-Silver processing with deduplication and merge logic. Each row represents one normalized job posting and includes fields such as:

- `job_id`
- `title`
- `company`
- `salary`
- `locations`
- `working_method`
- `posted_date`
- `skills`
- `description`
- `source`
- `url`
- `crawled_at`
- `ingested_at`

This table is the usable analytical output for downstream reporting. It gives a cleaner view of hiring demand than the raw scraped files because it casts dates, standardizes columns, removes duplicate jobs inside micro-batches, and upserts records by `job_id` and `source`.

Third, there is an operational monitoring store:

- Bronze validation results
- Silver validation results
- Scraper runtime failures
- URL validation failures
- Recent crash traces

Those records are written into the Delta-backed monitoring path:

- `/Volumes/workspace/techtalent_lakehouse/healthReport`

Together, the Silver warehouse table and the health-report table form the real project output: one table for business use, one table for operational trust.

## How the pipeline handles errors

This codebase handles errors at several layers: scraper retries, anti-bot detection, URL screening, Bronze quality gates, Silver quality checks, and non-blocking monitoring writes. Below is the practical list of error cases visible in the current implementation and how each one is handled.

### 1. Temporary function-level failures during scraping

- Where it appears:
  `retry()` and `async_retry()` in [scrapers/base_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/base_scraper.py)
- Typical causes:
  transient network instability, flaky browser automation, temporary remote failures
- How it is handled:
  the function is retried automatically with exponential backoff. If the last retry still fails, the exception is raised normally.

### 2. Databricks credential or connectivity failures when loading existing records

- Where it appears:
  `load_existing_records()` in [scrapers/base_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/base_scraper.py)
- Typical causes:
  missing `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, or `DATABRICKS_HTTP_PATH`; SQL connection failures; table not found on first run
- How it is handled:
  if credentials are missing, the scraper logs a warning and starts with an empty known-record set. If the Silver table does not exist yet, it logs that the run is starting fresh. If a different SQL error occurs, it still falls back to an empty set rather than blocking scraping.

### 3. Unreachable job detail URLs

- Where it appears:
  `validate_urls()` and `check_url()` in [src/validators/bronze/url_validator.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/src/validators/bronze/url_validator.py)
- Typical causes:
  `404`, `410`, `451`, `429`, `500`, `502`, `503`, `504`, DNS failure, timeout, connection refusal
- How it is handled:
  the pipeline sends a `HEAD` request before scraping detail pages. Failed URLs are excluded from the batch, converted into structured error records, and written to monitoring. The rest of the batch continues.

### 4. Browser disconnection during detail-page scraping

- Where it appears:
  `BrowserDisconnectedError` handling in [scrapers/base_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/base_scraper.py), [scrapers/itviec_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/itviec_scraper.py), and [scrapers/topdev_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/topdev_scraper.py)
- Typical causes:
  CDP session drop, page closed unexpectedly, browser/context shutdown, websocket disconnect
- How it is handled:
  the error is treated as a batch-level recoverable failure. The scraper logs the error, saves it into the monitoring store, reconnects the browser, waits briefly, and retries the same batch once. If the batch fails twice, it is logged as failed and the scraper moves on.

### 5. Anti-bot or challenge pages

- Where it appears:
  `AntiBotDetectedError` checks in [scrapers/itviec_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/itviec_scraper.py) and [scrapers/topdev_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/topdev_scraper.py)
- Typical causes:
  Cloudflare challenge pages, captcha pages, "checking your browser" pages, access-denied responses
- How it is handled:
  the scraper inspects the returned HTML for anti-bot markers. If found, the page is skipped, the event is logged as a structured failure, and the rest of the batch continues.

### 6. Unexpected per-page scraping exceptions

- Where it appears:
  `_process_batch()` in [scrapers/base_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/base_scraper.py)
- Typical causes:
  selector mismatch, parsing failure, timeout, malformed page content, site HTML changes
- How it is handled:
  the failed page is logged with context and traceback, then skipped. Other pages in the batch continue processing. This prevents a single broken posting from crashing the full scrape.

### 7. Empty or low-quality Bronze batches

- Where it appears:
  `validate_bronze_data()` in [src/validators/bronze/data_quality_validator.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/src/validators/bronze/data_quality_validator.py) and `save_batch()` in [scrapers/base_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/base_scraper.py)
- Typical causes:
  missing `title` or `company` in too many scraped records, parser drift, partial anti-bot rendering
- How it is handled:
  the scraper checks critical fields before saving a batch. If too many records are incomplete, the batch fails the quality gate and is not written to the raw volume. If only some records are incomplete but the ratio stays under threshold, the batch is still written and the quality status is reported to monitoring.

### 8. Failures while writing Bronze quality reports

- Where it appears:
  `validate_quality()` in [scrapers/base_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/base_scraper.py)
- Typical causes:
  Spark session initialization issues, Databricks connectivity issues, report table write problems
- How it is handled:
  the pipeline logs the reporting failure but does not let that reporting error hide the underlying batch validation result. The actual batch quality decision still stands.

### 9. Errors while persisting structured scraper failures

- Where it appears:
  `save_error_record()` in [scrapers/base_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/base_scraper.py)
- Typical causes:
  inability to initialize Spark, write failure to the health-report Delta path
- How it is handled:
  the code tries multiple ways to obtain a Spark session. If the monitoring write still fails, it logs both the write failure and the original error record to the application logs so the incident is not silently lost.

### 10. Empty Silver micro-batches

- Where it appears:
  `upsert_to_delta()` in [src/transformation/bronzeToSilver/01_transform_jobs.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/src/transformation/bronzeToSilver/01_transform_jobs.py)
- Typical causes:
  no new files, or all records filtered out because `job_id` is missing
- How it is handled:
  the function exits early and does not attempt a merge.

### 11. Silver data-quality issues: nulls in critical columns

- Where it appears:
  `check_nulls()` in [src/validators/silver/null_check.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/src/validators/silver/null_check.py)
- Typical causes:
  broken parsing logic, missing fields from source sites, invalid date casting
- How it is handled:
  the validator records pass/fail status for each row based on `job_id`, `title`, and `posted_date`. These results are written to monitoring for investigation.

### 12. Silver data-quality issues: duplicate records

- Where it appears:
  `check_duplicates()` in [src/validators/silver/duplicate_check.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/src/validators/silver/duplicate_check.py)
- Typical causes:
  repeated records in the same micro-batch, duplicated source input, merge logic anomalies
- How it is handled:
  duplicates are flagged in the monitoring output on a per-record basis. In parallel, the transformation logic also performs `dropDuplicates(["job_id", "source"])` before merge, which reduces duplicate propagation into the Silver table.

### 13. Missing Silver table on first write

- Where it appears:
  `upsert_to_delta()` in [src/transformation/bronzeToSilver/01_transform_jobs.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/src/transformation/bronzeToSilver/01_transform_jobs.py)
- Typical causes:
  first pipeline run, or table path has not been initialized yet
- How it is handled:
  if the target Delta table does not exist, the code creates it by writing the deduplicated batch instead of attempting a merge.

### 14. Merge or write failures in the Silver transformation

- Where it appears:
  `upsert_to_delta()` in [src/transformation/bronzeToSilver/01_transform_jobs.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/src/transformation/bronzeToSilver/01_transform_jobs.py)
- Typical causes:
  schema mismatch, Delta write error, storage issue, unexpected Spark failure
- How it is handled:
  the code prints the full exception and traceback, then re-raises the error. This is a deliberate hard failure because the curated warehouse layer should not silently continue after a broken upsert.

### 15. OPTIMIZE failures after Silver writes

- Where it appears:
  `upsert_to_delta()` in [src/transformation/bronzeToSilver/01_transform_jobs.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/src/transformation/bronzeToSilver/01_transform_jobs.py)
- Typical causes:
  unsupported environment, command failure, permissions issue
- How it is handled:
  the error is logged and the pipeline continues. Optimization is treated as useful but non-critical.

### 16. Failures writing monitoring data

- Where it appears:
  `write_report()` in [src/validators/common/quality_report.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/src/validators/common/quality_report.py)
- Typical causes:
  Delta append failure, transient object-store or network issue, Spark table-property issue
- How it is handled:
  the code retries report writes with exponential backoff. If all retries fail, it logs the failure and keeps the main pipeline moving. Monitoring is important, but the monitoring layer is intentionally not allowed to become a single point of pipeline failure.

### 17. Top-level scraper run failure

- Where it appears:
  `main()` functions in [scrapers/itviec_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/itviec_scraper.py) and [scrapers/topdev_scraper.py](/d:/Experimental%20Ground/TechTalent%20Lakehouse/scrapers/topdev_scraper.py)
- Typical causes:
  any unrecovered exception bubbling out of the scrape flow
- How it is handled:
  the error is first written as a structured monitoring event, then re-raised so Databricks can mark the job as failed. This preserves observability while still signaling orchestration-level failure correctly.

In short, recoverable errors are usually skipped, retried, or logged without stopping the whole scrape, while critical warehouse-write failures are allowed to fail loudly.

## Observability and monitoring

The repository includes SQL queries under [monitor](/d:/Experimental%20Ground/TechTalent%20Lakehouse/monitor) that turn the health-report Delta table and the Silver warehouse table into a simple operations dashboard. These queries are useful because they separate business output from pipeline reliability.

The included monitoring queries cover these metrics:

### 1. Pipeline errors today

File:
[01_pipeline_errors_today.sql](/d:/Experimental%20Ground/TechTalent%20Lakehouse/monitor/01_pipeline_errors_today.sql)

Metrics included:

- `timestamp`
- `stage`
- `source`
- `metric_name`
- `message`

What it shows:
all failed events written today, across Bronze and Silver monitoring output. This is the fastest query for checking whether the pipeline is currently healthy or failing.

### 2. Data quality summary

File:
[02_data_quality_summary.sql](/d:/Experimental%20Ground/TechTalent%20Lakehouse/monitor/02_data_quality_summary.sql)

Metrics included:

- `stage`
- `metric_name`
- `status`
- `total_incidents`

What it shows:
how often each validator is producing `FAIL` or `WARNING` outcomes. This helps identify repeating weak spots such as null-heavy batches or recurring duplicate patterns.

### 3. Total scraped jobs

File:
[03_total_scraped_jobs.sql](/d:/Experimental%20Ground/TechTalent%20Lakehouse/monitor/03_total_scraped_jobs.sql)

Metrics included:

- `ingestion_date`
- `source`
- `total_jobs_scraped`

What it shows:
daily warehouse growth by source, based on the Silver `fact_jobs` table. This is the most direct operational metric for pipeline throughput.

### 4. Daily health status trend

File:
[04_daily_health_status.sql](/d:/Experimental%20Ground/TechTalent%20Lakehouse/monitor/04_daily_health_status.sql)

Metrics included:

- `run_date`
- `stage`
- `error_count`

What it shows:
the number of failures per stage over the last 7 days. This is useful for spotting whether the system is getting more stable or less stable over time.

### 5. Recent system crashes

File:
[05_recent_system_crashes.sql](/d:/Experimental%20Ground/TechTalent%20Lakehouse/monitor/05_recent_system_crashes.sql)

Metrics included:

- `timestamp`
- `stage`
- `source`
- `error_type`
- `traceback`

What it shows:
the most recent high-signal failures, especially scraper system errors and non-quality-related exceptions. This is the debugging-oriented view.

Taken together, the monitoring layer gives the project two kinds of observability:

- throughput metrics, such as how many jobs reached the warehouse
- reliability metrics, such as failures, warnings, duplicate detections, null detections, and crash traces
