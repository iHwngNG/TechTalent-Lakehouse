from pyspark.sql import DataFrame
from pyspark.sql.functions import col


# Critical columns that must not be null in the Silver layer
CRITICAL_COLUMNS = ["job_id", "title", "posted_date"]


def check_nulls(df: DataFrame, source: str) -> dict:
    """
    Check null values on critical columns: job_id, title, posted_date.

    Returns a dict of metric results per column, e.g.:
    {
        "null_job_id":   {"value": 3,  "total": 100, "status": "FAIL"},
        "null_title":    {"value": 0,  "total": 100, "status": "PASS"},
        "null_posted_date": {"value": 5, "total": 100, "status": "FAIL"},
    }
    """
    try:
        results = {}
        # Collect to process per object (fine for micro-batches)
        records = df.collect()
        
        for row in records:
            job_id = row["job_id"] if row["job_id"] else "unknown"
            dict_key = f"null_check_{job_id}"
            
            missing = []
            for col_name in CRITICAL_COLUMNS:
                val = row[col_name]
                if val is None or str(val).strip() == "":
                    missing.append(col_name)
            
            status = "FAIL" if missing else "PASS"
            message = f"Missing: {', '.join(missing)}" if missing else "Valid"
            
            results[dict_key] = {
                "metric_name": "check_nulls",
                "object": job_id,
                "status": status,
                "source": source,
                "message": message
            }
            
        return results

    except Exception as e:
        return {
            "null_check_error": {
                "status": "ERROR",
                "source": source,
                "error": str(e),
            }
        }
