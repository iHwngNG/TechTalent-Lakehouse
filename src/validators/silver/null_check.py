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
        total = df.count()
        results = {}

        for col_name in CRITICAL_COLUMNS:
            metric_key = f"null_{col_name}"

            try:
                null_count = df.filter(
                    col(col_name).isNull() | (col(col_name).cast("string") == "")
                ).count()

                status = "FAIL" if null_count > 0 else "PASS"
                message = f"Found {null_count} nulls out of {total} records" if null_count > 0 else f"No nulls found in {total} records"
                
                results[metric_key] = {
                    "status": status,
                    "source": source,
                    "message": message
                }
            except Exception as col_err:
                results[metric_key] = {
                    "status": "ERROR",
                    "source": source,
                    "error": str(col_err),
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
