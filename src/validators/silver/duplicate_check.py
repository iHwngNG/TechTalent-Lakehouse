from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count


# Composite key used to identify a unique job record
COMPOSITE_KEY = ["job_id", "source"]


def check_duplicates(df: DataFrame, source: str) -> dict:
    """
    Check for duplicate records based on the composite key (job_id, source).

    A duplicate is defined as any row where the composite key (job_id + source)
    appears more than once in the same batch.

    Returns a dict with one metric:
    {
        "duplicate_composite_key": {
            "value": 5,       # number of duplicated rows
            "total": 100,
            "status": "FAIL"  # or "PASS"
        }
    }
    """
    try:
        results = {}
        
        # Count occurrences of each composite key combination
        key_counts = (
            df.groupBy(*COMPOSITE_KEY)
            .agg(count("*").alias("cnt"))
        )
        
        # Join back to original to get duplicate status per row
        # To avoid complex joins, we can just collect key_counts and then collect df
        counts_dict = {
            (r["job_id"], r["source"]): r["cnt"] for r in key_counts.collect()
        }
        
        records = df.collect()
        for row in records:
            jid = row["job_id"] if row["job_id"] else "unknown"
            src = row["source"]
            cnt = counts_dict.get((jid, src), 1)
            
            status = "FAIL" if cnt > 1 else "PASS"
            message = f"Found {cnt} occurrences" if cnt > 1 else "No duplicates"
            
            # Use unique key combining job_id and a random suffix or just job_id
            dict_key = f"dup_check_{jid}_{src}"
            
            results[dict_key] = {
                "metric_name": "check_duplicates",
                "object": jid,
                "status": status,
                "source": source,
                "message": message
            }
            
        return results
        
    except Exception as e:
        return {
            "duplicate_check_error": {
                "metric_name": "check_duplicates",
                "object": "system",
                "status": "ERROR",
                "source": source,
                "error": str(e),
            }
        }
