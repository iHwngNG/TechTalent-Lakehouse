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
        total = df.count()

        # Count occurrences of each composite key combination
        key_counts = (
            df.groupBy(*COMPOSITE_KEY)
            .agg(count("*").alias("cnt"))
            .filter(col("cnt") > 1)
        )

        # Sum up all duplicate rows (rows beyond the first occurrence)
        dup_rows = key_counts.agg({"cnt": "sum"}).collect()[0][0]
        duplicate_count = int(dup_rows) if dup_rows is not None else 0

        status = "FAIL" if duplicate_count > 0 else "PASS"

        return {
            "duplicate_composite_key": {
                "value": duplicate_count,
                "total": total,
                "status": status,
                "source": source,
            }
        }

    except Exception as e:
        return {
            "duplicate_composite_key": {
                "value": -1,
                "total": -1,
                "status": "ERROR",
                "source": source,
                "error": str(e),
            }
        }
