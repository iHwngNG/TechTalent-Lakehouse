from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.databricks_catalog import CATALOG, SCHEMA

# Path to the DQ metrics Delta table inside the healthReport volume
DQ_TABLE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/healthReport"


def build_report(all_results: dict, stage: str) -> list[dict]:
    """
    Flatten all validator result dicts into a single list of report rows.

    Each row has: timestamp, stage, source, metric_name, value, total, status, message.
    """
    timestamp = datetime.utcnow()
    rows = []

    for metric_name, result in all_results.items():
        rows.append(
            {
                "timestamp": timestamp,
                "stage": stage,
                "source": result.get("source", "unknown"),
                "metric_name": metric_name,
                "value": float(result.get("value", -1)),
                "total": float(result.get("total", -1)),
                "status": result.get("status", "UNKNOWN"),
                "message": result.get("error", result.get("message", "")),
            }
        )

    return rows


def write_report(
    spark: SparkSession, all_results: dict, stage: str, batch_id: int = 0
) -> None:
    """
    Summarize all validator results and append them as a Delta table
    at DQ_TABLE_PATH for audit and monitoring purposes.

    Args:
        spark:       Active SparkSession.
        all_results: Merged dict from null_check and duplicate_check results or error details.
        stage:       Pipeline stage ('bronze', 'silver', 'gold').
        batch_id:    Micro-batch ID for traceability (default 0 for standalone use).
    """
    try:
        report_schema = StructType(
            [
                StructField("timestamp", TimestampType(), False),
                StructField("stage", StringType(), False),
                StructField("source", StringType(), False),
                StructField("metric_name", StringType(), False),
                StructField("value", DoubleType(), True),
                StructField("total", DoubleType(), True),
                StructField("status", StringType(), False),
                StructField("message", StringType(), True),
            ]
        )

        rows = build_report(all_results, stage)
        if not rows:
            print(
                f"[DQ Report] Stage {stage} - Batch {batch_id}: no metrics to write, skipping."
            )
            return

        df_report = spark.createDataFrame(rows, schema=report_schema)

        # Append to the DQ metrics table (never overwrite historical records)
        df_report.write.format("delta").mode("append").save(DQ_TABLE_PATH)

        # Print summary to Databricks Job logs
        fail_count = sum(1 for r in rows if r["status"] == "FAIL")
        pass_count = sum(1 for r in rows if r["status"] == "PASS")
        print(
            f"[DQ Report] Stage {stage} - Batch {batch_id}: "
            f"{pass_count} PASS, {fail_count} FAIL — "
            f"written {len(rows)} metrics to {DQ_TABLE_PATH}"
        )

    except Exception as e:
        # Non-critical: DQ report failure must never block the pipeline
        print(
            f"[DQ Report] Stage {stage} - Batch {batch_id}: failed to write report — {e}"
        )
