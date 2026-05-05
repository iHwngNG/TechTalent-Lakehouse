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
        if isinstance(result, Exception):
            rows.append(
                {
                    "timestamp": timestamp,
                    "stage": stage,
                    "source": "unknown",
                    "object": "system",
                    "metric_name": type(result).__name__,
                    "status": "FAIL",
                    "message": str(result),
                }
            )
        else:
            rows.append(
                {
                    "timestamp": timestamp,
                    "stage": stage,
                    "source": result.get("source", "unknown"),
                    "object": result.get("object", "batch"),
                    "metric_name": result.get("metric_name", metric_name),
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
        all_results: Merged dict from null_check and duplicate_check results, OR an Exception object.
        stage:       Pipeline stage ('bronze', 'silver', 'gold').
        batch_id:    Micro-batch ID for traceability (default 0 for standalone use).
    """
    try:
        if isinstance(all_results, Exception):
            all_results = {
                type(all_results).__name__: all_results
            }
        report_schema = StructType(
            [
                StructField("timestamp", TimestampType(), False),
                StructField("stage", StringType(), False),
                StructField("source", StringType(), False),
                StructField("object", StringType(), True),
                StructField("metric_name", StringType(), False),
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

        # Coalesce to 1 partition to prevent multiple small files per batch write
        df_report = df_report.coalesce(1)

        # Ensure problematic autoOptimize table properties are unset to avoid CONFIG_NOT_AVAILABLE on Databricks Connect
        try:
            spark.sql(f"ALTER TABLE delta.`{DQ_TABLE_PATH}` UNSET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite', 'delta.autoOptimize.autoCompact', 'delta.targetFileSize')")
        except Exception:
            pass

        # Append to the DQ metrics table (never overwrite historical records)
        df_report.write.format("delta").mode("append").option("mergeSchema", "true").save(DQ_TABLE_PATH)

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
