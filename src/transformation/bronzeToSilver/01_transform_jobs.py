import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from delta.tables import DeltaTable
from src.utils.getProjectRoot import getRootPath
from src.validators.silver.null_check import check_nulls
from src.validators.silver.duplicate_check import check_duplicates
from src.validators.common.quality_report import write_report

# Bootstrap sys.path
PROJECT_ROOT = getRootPath()

from src.utils.databricks_catalog import CATALOG, SCHEMA

# Number of output Parquet partitions per micro-batch.
# Keep at 1 for small daily datasets (~thousands of jobs) to avoid Small Files.
TARGET_PARTITIONS = 1


def get_spark_session() -> SparkSession:
    """Get Spark session safely, compatible with Databricks Cluster and Serverless."""
    try:
        return SparkSession.builder.appName("BronzeToSilver_JobsTransformer").getOrCreate()
    except Exception as e:
        print(f"Standard SparkSession init failed: {e}")

    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except Exception as e:
        print(f"DatabricksSession init failed: {e}")

    raise RuntimeError("Cannot initialize Spark Session in this environment!")


def transform_jobs():
    """
    Read raw JSONL from Bronze Volume (ITviec + TopDev), normalize and upsert
    into Silver `fact_jobs` Delta table via Auto Loader + MERGE INTO.

    Small Files mitigation strategy:
    - coalesce(TARGET_PARTITIONS) before MERGE to consolidate tiny files.
    - Call OPTIMIZE after every MERGE for automatic Parquet compaction.
    """
    spark = get_spark_session()

    raws_volume = f"/Volumes/{CATALOG}/{SCHEMA}/raws"
    silver_table_path = f"/Volumes/{CATALOG}/{SCHEMA}/silver/fact_jobs"
    schema_location = f"/Volumes/{CATALOG}/{SCHEMA}/silver/_schemas/fact_jobs_raw"
    checkpoint_path = f"/Volumes/{CATALOG}/{SCHEMA}/silver/_checkpoints/fact_jobs_transformer"

    # 1. Raw schema covering both ITviec and TopDev output fields.
    # working_method only exists in ITviec; TopDev records will get null automatically.
    raw_schema = StructType([
        StructField("job_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("company", StringType(), True),
        StructField("salary", StringType(), True),
        StructField("locations", StringType(), True),
        StructField("working_method", StringType(), True),
        StructField("posted_date", StringType(), True),
        StructField("skills", ArrayType(StringType()), True),
        StructField("description", StringType(), True),
        StructField("source", StringType(), True),
        StructField("url", StringType(), True),
        StructField("crawled_at", StringType(), True),
    ])

    print("Bat dau Transform Bronze -> Silver (fact_jobs)...")

    # 2. Read stream via Auto Loader — tracks processed files automatically via checkpoint
    df_raw_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaLocation", schema_location)
        .schema(raw_schema)
        .load(raws_volume)
    )

    # 3. Transformation: cast types, drop records without job_id
    df_transformed = (
        df_raw_stream
        .filter(col("job_id").isNotNull() & (col("job_id") != ""))
        .select(
            col("job_id"),
            col("title"),
            col("company"),
            col("salary"),
            col("locations"),
            col("working_method"),
            expr("try_cast(posted_date as date)").alias("posted_date"),
            col("skills"),
            col("description"),
            col("source"),
            col("url"),
            expr("try_cast(crawled_at as timestamp)").alias("crawled_at"),
            current_timestamp().alias("ingested_at"),
        )
    )

    # 4. Upsert each micro-batch into the Delta table
    def upsert_to_delta(micro_batch_df, batch_id):
        """
        Consolidate partitions (coalesce), deduplicate, then MERGE INTO Silver.
        Call OPTIMIZE after MERGE to compact Parquet files.
        """
        if not micro_batch_df.head(1):
            return

        print(f"Micro-batch {batch_id}: starting...")

        # Consolidate before writing — prevents many 1-row Parquet files
        df_deduped = (
            micro_batch_df
            .dropDuplicates(["job_id", "source"])
            .coalesce(TARGET_PARTITIONS)
        )

        # --- Data Quality Validation ---
        # Run null and duplicate checks on the deduplicated batch
        dq_results = {}
        dq_results.update(check_nulls(df_deduped, source="silver.fact_jobs"))
        dq_results.update(check_duplicates(df_deduped, source="silver.fact_jobs"))

        # Write DQ report to Delta table (non-blocking — never fails the pipeline)
        write_report(spark, dq_results, stage="silver", batch_id=batch_id)
        # --- End Data Quality Validation ---

        try:
            # Table exists → MERGE (upsert)
            delta_table = DeltaTable.forPath(spark, silver_table_path)

            (
                delta_table.alias("target")
                .merge(
                    df_deduped.alias("source"),
                    "target.job_id = source.job_id AND target.source = source.source",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            print(f"Micro-batch {batch_id}: merge complete")

        except Exception:
            # Table does not exist yet → create it
            print(f"Micro-batch {batch_id}: table not found, creating...")
            (
                df_deduped.write
                .format("delta")
                .mode("overwrite")
                .save(silver_table_path)
            )
            print(f"Micro-batch {batch_id}: table created successfully")

        # Compact small Parquet files after each batch to speed up future queries
        try:
            spark.sql(f"OPTIMIZE delta.`{silver_table_path}`")
            print(f"Micro-batch {batch_id}: OPTIMIZE complete")
        except Exception as opt_err:
            # Non-critical: log and continue
            print(f"Micro-batch {batch_id}: OPTIMIZE skipped — {opt_err}")

    # 5. Trigger AvailableNow: process all new files then stop (behaves like a Batch job)
    query = (
        df_transformed.writeStream
        .foreachBatch(upsert_to_delta)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print("Transform complete!")


if __name__ == "__main__":
    transform_jobs()
