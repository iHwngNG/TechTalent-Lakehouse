import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.errors import NotFound

logger = logging.getLogger(__name__)

CATALOG = "workspace"
SCHEMA = "techtalent_lakehouse"


def ensure_schema_exists(
    schema_name: str = SCHEMA, catalog_name: str = CATALOG
) -> None:
    """Check if the schema exists on Databricks. Create it if not."""
    w = WorkspaceClient()
    full_name = f"{catalog_name}.{schema_name}"
    try:
        w.schemas.get(full_name=full_name)
        logger.info(f"Schema '{full_name}' already exists.")
    except NotFound:
        logger.info(f"Schema '{full_name}' not found. Creating...")
        w.schemas.create(name=schema_name, catalog_name=catalog_name)
        logger.info(f"Schema '{full_name}' created successfully.")
    except Exception as e:
        logger.error(f"Error checking schema '{full_name}': {e}")
        raise


def ensure_volume_exists(
    volume_name: str,
    schema_name: str = SCHEMA,
    catalog_name: str = CATALOG,
) -> None:
    """Check if the volume exists in the given schema. Create it if not."""
    w = WorkspaceClient()
    full_name = f"{catalog_name}.{schema_name}.{volume_name}"
    try:
        w.volumes.read(name=full_name)
        logger.info(f"Volume '{full_name}' already exists.")
    except NotFound:
        logger.info(f"Volume '{full_name}' not found. Creating...")
        w.volumes.create(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=volume_name,
            volume_type=VolumeType.MANAGED,
        )
        logger.info(f"Volume '{full_name}' created successfully.")
    except Exception as e:
        logger.error(f"Error checking volume '{full_name}': {e}")
        raise


def bootstrap_volumes() -> None:
    for vol in ("raws", "error", "silver"):
        ensure_volume_exists(volume_name=vol)
    logger.info("All required volumes are ready.")


def ensure_fact_jobs_table_exists(spark) -> None:
    """
    Kiểm tra và tạo table fact_jobs (định dạng Delta) trong Volume silver nếu chưa có.
    Yêu cầu truyền vào đối tượng SparkSession (spark).
    """
    from pyspark.sql.types import StructType, StructField, StringType, DateType, ArrayType, TimestampType

    table_path = f"/Volumes/{CATALOG}/{SCHEMA}/silver/fact_jobs"
    
    try:
        # Thử đọc để xem Delta table đã tồn tại ở đường dẫn này chưa
        spark.read.format("delta").load(table_path)
        logger.info(f"Table fact_jobs đã tồn tại tại {table_path}")
    except Exception as e:
        if "Path does not exist" in str(e) or "is not a Delta table" in str(e):
            logger.info(f"Table fact_jobs chưa có tại {table_path}. Bắt đầu tạo mới...")
            
            # Định nghĩa Schema (cấu trúc) cho bảng fact_jobs
            schema = StructType([
                StructField("job_id", StringType(), True),
                StructField("title", StringType(), True),
                StructField("company", StringType(), True),
                StructField("salary", StringType(), True),
                StructField("locations", StringType(), True),
                StructField("working_method", StringType(), True),
                StructField("posted_date", DateType(), True),
                StructField("skills", ArrayType(StringType()), True),
                StructField("description", StringType(), True),
                StructField("source", StringType(), True),
                StructField("url", StringType(), True),
                StructField("crawled_at", TimestampType(), True),
                StructField("ingested_at", TimestampType(), True)
            ])
            
            # Tạo DataFrame rỗng với schema trên
            empty_df = spark.createDataFrame([], schema)
            
            # Ghi xuống Volume dưới định dạng Delta
            empty_df.write.format("delta").mode("ignore").save(table_path)
            logger.info(f"Đã tạo thành công Delta table rỗng tại {table_path}")
        else:
            logger.error(f"Lỗi khi kiểm tra table fact_jobs: {e}")
            raise
