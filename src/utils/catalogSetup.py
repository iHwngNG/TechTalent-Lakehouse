import sys
import os
import logging
from pyspark.sql import SparkSession

# Đảm bảo Python có thể tìm thấy các module từ thư mục gốc của project
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.databricks_catalog import (
    bootstrap_volumes,
    ensure_fact_jobs_table_exists,
)

# Thiết lập logging để theo dõi tiến trình trên Databricks
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("CatalogSetup")


def main():
    logger.info("🚀 Khởi chạy Job thiết lập Databricks Catalog...")

    # Khởi tạo SparkSession. Nếu chạy trên Databricks, nó sẽ tự động dùng Spark của Cluster hiện tại.
    spark = SparkSession.builder.appName("CatalogSetupJob").getOrCreate()

    try:
        # Bước 1: Tạo Schema và các Volumes cần thiết (raws, error, silver)
        logger.info("--- Bước 1: Thiết lập Schema và Volumes ---")
        bootstrap_volumes()

        # Bước 2: Tạo sẵn cấu trúc bảng Delta cho fact_jobs trong Volume silver
        logger.info("--- Bước 2: Khởi tạo bảng fact_jobs (Delta) ---")
        ensure_fact_jobs_table_exists(spark)

        logger.info("✅ Catalog sẵn sàng!")

    except Exception as e:
        logger.error(f"❌ Quá trình thiết lập thất bại: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
