import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType

logger = logging.getLogger(__name__)


def ensure_schema_exists(
    schema_name: str = "talentech_lakehouse", catalog_name: str = "main"
):
    """
    Check if the specified schema exists on Databricks. If not, create it.
    """
    w = WorkspaceClient()
    full_schema_name = f"{catalog_name}.{schema_name}"

    try:
        w.schemas.get(full_name=full_schema_name)
        logger.info(f"Schema '{full_schema_name}' already exists.")
    except Exception as e:
        if "NOT_FOUND" in str(e):
            logger.info(f"Schema '{full_schema_name}' not found. Creating...")
            w.schemas.create(name=schema_name, catalog_name=catalog_name)
            logger.info(f"Schema '{full_schema_name}' created successfully.")
        else:
            logger.error(f"Error checking schema '{full_schema_name}': {e}")
            raise


def ensure_volume_exists(
    volume_name: str = "raws",
    schema_name: str = "talentech_lakehouse",
    catalog_name: str = "main",
):
    """
    Check if the specified volume exists in the given schema on Databricks. If not, create it.
    """
    w = WorkspaceClient()
    full_volume_name = f"{catalog_name}.{schema_name}.{volume_name}"

    try:
        w.volumes.read(full_name=full_volume_name)
        logger.info(f"Volume '{full_volume_name}' already exists.")
    except Exception as e:
        if "NOT_FOUND" in str(e):
            logger.info(f"Volume '{full_volume_name}' not found. Creating...")
            w.volumes.create(
                catalog_name=catalog_name,
                schema_name=schema_name,
                name=volume_name,
                volume_type=VolumeType.MANAGED,
            )
            logger.info(f"Volume '{full_volume_name}' created successfully.")
        else:
            logger.error(f"Error checking volume '{full_volume_name}': {e}")
            raise
