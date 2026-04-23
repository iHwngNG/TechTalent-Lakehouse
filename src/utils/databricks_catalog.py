import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.errors import NotFound

logger = logging.getLogger(__name__)


def ensure_schema_exists(
    schema_name: str = "techtalent_lakehouse", catalog_name: str = "workspace"
):
    """
    Check if the specified schema exists on Databricks. If not, create it.
    """
    w = WorkspaceClient()
    full_schema_name = f"{catalog_name}.{schema_name}"
    print(1)
    try:
        w.schemas.get(full_name=full_schema_name)
        logger.info(f"Schema '{full_schema_name}' already exists.")
    except NotFound:
        logger.info(f"Schema '{full_schema_name}' not found. Creating...")
        w.schemas.create(name=schema_name, catalog_name=catalog_name)
        logger.info(f"Schema '{full_schema_name}' created successfully.")
    except Exception as e:
        logger.error(f"Error checking schema '{full_schema_name}': {e}")
        raise


def ensure_volume_exists(
    volume_name: str = "raws",
    schema_name: str = "techtalent_lakehouse",
    catalog_name: str = "workspace",
):
    """
    Check if the specified volume exists in the given schema on Databricks. If not, create it.
    """
    w = WorkspaceClient()
    full_volume_name = f"{catalog_name}.{schema_name}.{volume_name}"

    try:
        w.volumes.read(name=full_volume_name)
        logger.info(f"Volume '{full_volume_name}' already exists.")
    except NotFound:
        logger.info(f"Volume '{full_volume_name}' not found. Creating...")
        w.volumes.create(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=volume_name,
            volume_type=VolumeType.MANAGED,
        )
        logger.info(f"Volume '{full_volume_name}' created successfully.")
    except Exception as e:
        logger.error(f"Error checking volume '{full_volume_name}': {e}")
        raise
