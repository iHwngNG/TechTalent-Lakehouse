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
    """
    Idempotent bootstrap: ensure all required Databricks volumes exist.
    Call this once at the start of a Databricks Job before any scraper runs.

    Volumes created under workspace.techtalent_lakehouse:
        - raws  : raw JSONL files per scraper
        - error : centralized error log (all scrapers share one daily file)
    """
    ensure_schema_exists()
    for vol in ("raws", "error"):
        ensure_volume_exists(volume_name=vol)
    logger.info("All required volumes are ready.")
