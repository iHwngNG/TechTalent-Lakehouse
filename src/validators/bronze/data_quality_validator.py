import logging

logger = logging.getLogger(__name__)

# Jobs will be flagged as data quality failures if empty-field ratio exceeds this
DQ_EMPTY_THRESHOLD = 0.5  # 50%


class DataQualityError(RuntimeError):
    """Raised when scraped data has too many empty critical fields."""
    pass


def validate_bronze_data(
    data: list,
    source_name: str,
    critical_fields: tuple = ("title", "company"),
    threshold: float = DQ_EMPTY_THRESHOLD,
) -> None:
    """
    Data Quality Gate for Bronze Layer — call this BEFORE saving to volume.

    Raises DataQualityError if the ratio of records with any empty
    critical field exceeds `threshold`. This prevents writing garbage
    data to the Lakehouse when the website changes its DOM structure.

    Args:
        data            : list of scraped job dicts
        source_name     : name of the scraper source (for logging)
        critical_fields : fields that must not be empty
        threshold       : max allowed ratio of bad records (default 0.5)
    """
    if not data:
        return

    bad = sum(
        1 for job in data
        if any(not job.get(f, "").strip() for f in critical_fields)
    )
    ratio = bad / len(data)

    if ratio > threshold:
        raise DataQualityError(
            f"Data quality check failed for '{source_name}': "
            f"{bad}/{len(data)} records ({ratio:.0%}) have empty "
            f"critical fields {critical_fields}. "
            "Website DOM may have changed — aborting save."
        )

    logger.info(
        f"[{source_name}] Quality check passed: {len(data) - bad}/{len(data)} records are clean."
    )
