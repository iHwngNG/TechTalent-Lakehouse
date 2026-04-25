import abc
import time
import json
import os
import logging
import functools
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Set, Tuple

from src.validators.bronze.data_quality_validator import (
    validate_bronze_data,
    DQ_EMPTY_THRESHOLD,
)

# Structured logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Base Databricks Volume path — each scraper writes to its own subdirectory
VOLUME_BASE = "/Volumes/workspace/techtalent_lakehouse/raws"

# Centralized error log — all scrapers share one daily file
ERROR_VOLUME = "/Volumes/workspace/techtalent_lakehouse/error"

# Structured logging configuration


def retry(max_retries=3, base_delay=2.0):
    """Retry decorator with exponential backoff for synchronous functions."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        logging.error(f"Failed after {max_retries} retries: {e}")
                        raise
                    delay = base_delay * (2 ** (retries - 1))
                    logging.warning(
                        f"Error occurred: {e}. Retrying {retries}/{max_retries} in {delay}s..."
                    )
                    time.sleep(delay)

        return wrapper

    return decorator


def async_retry(max_retries=3, base_delay=2.0):
    """Retry decorator with exponential backoff for asynchronous functions."""

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            import asyncio

            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        logging.error(f"Failed after {max_retries} retries: {e}")
                        raise
                    delay = base_delay * (2 ** (retries - 1))
                    logging.warning(
                        f"Error occurred: {e}. Retrying {retries}/{max_retries} in {delay}s..."
                    )
                    await asyncio.sleep(delay)

        return wrapper

    return decorator


class BaseScraper(abc.ABC):
    """
    Abstract base class defining the common interface for all scrapers.

    Provides shared logic for:
    - Incremental scraping: load existing (job_id, posted_date) pairs from the
      Volume directory so child scrapers can skip already-known jobs.
    - Micro-batch persistence: validate + append each page-worth of records
      immediately via save_batch(), so a crash only loses the current page.
    - Centralized error logging to a shared Volume path.
    """

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.logger = logging.getLogger(self.__class__.__name__)
        # Each scraper writes to its own sub-folder inside the volume
        self.volume_dir = os.path.join(VOLUME_BASE, source_name)

    # ──────────────────────────────────────────────────────────────────────────
    # Incremental scraping helpers
    # ──────────────────────────────────────────────────────────────────────────

    def load_existing_records(self) -> Set[Tuple[str, str]]:
        """
        Scan all JSONL files in the scraper's volume directory and return a set
        of (job_id, posted_date) tuples representing already-scraped jobs.

        A job is considered 'new' only when its job_id has never been seen OR
        when its posted_date has changed (e.g. a repost).
        Returns an empty set when the directory does not exist yet.
        """
        existing: Set[Tuple[str, str]] = set()
        vol_path = Path(self.volume_dir)
        if not vol_path.exists():
            return existing

        for jsonl_file in sorted(vol_path.glob("*.jsonl")):
            try:
                with open(jsonl_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        record = json.loads(line)
                        job_id = record.get("job_id", "")
                        posted_date = record.get("posted_date", "")
                        if job_id:
                            existing.add((job_id, posted_date))
            except Exception as e:
                self.logger.warning(f"Could not read {jsonl_file}: {e}")

        return existing

    def is_new_job(
        self, job_id: str, posted_date: str, existing: Set[Tuple[str, str]]
    ) -> bool:
        """
        Return True if the job should be scraped.
        A job is new if:
          - Its job_id has never been seen, OR
          - Its job_id exists but with a different posted_date (re-posted job).
        """
        # Collect all known posted_dates for this job_id
        known_dates = {pd for (jid, pd) in existing if jid == job_id}
        if not known_dates:
            return True  # Never seen before
        return posted_date not in known_dates  # Re-posted with a different date

    # ──────────────────────────────────────────────────────────────────────────
    # Persistence
    # ──────────────────────────────────────────────────────────────────────────

    def validate_quality(
        self,
        data: list,
        critical_fields: tuple = ("title", "company"),
        threshold: float = DQ_EMPTY_THRESHOLD,
    ) -> None:
        """
        Data Quality Gate — delegates to the centralized bronze layer validator.
        """
        validate_bronze_data(
            data=data,
            source_name=self.source_name,
            critical_fields=critical_fields,
            threshold=threshold,
        )

    def save_batch(
        self,
        batch: list,
        critical_fields: tuple = ("title", "company"),
    ) -> int:
        """
        Micro-batch checkpoint: validate quality then immediately append to Volume.

        Call this once per page inside the scraping loop instead of accumulating
        all results in memory. If the pipeline crashes mid-run, every batch
        written before the crash is preserved — the next run will skip those
        records via load_existing_records().

        Args:
            batch           : list of job dicts from a single page
            critical_fields : fields checked by the quality gate

        Returns:
            Number of records saved (0 if batch is empty or fails quality gate).

        Raises:
            DataQualityError: propagated from validate_quality() when the
                              website DOM has changed and data is mostly empty.
        """
        if not batch:
            return 0
        self.validate_quality(batch, critical_fields=critical_fields)
        self.save_to_volume(batch)
        return len(batch)

    def save_to_volume(self, data: list) -> str:
        """
        Append new job records to the scraper's daily JSONL file inside the
        Databricks Unity Catalog Volume directory.
        Returns the full path of the written file, or '' if nothing was saved.
        """
        if not data:
            self.logger.warning("No data to save.")
            return ""

        Path(self.volume_dir).mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")
        output_path = os.path.join(
            self.volume_dir, f"{self.source_name}_{date_str}.jsonl"
        )

        with open(output_path, "a", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

        self.logger.info(f"Saved {len(data)} records to {output_path}")
        return output_path

    def save(self, data: list, output_path: str) -> None:
        """Save data as JSONL to an arbitrary local path (for development/testing)."""
        if not data:
            self.logger.warning("No data to save.")
            return

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        self.logger.info(f"Saved {len(data)} records to {output_path}")

    # ──────────────────────────────────────────────────────────────────────────
    # Error logging
    # ──────────────────────────────────────────────────────────────────────────

    def save_error(self, error: Exception, context: str = "") -> None:
        """
        Append a structured error record to the shared daily error JSONL file
        in the Databricks Volume at /Volumes/workspace/techtalent_lakehouse/error/.

        Schema (minimal, queryable by Spark):
            timestamp   : ISO-8601 UTC string
            source      : scraper name (e.g. 'itviec')
            error_type  : exception class name
            error_msg   : short error message
            context     : where in the pipeline the error occurred
            traceback   : full stack trace for debugging
        """
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": self.source_name,
            "error_type": type(error).__name__,
            "error_msg": str(error),
            "context": context,
            "traceback": traceback.format_exc(),
        }
        self.save_error_record(record)

    def save_error_record(self, record: dict) -> None:
        """Save a pre-formatted error dictionary to the shared error Volume."""
        try:
            Path(ERROR_VOLUME).mkdir(parents=True, exist_ok=True)
            date_str = datetime.now().strftime("%Y%m%d")
            error_path = os.path.join(ERROR_VOLUME, f"errors_{date_str}.jsonl")
            with open(error_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
            self.logger.error(
                f"[{record.get('context', 'N/A')}] {record.get('error_type', 'Error')}: "
                f"{record.get('error_msg', '')} — logged to {error_path}"
            )
        except Exception as write_err:
            # Last-resort: if writing to Volume also fails, only log locally
            self.logger.error(f"Failed to write error to volume: {write_err}")
            self.logger.error(f"Original error record — {json.dumps(record)}")

    # ──────────────────────────────────────────────────────────────────────────
    # Abstract interface
    # ──────────────────────────────────────────────────────────────────────────

    @abc.abstractmethod
    async def scrape(self, *args, **kwargs) -> Any:
        """
        Core scraping function — must be implemented by each child scraper.
        Should accept an `existing` parameter (Set[Tuple[str,str]]) and use
        self.is_new_job() to filter out already-scraped records.
        """
        pass
