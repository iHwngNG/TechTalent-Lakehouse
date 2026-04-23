import abc
import time
import json
import os
import logging
import functools
from datetime import datetime
from pathlib import Path
from typing import Any, Set, Tuple

# Structured logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Base Databricks Volume path — each scraper writes to its own subdirectory
VOLUME_BASE = "/Volumes/workspace/techtalent_lakehouse/raws"


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
    - Volume-based persistence: append new records to a per-day JSONL file inside
      the Databricks Unity Catalog Volume.
    - Legacy JSONL save for local development.
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
