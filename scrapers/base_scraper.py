import abc
import asyncio
import time
import json
import os
import logging
import functools
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Set, Tuple

from src.validators.bronze.data_quality_validator import validate_bronze_data, DQ_EMPTY_THRESHOLD
from src.validators.bronze.url_validator import validate_urls

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

VOLUME_BASE = "/Volumes/workspace/techtalent_lakehouse/raws"
ERROR_VOLUME = "/Volumes/workspace/techtalent_lakehouse/error"


# ── Custom exceptions ──────────────────────────────────────────────────────────

class BrowserDisconnectedError(RuntimeError):
    """Raised when the remote Playwright browser/CDP connection is lost."""


class AntiBotDetectedError(RuntimeError):
    """Raised when a fetched page is an anti-bot / Cloudflare challenge page."""


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
                        raise
                    delay = base_delay * (2 ** (retries - 1))
                    logging.warning(f"Retry {retries}/{max_retries} in {delay}s: {e}")
                    time.sleep(delay)
        return wrapper
    return decorator


def async_retry(max_retries=3, base_delay=2.0):
    """Retry decorator with exponential backoff for asynchronous functions."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        raise
                    delay = base_delay * (2 ** (retries - 1))
                    logging.warning(f"Retry {retries}/{max_retries} in {delay}s: {e}")
                    await asyncio.sleep(delay)
        return wrapper
    return decorator


class BaseScraper(abc.ABC):
    """
    Abstract base class for all scrapers.

    Provides:
    - Incremental scraping via load_existing_records() + is_new_job().
    - Generic _filter_new_ids() and _process_batch() for reuse across all child scrapers.
    - Micro-batch checkpointing via save_batch() (validate + append atomically).
    - Centralized error logging to the shared Databricks error Volume.

    Child scrapers must implement:
    - scrape()             : top-level orchestration
    - _fetch_detail_page() : website-specific page fetching & parsing
    """

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.logger = logging.getLogger(self.__class__.__name__)
        self.volume_dir = os.path.join(VOLUME_BASE, source_name)

    # ── Incremental scraping ───────────────────────────────────────────────────

    def load_existing_records(self) -> Set[Tuple[str, str]]:
        """Return a set of (job_id, posted_date) tuples from all JSONL files in volume."""
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
                        if job_id:
                            existing.add((job_id, record.get("posted_date", "")))
            except Exception as e:
                self.logger.warning(f"Skipping unreadable file {jsonl_file.name}: {e}")

        return existing

    def is_new_job(self, job_id: str, posted_date: str, existing: Set[Tuple[str, str]]) -> bool:
        """Return True if this (job_id, posted_date) pair has not been scraped before."""
        known_dates = {pd for (jid, pd) in existing if jid == job_id}
        return not known_dates or posted_date not in known_dates

    def _filter_new_ids(self, all_ids: set, existing: Set[Tuple[str, str]]) -> Tuple[set, int]:
        """
        Filter a set of job IDs against the existing records.

        Returns:
            (new_ids, skipped_count)
        """
        known_ids = {jid for (jid, _) in existing}
        new_ids = all_ids - known_ids
        return new_ids, len(all_ids) - len(new_ids)

    # ── Abstract interface ─────────────────────────────────────────────────────

    @abc.abstractmethod
    async def scrape(self, *args, **kwargs) -> list:
        """Top-level scraping orchestration. Must be implemented by each child."""

    @abc.abstractmethod
    async def _fetch_detail_page(
        self, browser: Any, url: str, identifier: str, semaphore: asyncio.Semaphore
    ) -> dict:
        """
        Fetch and parse a single detail page.

        Returns a dict with at minimum: job_id, title, company, posted_date.
        Return {} on any failure — the batch loop will skip empty results.
        """

    # ── Shared batch orchestration ─────────────────────────────────────────────

    async def _process_batch(
        self,
        browser: Any,
        url_id_map: dict,
        semaphore: asyncio.Semaphore,
        existing: Set[Tuple[str, str]],
    ) -> int:
        """
        Validate URLs → fetch details concurrently → validate quality → checkpoint.

        Uses return_exceptions=True so one failing task never cancels the others.
        BrowserDisconnectedError and AntiBotDetectedError are surfaced once and
        re-raised / swallowed respectively — DataQualityError is never triggered
        by connection or bot-block failures.

        Returns:
            Number of records saved.
        """
        # Step 1: Drop unreachable URLs before opening browser contexts
        valid_urls, failures = validate_urls(list(url_id_map.keys()))
        for failure in failures:
            self.save_error_record(failure.to_error_record(self.source_name))
        if not valid_urls:
            return 0

        # Step 2: Fetch concurrently — exceptions are returned as values, not raised
        tasks = [
            self._fetch_detail_page(browser, url, url_id_map[url], semaphore)
            for url in valid_urls
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Step 3a: Browser disconnect — log once and re-raise to break the batch loop
        disconnect = next(
            (r for r in results if isinstance(r, BrowserDisconnectedError)), None
        )
        if disconnect:
            self.save_error(disconnect, context="browser_connection")
            raise disconnect

        # Step 3b: Anti-bot block — log once and skip batch (not a data quality issue)
        antibot = next(
            (r for r in results if isinstance(r, AntiBotDetectedError)), None
        )
        if antibot:
            self.save_error(antibot, context="antibot_detection")
            return 0

        # Step 4: Keep only successful dict results for new, valid jobs
        batch_jobs = [
            job for job in results
            if isinstance(job, dict)
            and job.get("title")
            and self.is_new_job(job["job_id"], job.get("posted_date", ""), existing)
        ]
        if not batch_jobs:
            return 0

        # Step 5: Quality gate + atomic Volume checkpoint
        saved = self.save_batch(batch_jobs)

        # Step 5: Update in-memory set so later batches skip these
        for job in batch_jobs:
            existing.add((job["job_id"], job.get("posted_date", "")))

        return saved

    # ── Persistence ────────────────────────────────────────────────────────────

    def validate_quality(
        self,
        data: list,
        critical_fields: tuple = ("title", "company"),
        threshold: float = DQ_EMPTY_THRESHOLD,
    ) -> None:
        """Data quality gate — delegates to the bronze layer validator."""
        validate_bronze_data(
            data=data,
            source_name=self.source_name,
            critical_fields=critical_fields,
            threshold=threshold,
        )

    def save_batch(self, batch: list, critical_fields: tuple = ("title", "company")) -> int:
        """Validate quality then immediately append batch to Volume (micro-batch checkpoint)."""
        if not batch:
            return 0
        self.validate_quality(batch, critical_fields=critical_fields)
        self.save_to_volume(batch)
        return len(batch)

    def save_to_volume(self, data: list) -> str:
        """Append records to the scraper's daily JSONL file in the Databricks Volume."""
        if not data:
            return ""

        Path(self.volume_dir).mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")
        output_path = os.path.join(self.volume_dir, f"{self.source_name}_{date_str}.jsonl")

        with open(output_path, "a", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

        self.logger.info(f"Saved {len(data)} records → {output_path}")
        return output_path

    def save(self, data: list, output_path: str) -> None:
        """Save data as JSONL to an arbitrary local path (for development/testing)."""
        if not data:
            return
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

    # ── Error logging ──────────────────────────────────────────────────────────

    def save_error(self, error: Exception, context: str = "") -> None:
        """Build a standard error record from an exception and persist it."""
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
        """Append a pre-formatted error dict to the shared daily error JSONL Volume."""
        try:
            Path(ERROR_VOLUME).mkdir(parents=True, exist_ok=True)
            date_str = datetime.now().strftime("%Y%m%d")
            error_path = os.path.join(ERROR_VOLUME, f"errors_{date_str}.jsonl")
            with open(error_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
            self.logger.error(
                f"[{record.get('context')}] {record.get('error_type')}: {record.get('error_msg')}"
            )
        except Exception as write_err:
            self.logger.error(f"Failed to write error to volume: {write_err}")
            self.logger.error(f"Original record: {json.dumps(record)}")
