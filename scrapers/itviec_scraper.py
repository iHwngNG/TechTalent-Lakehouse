"""
ITviec Job Crawler — Refactored using BaseScraper & Crawl4AI
"""

import os
import sys
import asyncio
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# Add project root to sys.path for cross-module imports from .env
PROJECT_ROOT = os.environ.get("PROJECT_ROOT")
if PROJECT_ROOT and PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
elif not PROJECT_ROOT:
    PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from typing import List, Set, Tuple, Dict
from urllib.parse import urljoin

from src.validators.bronze.url_validator import validate_urls
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

from scrapers.base_scraper import BaseScraper, async_retry

BASE_URL = "https://itviec.com"
LIST_URL = f"{BASE_URL}/it-jobs"


def parse_relative_date(text: str) -> str:
    """Convert relative time string to actual date (YYYY-MM-DD)."""
    if not text:
        return ""
    text_lower = text.strip().lower()
    unit_map = {
        "second": "seconds",
        "giây": "seconds",
        "minute": "minutes",
        "phút": "minutes",
        "hour": "hours",
        "giờ": "hours",
        "day": "days",
        "ngày": "days",
        "week": "weeks",
        "tuần": "weeks",
    }
    try:
        parts = text_lower.split()
        value, unit_raw = None, ""
        for i, part in enumerate(parts):
            if part.isdigit():
                value = int(part)
                unit_raw = parts[i + 1] if i + 1 < len(parts) else ""
                break
        if value is None or not unit_raw:
            return text
        now = datetime.now(timezone.utc)
        if "month" in unit_raw or "tháng" in unit_raw:
            return (now - timedelta(days=value * 30)).strftime("%Y-%m-%d")
        if "year" in unit_raw or "năm" in unit_raw:
            return (now - timedelta(days=value * 365)).strftime("%Y-%m-%d")
        for keyword, td_arg in unit_map.items():
            if keyword in unit_raw:
                return (now - timedelta(**{td_arg: value})).strftime("%Y-%m-%d")
        return text
    except (ValueError, IndexError):
        return text


class ItviecScraper(BaseScraper):
    def __init__(self):
        super().__init__(source_name="itviec")
        load_dotenv()
        self.browser_config = BrowserConfig(
            headless=True,
            viewport_width=1920,
            viewport_height=1080,
            cdp_url=os.environ.get("BROWSER_WS_URL"),
            ignore_https_errors=True,
        )

    def _extract_slugs_from_html(self, html: str) -> list:
        """Extract job slugs from the search results HTML."""
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select("div.job-card")
        return [
            card.get("data-search--job-selection-job-slug-value", "").strip()
            for card in cards
            if card.get("data-search--job-selection-job-slug-value", "").strip()
        ]

    def _get_max_pages(self, html: str) -> int:
        """Auto-detect total number of pages from pagination."""
        try:
            soup = BeautifulSoup(html, "html.parser")
            links = soup.select("nav div.page a")
            max_page = 1
            for link in links:
                text = link.get_text(strip=True)
                if text.isdigit():
                    max_page = max(max_page, int(text))
            return max_page
        except Exception as e:
            self.logger.warning(f"Could not detect max pages ({e}), defaulting to 1")
            return 1

    def _extract_detail_from_html(self, html: str, slug: str, url: str) -> dict:
        """Extract structured job details from the job detail HTML."""
        soup = BeautifulSoup(html, "html.parser")

        title_el = soup.select_one("h1.job-name, h1[class*='job-name'], h1")
        title = title_el.get_text(strip=True) if title_el else ""

        company_el = soup.select_one("div.employer-name")
        company = company_el.get_text(strip=True) if company_el else ""

        salary_el = soup.select_one("a.sign-in-view-salary")
        salary = salary_el.get_text(strip=True) if salary_el else ""

        info_els = soup.select("span.normal-text.text-rich-grey")
        locations = info_els[0].get_text(strip=True) if len(info_els) > 0 else ""
        working_method = info_els[1].get_text(strip=True) if len(info_els) > 1 else ""
        posted_raw = info_els[2].get_text(strip=True) if len(info_els) > 2 else ""
        posted_date = parse_relative_date(posted_raw)

        itags = soup.select("a.itag")
        skills = [tag.get_text(strip=True) for tag in itags]

        desc_el = soup.select_one("section.job-content")
        description = desc_el.get_text(separator="\n", strip=True) if desc_el else ""

        return {
            "job_id": f"itviec_{slug}",
            "title": title,
            "company": company,
            "salary": salary,
            "locations": locations,
            "working_method": working_method,
            "skills": skills,
            "posted_date": posted_date,
            "description": description,
            "source": self.source_name,
            "url": url,
            "crawled_at": datetime.now(timezone.utc).isoformat(),
        }

    @async_retry(max_retries=3, base_delay=2.0)
    async def scrape(self, max_pages: int = 0, existing=None) -> list:
        """
        Scrape ITviec job listings.

        existing: set of (job_id, posted_date) tuples from BaseScraper.load_existing_records().
        Only jobs that pass self.is_new_job() will have their details fetched.
        """
        if existing is None:
            existing = set()

        # Stealth init script — patches navigator BEFORE page navigation
        stealth_init_js = """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = {runtime: {}};
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en', 'vi']});
        """

        # override_navigator=True: patches navigator at context-level BEFORE nav (safe).
        # Do NOT use magic=True or remove_overlay_elements=True — they run JS
        # AFTER page load and crash with "Target page closed" on subsequent pages.
        crawler_config = CrawlerRunConfig(
            page_timeout=60000,
            wait_for="js:()=>document.querySelector('div.job-card') !== null",
            override_navigator=True,
        )

        async def _fetch_list_page(url: str) -> str:
            """Spawn a fresh AsyncWebCrawler per page to avoid shared-context crashes."""
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                res = await crawler.arun(url=url, config=crawler_config)
                return res.html if res.success else ""

        # ── Page 1: fetch + detect total pages ───────────────────────────────
        self.logger.info(f"Fetching page 1 from {LIST_URL}...")
        html1 = await _fetch_list_page(LIST_URL)
        if not html1:
            raise Exception("Failed to fetch page 1")

        slugs = self._extract_slugs_from_html(html1)
        self.logger.info(f"Page 1: Found {len(slugs)} job slugs.")

        if max_pages <= 0:
            max_pages = self._get_max_pages(html1)
            self.logger.info(f"Auto-detect mode: will crawl all {max_pages} pages")
        else:
            self.logger.info(f"Manual mode: will crawl {max_pages} pages")

        all_slugs: set = set(slugs)

        # ── Remaining pages: each uses a fresh crawler context ────────────────
        for page_num in range(2, max_pages + 1):
            self.logger.info(f"Fetching page {page_num}...")
            html = await _fetch_list_page(f"{LIST_URL}?page={page_num}")
            if html:
                page_slugs = self._extract_slugs_from_html(html)
                self.logger.info(f"Page {page_num}: Found {len(page_slugs)} job slugs.")
                if not page_slugs:
                    break
                all_slugs.update(page_slugs)
            else:
                self.logger.error(f"Failed to fetch page {page_num}")

        # ── Pre-filter by job_id (O(1) set lookup) ───────────────────────────
        existing_job_ids = {jid for (jid, _) in existing}
        new_slugs = {
            slug for slug in all_slugs if f"itviec_{slug}" not in existing_job_ids
        }
        skipped = len(all_slugs) - len(new_slugs)
        self.logger.info(
            f"Total slugs: {len(all_slugs)} | New: {len(new_slugs)} | Skipped (existing): {skipped}"
        )

        if not new_slugs:
            self.logger.info("No new jobs to scrape.")
            return []

        # ── Detail pages via direct Playwright CDP ────────────────────────────
        cdp_url = os.environ.get("BROWSER_WS_URL", "ws://localhost:3000")
        self.logger.info(
            f"Extracting details for {len(new_slugs)} new jobs via Playwright CDP..."
        )

        async def fetch_detail(
            pw_browser, url: str, slug: str, semaphore: asyncio.Semaphore
        ) -> dict:
            """Fetch a single job detail page with stealth init script applied."""
            async with semaphore:
                context = await pw_browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    ),
                    ignore_https_errors=True,
                )
                try:
                    await context.add_init_script(stealth_init_js)
                    page = await context.new_page()
                    self.logger.info(f"Scraping: {url}")
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_selector("h1", timeout=30000)
                    html = await page.content()
                    job = self._extract_detail_from_html(html, slug, url)
                    self.logger.info(
                        f"Done: [{job.get('title', 'N/A')}] @ {job.get('company', 'N/A')}"
                    )
                    return job
                except Exception as e:
                    self.logger.error(f"Failed to fetch {url}: {e}")
                    return {}
                finally:
                    await context.close()

        async with async_playwright() as pw:
            browser = await pw.chromium.connect_over_cdp(cdp_url)
            semaphore = asyncio.Semaphore(5)

            # Process slugs in page-sized batches so every completed batch is
            # checkpointed immediately via save_batch(). A crash only loses the
            # current batch — the next run resumes from where it left off.
            BATCH_SIZE = 20
            slug_list = list(new_slugs)
            total_saved = 0

            for i in range(0, len(slug_list), BATCH_SIZE):
                batch_slugs = slug_list[i : i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                self.logger.info(
                    f"Batch {batch_num}: fetching {len(batch_slugs)} jobs "
                    f"({i + 1}–{min(i + BATCH_SIZE, len(slug_list))} of {len(slug_list)})"
                )

                # Validate URLs before scraping
                slug_by_url = {f"{LIST_URL}/{slug}": slug for slug in batch_slugs}
                valid_urls, failures = validate_urls(list(slug_by_url.keys()))

                # Log and save unreachable URLs without crashing the pipeline
                for failure in failures:
                    self.save_error_record(failure.to_error_record(self.source_name))

                # Only spawn browser tasks for URLs that are actually alive
                tasks = [
                    fetch_detail(browser, url, slug_by_url[url], semaphore)
                    for url in valid_urls
                ]
                results = await asyncio.gather(*tasks)

                batch_jobs = [
                    job
                    for job in results
                    if job.get("title")
                    and self.is_new_job(
                        job["job_id"], job.get("posted_date", ""), existing
                    )
                ]

                if batch_jobs:
                    saved = self.save_batch(batch_jobs)  # validate + checkpoint
                    total_saved += saved
                    # Update in-memory set so later batches skip these records
                    for job in batch_jobs:
                        existing.add((job["job_id"], job.get("posted_date", "")))
                    self.logger.info(
                        f"Batch {batch_num}: checkpointed {saved} jobs "
                        f"(total so far: {total_saved})"
                    )

            await browser.close()

        self.logger.info(f"Scrape complete. Total new jobs saved: {total_saved}")
        # Return empty list — micro-batching already persisted everything above
        return []


async def main():
    parser = argparse.ArgumentParser(description="ITviec Crawler (OOP + Crawl4AI)")
    parser.add_argument(
        "--pages", type=int, default=0, help="Number of pages to crawl (0 = all pages)"
    )
    args = parser.parse_args()

    scraper = ItviecScraper()

    try:
        scraper.logger.info(f"Loading existing records from: {scraper.volume_dir}")
        existing = scraper.load_existing_records()
        scraper.logger.info(f"Found {len(existing)} existing records.")

        # scrape() now handles micro-batch checkpointing internally via save_batch()
        await scraper.scrape(max_pages=args.pages, existing=existing)

    except Exception as e:
        # Write structured error to /Volumes/workspace/techtalent_lakehouse/error/
        # so Databricks Jobs can keep running while errors are queryable via Spark SQL.
        scraper.save_error(e, context="main_pipeline")
        raise  # Re-raise so Databricks marks the Job run as FAILED

    finally:
        if sys.platform == "win32":
            await asyncio.sleep(0.25)


if __name__ == "__main__":
    if sys.platform == "win32":

        def custom_unraisablehook(unraisable):
            # Suppress known Python 3.9 Windows ProactorEventLoop cleanup noise
            if (
                unraisable.exc_type == ValueError
                and str(unraisable.exc_value) == "I/O operation on closed pipe"
            ):
                return
            if (
                unraisable.exc_type == RuntimeError
                and str(unraisable.exc_value) == "Event loop is closed"
            ):
                return
            sys.__unraisablehook__(unraisable)

        sys.unraisablehook = custom_unraisablehook

    asyncio.run(main())
