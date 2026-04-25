"""
ITviec Job Crawler — ItviecScraper (BaseScraper subclass)
"""

import os
import sys
import asyncio
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Set, Tuple

from dotenv import load_dotenv, find_dotenv

# Bootstrap sys.path from .env before any local imports
load_dotenv(find_dotenv())
PROJECT_ROOT = os.environ.get("PROJECT_ROOT") or str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from playwright.async_api import async_playwright

from scrapers.base_scraper import BaseScraper, async_retry

# ── Constants ──────────────────────────────────────────────────────────────────

BASE_URL = "https://itviec.com"
LIST_URL = f"{BASE_URL}/it-jobs"
BATCH_SIZE = 20
CONCURRENCY = 5

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Patches navigator properties before page navigation to avoid bot detection
STEALTH_JS = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    window.chrome = {runtime: {}};
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en', 'vi']});
"""


# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_relative_date(text: str) -> str:
    """Convert a relative time string (e.g. '3 days ago') to YYYY-MM-DD."""
    if not text:
        return ""
    text_lower = text.strip().lower()
    unit_map = {
        "second": "seconds", "giây": "seconds",
        "minute": "minutes", "phút": "minutes",
        "hour": "hours",   "giờ": "hours",
        "day": "days",     "ngày": "days",
        "week": "weeks",   "tuần": "weeks",
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


# ── Scraper ────────────────────────────────────────────────────────────────────

class ItviecScraper(BaseScraper):

    def __init__(self):
        super().__init__(source_name="itviec")
        # Crawl4AI config — override_navigator patches before nav (avoids "Target page closed")
        self._browser_config = BrowserConfig(
            headless=True,
            viewport_width=1920,
            viewport_height=1080,
            cdp_url=os.environ.get("BROWSER_WS_URL"),
            ignore_https_errors=True,
        )
        self._crawler_config = CrawlerRunConfig(
            page_timeout=60000,
            wait_for="js:()=>document.querySelector('div.job-card') !== null",
            override_navigator=True,
        )

    # ── List page helpers ──────────────────────────────────────────────────────

    def _extract_slugs_from_html(self, html: str) -> list:
        """Extract job slugs from a listing page."""
        soup = BeautifulSoup(html, "html.parser")
        return [
            card.get("data-search--job-selection-job-slug-value", "").strip()
            for card in soup.select("div.job-card")
            if card.get("data-search--job-selection-job-slug-value", "").strip()
        ]

    def _get_max_pages(self, html: str) -> int:
        """Auto-detect total pages from pagination links."""
        try:
            soup = BeautifulSoup(html, "html.parser")
            pages = [
                int(a.get_text(strip=True))
                for a in soup.select("nav div.page a")
                if a.get_text(strip=True).isdigit()
            ]
            return max(pages, default=1)
        except Exception as e:
            self.logger.warning(f"Could not detect max pages: {e}")
            return 1

    async def _fetch_list_page(self, url: str) -> str:
        """Fetch a listing page HTML using a fresh Crawl4AI context per call."""
        async with AsyncWebCrawler(config=self._browser_config) as crawler:
            result = await crawler.arun(url=url, config=self._crawler_config)
            return result.html if result.success else ""

    async def _collect_all_slugs(self, max_pages: int) -> set:
        """
        Fetch all listing pages and return the full set of job slugs.

        Uses a fresh crawler context per page to prevent shared-context crashes.
        Stops early if a page returns no slugs.
        """
        html1 = await self._fetch_list_page(LIST_URL)
        if not html1:
            raise RuntimeError("Failed to fetch listing page 1 — aborting.")

        all_slugs = set(self._extract_slugs_from_html(html1))
        total_pages = self._get_max_pages(html1) if max_pages <= 0 else max_pages
        self.logger.info(f"Crawling {total_pages} pages. Page 1: {len(all_slugs)} slugs.")

        for page in range(2, total_pages + 1):
            html = await self._fetch_list_page(f"{LIST_URL}?page={page}")
            if not html:
                self.logger.warning(f"Page {page}: empty response, stopping early.")
                break
            slugs = self._extract_slugs_from_html(html)
            if not slugs:
                break
            all_slugs.update(slugs)

        return all_slugs

    def _build_url_id_map(self, slugs: set) -> dict:
        """Build {full_url: job_id} mapping for a set of slugs."""
        return {f"{LIST_URL}/{slug}": f"itviec_{slug}" for slug in slugs}

    # ── Detail page ────────────────────────────────────────────────────────────

    def _extract_detail_from_html(self, html: str, slug: str, url: str) -> dict:
        """Parse job detail HTML into a structured record."""
        soup = BeautifulSoup(html, "html.parser")

        title_el = soup.select_one("h1.job-name, h1[class*='job-name'], h1")
        company_el = soup.select_one("div.employer-name")
        salary_el = soup.select_one("a.sign-in-view-salary")
        info_els = soup.select("span.normal-text.text-rich-grey")
        desc_el = soup.select_one("section.job-content")

        return {
            "job_id": f"itviec_{slug}",
            "title": title_el.get_text(strip=True) if title_el else "",
            "company": company_el.get_text(strip=True) if company_el else "",
            "salary": salary_el.get_text(strip=True) if salary_el else "",
            "locations": info_els[0].get_text(strip=True) if len(info_els) > 0 else "",
            "working_method": info_els[1].get_text(strip=True) if len(info_els) > 1 else "",
            "posted_date": parse_relative_date(
                info_els[2].get_text(strip=True) if len(info_els) > 2 else ""
            ),
            "skills": [tag.get_text(strip=True) for tag in soup.select("a.itag")],
            "description": desc_el.get_text(separator="\n", strip=True) if desc_el else "",
            "source": self.source_name,
            "url": url,
            "crawled_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _fetch_detail_page(
        self, browser: Any, url: str, identifier: str, semaphore: asyncio.Semaphore
    ) -> dict:
        """
        Fetch and parse a single ITviec job detail page via Playwright CDP.

        Returns {} on any failure so the batch loop can skip gracefully.
        """
        async with semaphore:
            context = await browser.new_context(
                user_agent=USER_AGENT, ignore_https_errors=True
            )
            try:
                await context.add_init_script(STEALTH_JS)
                page = await context.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_selector("h1", timeout=30000)
                html = await page.content()
                slug = identifier.removeprefix(f"{self.source_name}_")
                return self._extract_detail_from_html(html, slug, url)
            except Exception as e:
                self.logger.error(f"Detail fetch failed [{url}]: {e}")
                return {}
            finally:
                await context.close()

    # ── Main orchestration ─────────────────────────────────────────────────────

    @async_retry(max_retries=3, base_delay=2.0)
    async def scrape(self, max_pages: int = 0, existing=None) -> list:
        """
        Scrape ITviec job listings with incremental, micro-batch checkpointing.

        Phase 1: Collect all job slugs from listing pages.
        Phase 2: Filter against existing records (skip already-scraped jobs).
        Phase 3: Fetch details in batches — each batch is checkpointed immediately.

        Args:
            max_pages : pages to crawl (0 = auto-detect all)
            existing  : (job_id, posted_date) set from load_existing_records()
        """
        if existing is None:
            existing = set()

        # Phase 1: Collect slugs
        all_slugs = await self._collect_all_slugs(max_pages)
        url_id_map = self._build_url_id_map(all_slugs)

        # Phase 2: Filter new jobs
        new_job_ids, skipped = self._filter_new_ids(set(url_id_map.values()), existing)
        if not new_job_ids:
            self.logger.info(f"No new jobs found ({skipped} already exist).")
            return []

        self.logger.info(f"{len(new_job_ids)} new jobs to scrape, {skipped} skipped.")

        # Phase 3: Fetch details in micro-batches
        new_url_id_map = {url: jid for url, jid in url_id_map.items() if jid in new_job_ids}
        items = list(new_url_id_map.items())
        total_saved = 0
        total_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE
        cdp_url = os.environ.get("BROWSER_WS_URL", "ws://localhost:3000")

        async with async_playwright() as pw:
            browser = await pw.chromium.connect_over_cdp(cdp_url)
            semaphore = asyncio.Semaphore(CONCURRENCY)

            for i in range(0, len(items), BATCH_SIZE):
                batch = dict(items[i: i + BATCH_SIZE])
                batch_num = i // BATCH_SIZE + 1
                saved = await self._process_batch(browser, batch, semaphore, existing)
                total_saved += saved
                self.logger.info(
                    f"Batch {batch_num}/{total_batches}: saved {saved} jobs "
                    f"(total: {total_saved})"
                )

            await browser.close()

        self.logger.info(f"Scrape complete — {total_saved} new jobs saved.")
        return []


# ── Entry point ────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="ITviec Job Scraper")
    parser.add_argument(
        "--pages", type=int, default=0,
        help="Number of list pages to crawl (0 = all)"
    )
    args = parser.parse_args()

    scraper = ItviecScraper()

    try:
        existing = scraper.load_existing_records()
        scraper.logger.info(
            f"Loaded {len(existing)} existing records from {scraper.volume_dir}"
        )
        await scraper.scrape(max_pages=args.pages, existing=existing)

    except Exception as e:
        scraper.save_error(e, context="main")
        raise  # Re-raise so Databricks marks the Job as FAILED

    finally:
        if sys.platform == "win32":
            await asyncio.sleep(0.25)


if __name__ == "__main__":
    if sys.platform == "win32":
        def _suppress_win32_cleanup(unraisable):
            # Suppress known Python 3.9 Windows ProactorEventLoop noise
            msg = str(unraisable.exc_value)
            if msg in ("I/O operation on closed pipe", "Event loop is closed"):
                return
            sys.__unraisablehook__(unraisable)

        sys.unraisablehook = _suppress_win32_cleanup

    asyncio.run(main())
