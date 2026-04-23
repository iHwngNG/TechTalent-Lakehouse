"""
ITviec Job Crawler — Refactored using BaseScraper & Crawl4AI
"""

import os
import sys
import asyncio
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add project root to sys.path for cross-module imports
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

# Import base class and utils
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
        value = None
        unit_raw = ""
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

        from dotenv import load_dotenv

        load_dotenv()

        self.browser_config = BrowserConfig(
            headless=True,
            viewport_width=1920,
            viewport_height=1080,
            cdp_url=os.environ.get("BROWSER_WS_URL"),
            ignore_https_errors=True,
        )

    def _extract_slugs_from_html(self, html: str) -> list:
        """Extract job slugs from the search results HTML using BeautifulSoup."""
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select("div.job-card")
        slugs = []
        for card in cards:
            slug = card.get("data-search--job-selection-job-slug-value")
            if slug and slug.strip():
                slugs.append(slug.strip())
        return slugs

    def _get_max_pages(self, html: str) -> int:
        """Auto-detect total number of pages from the pagination."""
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
        """Extract structured job details from the job detail HTML using BeautifulSoup."""
        soup = BeautifulSoup(html, "html.parser")

        title_el = soup.select_one("h1.job-name, h1[class*='job-name'], h1")
        title = title_el.get_text(strip=True) if title_el else ""

        company_el = soup.select_one("div.employer-name")
        company = company_el.get_text(strip=True) if company_el else ""

        salary_el = soup.select_one("a.sign-in-view-salary")
        salary = salary_el.get_text(strip=True) if salary_el else ""

        # Text info elements like location, working method, posted date
        info_els = soup.select("span.normal-text.text-rich-grey")
        locations = info_els[0].get_text(strip=True) if len(info_els) > 0 else ""
        working_method = info_els[1].get_text(strip=True) if len(info_els) > 1 else ""
        posted_raw = info_els[2].get_text(strip=True) if len(info_els) > 2 else ""
        posted_date = parse_relative_date(posted_raw)

        # Skills and Expertise
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

        # Stealth init script injected BEFORE page navigation (context-level)
        # This patches navigator.webdriver before any Cloudflare checks run.
        stealth_init_js = """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = {runtime: {}};
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en', 'vi']});
        """

        crawler_config = CrawlerRunConfig(
            page_timeout=60000,
            wait_for="css:div.job-card",  # Wait for job cards to load
            remove_overlay_elements=True,
        )

        all_slugs = set()

        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            # Fetch page 1 first to get total pages and initial slugs
            self.logger.info(f"Fetching page 1 from {LIST_URL}...")
            result = await crawler.arun(url=LIST_URL, config=crawler_config)

            if not result.success:
                self.logger.error(f"Failed to fetch page 1: {result.error_message}")
                raise Exception(f"Failed to fetch page 1: {result.error_message}")

            slugs = self._extract_slugs_from_html(result.html)
            self.logger.info(f"Page 1: Found {len(slugs)} job slugs.")

            if max_pages <= 0:
                max_pages = self._get_max_pages(result.html)
                self.logger.info(f"Auto-detect mode: will crawl all {max_pages} pages")
            else:
                self.logger.info(f"Manual mode: will crawl {max_pages} pages")

            all_slugs.update(slugs)

            # Crawl remaining pages concurrently in batches of 5
            remaining_pages = list(range(2, max_pages + 1))
            batch_size = 5
            for i in range(0, len(remaining_pages), batch_size):
                batch = remaining_pages[i : i + batch_size]
                tasks = [
                    crawler.arun(url=f"{LIST_URL}?page={p}", config=crawler_config)
                    for p in batch
                ]
                batch_results = await asyncio.gather(*tasks)
                for page_num, res in zip(batch, batch_results):
                    if res.success:
                        page_slugs = self._extract_slugs_from_html(res.html)
                        self.logger.info(
                            f"Page {page_num}: Found {len(page_slugs)} job slugs."
                        )
                        if not page_slugs:
                            break
                        all_slugs.update(page_slugs)
                    else:
                        self.logger.error(f"Failed to fetch page {page_num}")

        # Pre-filter by job_id before fetching details (fast O(1) set lookup)
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

        # Crawl detail pages using direct Playwright CDP with add_init_script()
        # This injects stealth JS BEFORE any page JS runs, bypassing Cloudflare.
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
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    ignore_https_errors=True,
                )
                try:
                    # Inject stealth BEFORE any page navigation
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
            semaphore = asyncio.Semaphore(5)  # 5 concurrent detail pages for speed
            tasks = [
                fetch_detail(browser, f"{LIST_URL}/{slug}", slug, semaphore)
                for slug in new_slugs
            ]
            results = await asyncio.gather(*tasks)
            await browser.close()

        all_jobs = []
        for job_data in results:
            # Final check: apply posted_date dedup via BaseScraper.is_new_job()
            if job_data.get("title") and self.is_new_job(
                job_data["job_id"], job_data.get("posted_date", ""), existing
            ):
                all_jobs.append(job_data)

        self.logger.info(f"Successfully extracted {len(all_jobs)} new jobs.")
        return all_jobs


async def main():
    parser = argparse.ArgumentParser(description="ITviec Crawler (OOP + Crawl4AI)")
    parser.add_argument(
        "--pages", type=int, default=0, help="Number of pages to crawl (0 = all pages)"
    )
    args = parser.parse_args()

    scraper = ItviecScraper()

    # Load existing (job_id, posted_date) pairs from volume for incremental scraping
    scraper.logger.info(f"Loading existing records from: {scraper.volume_dir}")
    existing = scraper.load_existing_records()
    scraper.logger.info(f"Found {len(existing)} existing records.")

    # Scrape only new jobs
    jobs = await scraper.scrape(max_pages=args.pages, existing=existing)

    if jobs:
        scraper.save_to_volume(jobs)

    # Windows Proactor cleanup
    if sys.platform == "win32":
        await asyncio.sleep(0.25)


if __name__ == "__main__":
    if sys.platform == "win32":
        # Suppress the known bug: ValueError: I/O operation on closed pipe
        def custom_unraisablehook(unraisable):
            if unraisable.exc_type == ValueError and str(unraisable.exc_value) == "I/O operation on closed pipe":
                return
            sys.__unraisablehook__(unraisable)

        sys.unraisablehook = custom_unraisablehook

    asyncio.run(main())
