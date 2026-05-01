"""
TopDev Job Crawler — TopDevScraper (BaseScraper subclass)
"""

import os
import re
import sys
import asyncio
import argparse
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv, find_dotenv

# Bootstrap sys.path from .env before any local imports
from src.utils.getProjectRoot import getRootPath

load_dotenv(find_dotenv())
PROJECT_ROOT = getRootPath()

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from playwright.async_api import async_playwright

from scrapers.base_scraper import (
    BaseScraper,
    async_retry,
    BrowserDisconnectedError,
    AntiBotDetectedError,
)

# ── Constants ──────────────────────────────────────────────────────────────────

BASE_URL = "https://topdev.vn"
LIST_URL = f"{BASE_URL}/jobs/search"
BATCH_SIZE = 1  # Sequential processing for maximum stability over tunnels
CONCURRENCY = 1

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Patches navigator properties before page navigation to avoid bot detection
STEALTH_JS = """
    Object.defineProperty(navigator, 'webdriver', {get: () => false});
    window.chrome = {runtime: {}};
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en', 'vi']});
    // Add fake screen properties
    Object.defineProperty(screen, 'width', {get: () => 1920});
    Object.defineProperty(screen, 'height', {get: () => 1080});
"""

_ANTIBOT_MARKERS = [
    "cf-browser-verification",
    "checking your browser",
    "please wait while we verify",
    "enable javascript and cookies",
    "captcha",
    "access denied",
    "unusual traffic",
]

_DISCONNECT_KEYWORDS = [
    "target page, context or browser has been closed",
    "browser has been disconnected",
    "connection closed",
    "websocket",
    "playwright.async_api.error: target closed",
]


# ── Helpers ────────────────────────────────────────────────────────────────────


def _extract_job_id(slug: str) -> str:
    """Extract numeric TopDev job ID from URL slug."""
    match = re.search(r"-(\d+)$", slug)
    return f"topdev_{match.group(1)}" if match else f"topdev_{slug}"


def _parse_topdev_date(text: str) -> str:
    """Normalize TopDev deadline/posted date to YYYY-MM-DD."""
    if not text:
        return ""
    text = text.strip()
    match = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", text)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    return text


# ── Scraper ────────────────────────────────────────────────────────────────────


class TopDevScraper(BaseScraper):
    _validate_urls = False  # TopDev blocks HEAD probing

    def __init__(self):
        super().__init__(source_name="topdev")
        self._browser_config = BrowserConfig(
            headless=True,
            viewport_width=1920,
            viewport_height=1080,
            cdp_url=os.environ.get("BROWSER_WS_URL"),
            ignore_https_errors=True,
        )
        self._crawler_config = CrawlerRunConfig(
            page_timeout=60000,
            wait_for="css:a[href*='/detail-jobs/']",
            override_navigator=True,
        )

    # ── List page helpers ──────────────────────────────────────────────────────

    def _extract_slugs_from_html(self, html: str) -> list:
        """Extract job slugs from TopDev listing HTML."""
        soup = BeautifulSoup(html, "html.parser")
        slugs = []
        seen = set()
        for a in soup.select("a[href*='/detail-jobs/']"):
            href = a.get("href", "")
            href_clean = href.split("?")[0]
            slug = href_clean.split("/detail-jobs/")[-1].strip("/")
            if slug and slug not in seen:
                seen.add(slug)
                slugs.append(slug)
        return slugs

    def _get_max_pages(self, html: str) -> int:
        """Auto-detect total pages."""
        try:
            soup = BeautifulSoup(html, "html.parser")
            pages = [
                int(a.get_text(strip=True))
                for a in soup.select("a[href*='page=']")
                if a.get_text(strip=True).isdigit()
            ]
            return max(pages, default=1)
        except:
            return 1

    @async_retry(max_retries=3, base_delay=2.0)
    async def _fetch_list_page(self, url: str) -> str:
        """Fetch listing page via Crawl4AI."""
        async with AsyncWebCrawler(config=self._browser_config) as crawler:
            result = await crawler.arun(url=url, config=self._crawler_config)
            if not result.success:
                raise RuntimeError(
                    f"Crawl4AI failed to fetch {url}: {result.error_message}"
                )
            return result.html

    async def _collect_all_slugs(self, max_pages: int) -> set:
        """Phase 1: Collect slugs from pagination."""
        html1 = await self._fetch_list_page(LIST_URL)
        if not html1:
            raise RuntimeError("Failed to fetch listing page 1 — aborting.")

        all_slugs = set(self._extract_slugs_from_html(html1))
        total_pages = self._get_max_pages(html1) if max_pages <= 0 else max_pages
        self.logger.info(
            f"Crawling {total_pages} pages. Page 1: {len(all_slugs)} slugs."
        )

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
        """Map slugs to full URLs and IDs."""
        return {
            f"{BASE_URL}/detail-jobs/{slug}": _extract_job_id(slug) for slug in slugs
        }

    # ── Detail page ────────────────────────────────────────────────────────────

    def _extract_detail_from_html(self, html: str, job_id: str, url: str) -> dict:
        """Parse TopDev job detail HTML."""
        soup = BeautifulSoup(html, "html.parser")

        title_el = soup.select_one("h1") or soup.select_one(
            "a.text-brand-500.font-semibold"
        )
        company_el = soup.select_one("a[href*='/companies/'] span") or soup.select_one(
            "a[href*='/companies/']"
        )

        salary_el = soup.select_one("[class*='salary']")
        if not salary_el:
            for span in soup.find_all("span"):
                text = span.get_text().lower()
                if "salary" in text or "lương" in text:
                    salary_el = span
                    break

        location_el = soup.select_one("[class*='location']")
        deadline_text = ""
        for el in soup.find_all(["span", "p"]):
            text = el.get_text()
            if re.search(r"\d{1,2}[-/]\d{1,2}[-/]\d{4}", text):
                deadline_text = text
                break

        skills = list(
            {
                a.get_text(strip=True)
                for a in soup.select("a[href*='keyword='], a[href*='/skills/']")
            }
        )

        desc_el = soup.select_one("[class*='job-description']") or soup.select_one(
            "article"
        )

        return {
            "job_id": job_id,
            "title": title_el.get_text(strip=True) if title_el else "",
            "company": company_el.get_text(strip=True) if company_el else "",
            "salary": salary_el.get_text(strip=True) if salary_el else "",
            "locations": location_el.get_text(strip=True) if location_el else "",
            "posted_date": _parse_topdev_date(deadline_text),
            "skills": skills,
            "description": (
                desc_el.get_text(separator="\n", strip=True) if desc_el else ""
            ),
            "source": self.source_name,
            "url": url,
            "crawled_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _fetch_detail_page(
        self, browser: Any, url: str, identifier: str, semaphore: asyncio.Semaphore
    ) -> dict:
        """Fetch and parse a TopDev detail page via Playwright CDP."""
        async with semaphore:
            context = await browser.new_context(
                user_agent=USER_AGENT, ignore_https_errors=True
            )
            try:
                await asyncio.sleep(random.uniform(1.0, 3.0))
                await context.add_init_script(STEALTH_JS)
                page = await context.new_page()

                await page.goto(url, wait_until="commit", timeout=60000)
                await asyncio.sleep(5)  # Wait for React hydration

                if page.is_closed():
                    raise BrowserDisconnectedError(f"Page closed at {url}")

                html = await page.content()
                if any(m in html.lower() for m in _ANTIBOT_MARKERS):
                    raise AntiBotDetectedError(f"Anti-bot block at {url}")

                return self._extract_detail_from_html(html, identifier, url)
            except (BrowserDisconnectedError, AntiBotDetectedError):
                raise
            except Exception as e:
                if any(kw in str(e).lower() for kw in _DISCONNECT_KEYWORDS):
                    raise BrowserDisconnectedError(f"CDP Disconnect: {e}")
                self.logger.error(f"Detail fetch failed [{url}]: {e}")
                return {}
            finally:
                await context.close()

    # ── Main orchestration ─────────────────────────────────────────────────────

    @async_retry(max_retries=3, base_delay=2.0)
    async def scrape(self, max_pages: int = 0, existing=None) -> list:
        """Scrape TopDev with batching and incremental filtering."""
        if existing is None:
            existing = set()

        all_slugs = await self._collect_all_slugs(max_pages)
        url_id_map = self._build_url_id_map(all_slugs)
        new_job_ids, skipped = self._filter_new_ids(set(url_id_map.values()), existing)

        if not new_job_ids:
            self.logger.info(f"No new jobs found ({skipped} already exist).")
            return []

        self.logger.info(f"{len(new_job_ids)} new jobs to scrape, {skipped} skipped.")

        items = list({u: j for u, j in url_id_map.items() if j in new_job_ids}.items())
        total_saved = 0
        total_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE
        cdp_url = os.environ.get("BROWSER_WS_URL", "ws://localhost:3000")

        async with async_playwright() as pw:
            semaphore = asyncio.Semaphore(CONCURRENCY)
            for i in range(0, len(items), BATCH_SIZE):
                batch = dict(items[i : i + BATCH_SIZE])
                batch_num = i // BATCH_SIZE + 1

                # Retry each batch once on browser disconnect
                for attempt in range(2):
                    browser = await pw.chromium.connect_over_cdp(cdp_url, timeout=0)
                    try:
                        saved = await self._process_batch(
                            browser, batch, semaphore, existing
                        )
                        total_saved += saved
                        self.logger.info(
                            f"Batch {batch_num}/{total_batches}: saved {saved} jobs (total: {total_saved})"
                        )
                        break  # Success, move to next batch
                    except BrowserDisconnectedError:
                        if attempt == 0:
                            self.logger.warning(
                                f"Batch {batch_num} disconnected — retrying..."
                            )
                            await asyncio.sleep(5)
                            continue
                        else:
                            self.logger.error(
                                f"Batch {batch_num} failed twice due to disconnect."
                            )
                            break
                    finally:
                        await browser.close()

        self.logger.info(f"TopDev scrape complete — {total_saved} new jobs saved.")
        return []


# ── Entry point ────────────────────────────────────────────────────────────────


async def main():
    parser = argparse.ArgumentParser(description="TopDev Scraper")
    parser.add_argument("--pages", type=int, default=0)
    args = parser.parse_args()

    scraper = TopDevScraper()
    try:
        existing = scraper.load_existing_records()
        await scraper.scrape(max_pages=args.pages, existing=existing)
    except Exception as e:
        scraper.save_error(e, context="main")
        raise
    finally:
        if sys.platform == "win32":
            await asyncio.sleep(0.25)


if __name__ == "__main__":
    if sys.platform == "win32":

        def _cleanup(unraisable):
            if str(unraisable.exc_value) in (
                "I/O operation on closed pipe",
                "Event loop is closed",
            ):
                return
            sys.__unraisablehook__(unraisable)

        sys.unraisablehook = _cleanup
    asyncio.run(main())
