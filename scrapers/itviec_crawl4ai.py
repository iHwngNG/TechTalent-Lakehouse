"""
ITviec Job Crawler — Refactored using Crawl4AI
---------------------------------------------------------------
Usage:
    python scrapers/itviec_crawl4ai.py
    python scrapers/itviec_crawl4ai.py --pages 5
    python scrapers/itviec_crawl4ai.py --pages 5 --output itviec_jobs.jsonl

Requirements:
    pip install crawl4ai bs4
"""

import os
import json
import asyncio
import logging
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

# Add project root to sys.path for cross-module imports
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL = "https://itviec.com"
LIST_URL = f"{BASE_URL}/it-jobs"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_relative_date(text: str) -> str:
    """Convert relative time string to actual date (YYYY-MM-DD)."""
    if not text:
        return ""
    text_lower = text.strip().lower()
    unit_map = {
        "second": "seconds", "giây": "seconds",
        "minute": "minutes", "phút": "minutes",
        "hour": "hours", "giờ": "hours",
        "day": "days", "ngày": "days",
        "week": "weeks", "tuần": "weeks",
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

def extract_slugs_from_html(html: str) -> list:
    """Extract job slugs from the search results HTML using BeautifulSoup."""
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div.job-card")
    slugs = []
    for card in cards:
        slug = card.get("data-search--job-selection-job-slug-value")
        if slug and slug.strip():
            slugs.append(slug.strip())
    return slugs

def extract_detail_from_html(html: str, slug: str, url: str) -> dict:
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
        "source": "itviec",
        "url": url,
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }

# ── Main ──────────────────────────────────────────────────────────────────────

async def crawl(
    max_pages: int = 1,
    output_path: str = "itviec_jobs.jsonl",
):
    browser_config = BrowserConfig(
        headless=True,
        viewport_width=1920,
        viewport_height=1080,
    )
    
    crawler_config = CrawlerRunConfig(
        page_timeout=60000,
        wait_for="css:div.job-card",  # Wait for job cards to load
        remove_overlay_elements=True,
    )

    all_jobs = []
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        log.info(f"Fetching page 1 from {LIST_URL}...")
        result = await crawler.arun(url=LIST_URL, config=crawler_config)
        
        if not result.success:
            log.error(f"Failed to fetch page 1: {result.error_message}")
            return
            
        slugs = extract_slugs_from_html(result.html)
        log.info(f"Page 1: Found {len(slugs)} job slugs.")
        
        all_slugs = set(slugs)
        
        # Crawl subsequent pages
        for page_num in range(2, max_pages + 1):
            url = f"{LIST_URL}?page={page_num}"
            log.info(f"Fetching page {page_num} from {url}...")
            res = await crawler.arun(url=url, config=crawler_config)
            if res.success:
                page_slugs = extract_slugs_from_html(res.html)
                log.info(f"Page {page_num}: Found {len(page_slugs)} job slugs.")
                if not page_slugs:
                    break
                all_slugs.update(page_slugs)
            else:
                log.error(f"Failed to fetch page {page_num}")
                break
        
        log.info(f"Total unique slugs collected: {len(all_slugs)}")
        if not all_slugs:
            return

        detail_urls = [f"{LIST_URL}/{slug}" for slug in all_slugs]
        
        detail_crawler_config = CrawlerRunConfig(
            page_timeout=60000,
            wait_for="css:h1",  # Wait for title to load
            remove_overlay_elements=True,
        )

        log.info(f"Extracting details for {len(detail_urls)} jobs concurrently...")
        results = await crawler.arun_many(
            urls=detail_urls,
            config=detail_crawler_config,
            max_concurrent=5
        )
        
        all_slugs_list = list(all_slugs)
        for i, res in enumerate(results):
            if res.success:
                slug = all_slugs_list[i]
                job_data = extract_detail_from_html(res.html, slug, res.url)
                if job_data["title"]:
                    all_jobs.append(job_data)
            else:
                log.error(f"Failed to fetch {res.url}")
        
    log.info(f"Successfully extracted {len(all_jobs)} jobs.")
    
    # Save to JSON Lines format (DBFS landing zone compatible)
    with open(output_path, 'w', encoding='utf-8') as f:
        for job in all_jobs:
            f.write(json.dumps(job, ensure_ascii=False) + '\n')
            
    log.info(f"Done. Saved jobs to {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ITviec Crawler with Crawl4AI")
    parser.add_argument("--pages", type=int, default=1, help="Number of pages to crawl")
    parser.add_argument("--output", type=str, default="itviec_jobs.jsonl", help="Output JSONL file")
    args = parser.parse_args()
    
    asyncio.run(crawl(max_pages=args.pages, output_path=args.output))
