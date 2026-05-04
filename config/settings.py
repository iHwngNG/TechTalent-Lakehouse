import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# Load sensitive environment variables from .env
load_dotenv(find_dotenv())


class Settings:
    # ---------------------------------------------------------
    # 1. PATHS & ENVIRONMENT SECRETS
    # ---------------------------------------------------------
    PROJECT_ROOT = os.getenv(
        "PROJECT_ROOT", str(Path(__file__).resolve().parent.parent)
    )

    BROWSER_WS_URL = os.getenv("BROWSER_WS_URL")
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

    # ---------------------------------------------------------
    # 2. DATABRICKS LAKEHOUSE CONFIGURATION
    # ---------------------------------------------------------
    CATALOG = "workspace"
    SCHEMA = "techtalent_lakehouse"

    # ---------------------------------------------------------
    # 3. SCRAPING MODE CONFIGURATION
    # ---------------------------------------------------------
    # Set to True to use GLOBAL configs for all scrapers.
    # Set to False to configure each scraper individually.
    USE_UNIFIED_CONFIG = True

    # --- Unified (Global) Config ---
    GLOBAL_BATCH_SIZE = 5
    GLOBAL_CONCURRENCY = 1

    # --- Specific Scraper Configs ---
    ITVIEC_BATCH_SIZE = 5
    ITVIEC_CONCURRENCY = 1

    TOPDEV_BATCH_SIZE = 5
    TOPDEV_CONCURRENCY = 1

    # ---------------------------------------------------------
    # 4. BOT BYPASS & STEALTH CONFIGURATION
    # ---------------------------------------------------------
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
        // Add fake screen properties
        Object.defineProperty(screen, 'width', {get: () => 1920});
        Object.defineProperty(screen, 'height', {get: () => 1080});
    """

    @classmethod
    def get_batch_size(cls, scraper_name: str) -> int:
        """Returns the appropriate batch size based on the current mode."""
        if cls.USE_UNIFIED_CONFIG:
            return cls.GLOBAL_BATCH_SIZE

        scraper_name = scraper_name.lower()
        if "itviec" in scraper_name:
            return cls.ITVIEC_BATCH_SIZE
        elif "topdev" in scraper_name:
            return cls.TOPDEV_BATCH_SIZE
        return cls.GLOBAL_BATCH_SIZE

    @classmethod
    def get_concurrency(cls, scraper_name: str) -> int:
        """Returns the appropriate concurrency based on the current mode."""
        if cls.USE_UNIFIED_CONFIG:
            return cls.GLOBAL_CONCURRENCY

        scraper_name = scraper_name.lower()
        if "itviec" in scraper_name:
            return cls.ITVIEC_CONCURRENCY
        elif "topdev" in scraper_name:
            return cls.TOPDEV_CONCURRENCY
        return cls.GLOBAL_CONCURRENCY


# Global settings instance
settings = Settings()
