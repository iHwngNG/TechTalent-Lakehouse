import logging
import urllib.request
import urllib.error
import traceback
from datetime import datetime, timezone
from typing import NamedTuple, List, Tuple, Optional

logger = logging.getLogger(__name__)

# HTTP status codes that are considered permanently invalid (skip, do not retry)
PERMANENT_FAILURE_CODES = {404, 410, 451}

# HTTP status codes that are transient (could retry later)
TRANSIENT_FAILURE_CODES = {429, 500, 502, 503, 504}


class UrlCheckResult(NamedTuple):
    """Structured result of a single URL validation check."""
    url: str
    is_valid: bool
    status_code: int  # 0 when no HTTP response (DNS failure, timeout, etc.)
    reason: str
    error_type: str = ""
    tb_str: str = ""

    def to_error_record(self, source_name: str) -> dict:
        """Convert failure to the common Lakehouse error JSONL structure."""
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": source_name,
            "error_type": self.error_type,
            "error_msg": f"[{self.status_code}] {self.reason}",
            "context": f"url_validator: {self.url}",
            "traceback": self.tb_str,
        }


def check_url(url: str, timeout: int = 10) -> UrlCheckResult:
    """
    Send a HEAD request to `url` and return a UrlCheckResult.

    Never raises — all errors are caught and returned as is_valid=False,
    so callers can treat the result uniformly without try/except.

    Args:
        url     : URL to probe
        timeout : seconds before giving up (default 10)

    Returns:
        UrlCheckResult with is_valid=True only when the server responds 2xx/3xx.
    """
    try:
        req = urllib.request.Request(url, method="HEAD", headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return UrlCheckResult(
                url=url,
                is_valid=True,
                status_code=resp.status,
                reason="OK",
            )
    except urllib.error.HTTPError as e:
        return UrlCheckResult(
            url=url,
            is_valid=False,
            status_code=e.code,
            reason=str(e.reason),
            error_type="HTTPError",
            tb_str=traceback.format_exc(),
        )
    except urllib.error.URLError as e:
        # Covers: DNS failure, connection refused, timeout
        return UrlCheckResult(
            url=url,
            is_valid=False,
            status_code=0,
            reason=str(e.reason),
            error_type="URLError",
            tb_str=traceback.format_exc(),
        )
    except Exception as e:
        return UrlCheckResult(
            url=url,
            is_valid=False,
            status_code=0,
            reason=str(e),
            error_type=type(e).__name__,
            tb_str=traceback.format_exc(),
        )


def validate_urls(
    urls: List[str],
    timeout: int = 10,
) -> Tuple[List[str], List[UrlCheckResult]]:
    """
    Validate a list of URLs and partition them into reachable / unreachable.

    Pipeline usage pattern — call this before starting the scrape loop:

        valid_urls, failures = validate_urls(target_urls)
        for result in failures:
            self.save_error(ValueError(result.reason), context=result.url)
        # Continue with valid_urls only; pipeline never shuts down.

    Args:
        urls    : list of URLs to check
        timeout : per-URL timeout in seconds

    Returns:
        valid_urls : URLs that returned a successful HTTP response
        failures   : UrlCheckResult entries for every unreachable URL
    """
    valid_urls: List[str] = []
    failures: List[UrlCheckResult] = []

    for url in urls:
        result = check_url(url, timeout=timeout)
        if result.is_valid:
            valid_urls.append(url)
        else:
            level = (
                "warning"
                if result.status_code in TRANSIENT_FAILURE_CODES
                else "error"
            )
            getattr(logger, level)(
                f"URL unreachable [{result.status_code}] {url} — {result.reason}"
            )
            failures.append(result)

    logger.info(
        f"URL validation: {len(valid_urls)} reachable, {len(failures)} failed "
        f"out of {len(urls)} total."
    )
    return valid_urls, failures
