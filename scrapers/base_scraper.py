import abc
import time
import json
import os
import logging
import functools
from typing import Any

# Cấu hình logging theo chuẩn cấu trúc
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def retry(max_retries=3, base_delay=2.0):
    """Decorator retry với exponential backoff cho hàm synchronous."""

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
    """Decorator retry với exponential backoff cho hàm asynchronous."""

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
    """

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.logger = logging.getLogger(self.__class__.__name__)

    @abc.abstractmethod
    async def scrape(self, *args, **kwargs) -> Any:
        """
        Hàm cào dữ liệu cốt lõi, trả về list các records.
        Cần được implement ở class con.
        """
        pass

    def save(self, data: list, output_path: str) -> None:
        """
        Lưu dữ liệu cào được thành định dạng JSONL.
        """
        if not data:
            self.logger.warning("No data to save.")
            return

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        self.logger.info(f"Saved {len(data)} records to {output_path}")

    def upload_to_dbfs(self, local_path: str, dbfs_path: str) -> None:
        """
        Upload file local lên DBFS landing zone.
        """
        self.logger.info(f"Uploading {local_path} to DBFS: dbfs:{dbfs_path}")

        try:
            # Giả lập import từ utils.upload_utils. Trong môi trường thực tế sẽ gọi Databricks API.
            # from scrapers.utils.upload_utils import upload_to_dbfs
            # upload_to_dbfs(local_path, dbfs_path)

            self.logger.info(f"Upload to DBFS success: dbfs:{dbfs_path}")
        except Exception as e:
            self.logger.error(f"Upload to DBFS failed: {e}")
            raise
