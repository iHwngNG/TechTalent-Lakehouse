import sys
import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from src.utils.getProjectRoot import getRootPath

# Đảm bảo Python có thể tìm thấy thư mục 'scrapers' ở thư mục gốc
load_dotenv(find_dotenv())
PROJECT_ROOT = getRootPath()

from scrapers.itviec_scraper import main

if __name__ == "__main__":
    # Nếu chạy trên Databricks Job không có tham số dòng lệnh, mặc định quét tất cả các trang
    if len(sys.argv) == 1:
        sys.argv.extend(["--pages", "0"])

    # Xử lý policy cho Windows (nếu test ở local)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    # Kích hoạt Scraper
    print("🚀 Bắt đầu Job Scrape ITviec...")

    # Xử lý lỗi "asyncio.run() cannot be called from a running event loop" trên Databricks/Jupyter
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # Nếu đang ở trong một Event Loop (vd: Notebook), dùng nest_asyncio để patch
        import nest_asyncio

        nest_asyncio.apply()
        loop.run_until_complete(main())
    else:
        # Nếu chạy script bình thường trên terminal
        asyncio.run(main())
