import sys
import os
import asyncio
from dotenv import load_dotenv, find_dotenv

# Đảm bảo Python có thể tìm thấy thư mục 'scrapers' ở thư mục gốc
load_dotenv(find_dotenv())
PROJECT_ROOT = os.environ.get("PROJECT_ROOT") or str(
    Path(__file__).resolve().parent.parent
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scrapers.topdev_scraper import main

if __name__ == "__main__":
    # Nếu chạy trên Databricks Job không có tham số dòng lệnh, mặc định quét tất cả các trang
    if len(sys.argv) == 1:
        sys.argv.extend(["--pages", "0"])

    # Xử lý policy cho Windows (nếu test ở local)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    # Kích hoạt Scraper
    print("🚀 Bắt đầu Job Scrape TopDev...")
    asyncio.run(main())
