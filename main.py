import asyncio
import time
from datetime import datetime, timedelta

from enbridgescrape import metaDump
from src.enbridgescrape import runEnbridgeScrape
from src.enbridgescrape import scrapeHistoric, scrapeToday
# from src.enbridgescrape import runNN_Scrape


# from playwright.async_api import async_playwright

# from src.enbridgescrape import run
# from src.enbridgescrape.configs import pipeConfigs


if __name__ == "__main__":
    start_time = time.perf_counter()
    # asyncio.run(runIterScrape())
    # asyncio.run(runEnbridgeScrape(True))
    # asyncio.run(runNN_Scrape(scrape_date=datetime.today() - timedelta(days=6)))

    # asyncio.run(scrapeToday())
    asyncio.run(scrapeHistoric())
    metaDump()
    print(f"{'*'*15} completed in {time.perf_counter()-start_time: .2f}s {'*'*15}")
