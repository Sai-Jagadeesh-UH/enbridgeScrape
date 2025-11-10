import time
from datetime import datetime, timedelta

from src.enbridgescrape import runEnbridgeScrape
from src.enbridgescrape import runNN_Scrape


async def scrapeHistoric(startDate: datetime = datetime.today() - timedelta(days=365*3 + 1)):

    target_date = startDate

    while target_date <= datetime.today():
        start_time = time.perf_counter()
        print(f"{target_date=}")
        await runEnbridgeScrape(target_date)
        await runNN_Scrape(target_date)

        print(f"{'*'*15} completed in {time.perf_counter()-start_time: .2f}s {'*'*15}")

    print(f"{'#'*10}{'*'*20}{'-'*10}{'*'*20}{'#'*10}")
