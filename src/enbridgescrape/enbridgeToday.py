import time
from datetime import datetime, timedelta

from src.enbridgescrape import metaDump
from src.enbridgescrape import runEnbridgeScrape
from src.enbridgescrape import runNN_Scrape
from .utils import logger


async def scrapeToday():
    start_time = time.perf_counter()
    #  Yesterday
    target_date = datetime.today() - timedelta(days=1)
    logger.info(f"scrapeToday - {target_date=}")

    await runEnbridgeScrape(target_date)

    #  today
    target_date = datetime.today()
    logger.info(f"scrapeToday - {target_date=}")

    await runEnbridgeScrape(target_date)

    #  today (latest NN)
    logger.info(f"scrapeToday - runNN_Scrape {target_date=}")
    await runNN_Scrape(target_date)

    #  today (latest MetaData)
    logger.info(f"scrapeToday - metaDump {target_date=}")
    await metaDump()

    logger.info(
        f"{'*'*15} completed in {time.perf_counter()-start_time: .2f}s {'*'*15}")
