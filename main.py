# import os
import asyncio
import time
# from pathlib import Path
from src.artifacts import baseLogger
# from datetime import datetime, timedelta
# from src.artifacts import dirs
from src.enbridgescrape import scrapeToday
# from src.enbridgescrape import scrapeHistoric
# from src.enbridgescrape.cloudPush.handleSilver import cleanseOC
# from src.enbridgescrape.cloudPush import pushSilverOA
# from src.enbridgescrape.cloudPush import pushRawOA, pushRawOC, pushRawNN, pushRawMeta, pushRawLogs
# from src.enbridgescrape import  metaDump, formatOA, scrapeHistoric


# , getTableData


# from src.enbridgescrape import runEnbridgeScrape
# from src.enbridgescrape import scrapeHistoric, scrapeToday
# from src.enbridgescrape import runNN_Scrape


# from playwright.async_api import async_playwright

# from src.enbridgescrape import run
# from src.enbridgescrape.configs import pipeConfigs


if __name__ == "__main__":

    start_time = time.perf_counter()
    # asyncio.run(runIterScrape())
    # asyncio.run(runEnbridgeScrape(True))
    # asyncio.run(runNN_Scrape(scrape_date=datetime.today() - timedelta(days=6)))
    # print(f"{os.getenv("DELTA_STORAGE_KEY")}")
    asyncio.run(scrapeToday())
    # asyncio.run(scrapeHistoric())
    # asyncio.run(metaDump())

    # start__time = time.perf_counter()
    # asyncio.run(pushRawOA())
    # baseLogger.info(
    #     f"{'*'*15} OA completed in {time.perf_counter()-start__time: .2f}s {'*'*15}")

    # start__time = time.perf_counter()
    # asyncio.run(pushRawOC())
    # baseLogger.info(
    #     f"{'*'*15} OC completed in {time.perf_counter()-start__time: .2f}s {'*'*15}")

    # start__time = time.perf_counter()
    # asyncio.run(pushRawNN())
    # baseLogger.info(
    #     f"{'*'*15} NN completed in {time.perf_counter()-start__time: .2f}s {'*'*15}")

    # start__time = time.perf_counter()
    # asyncio.run(pushRawMeta())
    # baseLogger.info(
    #     f"{'*'*15} Meta completed in {time.perf_counter()-start__time: .2f}s {'*'*15}")

    # start__time = time.perf_counter()
    # asyncio.run(pushRawLogs())
    # baseLogger.info(
    #     f"{'*'*15} Logs completed in {time.perf_counter()-start__time: .2f}s {'*'*15}")

    # getTableData(tableName='Chekcing')
    # formatOA()

    # asyncio.run(
    #     cleanseOC(Path('AG_OC1_20251201_INTRDY_2025-12-02_0900.parquet')))

    print(f"{'*'*15} All completed in {time.perf_counter()-start_time: .2f}s {'*'*15}")
    baseLogger.info(
        f"{'*'*15} All completed in {time.perf_counter()-start_time: .2f}s {'*'*15}")
