import time
import asyncio

from datetime import datetime

from ..Scraper.enbridgeScrape import enbridgeRun
from ..utils import code2seg
from ..utils import logger


async def runFailedScrapes(pipecode: str, scrape_date: datetime, head_less: bool = True):
    start_time = time.perf_counter()
    async with asyncio.TaskGroup() as group:
        for pipecode in code2seg.keys():
            group.create_task(enbridgeRun(
                pipecode, head_less=head_less, scrape_date=scrape_date))
    logger.info(f"{' '*8} {time.perf_counter()-start_time: .2f}s {'-'*15} ")
