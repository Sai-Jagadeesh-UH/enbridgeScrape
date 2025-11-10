import time
import asyncio
from .enbridgeScrape import enbridgeRun
from .utils import code2seg
from .enbridgeLongScrape import enbridgeLongRun
from .utils import logger


async def runEnbridgeScrape(scrape_date=None, head_less: bool = True):
    start_time = time.perf_counter()
    async with asyncio.TaskGroup() as group:
        for pipecode in code2seg.keys():
            # for pipecode in ['TE', 'MNUS', 'ET']:

            # try:
            # try:
            # raise ValueError

            group.create_task(enbridgeRun(
                pipecode, head_less=head_less, scrape_date=scrape_date))
            # except Exception:
            #     print(f"failed : {pipecode} - trying long way {'*'*10}")
            #     await enbridgeLongRun(pipecode, head_less=head_less, scrape_date=scrape_date)
            # except Exception as e:
            #     print(f"failed: {pipecode} - {str(e)}")
            # finally:
    # print(f"{' '*8}{pipecode} - {time.perf_counter()-start_time: .2f}s {'-'*15} ")
    logger.info(f"{' '*8} {time.perf_counter()-start_time: .2f}s {'-'*15} ")
