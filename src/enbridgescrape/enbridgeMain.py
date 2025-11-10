import time

from .enbridgeScrape import enbridgeRun
from .utils import code2seg
from .enbridgeLongScrape import enbridgeLongRun


async def runEnbridgeScrape(scrape_date=None, head_less: bool = True):

    for pipecode in code2seg.keys():
        # for pipecode in ['TE', 'MNUS', 'ET']:
        start_time = time.perf_counter()
        try:
            try:
                # raise ValueError
                await enbridgeRun(pipecode, head_less=head_less, scrape_date=scrape_date)
            except Exception:
                print(f"failed : {pipecode} - trying long way {'*'*10}")
                await enbridgeLongRun(pipecode, head_less=head_less, scrape_date=scrape_date)
        except Exception as e:
            print(f"failed: {pipecode} - {str(e)}")
        finally:
            print(
                f"{' '*8}{pipecode} - {time.perf_counter()-start_time: .2f}s {'-'*15} ")
