import time
from datetime import datetime


from playwright.async_api import Playwright


from enbridgescrape.configs import pipeConfigs
from enbridgescrape.OprAvail import scrape_OA
from enbridgescrape.OprCap import scrape_OC
from enbridgescrape.StorCap import scrape_SC
from enbridgescrape.LongScraper import scrape_long


async def run(
    playwright: Playwright,
    pipeCode: str,
    headLess: bool = True,
    scrapeDate: datetime = datetime.now(),
):
    chromium = playwright.chromium
    browser = await chromium.launch(headless=headLess, slow_mo=100)

    try:
        async with await browser.new_context() as context:
            page = await context.new_page()

            start_time = time.perf_counter()

            await page.goto(
                rf"https://rtba.enbridge.com/InformationalPosting/Default.aspx?bu={pipeCode}&Type=OA"
            )

            await scrape_OA(page, scrapeDate=scrapeDate)

            if "SC" in pipeConfigs[pipeCode]:
                await scrape_SC(page)
                # print('going back ...........................')
                await page.go_back()

            if "OC" in pipeConfigs[pipeCode]:
                await scrape_OC(page)

    except Exception as e:
        print(f"failed with {e}")
        print(f"trying long way for {pipeCode}")
        start_time = time.perf_counter()

        try:
            await scrape_long(browser=browser, pipeCode=pipeCode, scrapeDate=scrapeDate)

        except Exception as e:
            print(f"scraping failed for {pipeCode} {e}")

    finally:
        # time.sleep(2)
        print(f"scraped {pipeCode} in {time.perf_counter() - start_time: .2f}s")
        await browser.close()
