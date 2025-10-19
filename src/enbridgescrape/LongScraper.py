from datetime import datetime


from enbridgescrape.OprAvail import scrape_OA
from enbridgescrape.OprCap import scrape_OC
from enbridgescrape.StorCap import scrape_SC

from enbridgescrape.configs import pipeConfigs


async def scrape_long(browser, pipeCode: str, scrapeDate: datetime = datetime.now()):
    try:
        async with await browser.new_context() as context:
            page = await context.new_page()

            await page.goto(
                rf"https://infopost.enbridge.com/InfoPost/{pipeCode}Home.asp?Pipe={pipeCode}"
            )

            await page.locator("li#Capacity.dropdown.sidebar-menu-item").click()

            await page.get_by_role("link", name="Operationally Available").click()

            iframe_locator = page.frame_locator("#ContentPaneIframe")

            # if ('OA' in pipeConfigs[pipeCode]):
            await scrape_OA(page=page, iframe=iframe_locator, scrapeDate=scrapeDate)

            if "SC" in pipeConfigs[pipeCode]:
                await scrape_SC(page=page, iframe=iframe_locator)
                # print("going back ..........................")
                await page.go_back()

            if "OC" in pipeConfigs[pipeCode]:
                await scrape_OC(page=page, iframe=iframe_locator)

    except Exception as e:
        print(f"scraping failed for {pipeCode} {e}")
