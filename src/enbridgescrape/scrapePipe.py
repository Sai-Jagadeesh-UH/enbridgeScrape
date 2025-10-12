import asyncio
from playwright.async_api import async_playwright, Playwright
import time
from datetime import datetime, timedelta
from configs import pipesMap, pipeConfigs


async def scrape_OA(page):
    date_box = page.locator("#ctl00_MainContent_ctl01_oaDefault_ucDate_rdpDate_dateInput")
    if date_box:
        await date_box.fill("")
        for i in range(10):
            await page.keyboard.press("Backspace",delay=100)

        await date_box.fill((datetime.now() - timedelta(days=2)).strftime("%m/%d/%Y"))

        await page.keyboard.press('Enter')

    async with page.expect_download() as download_info:
        await page.locator("a#LinkButton1.link").get_by_text('Downloadable Format').click()


    download = await download_info.value

    await download.save_as("./downloads/OA/" + download.suggested_filename)

async def scrape_SC(page):
    async with page.expect_navigation():
        await page.locator('a#lbStorageLink.link').click()


    storage_cap = page.locator('td#td1Data.cellData')

    await storage_cap.highlight()
    value = await storage_cap.text_content()

    print(value)

async def run(playwright: Playwright, pipeCode: str, headed: bool = True):
    chromium = playwright.chromium 
    browser = await chromium.launch(headless=headed, slow_mo=100)
    page = await browser.new_page()

    await page.goto(rf"https://rtba.enbridge.com/InformationalPosting/Default.aspx?bu={pipeCode}&Type=OA")


    if ('OA' in pipeConfigs[pipeCode]):
        await  scrape_OA(page)


    if ('SC' in pipeConfigs[pipeCode]):
        await scrape_SC(page)


    time.sleep(2)

    await browser.close()

async def main(pipeCode:str):
    async with async_playwright() as playwright:

        # for j in pipeConfigs:
        print(f"scraping {pipeCode}-{pipeConfigs[pipeCode]}")
        try:
            await run(playwright, pipeCode, False)
        except Exception as e:
            print(f"scraping failed for {pipeCode} - {e}")


if __name__=='__main__':
    asyncio.run(main('MNUS'))