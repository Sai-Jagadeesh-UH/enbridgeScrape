import asyncio
from playwright.async_api import async_playwright, Playwright
import time
from datetime import datetime, timedelta
from configs import pipeConfigs


async def run(playwright: Playwright, pipeCode: str):
    chromium = playwright.chromium
    browser = await chromium.launch(headless=False, slow_mo=100)
    page = await browser.new_page()

    await page.goto(
        rf"https://rtba.enbridge.com/InformationalPosting/Default.aspx?bu={pipeCode}&Type=OA"
    )

    date_box = page.locator(
        "#ctl00_MainContent_ctl01_oaDefault_ucDate_rdpDate_dateInput"
    )
    if date_box:
        await date_box.fill("")
        for i in range(10):
            await page.keyboard.press("Backspace", delay=100)

        await date_box.fill((datetime.now() - timedelta(days=2)).strftime("%m/%d/%Y"))

        await page.keyboard.press("Enter")

    async with page.expect_navigation():
        await page.locator("a#lbStorageLink.link").click()

    storage_cap = page.locator("td#td1Data.cellData")

    await storage_cap.highlight()
    value = await storage_cap.text_content()

    print(value)

    time.sleep(2)

    await browser.close()


async def main():
    SC_list = [i for i in pipeConfigs if "SC" in pipeConfigs[i]]
    for j in SC_list:
        print(f"scraping {j}")
        async with async_playwright() as playwright:
            await run(playwright, j)


if __name__ == "__main__":
    asyncio.run(main())
