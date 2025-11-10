import asyncio
from playwright.async_api import async_playwright, Playwright
import time
from datetime import datetime, timedelta


async def run(playwright: Playwright):
    chromium = playwright.chromium
    browser = await chromium.launch(headless=False, slow_mo=100)
    page = await browser.new_page()

    await page.goto(
        r"https://rtba.enbridge.com/InformationalPosting/Default.aspx?bu=AG&Type=OA"
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

    await page.locator("a#lmapAlgonquin.link").click()

    async with page.expect_download() as download_info:
        await page.locator("a#LinkButton1.link").get_by_text("Download Csv").click()

    download = await download_info.value

    await download.save_as("./" + download.suggested_filename)

    time.sleep(6)

    await browser.close()


async def main():
    async with async_playwright() as playwright:
        await run(playwright)


if __name__ == "__main__":
    asyncio.run(main())
