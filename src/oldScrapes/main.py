import asyncio
from playwright.async_api import async_playwright, Playwright
from datetime import datetime, timedelta


async def run(playwright: Playwright, pipeCode: str):
    chromium = playwright.chromium

    browser = await chromium.launch(headless=False)
    # context = await browser.new_context(accept_downloads=True)
    page = await browser.new_page()

    await page.goto(
        rf"https://infopost.enbridge.com/InfoPost/{pipeCode}Home.asp?Pipe={pipeCode}"
    )

    await page.locator("li#Capacity.dropdown.sidebar-menu-item").click()

    await page.get_by_role("link", name="Operationally Available").click()

    iframe_locator = page.frame_locator("#ContentPaneIframe")

    iframe_frame = iframe_locator.locator(
        "#ctl00_MainContent_ctl01_oaDefault_ucDate_rdpDate_dateInput"
    )
    if iframe_frame:
        await iframe_frame.fill("")
        for i in range(10):
            await page.keyboard.press("Backspace", delay=100)

        await iframe_frame.fill(
            (datetime.now() - timedelta(days=2)).strftime("%m/%d/%Y")
        )

        await page.keyboard.press("Enter")

    await iframe_locator.locator("a#lmapMaritimes.link").click()

    async with page.expect_download() as download_info:
        await (
            iframe_locator.locator("a#LinkButton1.link")
            .get_by_text("Download Csv")
            .click()
        )

    download = await download_info.value

    # time.sleep(3)
    await download.save_as("./downloads/OC/" + download.suggested_filename)

    # await context.close()
    await browser.close()


async def main():
    async with async_playwright() as playwright:
        await run(playwright, "MNUS")


if __name__ == "__main__":
    asyncio.run(main())
