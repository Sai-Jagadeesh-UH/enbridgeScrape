import asyncio
from playwright.async_api import async_playwright, Playwright
import time
from datetime import datetime, timedelta

async def run(playwright: Playwright):
    chromium = playwright.chromium 

    browser = await chromium.launch(headless=True)
    # context = await browser.new_context(accept_downloads=True) 
    page = await browser.new_page()


    await page.goto(r"https://infopost.enbridge.com/InfoPost/AGHome.asp?Pipe=AG")

    await page.locator('li#Capacity.dropdown.sidebar-menu-item').click()

    await page.get_by_role("link", name="Operationally Available").click()

    iframe_locator = page.frame_locator('#ContentPaneIframe')
    

    iframe_frame = iframe_locator.locator("#ctl00_MainContent_ctl01_oaDefault_ucDate_rdpDate_dateInput")
    if iframe_frame:
        await iframe_frame.fill("")
        for i in range(10):
            await page.keyboard.press("Backspace",delay=100)

        await iframe_frame.fill((datetime.now() - timedelta(days=2)).strftime("%m/%d/%Y"))

        await page.keyboard.press('Enter')

    async with page.expect_download() as download_info:
        await iframe_locator.locator("a#LinkButton1.link").get_by_text('Downloadable Format').click()




    # download = await page.wait_for_event("download")
    download = await download_info.value

    # print(download.page)
    # print(download.url)



    await download.save_as("./" + download.suggested_filename)
    

    # await context.close()
    await browser.close()

async def main():
    async with async_playwright() as playwright:
        await run(playwright)


if __name__=='__main__':
    asyncio.run(main())