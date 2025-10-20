import asyncio
from playwright.async_api import async_playwright, Playwright
import time
from datetime import datetime, timedelta


async def run(playwright: Playwright, pipecode :str):
    chromium = playwright.chromium
    browser = await chromium.launch(headless=False, slow_mo=100)
    page = await browser.new_page()

    await page.goto(
        rf"https://rtba.enbridge.com/InformationalPosting/Default.aspx?bu={pipecode}&Type=OA"
    )

    date_box = (
        page.get_by_text("Gas Date: ")
        .locator("xpath=./following-sibling::div")
        .get_by_role("textbox")
        .nth(0)
    )

    # await date_box.fill("")

    # date_box = page.locator(
    #     "#ctl00_MainContent_ctl01_oaDefault_ucDate_rdpDate_dateInput"
    # )

    if date_box:
        await date_box.fill("")
        for i in range(10):
            await page.keyboard.press("Backspace", delay=100)

        await date_box.fill((datetime.now() - timedelta(days=2)).strftime("%m/%d/%Y"))

        await page.keyboard.press("Enter")

    strg_cap = page.get_by_role('link', name="Storage Capacity Posting")

    op_cap = page.get_by_text("Operational Capacity Maps")

    if(strg_cap):
        await strg_cap.highlight()
        async with page.expect_navigation():
            await strg_cap.click()

        datePicked = await page.locator("div#divDate.header").text_content()
        # await datePicked.highlight()


        print( datePicked[6:])

        tableRows= page.locator("table#tblStorage.header").locator("tbody").locator('tr')
        
        rowCount = await tableRows.count()

        print(rowCount)

        for i in range(0,rowCount):
            childElement = tableRows.nth(i)
            await childElement.highlight()
            time.sleep(1)
            if(i%2):
                # textPrint= await childElement.locator("td.cellHeader").text_content()
                textPrint= await childElement.locator("td").text_content()

            else:
                textPrint= await childElement.locator("td.cellHeader").text_content()


            print(textPrint)
                
        time.sleep(2)


    if(op_cap):
        await op_cap.highlight()
        time.sleep(1)

    # div_items = (
    #     page.get_by_text("Operational Capacity Maps")
    #     .locator("xpath=./following-sibling::div")
    #     .get_by_role("link")
    # )

    # count = await div_items.count()

    # print(count)

    # Loop through each child element
    # for i in range(count):
    #     child_element = div_items.nth(i)

    #     async with page.expect_navigation():
    #         await child_element.click()

    #     await page.get_by_text("Download Csv").highlight()
    #     time.sleep(2)

    #     async with page.expect_navigation():
    #         await page.go_back()

    # async with page.expect_download() as download_info:
    #     await (
    #         page.locator("a#LinkButton1.link")
    #         .get_by_text("Downloadable Format")
    #         .click()
    #     )

    # download = await download_info.value

    # await download.save_as("./" + download.suggested_filename)

    time.sleep(5)

    await browser.close()


async def main():
    async with async_playwright() as playwright:
        # for i in ["AG","BGS", "TE"]:
        for i in [ "TE"]:
            await run(playwright,i)


if __name__ == "__main__":

    asyncio.run(main())
