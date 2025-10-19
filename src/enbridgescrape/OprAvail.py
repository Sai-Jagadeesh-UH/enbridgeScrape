from datetime import datetime


async def scrape_OA(page, iframe=None, scrapeDate: datetime = datetime.now()):
    if not iframe:
        # date_box = page.locator(
        #     "#ctl00_MainContent_ctl01_oaDefault_ucDate_rdpDate_dateInput"
        # )

        date_box = (
            page.get_by_text("Gas Date: ")
            .locator("xpath=./following-sibling::div")
            .get_by_role("textbox")
            .nth(0)
        )

    else:
        # date_box = iframe.locator(
        #     "#ctl00_MainContent_ctl01_oaDefault_ucDate_rdpDate_dateInput"
        # )

        date_box = (
            iframe.get_by_text("Gas Date: ")
            .locator("xpath=./following-sibling::div")
            .get_by_role("textbox")
            .nth(0)
        )

    if date_box:
        await date_box.fill("")
        for i in range(10):
            await page.keyboard.press("Backspace", delay=100)

        await date_box.fill(scrapeDate.strftime("%m/%d/%Y"))

        await page.keyboard.press("Enter")

    if not iframe:
        async with page.expect_download() as download_info:
            await page.get_by_role("link", name="Downloadable Format").click()
    else:
        async with page.expect_download() as download_info:
            await iframe.get_by_role("link", name="Downloadable Format").click()

    download = await download_info.value

    await download.save_as("../../downloads/OA/" + download.suggested_filename)
