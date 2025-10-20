import time

async def scrape_OC(mainpage, iframe=None):
    print(f"entered OC..........")

    if iframe is None:
        frame = page = mainpage
    else:
        frame = iframe
        page = mainpage

    div_items = (
            frame.get_by_text("Operational Capacity Maps")
                .locator("xpath=./following-sibling::div")
                .get_by_role("link")
        )

    count = await div_items.count()

    print(count)

    # Loop through each child element
    for i in range(count):
        print(i)

        child_element = div_items.nth(i)

        if iframe is None:
            # print("in page")
            async with page.expect_navigation():
                await child_element.click()     
        else:
            # print("in frame")
            await child_element.click()

        async with page.expect_download() as download_info:
            await frame.get_by_text("Download Csv").click()


        download = await download_info.value

        await download.save_as(
            f"./downloads/OC/F{i}_{download.suggested_filename}".replace("_OA_", "_OC_")
        )

        time.sleep(1)

        if iframe is None:
            # print("in page")
            async with page.expect_navigation():
                await page.go_back()
        else:
            # print("in frame")
            # await page.keyboard.press("Alt+ArrowLeft")

            try:
                await page.go_back(timeout=3000)
            except Exception as e:
                print("something failed " + str(e))
        


    print("OC success............")

