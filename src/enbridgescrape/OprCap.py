import time

async def scrape_OC(page, iframe=None):
    print("entered OC..........")

    if not iframe:
        await downlaod_OC(page)
    else:
        await downlaod_OC(iframe)

        # await traget_page.get_by_role("link", name="Operational Capacity Map").click()

        # list_items = traget_page.locator('[id^="#lmap"]')

        # cnt = await list_items.count()

        # print(cnt)

        # # Locate all the list items within that div
        # # list_items = parent_div_locator.locator("li")

        # # Iterate through each list item
        # for item_locator in await list_items.all():
        #     text_content = item_locator.text_content()
        #     print(f"List item text: {text_content}")

        # await traget_page.get_by_text('Operational Capacity Map').click()

        # async with page.expect_download() as download_info:
        #     await traget_page.locator("a#LinkButton1.link").get_by_text('Download Csv').click()

        # download = await download_info.value

        # await download.save_as("../../downloads/OC/" + download.suggested_filename.replace('_OA_','_OC_'))

    # else:
    #     traget_page = iframe

    #     await traget_page.locator("a#lmapMaritimes.link").click()

    #     async with page.expect_download() as download_info:
    #         await (
    #             traget_page.locator("a#LinkButton1.link")
    #             .get_by_text("Download Csv")
    #             .click()
    #         )

    #     download = await download_info.value

    #     await download.save_as(
    #         "../../downloads/OC/" + download.suggested_filename.replace("_OA_", "_OC_")
    #     )


async def downlaod_OC(page):

    div_items = (
            page.get_by_text("Operational Capacity Maps")
                .locator("xpath=./following-sibling::div")
                .get_by_role("link")
        )

    count = await div_items.count()

    print(count)

    # Loop through each child element
    for i in range(count):
        child_element = div_items.nth(i)

        async with page.expect_navigation():
            await child_element.click()

        async with page.expect_download() as download_info:
            await page.get_by_text("Download Csv").click()

        download = await download_info.value

        await download.save_as(
            f"./downloads/OC/F{i}_{download.suggested_filename}".replace("_OA_", "_OC_")
        )

        time.sleep(1)

        async with page.expect_navigation():
            await page.go_back()
