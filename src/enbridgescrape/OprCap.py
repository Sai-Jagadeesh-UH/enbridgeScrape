import time

from datetime import datetime, timedelta

from .utils import paths, error_detailed


ocap_downloads_path = paths.downloads / 'OC'
ocap_downloads_path.mkdir(exist_ok=True, parents=True)


async def refreshDump(mainpage, getByText, scrape_date=None):
    try:
        await mainpage.reload()

        await mainpage.locator("li#Capacity.dropdown.sidebar-menu-item").click()

        await mainpage.get_by_role("link", name="Operationally Available").click()

        iframe_locator = mainpage.frame_locator("#ContentPaneIframe")

        date_box = (
            iframe_locator.get_by_text("Gas Date: ")
            .locator("xpath=./following-sibling::div")
            .get_by_role("textbox")
            .nth(0)
        )

        target_date = scrape_date if scrape_date else (
            datetime.now() - timedelta(days=2))

        # print(f"{target_date=} - {scrape_date=}")

        await date_box.fill("")
        for i in range(10):
            await mainpage.keyboard.press("Backspace", delay=100)

        await date_box.fill((target_date).strftime("%m/%d/%Y"))

        await mainpage.keyboard.press("Enter")

        child_element = iframe_locator.get_by_role(
            "link").get_by_text(getByText)

        await child_element.highlight()

        await child_element.click()

        async with mainpage.expect_download() as download_info:
            await iframe_locator.get_by_text("Download Csv").click()

        download = await download_info.value

        await download.save_as(ocap_downloads_path / download.suggested_filename.replace('_OA_', f'_OC-{getByText}_'))
        # print(f"->OC-{getByText}")
        time.sleep(1)
    except Exception as e:
        print(f"Failed ->OC-{getByText}")
        # print(error_detailed(e))


async def scrape_OC(mainpage, iframe=None, scrape_date=None):

    try:

        div_items = (
            mainpage.get_by_text("Operational Capacity Maps")
            .locator("xpath=./following-sibling::div")
            .get_by_role("link")
        )

        textList = await div_items.all_text_contents()
        # print(textList)

        if iframe is None:
            for count, i in enumerate(textList, start=1):
                child_element = mainpage.get_by_role("link").get_by_text(i)

                await child_element.highlight()
                time.sleep(1)

                async with mainpage.expect_navigation():
                    await child_element.click()

                async with mainpage.expect_download() as download_info:
                    await mainpage.get_by_text("Download Csv").click()

                download = await download_info.value

                await download.save_as(ocap_downloads_path / download.suggested_filename.replace('_OA_', f'_OC{count}_'))
                print(f"->OC{i}", end="\t")
                time.sleep(1)

                async with mainpage.expect_navigation():
                    await mainpage.go_back()
        else:
            for eleText in textList:
                await refreshDump(mainpage=mainpage, getByText=eleText, scrape_date=scrape_date)
                time.sleep(1)

    except Exception as e:
        print(error_detailed(e))
