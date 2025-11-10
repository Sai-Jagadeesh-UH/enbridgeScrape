from .utils import paths

oa_downloads_path = paths.downloads / 'OA'
oa_downloads_path.mkdir(exist_ok=True, parents=True)


async def scrape_OA(page, iframe=None):

    if not iframe:
        async with page.expect_download() as download_info:
            await page.get_by_role("link", name="Downloadable Format").click()
    else:
        async with page.expect_download() as download_info:
            await iframe.get_by_role("link", name="Downloadable Format").click()

    download = await download_info.value

    await download.save_as(oa_downloads_path / download.suggested_filename)
