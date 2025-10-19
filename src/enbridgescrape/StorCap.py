async def scrape_SC(page, iframe=None):
    if not iframe:
        traget_page = page
    else:
        traget_page = iframe

    async with traget_page.expect_navigation():
        await traget_page.locator("a#lbStorageLink.link").click()

    storage_cap = traget_page.locator("td#td1Data.cellData")

    await storage_cap.highlight()
    value = await storage_cap.text_content()

    print(value)
