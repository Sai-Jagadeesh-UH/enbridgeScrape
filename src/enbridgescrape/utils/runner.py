from contextlib import asynccontextmanager

from playwright.async_api import async_playwright


@asynccontextmanager
async def openPage(headLess: bool = True, slow_mo: int = 100):
    async with async_playwright() as playwright:
        chromium = playwright.chromium
        browser = await chromium.launch(headless=headLess, slow_mo=100)
        try:
            page = await browser.new_page()
            yield page
        finally:
            await browser.close()
