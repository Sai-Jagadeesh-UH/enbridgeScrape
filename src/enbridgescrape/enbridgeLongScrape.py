import asyncio
from datetime import datetime, timedelta


from .utils import openPage, code2seg
# from .utils import logger
from .OprAvail import scrape_OA
from .OprCap import scrape_OC


async def enbridgeLongRun(pipeCode: str, scrape_date: datetime, head_less: bool = True, Only_OA: bool = False):
    async with openPage(headLess=head_less) as page:
        await page.goto(
            f"https://infopost.enbridge.com/InfoPost/{pipeCode}Home.asp?Pipe={pipeCode}"
        )

        await page.locator("li#Capacity.dropdown.sidebar-menu-item").click()

        await page.get_by_role("link", name="Operationally Available").click()

        iframe_locator = page.frame_locator("#ContentPaneIframe")

        date_box = (
            iframe_locator.get_by_text("Gas Date: ")
            .locator("xpath=./following-sibling::div")
            .get_by_role("textbox")
            .nth(0)
        )

        target_date = scrape_date if scrape_date else (
            datetime.now() - timedelta(days=2))

        await date_box.fill("")
        for i in range(10):
            await page.keyboard.press("Backspace", delay=100)

        await date_box.fill((target_date).strftime("%m/%d/%Y"))

        await page.keyboard.press("Enter")

        if 'OA' in code2seg[pipeCode]:
            await scrape_OA(page=page, iframe=iframe_locator)

        if not Only_OA:
            op_cap = iframe_locator.get_by_text("Operational Capacity Maps")
            if op_cap:
                await scrape_OC(pipecode=pipeCode, mainpage=page, iframe=iframe_locator, scrape_date=scrape_date)
            await asyncio.sleep(1)
