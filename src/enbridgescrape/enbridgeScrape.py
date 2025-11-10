import asyncio
from datetime import datetime, timedelta


from .utils import openPage, code2seg, error_detailed
from .utils import logger
from .OprAvail import scrape_OA
from .OprCap import scrape_OC
from .enbridgeLongScrape import enbridgeLongRun


async def enbridgeRun(pipecode: str, scrape_date=None, head_less: bool = True):
    try:
        if (pipecode in ['MNUS']):
            raise ValueError
        async with openPage(headLess=head_less) as page:
            await page.goto(
                f"https://rtba.enbridge.com/InformationalPosting/Default.aspx?bu={pipecode}&Type=OA"
            )

            date_box = (
                page.get_by_text("Gas Date: ")
                .locator("xpath=./following-sibling::div")
                .get_by_role("textbox")
                .nth(0)
            )

            target_date = scrape_date if scrape_date else (
                datetime.now() - timedelta(days=2))

            # print(f"{target_date=} - {scrape_date=}")

            await date_box.fill("")
            for i in range(10):
                await page.keyboard.press("Backspace", delay=100)

            await date_box.fill((target_date).strftime("%m/%d/%Y"))

            await page.keyboard.press("Enter")

            if 'OA' in code2seg[pipecode]:
                await scrape_OA(page=page)
                # print(f"->{pipecode} - OA {target_date}")

            op_cap = page.get_by_text("Operational Capacity Maps")

            if (op_cap):
                await scrape_OC(mainpage=page)

            # time.sleep(1)
            await asyncio.sleep(1)
    except Exception as e:
        logger.warning(
            f"""failed : enbridgeRun-{pipecode=} {scrape_date=} - trying long way {'*'*10}
            - {error_detailed(e)} """)
        try:
            await enbridgeLongRun(pipecode, head_less=head_less, scrape_date=scrape_date)
        except Exception as e:
            logger.error(
                f"""failed: enbridgeLongRun {pipecode=} {scrape_date=} {'!'*10}
                - {error_detailed(e)}""")
            logger.critical(
                f"""failed: enbridgeLongRun {pipecode=} {scrape_date=} {'!'*10}""")
