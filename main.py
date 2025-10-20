import asyncio
import time
from datetime import datetime, timedelta


from playwright.async_api import async_playwright


from src.enbridgescrape import run
from src.enbridgescrape.configs import pipeConfigs


async def main():
    start_all_time = time.perf_counter()
    async with async_playwright() as playwright:
        for pipe in pipeConfigs:
        # for pipe in ["AG","BGS", "ET", "TE"]:
        # for pipe in ["TE"]:
            try:
                print(f"scraping {pipe}-{pipeConfigs[pipe]}")
                await run(
                    playwright,
                    pipeCode=pipe,
                    headLess=False,
                    scrapeDate=datetime.now() - timedelta(days=1),
                )
            except Exception:
                continue

    print("-" * 50)
    print(f"Scrape completed in {time.perf_counter() - start_all_time: .2f}s")


if __name__ == "__main__":
    asyncio.run(main())
