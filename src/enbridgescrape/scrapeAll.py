import asyncio
from playwright.async_api import async_playwright, Playwright
import time
from datetime import datetime, timedelta
from configs import pipesMap, pipeConfigs
import time


async def scrape_OA(page, iframe = None):
    if not iframe:
        date_box = page.locator("#ctl00_MainContent_ctl01_oaDefault_ucDate_rdpDate_dateInput")
    else:
        date_box = iframe.locator("#ctl00_MainContent_ctl01_oaDefault_ucDate_rdpDate_dateInput")

    if date_box:
        await date_box.fill("")
        for i in range(10):
            await page.keyboard.press("Backspace",delay=100)

        await date_box.fill((datetime.now() - timedelta(days=2)).strftime("%m/%d/%Y"))

        await page.keyboard.press('Enter')

    if not iframe:
        async with page.expect_download() as download_info:
            await page.locator("a#LinkButton1.link").get_by_text('Downloadable Format').click()
    else:
        async with page.expect_download() as download_info:
            await iframe.locator("a#LinkButton1.link").get_by_text('Downloadable Format').click()

    download = await download_info.value

    await download.save_as("./downloads/OA/" + download.suggested_filename)




async def scrape_OC(page, iframe = None):
    
    if not iframe:
        traget_page = page


        await traget_page.get_by_role("link", name="Operational Capacity Map").click()

        # await traget_page.get_by_text('Operational Capacity Map').click()


        async with page.expect_download() as download_info:
            await traget_page.locator("a#LinkButton1.link").get_by_text('Download Csv').click()

        download = await download_info.value

        await download.save_as("./downloads/OC/" + download.suggested_filename.replace('_OA_','_OC_'))


    else:
        traget_page = iframe

        await traget_page.locator('a#lmapMaritimes.link').click()


        async with page.expect_download() as download_info:
            await traget_page.locator("a#LinkButton1.link").get_by_text('Download Csv').click()

        download = await download_info.value

        await download.save_as("./downloads/OC/" + download.suggested_filename.replace('_OA_','_OC_'))




async def scrape_SC(page, iframe = None):

    if not iframe:
        traget_page = page
    else:
        traget_page = iframe


    async with traget_page.expect_navigation():
        await traget_page.locator('a#lbStorageLink.link').click()


    storage_cap = traget_page.locator('td#td1Data.cellData')

    await storage_cap.highlight()
    value = await storage_cap.text_content()

    print(value)




async def scrape_long(browser,pipeCode:str):

        try:

            async with await browser.new_context() as context:

                page = await context.new_page()

                await page.goto(rf"https://infopost.enbridge.com/InfoPost/{pipeCode}Home.asp?Pipe={pipeCode}")

                await page.locator('li#Capacity.dropdown.sidebar-menu-item').click()

                await page.get_by_role("link", name="Operationally Available").click()

                iframe_locator = page.frame_locator('#ContentPaneIframe')
                
                # if ('OA' in pipeConfigs[pipeCode]):
                await  scrape_OA(page=page, iframe=iframe_locator)

                if ('SC' in pipeConfigs[pipeCode]):
                    await scrape_SC(page=page, iframe=iframe_locator)
                    # print("going back ..........................")
                    await page.go_back()

                

                if ('OC' in pipeConfigs[pipeCode]):
                    await  scrape_OC(page=page, iframe=iframe_locator)




        except Exception as e:
            print(f"scraping failed for {pipeCode} {e}")

async def run(playwright: Playwright, pipeCode: str, headed: bool = True):

    
    chromium = playwright.chromium 
    browser = await chromium.launch(headless=headed, slow_mo=100)

    try:
        # raise ValueError
        async with await browser.new_context() as context:

            page = await context.new_page()

            start_time = time.perf_counter() 

            await page.goto(rf"https://rtba.enbridge.com/InformationalPosting/Default.aspx?bu={pipeCode}&Type=OA")

            await  scrape_OA(page)


            if ('SC' in pipeConfigs[pipeCode]):
                await scrape_SC(page)
                # print('going back ...........................')
                await page.go_back()


            if ('OC' in pipeConfigs[pipeCode]):
                await  scrape_OC(page)



    except Exception as e:
        # print(f"failed with {e}")
        print(f"trying long way for {pipeCode}")
        start_time = time.perf_counter() 

        try:
            
            await scrape_long(browser=browser, pipeCode=pipeCode)

        except Exception as e:
            print(f"scraping failed for {pipeCode} {e}")

    finally:
        # time.sleep(2)
        print(f"scraped {pipeCode} in {time.perf_counter() - start_time : .2f}s")
        await browser.close()

async def main():

    start_all_time = time.perf_counter() 
    async with async_playwright() as playwright:

        for j in pipeConfigs:
        # for j in ['ET']:
            try:
                print(f"scraping {j}-{pipeConfigs[j]}")
                await run(playwright, j)
            except Exception as e:
                continue
    
    print("-"*50)
    print(f"Scrape completed in {time.perf_counter()-start_all_time : .2f}s")



if __name__=='__main__':
    asyncio.run(main())