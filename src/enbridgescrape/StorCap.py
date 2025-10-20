import time

async def scrape_SC(mainpage, iframe=None):

    if iframe is None:
        frame = page = mainpage
    else:
        frame = iframe
        page = mainpage

    if iframe is None:
        async with page.expect_navigation():
            await frame.get_by_role('link', name="Storage Capacity Posting").click()
    else:
        await frame.get_by_role('link', name="Storage Capacity Posting").click()


    # storage_cap = frame.locator("td#td1Data.cellData")

    # await storage_cap.highlight()
    # value = await storage_cap.text_content()

    datePicked = await frame.locator("div#divDate.header").text_content()
    # await datePicked.highlight()


    print( "Date of Storage " +datePicked[6:] + "-------")

    tableRows= frame.locator("table#tblStorage.header").locator("tbody").locator('tr')
    
    rowCount = await tableRows.count()

    print(rowCount)

    for i in range(0,rowCount):
        childElement = tableRows.nth(i)
        await childElement.highlight()
        time.sleep(1)
        if(i%2):
            # textPrint= await childElement.locator("td.cellHeader").text_content()
            textPrint= await childElement.locator("td").text_content()

        else:
            textPrint= await childElement.locator("td.cellHeader").text_content()


        print(textPrint)
            
    time.sleep(2)



    if iframe is None:
        # print("in page")
        async with page.expect_navigation():
            await page.go_back()
    else:

        await page.keyboard.press("Alt+ArrowLeft")

        # print("in frame")
        # try:
        #     await page.go_back(timeout=3000)
        # except Exception as e:
        #     print("something failed ")

    print("SC done .......")

