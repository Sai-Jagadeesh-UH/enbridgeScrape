import asyncio

from .handleRaw import pushRawOA, pushRawOC, pushRawNN, pushRawMeta, pushRawLogs
from .handleSilver import pushSilverOA, pushSilverOC


async def pushEnbridge():
    await pushRawMeta()

    async with asyncio.TaskGroup() as group:
        group.create_task(pushOA())
        group.create_task(pushOC())
        group.create_task(pushNN())

        # pushRaw
        # push Silver
        # push Gold
        # delete the raw silver and gold for next run

    await pushRawLogs()


async def pushOA():
    await pushRawOA()
    await pushSilverOA()


async def pushOC():
    await pushRawOC()
    await pushSilverOC()


async def pushNN():
    await pushRawNN()
    # await pushSilverNN()
