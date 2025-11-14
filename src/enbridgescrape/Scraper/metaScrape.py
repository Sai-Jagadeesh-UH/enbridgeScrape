import asyncio
import aiohttp
import aiofiles
import ssl
import time

from ..utils import metacodes, paths
from ..utils import logger

meta_download_path = paths.downloads / "MetaData"
meta_download_path.mkdir(exist_ok=True, parents=True)


async def runDump(file_url, local_filename):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
            async with session.get(url=file_url, raise_for_status=True,) as response:
                async with aiofiles.open(local_filename, mode='wb') as f:
                    # 1MB chunks
                    async for chunk in response.content.iter_chunked(8192):
                        await f.write(chunk)
    except aiohttp.ClientError as e:
        logger.error(f"Error downloading {file_url}: {e}")


async def metaDump():
    start_time = time.perf_counter()
    async with asyncio.TaskGroup() as group:
        for pipe_code in metacodes:
            file_url = f"https://linkwc.enbridge.com/Pointdata/{pipe_code}AllPoints.csv"
            local_filename = meta_download_path / f"{pipe_code}AllPoints.csv"
            group.create_task(runDump(file_url, local_filename))

        # try:
        #     response = requests.get(file_url, stream=True)
        #     response.raise_for_status()  # Check for HTTP errors

        #     with open(local_filename, 'wb') as local_file:
        #         for chunk in response.iter_content(chunk_size=8192):
        #             if chunk:
        #                 local_file.write(chunk)
        #     # print(f"File '{local_filename}' downloaded successfully.")

        # except requests.exceptions.RequestException as e:
        #     print(f"failed: metaDump {pipe_code} -  {e}")

    logger.info(
        f"metaDump completed in {time.perf_counter()-start_time: .2f}s  {'-'*10}")
