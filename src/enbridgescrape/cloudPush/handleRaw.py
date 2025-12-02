import os
import asyncio

from dotenv import load_dotenv
from contextlib import asynccontextmanager
from pathlib import Path

from azure.storage.blob import StandardBlobTier
from azure.storage.blob.aio import BlobServiceClient

from src.artifacts.BaseLogWriters import baseLogger
from src.artifacts.detLog import error_detailed
from src.artifacts.dirsFile import dirs
from ..utils import paths


load_dotenv(dirs.root / 'archives/.env')


oa_downloads_path = paths.downloads / 'OA_raw'
ocap_downloads_path = paths.downloads / 'OC_raw'
nn_downloads_path = paths.downloads / 'NN_raw'
metaData_downloads_path = paths.downloads / 'MetaData'


conn_string = os.environ["PROD_STORAGE_CONSTR"]


@asynccontextmanager
async def getBlobClient(blobPath: str, containerName: str, blob_service_client: BlobServiceClient):
    try:
        async with blob_service_client.get_blob_client(container=containerName, blob=blobPath) as client:
            yield client
    except Exception as e:
        baseLogger.critical(
            f"blob upload failed - {blobPath} - {error_detailed(e)}")


async def runRaw(filePath: Path, blob_path: str, blob_service_client: BlobServiceClient):
    async with getBlobClient(blobPath=blob_path, containerName='bronze', blob_service_client=blob_service_client) as blob_client:
        await blob_client.upload_blob(
            data=open(file=filePath, mode="rb"), overwrite=True, standard_blob_tier=StandardBlobTier.COLD)


async def pushRawOA():
    # AG_OA_20221202_INTRDY_2022-12-03_0900.csv
    blob_service_client = BlobServiceClient.from_connection_string(conn_string)

    async with asyncio.TaskGroup() as group:
        for file_path in oa_downloads_path.iterdir():

            pipeCode, _, effDate, _ = file_path.name.split('_', 3)
            blob_path = f'Enbridge/PointCapacity/{effDate[:-2]}/{pipeCode}/{file_path.name}'

            group.create_task(runRaw(filePath=file_path,
                              blob_service_client=blob_service_client,
                              blob_path=blob_path))

    await blob_service_client.close()


async def pushRawOC():
    # AG_OC1_20221206_INTRDY_2022-12-07_0900.csv
    blob_service_client = BlobServiceClient.from_connection_string(conn_string)

    async with asyncio.TaskGroup() as group:
        for file_path in ocap_downloads_path.iterdir():

            pipeCode, _, effDate, _ = file_path.name.split('_', 3)
            blob_path = f'Enbridge/SegmentCapacity/{effDate[:-2]}/{pipeCode}/{file_path.name}'

            group.create_task(runRaw(filePath=file_path,
                              blob_service_client=blob_service_client,
                              blob_path=blob_path))

    await blob_service_client.close()


async def pushRawNN():
    # TE_NN_20230906.csv

    blob_service_client = BlobServiceClient.from_connection_string(conn_string)

    async with asyncio.TaskGroup() as group:
        for file_path in nn_downloads_path.iterdir():

            pipeCode, _, effDate = file_path.name.split('_')
            blob_path = f'Enbridge/NoNotice/{effDate[:-4]}/{pipeCode}/{file_path.name}'

            group.create_task(runRaw(filePath=file_path,
                              blob_service_client=blob_service_client,
                              blob_path=blob_path))

    await blob_service_client.close()


async def pushRawMeta():
    # AG_AllPoints.csv

    blob_service_client = BlobServiceClient.from_connection_string(conn_string)

    async with asyncio.TaskGroup() as group:
        for file_path in metaData_downloads_path.iterdir():

            blob_path = f'Enbridge/Metadata/{file_path.name}'

            group.create_task(runRaw(filePath=file_path,
                              blob_service_client=blob_service_client,
                              blob_path=blob_path))

    await blob_service_client.close()


async def pushRawLogs():
    # Enbridge_error_20251201.log
    blob_service_client = BlobServiceClient.from_connection_string(conn_string)

    async with asyncio.TaskGroup() as group:
        for file_path in paths.logs.iterdir():
            if ('Enbridge' in file_path.name):
                parentPipe, _, effDate = file_path.name.split('_')
                blob_path = f'{parentPipe}/logs/{effDate[:-4]}/{file_path.name}'

                group.create_task(runRaw(filePath=file_path,
                                         blob_service_client=blob_service_client,
                                         blob_path=blob_path))

    await blob_service_client.close()
