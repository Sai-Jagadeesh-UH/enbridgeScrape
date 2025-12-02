import os
import asyncio

# from dotenv import load_dotenv
# from contextlib import asynccontextmanager
from pathlib import Path
import pandas as pd

from azure.storage.blob import StandardBlobTier
from azure.storage.blob.aio import BlobServiceClient


from .handleRaw import getBlobClient
from ..Munger import cleanseOA, cleanseOC
# from src.artifacts.BaseLogWriters import baseLogger
from src.artifacts.detLog import error_detailed
# from src.artifacts.dirsFile import dirs
from ..utils import paths, logger

oa_path = paths.downloads / 'OA'
ocap_path = paths.downloads / 'OC'
nn_path = paths.downloads / 'NN'


oa_path.mkdir(exist_ok=True, parents=True)
ocap_path.mkdir(exist_ok=True, parents=True)
nn_path.mkdir(exist_ok=True, parents=True)


conn_string = os.environ["PROD_STORAGE_CONSTR"]


async def runSilver(filePath: Path, blob_path: str, blob_service_client: BlobServiceClient):
    async with getBlobClient(blobPath=blob_path, containerName='silver', blob_service_client=blob_service_client) as blob_client:
        await blob_client.upload_blob(
            data=open(file=filePath, mode="rb"), overwrite=True, standard_blob_tier=StandardBlobTier.COLD)


async def processOA():
    emptyList = []
    try:
        for filePath in (paths.downloads / 'OA_raw').iterdir():
            try:
                pipeCode = filePath.name.split('_', 2)[0]
                df = pd.read_csv(filePath, header=0)
                if (len(df) < 1):
                    emptyList.append(filePath.name.replace('.csv', '.parquet'))
                df.assign(PipeCode=pipeCode)\
                    .to_parquet(paths.downloads / "OA" / filePath.name.replace('.csv', '.parquet'), index=False)
            except Exception as e:
                logger.critical(
                    f"""Raw OA file read Failed {filePath} - {error_detailed(e)}""")

        async with asyncio.TaskGroup() as group:
            for file_path in oa_path.iterdir():
                if (file_path.name not in emptyList):
                    group.create_task(cleanseOA(file_path))
    except Exception:
        logger.critical("processOA failed ")


async def pushSilverOA():

    await processOA()
    # AG_OA_20221202_INTRDY_2022-12-03_0900.csv
    blob_service_client = BlobServiceClient.from_connection_string(conn_string)

    async with asyncio.TaskGroup() as group:
        for file_path in oa_path.iterdir():

            pipeCode, _, effDate, _ = file_path.name.split('_', 3)
            blob_path = f'Enbridge/PointCapacity/{effDate[:-2]}/{pipeCode}/{file_path.name}'

            group.create_task(runSilver(filePath=file_path,
                              blob_service_client=blob_service_client,
                              blob_path=blob_path))

    await blob_service_client.close()


async def processOC():

    emptyList = []
    try:
        for filePath in (paths.downloads / 'OC_raw').iterdir():
            try:

                pipeCode, _, EffGasDayTime, _ = filePath.name.split('_', 3)

                try:
                    df = pd.read_csv(filePath,
                                     skiprows=1,
                                     usecols=['Station Name', 'Cap', 'Nom', 'Cap2'])
                    if (len(df) < 1):
                        emptyList.append(
                            filePath.name.replace('.csv', '.parquet'))
                except Exception as e:
                    logger.critical(
                        f"Raw OC file read Failed {filePath} - {error_detailed(e)}")
                    continue

                df.assign(PipeCode=pipeCode)\
                    .assign(EffGasDate=EffGasDayTime)[['PipeCode', 'EffGasDate', 'Station Name', 'Cap', 'Nom', 'Cap2']]\
                    .to_parquet(paths.downloads / "OC" / filePath.name.replace('.csv', '.parquet'), index=False)
            except Exception as e:
                logger.critical(
                    f"""Raw OC file read Failed {filePath} - {error_detailed(e)}""")

        async with asyncio.TaskGroup() as group:
            for file_path in ocap_path.iterdir():
                if (file_path.name not in emptyList):
                    group.create_task(cleanseOC(file_path))

    except Exception:
        logger.critical("processOC failed ")


async def pushSilverOC():
    # logger.error("starting OC processing .............")
    await processOC()
    # logger.error("completed OC processing .............")
    # AG_OC1_20221206_INTRDY_2022-12-07_0900.csv
    blob_service_client = BlobServiceClient.from_connection_string(conn_string)

    async with asyncio.TaskGroup() as group:
        for file_path in ocap_path.iterdir():

            pipeCode, _, effDate, _ = file_path.name.split('_', 3)
            blob_path = f'Enbridge/SegmentCapacity/{effDate[:-2]}/{pipeCode}/{file_path.name}'

            group.create_task(runSilver(filePath=file_path,
                              blob_service_client=blob_service_client,
                              blob_path=blob_path))

    await blob_service_client.close()


# async def pushRawNN():
#     # TE_NN_20230906.csv

#     blob_service_client = BlobServiceClient.from_connection_string(conn_string)

#     async with asyncio.TaskGroup() as group:
#         for file_path in nn_path.iterdir():

#             pipeCode, _, effDate = file_path.name.split('_')
#             blob_path = f'Enbridge/NoNotice/{effDate[:-4]}/{pipeCode}/{file_path.name}'

#             group.create_task(runSilver(filePath=file_path,
#                               blob_service_client=blob_service_client,
#                               blob_path=blob_path))

#     await blob_service_client.close()


# async def pushRawMeta():
#     # AG_AllPoints.csv

#     blob_service_client = BlobServiceClient.from_connection_string(conn_string)

#     async with asyncio.TaskGroup() as group:
#         for file_path in metaData_path.iterdir():

#             blob_path = f'Enbridge/Metadata/{file_path.name}'

#             group.create_task(runSilver(filePath=file_path,
#                               blob_service_client=blob_service_client,
#                               blob_path=blob_path))

#     await blob_service_client.close()
