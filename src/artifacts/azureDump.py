import os

import pandas as pd
from contextlib import contextmanager
from azure.data.tables import TableServiceClient

from .BaseLogWriters import baseLogger
from .detLog import error_detailed
from .dirsFile import dirs


@contextmanager
def getTable(tableName: str):
    deltaConstr = os.getenv('DELTA_STORAGE_CONSTR', '')
    try:
        with TableServiceClient.from_connection_string(conn_str=deltaConstr) as client:
            yield client.create_table_if_not_exists(table_name=tableName)
    except Exception as e:
        baseLogger.critical(
            f"table creation/retrieval failed - {error_detailed(e)}")


def dumpPipeConfigs():
    baseLogger.critical("Dumping PipeConfigs from Azure Table Storage")
    with getTable('PipeConfigs') as table_client:
        df = pd.DataFrame(table_client.query_entities("", select=[
                          'ParentPipe', 'PipeName', 'GFPipeID', 'PipeCode', 'MetaCode', 'PointCapCode', 'SegmentCapCode', 'NoNoticeCode']))
        try:
            df['GFPipeID'] = df['GFPipeID'].apply(lambda x: x.value)
            df.to_parquet(dirs.configFiles /
                          'PipeConfigs.parquet', index=False)
        except Exception as e:
            baseLogger.critical(
                f"There was an error dumping PipeConfigs - {error_detailed(e)}")
