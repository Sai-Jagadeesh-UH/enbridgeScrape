import duckdb
import pandas as pd

from ..utils import paths


metaPath = paths.downloads / 'MetaData'

dbFile = paths.dbFile
ParentPipe = 'Enbridge'
metaTableName = 'EnbridgeSegmentsTable'


def updatePipes(df: pd.DataFrame):
    return
    with duckdb.connect(dbFile) as con:
        con.execute(f"""
            MERGE INTO {metaTableName} AS target
            USING df AS source
            ON target.TSP = source.TSP and target.TSP_Name = source.TSP_Name and target.ParentPipe = source.ParentPipe
            WHEN MATCHED THEN
                UPDATE SET TSP = source.TSP, TSP_Name = source.TSP_Name
            WHEN NOT MATCHED THEN
                INSERT (TSP,TSP_Name,ParentPipe) VALUES (source.TSP, source.TSP_Name, source.ParentPipe);
        """)
