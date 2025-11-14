import duckdb
import polars as pl
import pandas as pd
from datetime import datetime

from ...artifacts import updatePipes, error_detailed, getPipes
from ..utils import logger
from ..utils import paths


metaPath = paths.downloads / 'MetaData'

dbFile = paths.dbFile


def updateEnbridgePipes(source: pd.DataFrame):
    try:
        df_TSP = pl.DataFrame(source[['TSP', 'TSP Name']].drop_duplicates())\
            .with_columns(
                pl.col('TSP Name').alias('TSP_Name')
        )

        updatePipes(source=df_TSP)

    except Exception as e:
        logger.critical(
            f"updateEnbridgePipes : {datetime.now().strftime("%d/%m/%Y")}")
        logger.error(
            f"""updateEnbridgePipes : {datetime.now().strftime("%d/%m/%Y")}
            -  {error_detailed(e)}""")


def metaMunge():

    dfList = []

    for filePath in metaPath.iterdir():
        tempDf = pd.read_csv(filePath)
        dfList.append(tempDf)

    metaDF = pd.concat(dfList)

    updateEnbridgePipes(source=metaDF[['TSP', 'TSP Name']])
