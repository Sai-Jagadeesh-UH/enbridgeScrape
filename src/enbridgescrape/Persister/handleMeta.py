import pandas as pd
from datetime import datetime

from src.artifacts import updatePipes, error_detailed, getPipes
from ..utils import logger
from ..utils import paths


metaPath = paths.downloads / 'MetaData'

dbFile = paths.dbFile

ParentPipe = 'Enbridge'


def updateEnbridgePipes(source: pd.DataFrame):

    try:
        df_TSP = source[['TSP', 'TSP Name', 'ParentPipe']
                        ].drop_duplicates().rename({'TSP Name': 'TSP_Name'}, axis='columns')

        updatePipes(df=df_TSP)

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
    metaDF['ParentPipe'] = ParentPipe

    updateEnbridgePipes(source=metaDF[['ParentPipe', 'TSP', 'TSP Name']])

    getPipes(ParentPipe)[['GFPipeID', 'TSP']].merge(
        metaDF,
        on='TSP',
        how='inner',
    ).to_csv(paths.models / 'EnbridgeMetaData.csv', index=False)
    # .collect().write_parquet(paths.models / 'MetaData.parquet')
