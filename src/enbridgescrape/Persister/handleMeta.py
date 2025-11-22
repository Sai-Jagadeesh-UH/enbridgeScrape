import pandas as pd
from datetime import datetime

from src.artifacts import updatePipes, error_detailed
from ..utils import logger
from ..utils import paths


metaPath = paths.downloads / 'MetaData'

dbFile = paths.dbFile

ParentPipe = 'Enbridge'


def updateEnbridgePipes(source: pd.DataFrame):

    try:
        # if ('TSP Name' in source.columns):
        df_TSP = source[['TSP', 'TSP Name']
                        ].drop_duplicates().rename({'TSP Name': 'TSP_Name'}, axis='columns')
        # else:
        #     df_TSP = source.drop_duplicates()

        updatePipes(df=df_TSP, parentPipeName=ParentPipe)

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
    # metaDF['ParentPipe'] = ParentPipe
    # return metaDF

    updateEnbridgePipes(source=metaDF[['TSP', 'TSP Name']])

    # getPipes(ParentPipe)[['GFPipeID', 'TSP']].merge(
    #     metaDF,
    #     on='TSP',
    #     how='inner',
    # ).to_csv(paths.models / 'EnbridgeMetaData.csv', index=False)
    # .collect().write_parquet(paths.models / 'MetaData.parquet')
