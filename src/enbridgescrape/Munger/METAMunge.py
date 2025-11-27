import pandas as pd
from datetime import datetime

from src.artifacts import updatePipes, error_detailed
from ..utils import logger
from ..utils import paths
from .MungeAll import processMeta


metaPath = paths.downloads / 'MetaData'

processedPath = paths.processed

dbFile = paths.dbFile

ParentPipe = 'Enbridge'


def metaMunge():

    processMeta()

    metaDF = pd.read_parquet(processedPath / 'MetaData.parquet')\
        .drop_duplicates(['PipeCode'], keep='first')\
        .rename({'TSP Name': 'TSP_Name'}, axis='columns')

    updatePipes(df=metaDF[['PipeCode', 'TSP_Name']], parentPipeName=ParentPipe)
