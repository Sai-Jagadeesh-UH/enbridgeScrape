from datetime import date

import polars as pl
import pandas as pd

from .utils import paths

OC_path = paths.downloads / 'OC'

ALL_OC_Files = [i for i in OC_path.rglob("*_OC*.csv")]


def parseDate(dateString: str) -> date:
    return date(year=int(dateString[:4]), month=int(dateString[4:6]), day=int(dateString[6:]))


def batchTDMapper(inSeries: pl.Series) -> pl.Series:
    def check(val):
        if (val < 0):
            return 'TD2'
        if (val > 0):
            return 'TD1'
        return 'NA'
    return pl.Series(map(check, inSeries))


def batchTDMapperZero(inSeries: pl.Series) -> pl.Series:
    def check(val):
        if (val < 0):
            return 'TD2'
        return 'TD1'

    return pl.Series(map(check, inSeries))


def batchAbsolute(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(float, map(abs, inSeries)))


def getConcatDf() -> pd.DataFrame:
    framesList = []
    for index, filePath in enumerate(ALL_OC_Files):
        temp = ALL_OC_Files[0].name.split('_')
        tempDf = pd.read_csv(filePath, header=1, usecols=[0, 1, 2, 3])
        tempDf['pipeCode'] = temp[0]
        tempDf['EffDate'] = parseDate(temp[2])

        framesList.append(tempDf)

    return pd.concat(framesList).drop_duplicates()


def formatOC() -> pl.DataFrame:

    lz = pl.LazyFrame(getConcatDf()).with_columns(
        pl.col('Cap').cast(pl.Float64),
        pl.col('Cap2').cast(pl.Float64),
        pl.col('Nom').cast(pl.Float64),
    )

    lazyList = []

    dfTD1 = lz.filter(pl.col('Cap2').is_null())\
        .with_columns(
            pl.col('Cap').alias('OpCap'),
            pl.col('Nom').map_batches(batchTDMapper,
                                      return_dtype=pl.String).alias('FlowInd'))\
        .select(['pipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffDate'])

    dfTD1NonZero = dfTD1.filter(pl.col('FlowInd') != 'NA')

    lazyList.append(dfTD1NonZero)

    dfTD1Zero = dfTD1.filter(pl.col('FlowInd') == 'NA')\
        .with_columns(
        pl.col('OpCap').map_batches(batchTDMapperZero,
                                    return_dtype=pl.String).alias('FlowInd'))

    lazyList.append(dfTD1Zero)

    dfTD2 = lz.filter(~pl.col('Cap2').is_null())

    dfTD2Cap1NomNeg = dfTD2.filter(pl.col('Nom') < 0)\
        .with_columns(
        pl.col('Cap').alias('OpCap'),
        pl.lit(float(0)).alias('Nom'),
        pl.lit('TD1').alias('FlowInd'))\
        .select(['pipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffDate'])

    lazyList.append(dfTD2Cap1NomNeg)

    dfTD2Cap2NomNeg = dfTD2.filter(pl.col('Nom') < 0)\
        .with_columns(
        pl.col('Cap2').alias('OpCap'),
        pl.lit('TD2').alias('FlowInd'))\
        .select(['pipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffDate'])

    lazyList.append(dfTD2Cap2NomNeg)

    dfTD2Cap1NomPos = dfTD2.filter(pl.col('Nom') > 0)\
        .with_columns(
        pl.col('Cap').alias('OpCap'),
        pl.lit('TD1').alias('FlowInd'))\
        .select(['pipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffDate'])

    lazyList.append(dfTD2Cap1NomPos)

    dfTD2Cap2NomPos = dfTD2.filter(pl.col('Nom') > 0)\
        .with_columns(
        pl.col('Cap2').alias('OpCap'),
        pl.lit(float(0)).alias('Nom'),
        pl.lit('TD2').alias('FlowInd'))\
        .select(['pipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffDate'])

    lazyList.append(dfTD2Cap2NomPos)

    dfTD2Cap1NomZero = dfTD2.filter(pl.col('Nom') == 0)\
        .with_columns(
        pl.col('Cap').alias('OpCap'),
        pl.lit('TD1').alias('FlowInd'))\
        .select(['pipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffDate'])

    lazyList.append(dfTD2Cap1NomZero)

    dfTD2Cap2NomZero = dfTD2.filter(pl.col('Nom') == 0)\
        .with_columns(
        pl.col('Cap2').alias('OpCap'),
        pl.lit('TD2').alias('FlowInd'))\
        .select(['pipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffDate'])

    lazyList.append(dfTD2Cap2NomZero)

    return pl.concat(lazyList, how="vertical").with_columns(
        pl.col('OpCap').map_batches(batchAbsolute, return_dtype=pl.Float64),
        pl.col('Nom').map_batches(batchAbsolute, return_dtype=pl.Float64),
    ).collect()
