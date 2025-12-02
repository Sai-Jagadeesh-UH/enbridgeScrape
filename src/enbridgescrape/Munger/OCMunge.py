from datetime import datetime

from pathlib import Path
import functools
import operator

import polars as pl

from src.artifacts import error_detailed
from ..utils import paths, logger, pipeConfigs_df
# from .MungeAll import processOC

OC_path = paths.downloads / 'OC'
ParentPipe = 'Enbridge'


# def parseDate(dateString: str) -> date:
#     return date(year=int(dateString[:4]), month=int(dateString[5:7]), day=int(dateString[8:]))


def batchDateParse(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: datetime.strptime(inString, "%Y%m%d").date(), inSeries))


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


# def getConcatDf() -> pd.DataFrame:

#     orderdList = []
#     for filename in glob.glob(str(OC_path / "*.csv")):
#         with open(filename) as file:
#             orderdList.append((filename, file.readline().split('  ')[0][10:]))

#     combined_df_concise = pd.concat([
#         pd.read_csv(item[0], usecols=['Station Name', 'Cap',
#                     'Nom', 'Cap2'], skiprows=1).assign(EffGasDayTime=item[1])
#         for item in orderdList], ignore_index=True).drop_duplicates(keep='first')

#     combined_df_concise['EffGasDayTime'] = combined_df_concise['EffGasDayTime'].apply(
#         parseDate)
#     # parseDate(temp[2])

#     # framesList = []
#     # for index, filePath in enumerate(ALL_OC_Files):
#     #     temp = ALL_OC_Files[0].name.split('_')
#     #     tempDf = pd.read_csv(filePath, header=1, usecols=[0, 1, 2, 3])
#     #     tempDf['PipeCode'] = temp[0]
#     #     tempDf['EffDate'] = parseDate(temp[2])

#     #     framesList.append(tempDf)

#     return combined_df_concise

async def cleanseOC(filePath: Path):
    try:
        lz = pl.scan_parquet(filePath)\
            .filter(
            ~functools.reduce(
                operator.and_,
                map(lambda col: pl.col(col).is_null(),
                    ['Station Name', 'Cap', 'Nom', 'Cap2'])
            ))\
            .with_columns(
            pl.col('Cap').cast(pl.Float64),
            pl.col('Cap2').cast(pl.Float64),
            pl.col('Nom').cast(pl.Float64),
        )

        lazyList: list[pl.LazyFrame] = []

        dfTD1 = lz.filter(pl.col('Cap2').is_null())\
            .with_columns(
                pl.col('Cap').alias('OpCap'),
                pl.col('Nom').map_batches(batchTDMapper,
                                          return_dtype=pl.String).alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

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
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

        lazyList.append(dfTD2Cap1NomNeg)

        dfTD2Cap2NomNeg = dfTD2.filter(pl.col('Nom') < 0)\
            .with_columns(
            pl.col('Cap2').alias('OpCap'),
            pl.lit('TD2').alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

        lazyList.append(dfTD2Cap2NomNeg)

        dfTD2Cap1NomPos = dfTD2.filter(pl.col('Nom') > 0)\
            .with_columns(
            pl.col('Cap').alias('OpCap'),
            pl.lit('TD1').alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

        lazyList.append(dfTD2Cap1NomPos)

        dfTD2Cap2NomPos = dfTD2.filter(pl.col('Nom') > 0)\
            .with_columns(
            pl.col('Cap2').alias('OpCap'),
            pl.lit(float(0)).alias('Nom'),
            pl.lit('TD2').alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

        lazyList.append(dfTD2Cap2NomPos)

        dfTD2Cap1NomZero = dfTD2.filter(pl.col('Nom') == 0)\
            .with_columns(
            pl.col('Cap').alias('OpCap'),
            pl.lit('TD1').alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

        lazyList.append(dfTD2Cap1NomZero)

        dfTD2Cap2NomZero = dfTD2.filter(pl.col('Nom') == 0)\
            .with_columns(
            pl.col('Cap2').alias('OpCap'),
            pl.lit('TD2').alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

        lazyList.append(dfTD2Cap2NomZero)

        df = pl.concat(lazyList, how="vertical")\
            .with_columns(
            pl.col('OpCap').map_batches(
                batchAbsolute, return_dtype=pl.Float64),
            pl.col('Nom').map_batches(batchAbsolute, return_dtype=pl.Float64),
            pl.col('EffGasDate').map_batches(
                batchDateParse, return_dtype=pl.Date))


# ['GFPipeID', 'PipeCode', 'PipeName']

        df.join(pl.LazyFrame(pipeConfigs_df.loc[pipeConfigs_df['ParentPipe'] == ParentPipe, ['GFPipeID', 'PipeCode', 'PipeName']]), on='PipeCode', how='inner')\
            .with_columns(
                pl.concat_str([pl.col('GFPipeID'),
                               pl.lit("SG"),
                               pl.col('tempLoc'),
                               pl.col('FlowInd')], separator='')
                .alias('GFLOC')
        )\
            .with_columns(
                pl.lit('Enbridge').cast(pl.String).alias('ParentPipe'),
                pl.lit('Enbridge').cast(pl.String).alias('ParentPipe'),
                pl.lit('Enbridge').cast(pl.String).alias('ParentPipe'),
                pl.lit('Enbridge').cast(pl.String).alias('ParentPipe'),
                pl.col('EffGasDate').alias('EffGasDayTime'),

        )\
            .select(['ParentPipe',
                     'PipelineName',
                     'GFLOC',
                     'EffGasDayTime',
                     'CycleDesc',
                     'LocPurpDesc',
                     'Loc',
                     'LocName',
                     'LocZn',
                     'LocSegment',
                     'DesignCapacity',
                     'OperatingCapacity',
                     'TotalScheduledQuantity',
                     'OperationallyAvailableCapacity',
                     'IT',
                     'FlowInd',
                     'AllQtyAvail',
                     'QtyReason',
                     'Timestamp']).collect().write_parquet(filePath)

    except Exception as e:
        print(f"cleanseOC failed for {filePath} - {error_detailed(e)}")
        logger.critical(
            f"cleanseOC failed for {filePath} - {error_detailed(e)}")


def formatOC() -> pl.DataFrame:
    # 'PipeCode', 'EffGasDate', 'Station Name', 'Cap', 'Nom', 'Cap2']
    return pl.DataFrame([])

    # processOC()

    lz = pl.scan_csv(OC_path / "*_OC*.csv",
                     glob=True,
                     has_header=True,
                     )\
        .filter(
        ~functools.reduce(
            operator.and_,
            map(lambda col: pl.col(col).is_null(),
                ['Station Name', 'Cap', 'Nom', 'Cap2'])
        ))\
        .with_columns(
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
        .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

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
        .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

    lazyList.append(dfTD2Cap1NomNeg)

    dfTD2Cap2NomNeg = dfTD2.filter(pl.col('Nom') < 0)\
        .with_columns(
        pl.col('Cap2').alias('OpCap'),
        pl.lit('TD2').alias('FlowInd'))\
        .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

    lazyList.append(dfTD2Cap2NomNeg)

    dfTD2Cap1NomPos = dfTD2.filter(pl.col('Nom') > 0)\
        .with_columns(
        pl.col('Cap').alias('OpCap'),
        pl.lit('TD1').alias('FlowInd'))\
        .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

    lazyList.append(dfTD2Cap1NomPos)

    dfTD2Cap2NomPos = dfTD2.filter(pl.col('Nom') > 0)\
        .with_columns(
        pl.col('Cap2').alias('OpCap'),
        pl.lit(float(0)).alias('Nom'),
        pl.lit('TD2').alias('FlowInd'))\
        .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

    lazyList.append(dfTD2Cap2NomPos)

    dfTD2Cap1NomZero = dfTD2.filter(pl.col('Nom') == 0)\
        .with_columns(
        pl.col('Cap').alias('OpCap'),
        pl.lit('TD1').alias('FlowInd'))\
        .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

    lazyList.append(dfTD2Cap1NomZero)

    dfTD2Cap2NomZero = dfTD2.filter(pl.col('Nom') == 0)\
        .with_columns(
        pl.col('Cap2').alias('OpCap'),
        pl.lit('TD2').alias('FlowInd'))\
        .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate'])

    lazyList.append(dfTD2Cap2NomZero)

    return pl.concat(lazyList, how="vertical").with_columns(
        pl.col('OpCap').map_batches(batchAbsolute, return_dtype=pl.Float64),
        pl.col('Nom').map_batches(batchAbsolute, return_dtype=pl.Float64),
        pl.col('EffGasDate').map_batches(batchDateParse, return_dtype=pl.Date),
    ).collect()
