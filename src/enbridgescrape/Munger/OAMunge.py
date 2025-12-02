from datetime import datetime

import functools
import operator
import polars as pl
from pathlib import Path

from ..utils import paths, pipeConfigs_df, logger, error_detailed
# from .MungeAll import processOA
# from src.artifacts import getPipes, updatePipes

ParentPipe = 'Enbridge'

OA_path = paths.downloads / 'OA'

selectedCols = ['Cycle_Desc', 'Eff_Gas_Day', 'Loc', 'Loc_Name', 'Loc_Zn', 'Flow_Ind_Desc', 'Loc_Purp_Desc', 'IT', 'All_Qty_Avail',
                'Total_Design_Capacity', 'Operating_Capacity', 'Total_Scheduled_Quantity', 'Operationally_Available_Capacity']


def saveFile(df: pl.DataFrame) -> None:
    df.write_csv(paths.processed /
                 f'OA_Processed{datetime.now().strftime("%d%m%Y%H%M%S")}.csv')


def batchDateParse(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: datetime.strptime(inString, "%m-%d-%Y").date(), inSeries))


def paddedString(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: str(inString).rjust(6, '0'), inSeries))


def batchFloatParse(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: float(inString.replace(',', '')), inSeries))


def batchFIMapper(inSeries: pl.Series) -> pl.Series:
    flow_Map = {
        'Delivery': 'D',
        'Receipt': 'R',
        'Storage Injection': 'D',
        'Storage Withdrawal': 'R'
    }
    return pl.Series(map(lambda inString: flow_Map.get(inString, 'D'), inSeries))


async def cleanseOA(filePath: Path):
    try:
        df = pl.scan_parquet(filePath)\
            .select([*selectedCols, 'PipeCode'])\
            .filter(
            ~functools.reduce(
                operator.and_,
                map(lambda col: pl.col(col).is_null(), selectedCols)
            ))

        df = df.with_columns(
            pl.col("Eff_Gas_Day")
            .map_batches(batchDateParse, return_dtype=pl.Date)
            .alias('EffGasDayTime'),

            pl.col("Total_Design_Capacity")
            .map_batches(batchFloatParse, return_dtype=pl.Float64)
            .alias('DesignCapacity'),

            pl.col("Operating_Capacity")
            .map_batches(batchFloatParse, return_dtype=pl.Float64)
            .alias('OperatingCapacity'),

            pl.col("Total_Scheduled_Quantity")
            .map_batches(
                batchFloatParse, return_dtype=pl.Float64)
            .alias('TotalScheduledQuantity'),

            pl.col("Operationally_Available_Capacity")
            .map_batches(batchFloatParse, return_dtype=pl.Float64)
            .alias('OperationallyAvailableCapacity'),

            pl.col("Flow_Ind_Desc")
            .map_batches(batchFIMapper, return_dtype=pl.String)
            .alias('FlowInd'),

            pl.lit(datetime.now())
            .cast(pl.Datetime)
            .alias('Timestamp'),

            pl.col("Loc")
            .cast(pl.String)
            .map_batches(paddedString, return_dtype=pl.String),

            pl.lit('Enbridge').cast(pl.String).alias('ParentPipe'),

            pl.lit(None).cast(pl.String).alias('LocSegment'),
            pl.lit(None).cast(pl.String).alias('LocSegment'))\
            .rename({
                'Cycle_Desc': 'CycleDesc',
                'Loc_Name': 'LocName',
                'Loc_Zn': 'LocZn',
                'Loc_Purp_Desc': 'LocPurpDesc',
                'All_Qty_Avail': 'AllQtyAvail',
                'IT': 'IT'
            })

        df.join(pl.LazyFrame(pipeConfigs_df.loc[pipeConfigs_df['ParentPipe'] == ParentPipe, ['GFPipeID', 'PipeCode', 'PipeName']]), on='PipeCode', how='inner')\
            .with_columns(
                pl.col('GFPipeID').cast(pl.String),

                pl.col('Loc')
                .map_batches(paddedString, return_dtype=pl.String)
                .alias('tempLoc'),

                pl.col('PipeName').cast(pl.String).alias('PipelineName'))\
            .with_columns(
                pl.concat_str([pl.col('GFPipeID'),
                               pl.lit("OA"),
                               pl.col('tempLoc'),
                               pl.col('FlowInd')], separator='')
                .alias('GFLOC'))\
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
        # .select([
        #     'GFLOC',
        #     'LocPurpDesc',
        #     'Loc',
        #     'LocName',
        #     'LocZn',
        #     'IT',
        #     'FlowInd',
        #     'LocSegment',
        #     'DesignCapacity',
        #     'OperatingCapacity',
        #     'TotalScheduledQuantity',
        #     'OperationallyAvailableCapacity',
        #     'AllQtyAvail',
        #     'QtyReason',
        #     'Timestamp',
        #     # 'TSP',
        #     'GFPipeID',
        #     'EffGasDayTime',
        #     'TSP_Name',
        #     'PipeCode',
        #     'CycleDesc'
        # ]).collect().write_parquet(filePath)
    except Exception as e:
        print(f"cleanseOA failed for {filePath} - {error_detailed(e)}")
        logger.critical(
            f"cleanseOA failed for {filePath} - {error_detailed(e)}")


def formatOA():
    return
    df = pl.scan_csv(OA_path / "*_OA_*.parquet",
                     glob=True,
                     has_header=True,
                     )\
        .select([*selectedCols, 'TSP_Name', 'PipeCode'])\
        .filter(
        ~functools.reduce(
            operator.and_,
            map(lambda col: pl.col(col).is_null(), selectedCols)
        ))

    df = df.with_columns(
        pl.col("Eff_Gas_Day").map_batches(
            batchDateParse, return_dtype=pl.Date),
        pl.col("Total_Design_Capacity").map_batches(
            batchFloatParse, return_dtype=pl.Float64),
        pl.col("Operating_Capacity").map_batches(
            batchFloatParse, return_dtype=pl.Float64),
        pl.col("Total_Scheduled_Quantity").map_batches(
            batchFloatParse, return_dtype=pl.Float64),
        pl.col("Operationally_Available_Capacity").map_batches(
            batchFloatParse, return_dtype=pl.Float64),
        pl.col("Flow_Ind_Desc").map_batches(
            batchFIMapper, return_dtype=pl.String),
        pl.lit(None).cast(pl.String).alias('LocSegment'),
        pl.lit(None).cast(pl.String).alias('QtyReason'),
        pl.lit(datetime.now()).cast(
            pl.Datetime).alias('Timestamp'),
        # pl.col("TSP").cast(pl.String),
        pl.col("Loc").cast(pl.String).map_batches(
            paddedString, return_dtype=pl.String).alias('PaddedLoc'),
    )\
        .rename({
            'Cycle_Desc': 'CycleDesc',
            'Eff_Gas_Day': 'EffGasDayTime',
            'Loc_Name': 'LocName',
            'Loc_Zn': 'LocZn',
            'Flow_Ind_Desc': 'FlowInd',
            'Loc_Purp_Desc': 'LocPurpDesc',
            'All_Qty_Avail': 'AllQtyAvail',
            'Total_Design_Capacity': 'DesignCapacity',
            'Operating_Capacity': 'OperatingCapacity',
            'Total_Scheduled_Quantity': 'TotalScheduledQuantity',
            'Operationally_Available_Capacity': 'OperationallyAvailableCapacity',
            # 'TSP_Name': 'PipelineName'
        })

    df = df.join(pl.LazyFrame(pipeConfigs_df.loc[pipeConfigs_df['ParentPipe'] == ParentPipe, ['GFPipeID', 'PipeCode']]), on='PipeCode', how='inner')\
        .with_columns(
            pl.col('GFPipeID').cast(pl.String),
            pl.col('Loc').map_batches(paddedString,
                                      return_dtype=pl.String).alias('tempLoc')
    )\
        .with_columns(
            pl.concat_str([pl.col('GFPipeID'),
                           pl.lit("OA"),
                           pl.col('tempLoc'),
                           pl.col('FlowInd')], separator='').alias('GFLOC')
    ).select([
        'GFLOC',
        'LocPurpDesc',
        'Loc',
        'LocName',
        'LocZn',
        'IT',
        'FlowInd',
        'LocSegment',
        'DesignCapacity',
        'OperatingCapacity',
        'TotalScheduledQuantity',
        'OperationallyAvailableCapacity',
        'AllQtyAvail',
        'QtyReason',
        'Timestamp',
        # 'TSP',
        'GFPipeID',
        'EffGasDayTime',
        'TSP_Name',
        'PipeCode',
        'CycleDesc'
    ])

    saveFile(df.collect())
    # df.write_csv(paths.models / 'OA_Processed.csv')
    # return df.collect()
