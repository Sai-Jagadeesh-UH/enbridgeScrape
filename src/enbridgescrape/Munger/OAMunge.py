from datetime import datetime

import functools
import operator
import polars as pl

from ..utils import paths
from .MungeAll import processOA
from src.artifacts import getPipes, updatePipes

ParentPipe = 'Enbridge'

OA_path = paths.downloads / 'OA'

selectedCols = ['Cycle_Desc', 'Eff_Gas_Day', 'Loc', 'Loc_Name', 'Loc_Zn', 'Flow_Ind_Desc', 'Loc_Purp_Desc', 'IT', 'All_Qty_Avail',
                'Total_Design_Capacity', 'Operating_Capacity', 'Total_Scheduled_Quantity', 'Operationally_Available_Capacity']


def saveFile(df: pl.DataFrame) -> None:
    df.write_csv(paths.models /
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


def formatOA():

    processOA()

    df = pl.scan_csv(OA_path / "*_OA_*.csv",
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

    # .collect()

    updatePipes(df=df.select(['PipeCode', 'TSP_Name']
                             ).collect().unique(subset=['PipeCode'], keep='first'), parentPipeName=ParentPipe)

    df = df.join(pl.LazyFrame(getPipes(parentPipeName=ParentPipe)[['GFPipeID', 'PipeCode']]), on='PipeCode', how='inner')\
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
