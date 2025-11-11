from datetime import datetime

import functools
import operator
import polars as pl

from .utils import paths

OA_path = paths.downloads / 'OA'

sleectedCols = ['Cycle_Desc', 'Eff_Gas_Day', 'Loc', 'Loc_Name', 'Loc_Zn', 'Flow_Ind_Desc', 'Loc_Purp_Desc', 'IT', 'All_Qty_Avail',
                'Total_Design_Capacity', 'Operating_Capacity', 'Total_Scheduled_Quantity', 'Operationally_Available_Capacity', 'TSP_Name']


def batchDateParse(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: datetime.strptime(inString, "%m-%d-%Y").date(), inSeries))


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
    df = pl.scan_csv(OA_path / "*_OA_*.csv",
                     glob=True,
                     has_header=True,
                     )\
        .select([*sleectedCols, 'TSP'])\
        .filter(
        ~functools.reduce(
            operator.or_,
            map(lambda col: pl.col(col).is_null(), sleectedCols)
        ))

    df = df.with_columns(pl.col("Eff_Gas_Day").map_batches(batchDateParse, return_dtype=pl.Date),
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
        pl.Datetime).alias('Timestamp'))\
        .with_columns(pl.concat_str(
            [
                pl.col("Loc"),
                pl.lit("OA"),
                pl.col("TSP"),
                pl.col("Flow_Ind_Desc")
            ]
        ).alias("GFLOC"))\
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
            'TSP_Name': 'PipelineName'
        })\
        .select([
            'EffGasDayTime',
                'PipelineName',
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
                'Timestamp',
                'GFLOC'
                ]).collect()

    return df
