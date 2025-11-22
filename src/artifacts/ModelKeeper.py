import duckdb

from .dirsFile import dirs
from .push2Cloud import upsertTable
from .detLog import error_detailed
import pandas as pd
import polars as pl
modelsPath = dirs.models

dbFile = modelsPath / "GFPipes.db"

with duckdb.connect(dbFile) as con:
    # Creating Sequence for GFPipeID generation
    con.execute("""
                CREATE SEQUENCE IF NOT EXISTS GFPipeID_sequence
                INCREMENT BY 1
                MINVALUE 100
                MAXVALUE 999
                START WITH 100
                """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS GFPipes_table (
            GFPipeID INTEGER PRIMARY KEY DEFAULT nextval('GFPipeID_sequence'),
            TSP INTEGER UNIQUE NOT NULL,
            TSP_Name VARCHAR UNIQUE NOT NULL,
            ParentPipe VARCHAR 
        );
    """)


def updatePipes(df: pd.DataFrame | pl.DataFrame, parentPipeName: str) -> None:
    try:
        # inserting pipes into local db
        with duckdb.connect(dbFile) as con:
            con.execute(f"""
                MERGE INTO GFPipes_table AS target
                USING df AS source
                ON target.TSP = source.TSP
                WHEN NOT MATCHED THEN
                    INSERT (TSP,TSP_Name,ParentPipe) VALUES (source.TSP, source.TSP_Name,'{parentPipeName}');
            """)
            # WHEN MATCHED THEN
            #     UPDATE SET TSP = source.TSP, TSP_Name = source.TSP_Name, ParentPipe = '{parentPipeName}'

        #  pushing changes to cloud
        upsertTable(entityFrame=getPipes().to_pandas(), tableName='GFPipes')
        print("Pipes updated successfully")
    except Exception as e:
        print(f"Error updating pipes - {error_detailed(e)}")


def getPipes(parentPipeName: str | None = None) -> pl.DataFrame:

    if parentPipeName is not None:
        QueryStr = f"SELECT * from GFPipes_table WHERE ParentPipe = '{parentPipeName}'"
    else:
        QueryStr = "SELECT * from GFPipes_table"

    with duckdb.connect(dbFile) as con:
        return pl.DataFrame(data=con.execute(QueryStr).fetchall(),
                            schema=['GFPipeID', 'TSP', 'TSP_Name', 'ParentPipe'], orient="row")
        # .select(['GFPipeID', 'TSP', 'TSP_Name', 'ParentPipe'])
