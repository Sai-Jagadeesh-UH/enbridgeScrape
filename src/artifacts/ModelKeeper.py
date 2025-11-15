import duckdb

from .dirsFile import dirs
import pandas as pd
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
            ParentPipe VARCHAR NOT NULL
        );
    """)


def updatePipes(df: pd.DataFrame):

    with duckdb.connect(dbFile) as con:
        con.execute("""
            MERGE INTO GFPipes_table AS target
            USING df AS source
            ON target.TSP = source.TSP and target.TSP_Name = source.TSP_Name and target.ParentPipe = source.ParentPipe
            WHEN MATCHED THEN
                UPDATE SET TSP = source.TSP, TSP_Name = source.TSP_Name
            WHEN NOT MATCHED THEN
                INSERT (TSP,TSP_Name,ParentPipe) VALUES (source.TSP, source.TSP_Name, source.ParentPipe);
        """)


def getPipes(parentPipeName: str | None = None) -> pd.DataFrame:

    if parentPipeName is not None:
        QueryStr = f"SELECT * from GFPipes_table WHERE ParentPipe = '{parentPipeName}'"
    else:
        QueryStr = "SELECT * from GFPipes_table"

    with duckdb.connect(dbFile) as con:
        return pd.DataFrame(data=con.execute(QueryStr).fetchall(),
                            columns=['GFPipeID', 'TSP', 'TSP_Name', 'ParentPipe'])
        # .select(['GFPipeID', 'TSP', 'TSP_Name', 'ParentPipe'])
