import duckdb

from .dirsFile import dirs
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
            TSP_Name VARCHAR UNIQUE NOT NULL
        );
    """)


def updatePipes(source: pl.DataFrame):

    with duckdb.connect(dbFile) as con:
        con.execute("""
            MERGE INTO GFPipes_table AS target
            USING df_TSP AS source
            ON target.TSP = source.TSP and target.TSP_Name = source.TSP_Name
            WHEN MATCHED THEN
                UPDATE SET TSP = source.TSP, TSP_Name = source.TSP_Name
            WHEN NOT MATCHED THEN
                INSERT (TSP,TSP_Name) VALUES (source.TSP, source.TSP_Name);
        """)


def getPipes() -> pl.LazyFrame:
    with duckdb.connect(dbFile) as con:
        return pl.LazyFrame(data=con.execute(" SELECT * from GFPipes_table").fetchall(),
                            schema=['GFPipeID', 'TSP', 'TSP_Name'], orient='row')\
            .select(['GFPipeID', 'TSP'])
