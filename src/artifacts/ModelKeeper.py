import duckdb
import sqlite3
import contextlib

from .dirsFile import dirs
# from .push2Cloud import upsertTable
from .detLog import error_detailed
import pandas as pd
import polars as pl

modelsPath = dirs.models

dbFile = modelsPath / "GFPipes.db"

db_Name = "GFPipes.db"


@contextlib.contextmanager
def conect_db(db_name: str = "GFPipes.db"):
    conn = None
    try:
        conn = sqlite3.connect(modelsPath / db_Name, timeout=10.0)
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA foreign_keys = ON;')
        yield conn  # Execution pauses here and returns the connection object
        conn.commit()  # This runs if the 'with' block finishes without errors
    except Exception as e:
        print(f"{db_name} Connection Failed Rolling Back - {error_detailed(e)}")
        if conn:
            conn.rollback()  # This runs if an exception occurs
        raise
    finally:
        if conn:
            conn.close()  # This runs regardless of whether an exception occurred
            # print("Connection closed automatically.")


with conect_db() as conn:
    # cur = conn.cursor()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS GFPipes_table (
        GFPipeID INTEGER PRIMARY KEY AUTOINCREMENT,
        PipeCode VARCHAR UNIQUE NOT NULL,
        TSP_Name VARCHAR NOT NULL,
        ParentPipe VARCHAR 
    );
    """)
    conn.execute(
        "INSERT OR IGNORE INTO sqlite_sequence (name, seq) VALUES (?, ?)", ('GFPipes_table', 99))


def updatePipes(df: pd.DataFrame | pl.DataFrame, parentPipeName: str) -> None:
    try:

        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()

        with conect_db() as con:

            df.to_sql('stg_GFPipes', con, if_exists='replace', index=False)

            con.execute("""
                INSERT INTO GFPipes_table (PipeCode, TSP_Name, ParentPipe)
                SELECT 
                    stg.PipeCode, 
                    stg.TSP_Name, 
                    ? AS ParentPipeValue
                FROM 
                    stg_GFPipes AS stg
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM GFPipes_table AS final
                    WHERE final.PipeCode = stg.PipeCode
                );
            """, (parentPipeName,))

            con.execute("DROP TABLE stg_GFPipes;")

        # upsertTable(entityFrame=getPipes().to_pandas(), tableName='GFPipes')
        print("Pipes updated successfully")

    except Exception as e:
        print(f"Error updating pipes - {error_detailed(e)}")


def getPipes(parentPipeName: str | None = None) -> pl.DataFrame:

    if parentPipeName is not None:
        QueryStr = f"SELECT * from GFPipes_table WHERE ParentPipe = '{parentPipeName}'"
    else:
        QueryStr = "SELECT * from GFPipes_table"

    with duckdb.connect(dbFile) as con:
        # Only needs to be run once per environment
        con.install_extension("sqlite")
        con.load_extension("sqlite")
        con.execute(f"ATTACH '{dbFile}' AS sqlite_db (TYPE sqlite);")

        return pl.DataFrame(data=con.execute(QueryStr).fetchall(),
                            schema=['GFPipeID', 'PipeCode', 'TSP_Name', 'ParentPipe'], orient="row")
