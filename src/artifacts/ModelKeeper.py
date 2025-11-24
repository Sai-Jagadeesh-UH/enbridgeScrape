import duckdb
import sqlite3
import contextlib

from .dirsFile import dirs
from .push2Cloud import upsertTable
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
        TSP INTEGER UNIQUE NOT NULL,
        TSP_Name VARCHAR UNIQUE NOT NULL,
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

            # cur = conn.cursor()

            # 1. Write the DataFrame to a temporary staging table
            #    If the table exists, replace it with the new data
            df.to_sql('stg_GFPipes', con, if_exists='replace', index=False)

            con.execute("""
                INSERT INTO GFPipes_table (TSP, TSP_Name, ParentPipe)
                SELECT 
                    stg.TSP, 
                    stg.TSP_Name, 
                    ? AS ParentPipeValue
                FROM 
                    stg_GFPipes AS stg
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM GFPipes_table AS final
                    WHERE final.TSP = stg.TSP
                );
            """, (parentPipeName,))

            # 2. Perform the UPSERT from the staging table to the main table using a single statement
            #    ON CONFLICT (TSP) DO NOTHING means if a row with the same TSP already exists, do nothing
            # con.execute(f"""
            #     INSERT INTO GFPipes_table (TSP, TSP_Name, ParentPipe)
            #     SELECT TSP, TSP_Name, '{parentPipeName}'
            #     FROM stg_GFPipes
            #     WHERE true
            #     ON CONFLICT(TSP) DO NOTHING;
            # """)

            # 3. Drop the staging table after the operation is complete
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
                            schema=['GFPipeID', 'TSP', 'TSP_Name', 'ParentPipe'], orient="row")
        # .select(['GFPipeID', 'TSP', 'TSP_Name', 'ParentPipe'])

    # Install and load the SQLite extension (autoloaded on first use, but explicit is clear)
    # duck_con.install_extension("sqlite") # Only needs to be run once per environment
    # duck_con.load_extension("sqlite")

    # Attach the SQLite database file to DuckDB
    # The 'sqlite:' prefix or 'TYPE sqlite' specifies the extension
    # duck_con.execute(f"ATTACH '{db_path}' AS sqlite_db (TYPE sqlite);")
