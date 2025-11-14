import duckdb

from ..utils import paths


with duckdb.connect(paths.dbFile) as con:
    pass
    # con.execute("""
    #     CREATE TABLE IF NOT EXISTS GFPipes_table (
    #         GFPipeID INTEGER PRIMARY KEY DEFAULT nextval('GFPipeID_sequence'),
    #         TSP INTEGER UNIQUE NOT NULL,
    #         TSP_Name VARCHAR UNIQUE NOT NULL
    #     );
    # """)
