# import asyncio

# import duckdb

from .handleMeta import metaMunge
# from ..Scraper import metaDump
from ..utils import paths
from src.artifacts import conect_db

metaPath = paths.downloads / 'MetaData'

dbFile = paths.dbFile

ParentPipe = 'Enbridge'


with conect_db(paths.dbName) as con:
    pass


__all__ = ['metaMunge']
