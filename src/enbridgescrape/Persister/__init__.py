# import asyncio

import duckdb

from .handleMeta import metaMunge
# from ..Scraper import metaDump
from ..utils import paths

metaPath = paths.downloads / 'MetaData'

dbFile = paths.dbFile

ParentPipe = 'Enbridge'


with duckdb.connect(paths.dbFile) as con:
    pass


__all__ = ['metaMunge']
