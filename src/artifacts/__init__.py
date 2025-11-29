from dotenv import load_dotenv

from .dirsFile import dirs
from .detLog import error_detailed
from .azureDump import dumpPipeConfigs
from .BaseLogWriters import baseLogger


load_dotenv(dirs.root / 'archives/.env')


dumpPipeConfigs()


__all__ = ["dirs", 'error_detailed', 'baseLogger']
