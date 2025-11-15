from dotenv import load_dotenv

from .dirsFile import dirs
from .ModelKeeper import updatePipes, getPipes
from .detLog import error_detailed


load_dotenv(dirs.artifacts / '.env')


__all__ = ["dirs", 'updatePipes', 'getPipes', 'error_detailed']
