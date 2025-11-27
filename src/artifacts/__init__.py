from dotenv import load_dotenv

from .dirsFile import dirs
from .ModelKeeper import updatePipes, getPipes, conect_db
from .detLog import error_detailed


# load_dotenv(dirs.artifacts / '.env')


__all__ = ["dirs", 'updatePipes', 'getPipes', 'error_detailed', 'conect_db']
