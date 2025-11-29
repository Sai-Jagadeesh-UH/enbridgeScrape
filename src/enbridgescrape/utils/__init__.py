import pandas as pd

from src.artifacts import error_detailed, dirs
# dumpPipeConfigs

from .pathFile import paths
from .runnerContext import openPage
from .logWriters import logger

# if not (dirs.configFiles / 'PipeConfigs.parquet').exists():
#     dumpPipeConfigs()

pipeConfigs_df = pd.read_parquet(dirs.configFiles / 'PipeConfigs.parquet')

__all__ = ["paths", "openPage", "error_detailed",
           "logger", 'dirs', 'pipeConfigs_df']
