import yaml

from .pathFile import paths
from .runner import openPage
from ...artifacts import error_detailed
from .logWriters import logger

with open(paths.configs / r"pipeMaps.yml",    "r") as file:
    code2name: dict = yaml.safe_load(file)

with open(paths.configs / r"configs.yml",    "r") as file:
    code2seg: dict = yaml.safe_load(file)

with open(paths.configs / r"metaCodes.yml",    "r") as file:
    metacodes = yaml.safe_load(file)


__all__ = ["paths", "openPage", "error_detailed",
           "code2name", "code2seg", "metacodes", "logger"]
