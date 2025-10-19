import yaml


with open(
    r"/home/sai/Documents/Coding/Python/enbridgeScrape/src/enbridgescrape/configs/pipeMaps.yml",
    "r",
) as file:
    pipesMap: dict = yaml.safe_load(file)


with open(
    r"/home/sai/Documents/Coding/Python/enbridgeScrape/src/enbridgescrape/configs/configs.yml",
    "r",
) as file:
    pipeConfigs: dict = yaml.safe_load(file)
