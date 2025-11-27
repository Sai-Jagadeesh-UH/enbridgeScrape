import pandas as pd

from ..utils import paths

# from ..Persister import metaMunge

processed_Path = paths.processed
paths.processed.mkdir(exist_ok=True, parents=True)


def processMeta():
    dfList = []
    for filePath in (paths.downloads / "MetaData").iterdir():
        pipeCode = filePath.name.split('_', 2)[0]
        dfList.append(pd.read_csv(filePath).assign(PipeCode=pipeCode))

    pd.concat(dfList).astype(str).to_parquet(
        processed_Path / 'MetaData.parquet')


def processOA():
    for filePath in (paths.downloads / "OA").iterdir():
        pipeCode = filePath.name.split('_', 2)[0]
        pd.read_csv(filePath)\
            .assign(PipeCode=pipeCode)\
            .to_csv(filePath, index=False, header=True)


def processOC():
    for filePath in (paths.downloads / "OC").iterdir():
        pipeCode = filePath.name.split('_', 2)[0]
        with open(filePath) as file:
            EffGasDayTime = file.readline().split('  ')[0][10:]
        try:
            pd.read_csv(filePath,
                        skiprows=1,
                        usecols=['Station Name', 'Cap', 'Nom', 'Cap2'])\
                .assign(PipeCode=pipeCode)\
                .assign(EffGasDate=EffGasDayTime)[['PipeCode', 'EffGasDate', 'Station Name', 'Cap', 'Nom', 'Cap2']]\
                .to_csv(filePath, index=False, header=True)
        except Exception as e:
            print(f"Error processing {filePath.name}: {e}")
