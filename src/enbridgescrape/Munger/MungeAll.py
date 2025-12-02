import pandas as pd

from ..utils import paths

# from ..Persister import metaMunge

processed_Path = paths.processed
paths.processed.mkdir(exist_ok=True, parents=True)
(paths.downloads / "OA").mkdir(exist_ok=True, parents=True)
(paths.downloads / "OC").mkdir(exist_ok=True, parents=True)
(paths.downloads / "NN").mkdir(exist_ok=True, parents=True)


def processMeta():
    dfList = []
    for filePath in (paths.downloads / "MetaData").iterdir():
        pipeCode = filePath.name.split('_', 2)[0]
        dfList.append(pd.read_csv(filePath).assign(PipeCode=pipeCode))

    pd.concat(dfList).astype(str).to_parquet(
        processed_Path / 'MetaData.parquet')


def processOA():
    return
    for filePath in (paths.downloads / 'OA_raw').iterdir():
        pipeCode = filePath.name.split('_', 2)[0]
        pd.read_csv(filePath)\
            .assign(PipeCode=pipeCode)\
            .to_parquet(paths.downloads / "OA" / filePath.name.replace('.csv', '.parquet'), index=False)


def processOC():
    return
    for filePath in (paths.downloads / 'OC_raw').iterdir():
        pipeCode = filePath.name.split('_', 2)[0]
        with open(filePath) as file:
            EffGasDayTime = file.readline().split('  ')[0][10:]
        try:
            pd.read_csv(filePath,
                        skiprows=1,
                        usecols=['Station Name', 'Cap', 'Nom', 'Cap2'])\
                .assign(PipeCode=pipeCode)\
                .assign(EffGasDate=EffGasDayTime)[['PipeCode', 'EffGasDate', 'Station Name', 'Cap', 'Nom', 'Cap2']]\
                .to_csv(paths.downloads / "OC" / filePath.name, index=False, header=True)
        except Exception as e:
            print(f"Error processing {filePath.name}: {e}")


def processNN():
    # downloads/enbridge/NN_raw/AG_NN_20251128.csv

    for filePath in (paths.downloads / 'NN_raw').iterdir():
        try:
            pipeCode, _, EffGasDayTime = filePath.name.split('_', 2)
            pd.read_csv(filePath)\
                .assign(PipeCode=pipeCode)\
                .to_parquet(paths.downloads / "NN" / filePath.name.replace('.csv', '.parquet'), index=False)

        except Exception as e:
            print(f"Error processing {filePath.name}: {e}")
            continue
