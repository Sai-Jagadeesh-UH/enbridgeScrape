from dataclasses import dataclass
from pathlib import Path


@dataclass
class Paths:

    root = Path('.').resolve()

    src = root / 'src'
    logs = root / 'logs'
    downloads = root / 'downloads' / 'enbridge'

    processed = downloads / 'processed'

    artifacts = src / 'artifacts'
    base = src / 'enbridgescrape'

    # models = src / 'Models'
    # configs = base / 'configs'
    # dbFile = models / "EnbridgeMeta.db"
    # dbName = "EnbridgeMeta.db"


paths = Paths()

paths.logs.mkdir(exist_ok=True, parents=True)
paths.downloads.mkdir(exist_ok=True, parents=True)
