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
    models = src / 'Models'
    base = src / 'enbridgescrape'
    configs = base / 'configs'
    dbFile = models / "EnbridgeMeta.db"
    dbName = "EnbridgeMeta.db"


paths = Paths()
