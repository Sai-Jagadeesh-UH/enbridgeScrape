from dataclasses import dataclass
from pathlib import Path


@dataclass
class Paths:

    root = Path('.').resolve()

    src = root / 'src'
    logs = root / 'logs'
    downloads = root / 'downloads' / 'enbridge'

    artifacts = src / 'artifacts'
    models = src / 'Models'
    base = src / 'enbridgescrape'
    configs = base / 'configs'
    dbFile = models / "EnbridgeMeta.db"


paths = Paths()
