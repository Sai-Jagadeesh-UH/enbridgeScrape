from dataclasses import dataclass
from ...artifacts import dirs


@dataclass
class Paths:
    root = dirs.root
    src = dirs.src
    logs = dirs.logs
    models = dirs.models

    downloads = dirs.downloads / 'enbridge'

    base = src / 'enbridgescrape'
    configs = base / 'configs'

    dbFile = models / "EnbridgeMeta.db"


paths = Paths()
