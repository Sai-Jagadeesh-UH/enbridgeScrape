from pathlib import Path
from dataclasses import dataclass


@dataclass
class Directories:
    root = Path('.').resolve()

    src = root / 'src'
    artifacts = src / 'artifacts'

    downloads = root / 'downloads'
    logs = root / 'logs'

    models = src / 'Models'


dirs = Directories()

dirs.logs.mkdir(exist_ok=True, parents=True)
dirs.downloads.mkdir(exist_ok=True, parents=True)
dirs.models.mkdir(exist_ok=True, parents=True)
