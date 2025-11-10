from pathlib import Path
from dataclasses import dataclass


@dataclass
class Paths:
    root = Path('.').resolve()

    src = root / 'src'
    archive = root / 'archive'
    downloads = root / 'downloads' / 'enbridge'

    base = src / 'enbridgescrape'

    configs = base / 'configs'
    utils = base / 'utils'


paths = Paths()
