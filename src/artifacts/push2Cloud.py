import os
from typing import Callable

from contextlib import contextmanager

import pandas as pd

from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient, TableClient

os.getenv("ARCHIVE_STORAGE_CONSTR", '')
os.getenv("ARCHIVE_STORAGE_KEY")
