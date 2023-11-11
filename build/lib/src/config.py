from pathlib import Path
from sys import platform

home = Path.home()
DATA_PATH = Path(home / 'miggydb')
TEMP_DB_PATH = Path(DATA_PATH / 'temp')

MAX_PARTITION_SIZE = 100 * 1024 * 1024