from pathlib import Path
from sys import platform
# if platform == "linux" or platform == "linux2":
#     DATA_PATH = Path('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/data/')
# elif platform == "darwin":
#     DATA_PATH = Path('/Users/candicewu/USC_Fall_2023/DSCI551-Final_Project/data')

# TEST_DB_PATH = Path(DATA_PATH / 'test')

home = Path.home()
DATA_PATH = Path(home / 'miggydb')
TEMP_DB_PATH = Path(DATA_PATH / 'temp')

MAX_PARTITION_SIZE = 100 * 1024 * 1024