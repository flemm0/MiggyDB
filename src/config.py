from pathlib import Path

DATA_PATH = Path('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/data/')
TEST_DB_PATH = Path(DATA_PATH / 'test')
TEMP_DB_PATH = Path(DATA_PATH / 'temp')

MAX_PARTITION_SIZE = 100 * 1024 * 1024