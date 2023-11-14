#%%
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import polars as pl
from pathlib import Path

offset, limit = 1, None

dataset = ds.dataset('~/miggydb/test/audio_features', format='parquet')

if offset is not None or limit is not None:
xc
# %%
