#%%
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from pathlib import Path
import pyarrow.dataset as ds

base = Path('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/testing')

dataset = ds.dataset(base / 'step_1', format='parquet')
# %%

# sort
step_2_dir = base / 'step_2'

for file in dataset.files:
    data = pl.read_parquet(file).rows()
    data.sort(key=lambda x: x[1])
    data = pl.DataFrame(data, schema=list(pl.read_parquet_schema(file).keys()))
    out_file = file.split('.')[0] + '_sorted.parquet'
    data.write_parquet(step_2_dir / out_file)

# %%

# merge
# 100 MB is the memory limit
dataset = ds.dataset(base / 'step_2', format='parquet')
n_buffers = len(dataset.files) + 1

sort_col = 'age'

# create iterators of each of the files
iterators = {}
for idx, file in enumerate(dataset.files):
    pf = pq.ParquetFile(file)
    num_rows = pf.metadata.num_rows
    iterators[Path(file).stem] = pf.iter_batches(batch_size=num_rows // n_buffers)

chunk_1 = next(iterators['data_1_sorted'], False)
chunk_2 = next(iterators['data_2_sorted'], False)
chunk_3 = next(iterators['data_3_sorted'], False)
chunk_4 = next(iterators['data_4_sorted'], False)

merged_data = []
i = j = k = l = 0
while chunk_1 and chunk_2 and chunk_3 and chunk_4:
    current_min = min(
        chunk_1.column(sort_col)[i].as_py(),
        chunk_2.column(sort_col)[j].as_py(),
        chunk_3.column(sort_col)[k].as_py(),
        chunk_4.column(sort_col)[l].as_py(),
    )
    if current_min == (chunk_1.column('age')[i].as_py()):
        merged_data.append(pl.from_arrow(chunk_1).rows()[i])
        i += 1
        if i >= len(chunk_1):
            chunk_1 = next(iterators['data_1_sorted'], False)
            i = 0
    elif current_min == (chunk_2.column('age')[j].as_py()):
        merged_data.append(pl.from_arrow(chunk_2).rows()[j])
        j += 1
        if j >= len(chunk_2):
            chunk_2 = next(iterators['data_2_sorted'], False)
            j = 0
    elif current_min == (chunk_3.column('age')[k].as_py()):
        merged_data.append(pl.from_arrow(chunk_3).rows()[k])
        k += 1
        if k >= len(chunk_3):
            chunk_3 = next(iterators['data_3_sorted'], False)
            k = 0
    else:
        merged_data.append(pl.from_arrow(chunk_4).rows()[l])
        l += 1
        if l >= len(chunk_4):
            chunk_4 = next(iterators['data_4_sorted'], False)
            l = 0

    # add logic to check size of merged dataset, and write to disk once reached

print(merged_data)
# %%
dataset = ds.dataset(base / "step_2", format="parquet")

dataset.to_table().sort_by('age').to_pandas().head(10)

# %%
