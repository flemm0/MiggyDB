#%%

# create dummy data
import polars as pl
import names
import random
import os

## setup
random.seed(999)

df1 = pl.DataFrame({
    'name': [names.get_first_name() for i in range(1000)],
    'age': [random.randint(0, 100) for i in range(1000)]
})
df2 = pl.DataFrame({
    'name': [names.get_first_name() for i in range(1000)],
    'age': [random.randint(0, 100) for i in range(1000)]
})
df3 = pl.DataFrame({
    'name': [names.get_first_name() for i in range(1000)],
    'age': [random.randint(0, 100) for i in range(1000)]
})
df4 = pl.DataFrame({
    'name': [names.get_first_name() for i in range(1000)],
    'age': [random.randint(0, 100) for i in range(1000)]
})

df1.write_parquet('/Users/candicewu/USC_Fall_2023/DSCI551-Final_Project/testing/step_1/data_1.parquet')
df2.write_parquet('/Users/candicewu/USC_Fall_2023/DSCI551-Final_Project/testing/step_1/data_2.parquet')
df3.write_parquet('/Users/candicewu/USC_Fall_2023/DSCI551-Final_Project/testing/step_1/data_3.parquet')
df4.write_parquet('/Users/candicewu/USC_Fall_2023/DSCI551-Final_Project/testing/step_1/data_4.parquet')

#%%
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from pathlib import Path
import pyarrow.dataset as ds

# linux
# base = Path('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/testing')

# mac
base = Path('/Users/candicewu/USC_Fall_2023/DSCI551-Final_Project/testing/')

dataset = ds.dataset(base / 'step_1', format='parquet')

sort_col = 'age'
# %%

# sort

def partial_sort(prev_step_path, sort_col):
    dataset = ds.dataset(prev_step_path, format='parquet')

    for partition in dataset.files:
        partition = Path(partition)
        data = pl.read_parquet(partition)
        idx = data.columns.index(sort_col)
        data = data.rows()
        data.sort(key=lambda x: x[idx])
        data = pl.DataFrame(data, schema=list(pl.read_parquet_schema(partition).keys())).to_arrow()
        yield data, partition.stem

for partition, name in partial_sort('./step_1/', 'age'):
    pq.write_table(table=partition, where=(base / 'step_2' / name).with_suffix('.parquet'))

# %%

# merge
# 100 MB is the memory limit
# use 1 KB (1024 bytes) as limit for this example

# dataset = ds.dataset(base / 'step_2', format='parquet')

def merge_sorted_runs(prev_step_path, sort_col):
    dataset = ds.dataset(prev_step_path, format='parquet')
    n_buffers = len(dataset.files) + 1 # add 1 for output buffer

    # create iterators of each of the files
    iterators = {}
    for partition in dataset.files:
        pf = pq.ParquetFile(partition)
        num_rows = pf.metadata.num_rows
        iterators[Path(partition).stem] = pf.iter_batches(batch_size=num_rows // n_buffers)

    # store chunks of iterators in dictionary
    chunks = {}
    for name, iterator in iterators.items():
        chunks[name] = next(iterator, False)

    # create tuple iterators for individual rows
    # TODO change to use dictionary like the chunk iterator
    row_iterators = [iter([tuple(r.values()) for r in c.to_pylist()]) for c in chunks.values()]
    rows = [next(row_iterator, None) for row_iterator in row_iterators] # list of tuples, 1 for each chunk
    merged_data = []

    while any(chunks.values()):
        min_tuple = min(rows, key=lambda x: x[1])
        min_index = rows.index(min_tuple)
        merged_data.append(min_tuple)
        # if len(merged_data) > min([len(chunk) for chunk in chunks.values]):
        #   write_table()
        rows[min_index] = next(row_iterators[min_index], None)
        if not rows[min_index]:
            pass
            # call `next()` on the chunk to load next chunk

    # add logic to check size of merged dataset, and write to disk once reached

    print(merged_data)
# %%

# check
dataset = ds.dataset(base / "step_2", format="parquet")

dataset.to_table().sort_by('age').to_pandas().head(10)

# %%
