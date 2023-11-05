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

df1.write_parquet('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/testing/step_1/data_1.parquet')
df2.write_parquet('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/testing/step_1/data_2.parquet')
df3.write_parquet('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/testing/step_1/data_3.parquet')
df4.write_parquet('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/testing/step_1/data_4.parquet')

#%%
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from pathlib import Path
import pyarrow.dataset as ds

# linux
base = Path('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/testing')

# mac
#base = Path('/Users/candicewu/USC_Fall_2023/DSCI551-Final_Project/testing/')

dataset = ds.dataset(base / 'step_1', format='parquet')

sort_col = 'age'
# %%

# sort

def partial_sort(prev_step_path, sort_col):
    dataset = ds.dataset(prev_step_path, format='parquet')
    sort_idx = dataset.schema.names.index(sort_col)

    for partition in dataset.files:
        partition = Path(partition)
        data = pl.read_parquet(partition).rows()
        data.sort(key=lambda x: x[sort_idx])
        data = pl.DataFrame(data, schema=list(pl.read_parquet_schema(partition).keys())).to_arrow()
        yield data, partition.stem

for partition, name in partial_sort('./step_1/', 'age'):
    pq.write_table(table=partition, where=(base / 'step_2' / 'partial_sorted' / name).with_suffix('.parquet'))

# %%

# merge
# 100 MB is the memory limit
# use 1 KB (1024 bytes) as limit for this example

# dataset = ds.dataset(base / 'step_2', format='parquet')

def merge_sorted_runs(prev_step_path, sort_col):
    dataset = ds.dataset(prev_step_path, format='parquet')
    sort_idx = dataset.schema.names.index(sort_col)
    n_buffers = len(dataset.files) + 1 # add 1 for output buffer
    out_buffer_len = dataset.count_rows() // len(dataset.files)

    # create iterators of each of the files
    chunk_iterators = {}
    for partition in dataset.files:
        pf = pq.ParquetFile(partition)
        num_rows = pf.metadata.num_rows
        chunk_iterators[Path(partition).stem] = pf.iter_batches(batch_size=num_rows // n_buffers)
    out_partition_names = list(chunk_iterators.keys())
    # store chunks of iterators in dictionary
    chunks = {
        name: next(chunk_iterator, False) for name, chunk_iterator in chunk_iterators.items()
    }

    # create tuple iterators for individual rows 
    row_iterators = {
        name: iter([tuple(t.values()) for t in chunk.to_pylist()]) for name, chunk in chunks.items()
    }

    # dict of tuples, 1 for each chunk
    rows = { 
        name: next(row_iterator, None) for name, row_iterator in row_iterators.items()
     } 
    
    merged_data = [] # list to hold output buffer
    out_partition_counter = 0
    while any(chunks.values()):
        min_tuple = min(rows.values(), key=lambda x: x[sort_idx])
        min_chunk = list(filter(lambda x: rows[x] == min_tuple, rows))[0]
        merged_data.append(min_tuple)
        # write to disk if output buffer is full
        if len(merged_data) > out_buffer_len:
          yield pl.DataFrame(merged_data, schema=dataset.schema.names).to_arrow(), out_partition_names[out_partition_counter]
          out_partition_counter += 1
          merged_data = []
        rows[min_chunk] = next(row_iterators[min_chunk], None)
        if not rows[min_chunk]:
            # update chunks dict, load next chunk
            chunks[min_chunk] = next(chunk_iterators[min_chunk], False)
            # if still chunks to iterate, load next
            if chunks[min_chunk] != False:
                # update row_iterators dict - create new tuple iterator
                row_iterators[min_chunk] = iter([tuple(t.values()) for t in chunks[min_chunk].to_pylist()])
                # update rows_dict
                rows[min_chunk] = next(row_iterators[min_chunk], False)
            # if no more chunks, remove from dict
            else:
                del rows[min_chunk]

    yield pl.DataFrame(merged_data, schema=dataset.schema.names).to_arrow(), out_partition_names[out_partition_counter]

for chunk, name in merge_sorted_runs(Path(base / 'step_2' / 'partial_sorted'), 'age'):
    pq.write_table(table=chunk, where=(base / 'step_2' / name).with_suffix('.parquet'))

#%%
# check
dataset1 = ds.dataset(base / "step_1", format="parquet")
table1 = dataset1
#table1.take([0,1,2,3])

dataset2 = ds.dataset(base / 'step_3', format='parquet')
table2 = dataset2.to_table()

table2
# %%
pl.DataFrame._from_arrow(table2)
# %%
