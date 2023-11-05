#%%
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import pyarrow.dataset as ds
import polars as pl

import random

base = Path('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/testing')

random.seed(9)

states = ['CA', 'WA', 'OR', 'UT', 'TX', 'FL', 'MD', 'CO', 'NV', 'AL', 'AK', 'AZ', 'HI', 'KS', 'TN', 'VA', 'WY', 'SC', 'NY', 'RI', 'NH']

table = pl.DataFrame({
    'states': [random.choice(states) for i in range(10000)],
    'purchases': [random.randint(1, 50000) for i in range(10000)]
}).to_arrow()

batch_counter = 1
for batch in table.to_batches(2500):
    name = base / 'step_1' / f'data_{batch_counter}.parquet'
    pl.DataFrame._from_arrow(batch).write_parquet(file=name)
    batch_counter += 1
# %%
def partial_sort(prev_step_path, sort_col):
    dataset = ds.dataset(prev_step_path, format='parquet')
    sort_idx = dataset.schema.names.index(sort_col)

    for partition in dataset.files:
        partition = Path(partition)
        data = pl.read_parquet(partition).rows()
        data.sort(key=lambda x: x[sort_idx])
        data = pl.DataFrame(data, schema=list(pl.read_parquet_schema(partition).keys())).to_arrow()
        yield data, partition.stem

for partition, name in partial_sort('step_1', 'states'):
    pq.write_table(table=partition, where=(base / 'step_2' / 'partial_sorted' / name).with_suffix('.parquet'))
# %%
def group_by(prev_step_path, group_col, agg_col, agg_func):
    dataset = ds.dataset(prev_step_path, format='parquet')
    group_idx = dataset.schema.names.index(group_col)
    agg_idx = dataset.schema.names.index(agg_col)
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

    grouped_data = [] # list to hold output buffer 
    #out_partition_counter = 0
    while any(chunks.values()):
        current_agg_val = min(rows.values(), key=lambda x: x[group_idx])[group_idx]
        vals = []
        for name in rows.keys():
            while rows[name] is not None and rows[name] and rows[name][group_idx] == current_agg_val:
                vals.append(rows[name][agg_idx])
                rows[name] = next(row_iterators[name], None)
                if not rows[name]:
                    chunks[name] = next(chunk_iterators[name], False)
                    if chunks[name] != False:
                        row_iterators[name] = iter([tuple(t.values()) for t in chunks[name].to_pylist()])
                        rows[name] = next(row_iterators[name], False)
                    else:
                        rows[name] = None
                        break
        if agg_func == 'sum':
            yield (current_agg_val, sum(vals))
        elif agg_func == 'count':
            yield (current_agg_val, len(vals))
        elif agg_func == 'average':
            yield (current_agg_val, sum(vals) / len(vals))
        elif agg_func == 'min':
            yield (current_agg_val, min(vals))
        elif agg_func == 'max':
            yield (current_agg_val, max(vals))


with open('output.txt', 'w') as file:
    for data in group_by(base / 'step_2' / 'partial_sorted', 'states', 'purchases', 'average'):
        file.write(str(data).rstrip() + '\n')
# %%
