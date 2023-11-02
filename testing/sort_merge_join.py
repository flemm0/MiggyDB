# %%
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import pyarrow.dataset as ds
import polars as pl

import random
import names

base = Path('/Users/candicewu/USC_Fall_2023/DSCI551-Final_Project/testing/')

# create data

random.seed(9)

ids = [random.randint(1, 10000) for i in range(1000)]

table1 = pl.DataFrame({'id': ids, 'name': [names.get_first_name() for i in range(1000)]}).to_arrow()
table2 = pl.DataFrame({'department': [
        random.choice(['HR', 'Engineering', 'Management', 'Janitorial', 'Sales', 'IT']) for i in range(1000)
    ],'id': ids}).to_arrow()

for num, batch in enumerate(table1.to_batches(table1.num_rows / 4)):
    df = pl.DataFrame._from_arrow(batch)
    df.write_parquet(f'employees/employees_{num}.parquet')

for num, batch in enumerate(table2.to_batches(table2.num_rows / 4)):
    df = pl.DataFrame._from_arrow(batch)
    df.write_parquet(f'departments/departments_{num}.parquet')
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

for partition, name in partial_sort('employees', 'id'):
    pq.write_table(table=partition, where=(base / 'step_1/' / 'r' / name).with_suffix('.parquet'))

for partition, name in partial_sort('departments', 'id'):
    pq.write_table(table=partition, where=(base / 'step_1/' / 's' / name).with_suffix('.parquet'))

#%%
def sort_merge_join(r_path, s_path, join_col):
    dataset_r = ds.dataset(r_path, format='parquet')
    join_idx_r = dataset_r.schema.names.index(join_col)
    dataset_s = ds.dataset(s_path, format='parquet')
    join_idx_s = dataset_s.schema.names.index(join_col)

    n_buffers = len(dataset_r.files) + len(dataset_s.files) + 1

    chunk_iterators_r, chunk_iterators_s = {}, {}
    for partition_r, partition_s in zip(dataset_r.files, dataset_s.files):
        pf_r, pf_s = pq.ParquetFile(partition_r), pq.ParquetFile(partition_s)
        num_rows_r, num_rows_s = pf_r.metadata.num_rows, pf_s.metadata.num_rows
        chunk_iterators_r[Path(partition_r).stem] = pf_r.iter_batches(batch_size=num_rows_r // n_buffers)
        chunk_iterators_s[Path(partition_s).stem] = pf_s.iter_batches(batch_size=num_rows_s // n_buffers)
    
    chunks_r = {
        name: next(chunk_iterator_r, False) for name, chunk_iterator_r in chunk_iterators_r.items()
    }
    chunks_s = {
        name: next(chunk_iterator_s, False) for name, chunk_iterator_s in chunk_iterators_s.items()
    }

    row_iterator_r = {
        name: iter([tuple(t.values()) for t in chunk.to_pylist()]) for name, chunk in chunks_r.items()
    }
    row_iterator_s = {
        name: iter([tuple(t.values()) for t in chunk.to_pylist()]) for name, chunk in chunks_s.items()
    }

    rows_r = {name: next(row_iterator, None) for name, row_iterator in row_iterator_r.items()}
    rows_s = {name: next(row_iterator, None) for name, row_iterator in row_iterator_s.items()}

    joined_data, r_tuples, s_tuples = [], [], []

    while any(chunks_r.values()) and any(chunks_s.values()):
        min_value = min(min([val for val in rows_r.values() if val], key=lambda x: x[join_idx_r])[join_idx_r], min([val for val in rows_s.values() if val], key=lambda x: x[join_idx_s])[join_idx_s])
        for name in rows_r.keys():
            while rows_r[name] is not None and rows_r[name] and rows_r[name][join_idx_r] == min_value: 
                r_tuples.append(rows_r[name])
                rows_r[name] = next(row_iterator_r[name], None)
                if not rows_r[name]:
                    chunks_r[name] = next(chunk_iterators_r[name], False)
                    if chunks_r[name] != False:
                        row_iterator_r[name] = iter([tuple(t.values()) for t in chunks_r[name].to_pylist()])
                        rows_r[name] = next(row_iterator_r[name], False)
                    else:
                        rows_r[name]
                        break
        for name in rows_s.keys():
            while rows_s[name] is not None and rows_s[name][join_idx_s] == min_value:
                s_tuples.append(rows_s[name])
                rows_s[name] = next(row_iterator_s[name], None)
                if not rows_s[name]:
                    chunks_s[name] = next(chunk_iterators_s[name], False)
                    if chunks_s[name] != False:
                        row_iterator_s[name] = iter([tuple(t.values()) for t in chunks_s[name].to_pylist()])
                        rows_s[name] = next(row_iterator_s[name], False)
                    else:
                        rows_s[name] = None
                        break
        #joined_data.append([(r + s) for r in r_tuples for s in s_tuples])
        yield [(r + s) for r in r_tuples for s in s_tuples]
        r_tuples, s_tuples = [], []

    #return joined_data

#sort_merge_join('./step_1/r', './step_1/s', 'id')
#%%
output_file_path = 'output.txt'
with open(output_file_path, 'w') as file:
    for data in sort_merge_join('./step_1/r', './step_1/s', 'id'):
        for tuple_data in data:
            file.write(str(tuple_data).rstrip() + "\n")

# %%
# pyarrow.parquet.ParquetWriter -- incrementally writing to parquet file

def append_to_parquet_table(dataframe, filepath=None, writer=None):
    table = dataframe.to_arrow()
    if writer is None:
        writer = pq.ParquetWriter(filepath, table.schema)
    writer.write_table(table=table)
    return writer

table1 = pl.DataFrame({'one': [-1, 2, 2.5], 'two': ['foo', 'bar', 'baz'], 'three': [True, False, True]})
table2 = pl.DataFrame({'one': [-1, 2, 2.5], 'two': ['foo', 'bar', 'baz'], 'three': [True, False, True]})
table3 = pl.DataFrame({'one': [-1, 2, 2.5], 'two': ['foo', 'bar', 'baz'], 'three': [True, False, True]})
writer = None
filepath = 'test.parquet'
table_list = [table1, table2, table3]

for table in table_list:
    writer = append_to_parquet_table(table, filepath, writer)

if writer:
    writer.close()

df = pl.read_parquet(filepath)
print(df)
