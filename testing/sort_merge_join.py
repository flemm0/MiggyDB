# %%
import pyarrow as pa
from pathlib import Path
import pyarrow.dataset as ds
import polars as pl
from sys import getsizeof

table1 = pa.Table.from_arrays(
    [
        pa.array(['foo', 'bar', 'foobar', 'baz', 'fizz', 'buzz']),
        pa.array([22, 28, 31, 31, 44, 58])
    ], 
    names=['usr', 'id']
)

table2 = pa.Table.from_arrays(
    [
        pa.array([28, 28, 31, 31, 42, 58]),
        pa.array([103, 104, 101, 102, 142, 107])
    ], 
    names=['id', 'pid']
)
#%%
def get_scalar(table: pa.Table, col: str, idx: int):
    return table.column(col)[idx].as_py()

def concat_tuples(table1: pa.Table, idx1, table2: pa.Table, idx2,):
    l_row = tuple(table1.slice(offset=idx1, length=1).to_pylist()[0].values())
    r_row = tuple(table2.slice(offset=idx2, length=1).to_pylist()[0].values())
    return l_row + r_row # TODO change to eliminate duplicate columns?

def sort_merge_join(table1: pa.Table, table2: pa.Table, merge_col: str):
    result = []
    mark = None
    l, r = 0, 0
    while l < table1.num_rows and r < table2.num_rows:
        if not mark:
            while get_scalar(table1, merge_col, l) < get_scalar(table2, merge_col, r):
                l += 1
            while get_scalar(table1, merge_col, l) > get_scalar(table2, merge_col, r):
                r += 1
            mark = r
        if get_scalar(table1, merge_col, l) == get_scalar(table2, merge_col, r):
            result.append(concat_tuples(table1, l, table2, r))
            r += 1
        else:
            r = mark
            l += 1
            mark = None
    return result

sort_merge_join(table1, table2, merge_col='id')
# %%
'''
out-of-memory sort-merge join requires 3 buffers:
1 for left table
1 for right table
1 for output buffer
the output buffer should be double the size of an input buffer, 
since it'll hold combined tuples from left and right

left_nrows / 4, right_nrows / 4
'''

def sort_merge_join(dataset1: ds.Dataset, dataset2: ds.Dataset, merge_col: str):
    l_n_files = len(dataset1.files)
    l_total_rows = dataset1.count_rows()
    iterator_1 = dataset1.to_batches(batch_size=(l_total_rows // l_n_files) // 4)

    r_n_files = len(dataset2.files)
    r_total_rows = dataset2.count_rows()
    iterator_2 = dataset2.to_batches(batch_size=(r_total_rows // r_n_files) // 4)

    l_buffer = next(iterator_1, False)
    r_buffer = next(iterator_2, False)
    result = []
    mark = None
    l, r = 0, 0

    while l_buffer and r_buffer:
        if not mark: 
            while get_scalar(l_buffer, merge_col, l) < get_scalar(r_buffer, merge_col, r):
                l += 1
                if l >= len(l_buffer):
                    l_buffer = next(iterator_1, False)
                    l = 0
            while get_scalar(l_buffer, merge_col, l) > get_scalar(r_buffer, merge_col, r):
                r += 1
                if r >= len(r_buffer):
                    r_buffer = next(iterator_2, False)
                    r = 0
            mark = r
        if get_scalar(l_buffer, merge_col, l) == get_scalar(r_buffer, merge_col, r):
            result.append(concat_tuples(table1, l, table2, r))
            if getsizeof(result) > 50 * (2 ** 20):
                pl.DataFrame(result).write_parquet('./step_3/')
            r += 1
            if r >= len(r_buffer):
                r_buffer = next(iterator_2, False)
                r = 0
        else:
            r = mark
            l += 1
            if l >= len(l_buffer):
                l_buffer = next(iterator_1, False)
                l = 0
            mark = None
    
#%%

base = Path('/Users/candicewu/USC_Fall_2023/DSCI551-Final_Project/testing')
dataset = ds.dataset(base / 'step_1', format='parquet')

iterator = dataset.to_batches(batch_size=2)
data = next(iterator)

# %%

## nested-loop join
import pyarrow.parquet as pq
import polars as pl

def nested_loop_join(prev_step_path_r, prev_step_path_s, join_col):
    dataset_r = ds.dataset(prev_step_path_r, format='parquet')
    dataset_s = ds.dataset(prev_step_path_s, format='parquet')


    chunk_iterators_r, chunk_iterators_s = {}, {}
    for partition_r, partition_s in zip(dataset_r.files, dataset_s.files):
        pf_r, pf_s = pq.ParquetFile(partition_r), pq.ParquetFile(partition_s)
        pass



# [(r + s) for r in l1 for s in l2]
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

# %%
