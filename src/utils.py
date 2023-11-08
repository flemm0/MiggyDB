import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.compute as pc
from pyarrow import csv, json
import os
from subprocess import check_output
import re
import subprocess
from pathlib import Path
from collections import defaultdict
import datetime
import ast
import shutil

import polars as pl
import math

from .config import *

pl.Config.set_tbl_hide_dataframe_shape(True)

# Utility functions

def say_hi():
    print("hello!")

def get_all_partitions(database, table_name):
    '''Returns list of all partitions of a table'''
    files = os.listdir(os.path.join(DATA_PATH, database, table_name))
    pattern = f'^{table_name}_\d+\.parquet$'
    return [f for f in files if re.match(pattern, f)]


# Create

def wc(path):
    return int(check_output(['wc', '-l', path]).split()[0])

def create_table_from_csv(path, database, table_name=None):
    '''Create a new table in the database system from input csv file. Processes it in 100 MB chunks'''
    if table_name is None:
        table_name = os.path.splitext(os.path.basename(path))[0]
    table_path = Path(DATA_PATH / database / table_name)
    if not table_path.exists():
        Path.mkdir(table_path)

    total_rows = wc(path)
    n_partitions = math.ceil(os.path.getsize(path) / MAX_PARTITION_SIZE) ## TODO verify this makes 100 MB chunks
    n_rows = int(total_rows / n_partitions)

    for i in range(0, n_partitions):
        skip_rows_after_header = n_rows * i
        data = pl.read_csv(path, n_rows=n_rows, skip_rows_after_header=skip_rows_after_header)
        name = f'{table_name}_{str(i)}.parquet'
        data.write_parquet(os.path.join(table_path, name))

def create_table_from_json(path, database, table_name=None):
    '''Create a new table in the database system from input json file'''
    # check if JSON file is newline-delimited:
    if wc(path) == 0:
        try:
            subprocess.run(f'cat {path} | jq -c \'.[]\' > {path}', shell=True, check=True) # overwrite file with newline-delimited file
        except subprocess.CalledProcessError as e:
            print(f'Command failed with exit code: {e.returncode}')
    if table_name is None:
        table_name = os.path.splitext(os.path.basename(path))[0]
    table_path = os.path.join(DATA_PATH, database, table_name)
    if not os.path.exists(table_path):
        os.makedirs(table_path)

    total_rows = wc(path)
    n_partitions = math.ceil(os.path.getsize(path) / (1024 ** 2) / 100)
    n_rows = int(total_rows / n_partitions)

    for i in range(0, n_partitions):
        data = pl.scan_ndjson(path, batch_size=n_rows)
        name = f'{table_name}_{str(i)}.parquet'
        data.write_parquet(os.path.join(table_path, name))

def infer_datatypes(type):
    '''Returns Polars datatype for creating table schema'''
    type = type.lower()
    if type.startswith('int'):
        return pl.Int64
    elif type.startswith('str'):
        return pl.Utf8
    elif type.startswith('float'):
        return pl.Float64
    elif type == 'datetime':
        return pl.Datetime
    elif type.startswith('date'):
        return pl.Date
    elif type.startswith('bool'):
        return pl.Boolean
    elif type in ['none', 'null']:
        return pl.Null

def create_table_from_cli(database, table_name, schema, primary_key=None, partition=0):
    '''Create new table with schema defined in cli input
    Schema should be a list of tuples
    '''
    table_path = os.path.join(DATA_PATH, database, table_name)
    if not os.path.exists(table_path):
        os.makedirs(table_path)
    data = pl.DataFrame([], schema=schema)
    partition = '_' + str(partition)
    data.write_parquet(os.path.join(table_path, table_name + partition + '.parquet'))

def check_latest_data_partition_size(database, table_name):
    '''Checks most recent parquet file partition size.'''
    partitions = get_all_partitions(database=database, table_name=table_name)
    latest_partition = sorted(partitions)[-1]    
    latest_partition_path = os.path.join(DATA_PATH, database, latest_partition)
    return os.path.getsize(latest_partition_path) <= MAX_PARTITION_SIZE, latest_partition_path

def insert_into(database, table_name, columns, values):
    '''
    Checks most recent parquet file partition size. If it is less than 100 MB, append to the end of the file. Otherwise,
    create a new partition file (If this is the nth partition, the name is: data_n.parquet) and add to that.
    '''
    latest_partition_available, latest_partition_path = check_latest_data_partition_size(database=database, table_name=table_name)
    if latest_partition_available:
        data = pl.read_parquet(latest_partition_path)
        new_data = pl.DataFrame({col: [val] for col, val in zip(columns, values)})
        data.extend(new_data)
        data.write_parquet(latest_partition_path)
    else:
        schema = pl.read_parquet_schema(latest_partition_path)
        data = pl.DataFrame({col: [val] for col, val in zip(columns, values)}, schema=schema)
        data.write_parquet(latest_partition_path)




# Read

'''
Query operations should be as follows:

query order: FROM <table> JOIN <table> WHERE <cond> GROUP BY <cols> HAVING <cond> SELECT <cols> DISTINCT <> ORDER BY <cols> LIMIT/OFFSET <cols>
custom syntax FROM <table> MERGE_WITH <table> COND <cond> GROUP <cols> GROUP_COND <cond> COLS <cols>  

- when a query is made, a directory will be created under the data/temp/ directory with a query id
- for each step of the query, a directory will be created under query id directory where the partitions for query results of each step are stored
- the first directory will store results from the FROM/JOIN query
- the next step will read in results from the directory of the previous step

- queries begin with the FROM or JOIN clauses, so these functions will take a database and table_name as input
- the FROM clause will select the dataset needed and write table in batches to temporary directory
- if this is the end of the query, read in head and tail of dataset in temp directory, print to console, and clear directory
- if this is not the end of the query, return the dataset object pointing to the dataset created in the temp directory
- subsequent steps of the query will operate and overwrite the tables in the temporary directory, until the end of query is reached,
which will cause results to be printed and temporary directory to be cleared

'''

def execute_query(database: str, table_name: str, 
                  join_table_name: str = None, join_col: str = None, # FROM/JOIN
                  filters: list = [],  # WHERE
                  group_col: str = None, agg_col: str = None, agg_func: str = None, # GROUP BY
                  having: bool = None, # HAVING
                  columns: [list] = [], # SELECT (projection)
                  distinct: bool = None, 
                  sort_col: str = None, # TODO add ASC/DESC option
                  limit: bool = None, offset: bool = None):
    '''Master query execution function'''
    step = 0
    query_id = 'query_' + datetime.datetime.now().strftime("%y%m%d_%H%M%S") 
    query_path = Path(TEMP_DB_PATH / query_id)
    if not query_path.exists():
        Path.mkdir(TEMP_DB_PATH / query_id)

    step_dir = f'step_{step}'
    
    current_step_path = Path(query_path / step_dir)
    if not current_step_path.exists():
        Path.mkdir(current_step_path)

    if not join_table_name and not join_col: # both should be None if join is true
        for partition, name in read_table(database=database, table_name=table_name):
            pq.write_table(table=partition, where=(current_step_path / name).with_suffix('.parquet'))
    else: # should execute if select is false
        # partial sort
        Path.mkdir(current_step_path / 'r')
        Path.mkdir(current_step_path / 's')
        for partition, name in partial_sort(prev_step_path=Path(DATA_PATH / database / table_name), sort_col=join_col):
            pq.write_table(table=partition, where=(current_step_path / 'r' / name).with_suffix('.parquet'))
        for partition, name in partial_sort(prev_step_path=Path(DATA_PATH / database / join_table_name), sort_col=join_col):
            pq.write_table(table=partition, where=(current_step_path / 's' / name).with_suffix('.parquet'))

        # merge join
        output_file_path = current_step_path / 'output.txt'
        line_count = 0
        # write algorithm outputs to single txt file
        with open(output_file_path, 'w') as file:
            for data in sort_merge_join((current_step_path / 'r'), (current_step_path / 's'), join_col):
                for tuple_data in data:
                    file.write(str(tuple_data).rstrip() + "\n")
                    line_count += 1
        # convert txt file back to partitioned parquet files
        n_partitions = math.ceil(os.path.getsize(output_file_path) / MAX_PARTITION_SIZE)
        partition_counter = 0
        schema_r = [name + '_x' if name == join_col else name for name in ds.dataset(current_step_path / 'r').schema.names]
        schema_s = [name + '_y' if name == join_col else name for name in ds.dataset(current_step_path / 's').schema.names]
        schema = schema_r + schema_s
        with open(output_file_path, 'r') as file:
            lines = []
            for line in file:
                lines.append(ast.literal_eval(line.rstrip()))
                if len(lines) > line_count // n_partitions:
                    name = f'{table_name}_{join_table_name}_{partition_counter}.parquet'
                    pl.DataFrame(lines, schema=schema).write_parquet(file=(current_step_path / name))
                    partition_counter += 1
                    lines = []
            if lines:
                name = f'{table_name}_{join_table_name}_{partition_counter}.parquet'
                pl.DataFrame(lines, schema=schema).write_parquet(file=(current_step_path / name))
        # clean up directories
        shutil.rmtree(current_step_path / 'r')
        shutil.rmtree(current_step_path / 's')
        os.remove(output_file_path)

    if len(filters):
        prev_step_path = current_step_path
        step += 1
        step_dir = f'step_{step}'
        current_step_path = Path(query_path / step_dir)
        if not current_step_path.exists():
            Path.mkdir(current_step_path)
        for partition, name in filter_rows(prev_step_path=prev_step_path, filters=filters):
            pq.write_table(table=partition, where=(current_step_path / name).with_suffix('.parquet'))
    
    if group_col and agg_col and agg_func:
        prev_step_path = current_step_path
        step += 1
        step_dir = f'step_{step}'
        current_step_path = Path(query_path / step_dir)
        if not current_step_path.exists():
            Path.mkdir(current_step_path)
        # partial sort
        partial_sort_path = Path(current_step_path / 'partial_sorted')
        if not partial_sort_path.exists():
            Path.mkdir(partial_sort_path)

        for partition, name in partial_sort(prev_step_path=prev_step_path, sort_col=group_col):
            pq.write_table(table=partition, where=(current_step_path / 'partial_sorted' / name).with_suffix('.parquet'))

        output_file_path = Path(current_step_path / 'output').with_suffix('.txt')
        if not output_file_path.exists():
            Path.touch(output_file_path)
        line_count = 0
        with open(output_file_path, 'w') as file:
            for data in group_by(prev_step_path=partial_sort_path, group_col=group_col, agg_col=agg_col, agg_func=agg_func):
                file.write(str(data).rstrip() + '\n')
                line_count += 1
        # convert txt file back to partitioned parquet files
        n_partitions = math.ceil(os.path.getsize(output_file_path) / MAX_PARTITION_SIZE)
        partition_counter = 0
        schema = [group_col, f'{agg_func}({agg_col})']
        with open(output_file_path, 'r') as file:
            lines = []
            for line in file:
                lines.append(ast.literal_eval(line.rstrip()))
                if len(lines) > line_count // n_partitions:
                    name = f'{table_name}_{partition_counter}.parquet'
                    pl.DataFrame(lines, schema=schema).write_parquet(file=(current_step_path / name))
            if lines:
                name = f'{table_name}_{partition_counter}.parquet'
                pl.DataFrame(lines, schema=schema).write_parquet(file=(current_step_path / name))
        # clean up partial sorted data
        shutil.rmtree(current_step_path / 'partial_sorted')
        os.remove(output_file_path)

    if len(columns):
        selected_cols = columns[0]
        new_col_names = columns[1] if len(columns[1]) else columns[0] # TODO set to selected_cols if no new names provided

        prev_step_path = current_step_path
        step += 1
        step_dir = f'step_{step}'
        current_step_path = Path(query_path / step_dir)
        if not current_step_path.exists():
            Path.mkdir(current_step_path)
        for partition, name in projection(prev_step_path, selected_cols, new_col_names):
            pq.write_table(table=partition, where=(current_step_path / name).with_suffix('.parquet'))

    if sort_col:
        prev_step_path = current_step_path
        step += 1
        step_dir = f'step_{step}'
        current_step_path = Path(query_path / step_dir)
        if not current_step_path.exists():
            Path.mkdir(current_step_path)
        # partial sort

        partial_sort_path = Path(current_step_path / 'partial_sorted')
        if not partial_sort_path.exists():
            Path.mkdir(partial_sort_path)

        for partition, name in partial_sort(prev_step_path=prev_step_path, sort_col=sort_col):
            pq.write_table(table=partition, where=(current_step_path / 'partial_sorted' / name).with_suffix('.parquet'))
        # merge phase
        for chunk, name in merge_sorted_runs(prev_step_path=partial_sort_path, sort_col=sort_col):
            pq.write_table(table=chunk, where=(current_step_path / name).with_suffix('.parquet'))
        shutil.rmtree(partial_sort_path)

    
    # print data set
    result_dataset = ds.dataset(current_step_path, format='parquet')
    n_rows = result_dataset.count_rows()
    n_cols = len(result_dataset.schema.names)
    # read first partition
    data = pl.read_parquet(result_dataset.files[0])
    print(f'{n_rows} rows\t{n_cols} columns')
    shutil.rmtree(query_path)
    return data


def read_table(database, table_name):
    '''Reads specified table to temporary database'''
    dataset = ds.dataset(DATA_PATH / database / table_name, format='parquet')
    for partition in dataset.files:
        partition = Path(partition)
        data = pq.read_table(partition)
        yield data, partition.stem

def filter_rows(prev_step_path, filters):
    '''
    Reads intermediate query results from prev_query_path and performs filtering on data partitions.
    Yields filtered data partitions and partition name
    '''
    dataset = ds.dataset(prev_step_path, format='parquet')
    for partition in dataset.files:
        partition = Path(partition)
        data = pq.read_table(partition, filters=filters) # list of tuples e.g. ('acousticness', '<', 1)
        yield data, partition.stem

def projection(prev_step_path, selected_cols, new_col_names):
    '''
    Reads intermediate query results from prev_query_path and selects only specified columns. Assigns new column names.
    Yields filtered data partitions and partition name
    '''
    dataset = ds.dataset(prev_step_path, format='parquet')
    for partition in dataset.files:
        partition = Path(partition)
        data = pq.read_table(partition, columns=selected_cols) # list of column names
        data = data.rename_columns(new_col_names)
        yield data, partition.stem

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
        vals = [] # list to hold all values to aggregate
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

def partial_sort(prev_step_path, sort_col):
    '''Sorts each partition of a table sequentially and writes to disk'''
    dataset = ds.dataset(prev_step_path, format='parquet')
    sort_idx = dataset.schema.names.index(sort_col)

    for partition in dataset.files:
        partition = Path(partition)
        data = pl.read_parquet(partition).rows()
        data = [row for row in data if row[sort_idx] is not None] # ignore None values when sorting
        data.sort(key=lambda x: (x[sort_idx] is None, x[sort_idx]))
        data = pl.DataFrame(data, schema=list(pl.read_parquet_schema(partition).keys())).to_arrow()
        yield data, partition.stem

def merge_sorted_runs(prev_step_path, sort_col):
    '''Merge phase of external merge sort'''
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

def sort_merge_join(r_path, s_path, join_col):
    '''r_path and s_path should point to sorted directories of sorted partitioned files'''
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

    r_tuples, s_tuples = [], []

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
        yield [(r + s) for r in r_tuples for s in s_tuples]
        r_tuples, s_tuples = [], []

# Update

def modify(database, table_name, condition, update_val):
    '''
    condition = list of tuples e.g. ('acousticness', '<', 1)
    update_val = data to update with
    '''
    def generate_filter(condition):
        operator, comparison = condition[1], condition[2]
        if operator == '<':
            return lambda x: update_val if x < comparison else x
        elif operator == '<=':
            return lambda x: update_val if x <= comparison else x
        elif operator == '=':
            return lambda x: update_val if x == comparison else x
        elif operator == '!=':
            return lambda x: update_val if x != comparison else x
        elif operator == '>=':
            return lambda x: update_val if x >= comparison else x
        elif operator == '>':
            return lambda x: update_val if x > comparison else x
        
    for partition, name in read_table(database=database, table_name=table_name):
        partition = pl.DataFrame._from_arrow(partition)
        partition = partition.with_columns(pl.col(condition[0]).apply(generate_filter(condition=condition)))
        partition.write_parquet(file=(DATA_PATH / database / table_name / name).with_suffix('.parquet'))


# Delete
def drop_rows(database, table_name, filters):
    for partition, name in filter_rows(prev_step_path=(DATA_PATH / database / table_name), filters=filters):
        pq.write_table(table=partition, where=(DATA_PATH / database / table_name / name).with_suffix('.parquet'))
    negated_filters = []
    for filter in filters:
        if filter[1] == '<':
            negated_filters.append((filter[0], '>=', filter[2]))
        if filter[1] == '>':
            negated_filters.append((filter[0], '<=', filter[2]))
        if filter[1] == '<=':
            negated_filters.append((filter[0], '>', filter[2]))
        if filter[1] == '>=':
            negated_filters.append((filter[0], '<', filter[2]))
        if filter[1] == '=':
            negated_filters.append((filter[0], '!=', filter[2]))
        if filter[1] == '!=':
            negated_filters.append((filter[0], '==', filter[2]))
    for partition, name in filter_rows(prev_step_path=(DATA_PATH / database / table_name), filters=negated_filters):
        if partition.num_rows != 0:
            print('Error: row deletion failed')
            return
    print('Row deletion successfully completed!')

def drop_table(database, table_name):
    '''Removes all partitions of a table from database directory'''
    shutil.rmtree(DATA_PATH / database / table_name)
    if not (DATA_PATH / database / table_name).exists():
        print(f'{table_name} successfully removed')