import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pyarrow import csv, json
import os
from subprocess import check_output
import re
import subprocess
import pathlib

import polars as pl
import math

pl.Config.set_tbl_hide_dataframe_shape(True)

DATA_PATH = '/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/data/'
TEST_DB_PATH = os.path.join(DATA_PATH, 'test')

MAX_PARTITION_SIZE = 100 * 1024 * 1024

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
    table_path = os.path.join(DATA_PATH, database, table_name)
    if not os.path.exists(table_path):
        os.makedirs(table_path)

    total_rows = wc(path)
    n_partitions = math.ceil(os.path.getsize(path) / (1024 ** 2) / 100) ## TODO verify this makes 100 MB chunks
    n_rows = int(total_rows / n_partitions)

    for i in range(0, n_partitions):
        skip_rows_after_header = n_rows * i
        data = pl.read_csv(path, n_rows=n_rows, skip_rows_after_header=skip_rows_after_header)
        fname = table_name + '_' + str(i) + '.parquet'
        data.write_parquet(os.path.join(table_path, fname))

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
        fname = table_name + '_' + str(i) + '.parquet'
        data.write_parquet(os.path.join(table_path, fname))

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

def read_full_table(database, table_name):
    '''Queries table
    
    Since data is expected to be larger than the memory limit, flush results of query to /temp directory
    when 100 MB data limit is reached
    '''
    base = pathlib.Path(os.path.join(DATA_PATH, database))
    dataset = ds.dataset(base / table_name, format='parquet')
    n_rows = dataset.count_rows()
    n_cols = dataset.head(1).num_columns
    
    if len(dataset.files) == 1: # if only 1 partition, read it all in as it fits into the memory limits
        data = pl.DataFrame._from_arrow(pq.read_table(dataset.files[0]))
    else: # otherwise, read in first half of first partition, and last half of last partition
        head_pf = pq.ParquetFile(dataset.files[0])
        data = pl.DataFrame._from_arrow(head_pf.read_row_group(i=1)) # read first row group
        tail_pf = pq.ParquetFile(dataset.files[-1])
        data.extend(pl.DataFrame._from_arrow(tail_pf.read_row_group(i=tail_pf.metadata.num_row_groups - 1)))
    
    print(f'{n_rows} rows\t{n_cols} columns')
    print(data)

def write_query_to_file():
    pass

def projection(database, table_name, columns, new_column_names):
    '''Queries a subset of columns
    
    Should be fast, parquet files are columnar-files and are optimized to read in columns.
    '''
    base = pathlib.Path(os.path.join(DATA_PATH, database))
    dataset = ds.dataset(base / table_name, format='parquet')
    n_rows = dataset.count_rows()
    n_cols = len(columns)
    
    if len(dataset.files) == 1: # if only 1 partition, read it all in as it fits into the memory limits
        data = pl.DataFrame._from_arrow(
            pq.read_table(dataset.files[0], columns=columns),
            schema=new_column_names
        )
    else: # otherwise, read in first half of first partition, and last half of last partition
        head_pf = pq.ParquetFile(dataset.files[0])
        data = pl.DataFrame._from_arrow( # read first row group
            head_pf.read_row_group(i=1, columns=columns),
            schema=new_column_names
        ) 
        tail_pf = pq.ParquetFile(dataset.files[-1])
        data.extend(pl.DataFrame._from_arrow(
            tail_pf.read_row_group(i=tail_pf.metadata.num_row_groups - 1, columns=columns),
            schema=new_column_names
            )
        )
    
    print(f'{n_rows} rows\t{n_cols} columns')
    print(data)

def filter():
    pass

def group_by(name, keys, aggfunc):
    pass

def join():
    pass

def order():
    pass


# Update

def modify():
    pass


# Delete

def drop_table(database, table_name):
    '''Removes all partitions of a table from database directory'''
    partitions = get_all_partitions(database=database, table_name=table_name)
    for partition in partitions:
        path = os.path.join(DATA_PATH, database, partition)
        os.remove(path)