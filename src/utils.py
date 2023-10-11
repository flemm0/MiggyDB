import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pyarrow import csv, json
import os
from subprocess import check_output
import re
import subprocess
from pathlib import Path
from collections import defaultdict

import polars as pl
import math

pl.Config.set_tbl_hide_dataframe_shape(True)

DATA_PATH = Path('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/data/')
TEST_DB_PATH = Path(DATA_PATH / 'test')
TEMP_DATA_PATH = Path(DATA_PATH / 'temp')

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
    table_path = Path(DATA_PATH / database / table_name)
    if not table_path.exists():
        Path.mkdir(table_path)

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

## TODO change functions to utilize tempfile.TemporaryDirectory() to flush immediate results to disk

'''
Query operations should be as follows:

- queries begin with the FROM or JOIN clauses, so these functions will take a database and table_name as input
- the FROM clause will select the dataset needed and write table in batches to temporary directory
- if this is the end of the query, read in head and tail of dataset in temp directory, print to console, and clear directory
- if this is not the end of the query, return the dataset object pointing to the dataset created in the temp directory
- subsequent steps of the query will operate and overwrite the tables in the temporary directory, until the end of query is reached,
which will cause results to be printed and temporary directory to be cleared

'''

def write_query_to_temp_dir(table, temp_dir_name, partition):
    where = TEMP_DATA_PATH / temp_dir_name / partition
    pq.write_table(table=table, where=where)
    

def print_results_to_console(dataset):
    pass

def read_full_table(database, table_name):
    '''Queries table
    
    Since data is expected to be larger than the memory limit, flush results of query to /temp directory
    when 100 MB data limit is reached
    '''
    base = Path(os.path.join(DATA_PATH, database))
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
    return data

def projection(database, table_name, columns, new_column_names):
    '''Queries a subset of columns
    
    Should be fast, parquet files are columnar-files and are optimized to read in columns.
    '''
    base = Path(os.path.join(DATA_PATH, database))
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
    return data

def filter(database, table_name, filters):
    # use pq.read_table(..., filter=pyarrow.compute.Expression)
    base = Path(os.path.join(DATA_PATH, database))
    dataset = ds.dataset(base / table_name, format='parquet')

    for partition in dataset.files:
        data = pq.read_table(partition, filters=filters) # list of tuples e.g. ('acousticness', '<', 1)
        data = pl.DataFrame._from_arrow(data)
        print(data)

def group_by(name, keys, aggfunc):
    pass

def hash_join(table1, index1, table2, index2):
    '''implement hash join that accepts table partitions
    
    the hash phase should wrap a for loop above `for s in table1` for all the partitions and store the join values in the hash
    '''
    hash_table = defaultdict(list)
    result = []
    # hash phase
    for batch in table1.to_batches():
        rows = pl.DataFrame._from_arrow(batch).rows()
        for row in rows:
            hash_table[row[index1]].append(row)

    # join phase
    for batch in table2.to_batches():
        rows = pl.DataFrame._from_arrow(batch).rows()
        for row in rows:
            for entry in hash_table[row[index2]]:
                result.append(entry + row)

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