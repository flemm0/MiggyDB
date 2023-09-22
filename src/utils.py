import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv, json
import os
from subprocess import check_output
import re

import polars as pl
import math

DATA_PATH = '/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/data/'
TEST_DB_PATH = os.path.join(DATA_PATH, 'test2')

MAX_PARTITION_SIZE = 100 * 1024 * 1024

# Utility functions

def say_hi():
    print("hello!")

def get_all_partitions(database, table_name):
    '''Returns list of all partitions of a table'''
    files = os.listdir(os.path.join(DATA_PATH, database))
    pattern = f'^{table_name}_\d+\.parquet$'
    return [f for f in files if re.match(pattern, f)]


# Create

def wc(path):
    return int(check_output(['wc', '-l', path]).split()[0])

def create_table_from_csv(path):
    '''Create a new table in the database system from input csv file. Processes it in 100 MB chunks'''
    total_rows = wc(path)
    n_partitions = math.ceil(os.path.getsize(path) / (1024 ** 2) / 100) ## TODO verify this makes 100 MB chunks
    n_rows = int(total_rows / n_partitions)

    for i in range(0, n_partitions):
        skip_rows_after_header = n_rows * i
        data = pl.read_csv(path, n_rows=n_rows, skip_rows_after_header=skip_rows_after_header)
        fname = os.path.splitext(os.path.basename(path))[0] + '_' + str(i) + '.parquet'
        data.write_parquet(os.path.join(TEST_DB_PATH, fname))

def create_table_from_json(path):
    '''Create a new table in the database system from input json file'''
    data = json.read_json(path)
    fname = os.path.splitext(os.path.basename(path))[0]
    pq.write_table(data, os.path.join(TEST_TABLE_PATH, fname + '.parquet'))

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
    data = pl.DataFrame([], schema=schema)
    partition = '_' + str(partition)
    data.write_parquet(os.path.join(DATA_PATH, database, table_name + partition + '.parquet'))

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




# Read

def query_data(name):
    '''Queries table
    
    Since data is expected to be larger than the memory limit, flush results of query to /temp directory
    when 100 MB data limit is reached
    '''
    path = os.path.join(TEST_DB_PATH, name + '.parquet')
    data = pl.read_parquet(path)
    return data.head()

def group_by(name, keys, aggfunc):
    data = query_data(name)
    data.group_by(keys)
    # perform aggregation function

def projection(database, table_name, columns):
    partitions = get_all_partitions(database=database, table_name=table_name)


def filter():
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