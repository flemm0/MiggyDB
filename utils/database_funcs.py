import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv, json
import os

import polars as pl

data_path = '/home/flemm0/school_stuff/USC_Fall_2023/DSCI551/Final_Project/data/'

# Create

def create_table_from_csv(path):
    '''Create a new table in the database system from input csv file'''
    data = csv.read_csv(path)
    fname = os.path.splitext(os.path.basename(path))[0]
    pq.write_table(data, os.path.join(data_path, fname + '.parquet'))

def create_table_from_json(path):
    '''Create a new table in the database system from input json file'''
    data = json.read_json(path)
    fname = os.path.splitext(os.path.basename(path))[0]
    pq.write_table(data, os.path.join(data_path, fname + '.parquet'))

def create_table_from_cli():
    '''Create new table with schema defined in cli input'''
    pass


# Read

def query_data(name):
    '''Queries table'''
    path = os.path.join(data_path, name + '.parquet')
    data = pl.read_parquet(path)
    return data.head()

def group_by(name, keys, aggfunc):
    data = query_data(name)
    data.group_by(keys)
    # perform aggregation function

def projection():
    pass

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