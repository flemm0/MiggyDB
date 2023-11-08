from cmd2 import Cmd
import os
from pathlib import Path
import polars as pl
import re

from . import utils
from .config import DATA_PATH
#data_dir = Path('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/data/')
'''
TODO set default data dir

/var/lib/miggydb for linux
/usr/local/var/miggydb for mac
'''


class DatabaseCLI(Cmd):
    prompt = '\U0001F911 > '

    def __init__(self):
        super().__init__()
        if not (DATA_PATH / 'temp').exists():
            Path.mkdir(DATA_PATH / 'temp')
        self.dbs = [dir.stem for dir in list(DATA_PATH.iterdir())]
        self.current_db = None
        self.tables = None

    def do_new(self, arg):
        '''argument parser for creating a new database'''
        args = arg.split()
        if args[0] in ['db', 'database']:
            Path.mkdir(DATA_PATH / args[1])
            if (DATA_PATH / args[1]).exists():
                print(f'Database {args[1]} successfully created')
        else:
            print('Unrecognized command')

    def do_show(self, arg):
        '''argument parser for viewing existing databases and tables'''
        if arg in ['dbs', 'databases']:
            print('Existing databases:\n')
            print(*self.dbs, sep='\n')
        elif arg == 'tables':
            if self.current_db is not None:
                print(f'Existing tables under {Path(self.current_db).stem}:\n')
                print(*self.tables, sep='\n')
            else:
                print(f'Database not set. Please set database to use to view existing tables.')
            
    def do_use(self, arg):
        '''
        arugment parser for switching to a database
        sets the path to the database to be queried
        loads tables names from database into memory.
        '''
        args = arg.split()
        if args[0] in ['db', 'database']:
            db_name = args[1]
            db_path = DATA_PATH / db_name

            if os.path.exists(db_path):
                self.current_db = db_path
                self.tables = [dir.stem for dir in list(db_path.iterdir())]
                print(f'Using database: {db_name}')
            else:
                print(f'Database {db_name} not found')
        else:
            print('Unrecognized command')

    def do_query(self, arg):
        '''
        Example:
        gimme <col a>, <col b>, <col c>
        from <table> <+ join table> e.g. employees + departments
        filter <expr> e.g. age > 10
        group <col>
        agg <expr> count(age)
        groupfilter <expr> e.g. count(age) > 10
        sort <col>
        trunc <int>
        skip <int>
        '''
        keywords = [
            'gimme',
            'from',
            'filter',
            'group',
            'agg',
            'groupfilter',
            'sort',
            'trunc',
            'skip'
        ]
        pattern = r'({})'.format('|'.join(map(re.escape, keywords)))
        result = list(filter(None, re.split(pattern, arg)))
        query_dict, key = {}, None
        for item in result:
            if item in keywords:
                key = item
                query_dict[key] = ''
            else:
                query_dict[key] += item.strip()
        utils.execute_query(
            database=self.current_db,
            table_name=query_dict['from'] if '+' not in query_dict['from'] else query_dict['from'].split(' + ')[0],
            select='+' in query_dict['from']
        )




    def do_create_table(self, arg): ## TODO add option to import csv or json from path
        '''
        Creates a table from CLI.
        Accepts column name followed by datatype as input.
        Useage:
            create_table name string id integer date_of_birth date
        '''
        args = arg.split(',')
        if len(args) < 3:
            print('Invalid input. Please provide a table name, and at least one column name followed by its data type')
            return
        table_name = args[0].strip()
        schema_info = args[1:]
        schema = [(schema_info[i], utils.infer_datatypes(schema_info[i + 1])) for i in range(0, len(schema_info), 2)]
        data = pl.DataFrame([], schema=schema)
        print(data)

    def do_read_table(self, arg):
        '''
        Reads in parquet table from disk and displays to console
        '''
        table_name = arg.split()[0]
        utils.read_full_table(database=self.current_db, table_name=table_name)

    def do_exit(self, arg):
        """Exit the CLI."""
        print('Bye!', '\U0001F923')
        return True

def main():
    cli = DatabaseCLI()
    cli.cmdloop('Welcome to MiggyDB. \U0001F60E Type "exit" to exit.')

if __name__ == '__main__':
    main()