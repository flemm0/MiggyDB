from cmd2 import Cmd
import os
from pathlib import Path
import polars as pl
import re
import ast

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
                self.current_db = db_path.stem
                self.tables = [dir.stem for dir in list(db_path.iterdir())]
                print(f'Using database: {self.current_db}')
            else:
                print(f'Database {db_name} not found')
        else:
            print('Unrecognized command')

    def parse_from_join(self, query_dict):
        table_name = query_dict['from'] if '+' not in query_dict['from'] else query_dict['from'].split(' + ')[0]
        join_table_name = query_dict['from'].split(' + ')[1].split('by')[0].strip() if '+' in query_dict['from'] else None
        if join_table_name:
            join_col = query_dict['from'].split(' + ')[1].split('by')[1].strip()
        else:
            join_col = None
        return table_name, join_table_name, join_col
    
    def parse_filters(self, query_dict):
        if 'filter' not in query_dict.keys():
            return []
        print(query_dict['filter']) # debug
        filter_str = query_dict['filter']
        filters = []
        for f in filter_str.split(' & '):
            matches = re.findall(r"[A-Za-z_]+|'[^']*'|\d+(?:\.\d+)?|\[[^\]]+\]", f)
            if matches[1] == 'not':
                matches[1] = 'not in'
                matches.remove('in')
            filters.append(matches)
        print(filters) # debug
        for filter in filters:
            if filter[1] == 'gt':
                filter[1] = '>'
            elif filter[1] == 'gte':
                filter[1] = '>='
            elif filter[1] == 'lt':
                filter[1] = '<'
            elif filter[1] == 'lte':
                filter[1] = '<='
            elif filter[1] == 'eq':
                filter[1] = '='
            elif filter[1] == 'ne':
                filter[1] = '!='
            elif filter[1] == 'in' or filter[1] == 'not in':
                filter[2] = ast.literal_eval(filter[2])
        for filter in filters:
            if not isinstance(filter[2], list):
                if filter[2].replace('.', '').isnumeric():
                    if "'" in filter[-1]:
                        filter[2] = filter[2][1:-1]
                    else:
                        filter[2] = ast.literal_eval(filter[-1])
                else:
                    filter[2] = filter[2][1:-1]
        return [tuple(filter) for filter in filters]
            
    def do_query(self, arg):
        '''
        Example:
        gimme <col a>, <col b>, <col c>
        from <table> <+ join table by col a> e.g. employees + departments by id
        filter <expr> e.g. age > 10 & name = 'john'
        group <col>
        agg <expr> count(age)
        groupfilter <expr> e.g. count(age) > 10
        sort <col>
        trunc <int>
        skip <int>
        '''
        if self.current_db is None:
            print('No database selected. Please use the "use" command to select a database')
            return
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
        table_name, join_table_name, join_col = self.parse_from_join(query_dict)
        filters = self.parse_filters(query_dict)
        print(filters)
        result = utils.execute_query(
            database=self.current_db,
            table_name=table_name,
            join_table_name=join_table_name,
            join_col=join_col,
            filters=filters
        )
        print(result)




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