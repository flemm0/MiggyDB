from cmd2 import Cmd
import os
from pathlib import Path
import polars as pl
import pyarrow.parquet as pq
import pyarrow.csv as csv
import re
import ast
import shutil
import datetime
import time

from . import utils
from .config import DATA_PATH, TEMP_DB_PATH


class DatabaseCLI(Cmd):
    prompt = '\U0001F911 > '

    def __init__(self):
        super().__init__()
        if not DATA_PATH.exists():
            Path.mkdir(DATA_PATH)
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
            self.dbs.append(args[1])
            if (DATA_PATH / args[1]).exists():
                print(f'Database {args[1]} successfully created')
        elif args[0] == 'table':
            if self.current_db is None:
                print(f'Database not set. Please specify a database for the new table.')
            elif args[1] in self.tables:
                print(f'Table {args[2]} already exists.')
            else:
                if args[1] == 'from' and args[2] == 'csv':
                    try:
                        utils.create_table_from_csv(
                            path=ast.literal_eval(args[3]),
                            database=self.current_db,
                            table_name=args[-1] if len(args) == 5 else None
                        )
                        if len(args) == 5:
                            self.tables.append(args[-1])
                        else:
                            self.tables.append(Path(args[3]).stem)
                    except Exception as e:
                        print(f'An error occurred: {e}')
                else:
                    if len(args) < 3:
                        print('Invalid input. Please provide a table name, and at least one column name followed by its data type')
                        return
                    try:
                        schema = args[2:]
                        schema = list(zip(schema[::2], schema[1::2]))
                        schema = [(x[0], utils.infer_datatypes(x[1])) for x in schema]
                        utils.create_table_from_cli(database=self.current_db, table_name=args[1], schema=schema)
                        self.tables.append(args[1])
                        print(f'Table {args[1]} successfully created')
                    except Exception as e:
                        print('An error occurred: {e}')
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

    def do_add(self, arg):
        '''
        argument parser for insering values into a table
        '''
        if self.current_db is None:
            print('Database not set. Please set a database before inserting into table')
            return
        pattern = r"(\w+|'.+?'|\(.+?\))"
        args = re.findall(pattern, arg)
        if args[0] == 'rows':
            # parse tuples
            values = [ast.literal_eval(tup) for tup in args[3:]]
            # transpose tuples into column-orientation
            values = list(zip(*values))
            try: 
                utils.insert_into(
                    database=self.current_db,
                    table_name=args[2],
                    values=values
                )
            except Exception as e:
                print(f'An exception occurred: {e}')
        else:
            print('Unrecognized command')

    def do_obliterate(self, arg):
        '''
        argument parser to remove a table or database
        '''
        args = arg.split()
        if args[0] in ['db', 'database']:
            try:
                shutil.rmtree(DATA_PATH / args[1])
                self.dbs.remove(args[1])
                if not (DATA_PATH / args[1]).exists():
                    print(f'Database {args[1]} successfully removed')
                if args[1] == self.current_db:
                    self.current_db = None
            except Exception as e:
                print(f'An exception occurred: {e}')
        elif args[0] == 'table':
            if self.current_db is None:
                print('Database not set. Please set a database before obliterating a table')
                return
            else:
                try:
                    shutil.rmtree(DATA_PATH / self.current_db / args[1])
                    self.tables.remove(args[1])
                    if not (DATA_PATH / self.current_db / args[1]).exists():
                        print(f'Table {args[1]} successfully removed from {self.current_db}')
                except Exception as e:
                    print(f'An exception occurred: {e}')
        else:
            print('Unrecognized command')

    def do_remove(self, arg):
        '''
        argument parser to remove rows from table given filter condition
        remove rows from <table> filter (<co>, <op>, <val>)
        '''
        if self.current_db is None:
            print('Database not set. Please set a database before inserting into table')
            return
        keywords = ['remove', 'from', 'filter']
        pattern = r'({})'.format('|'.join(map(re.escape, keywords)))
        result = ['remove'] + [s.strip() for s in re.split(pattern, arg) if s]
        query_dict, key = {}, None
        for item in result:
            if item in keywords:
                key = item
                query_dict[key] = ''
            else:
                query_dict[key] += item.strip()
        filter_str = query_dict['filter']
        # replace and negate comparison operators with ', <op>',
        filter_str = filter_str.replace(' gte', "', '<',")\
            .replace(' eq', "', '!=',")\
            .replace(' in', "', 'not in',")\
            .replace(' nin', "', 'in',")\
            .replace(' lte', "', '>',")\
            .replace(' lt', "', '>=',")\
            .replace(' gt', "', '<=',")\
            .replace(' ne', "', '=',")
        # replace all instance of '(' followed by letter to "'" separated
        filter_str = re.sub(r'\((\w)', r"('\1", filter_str)
        if isinstance(ast.literal_eval(filter_str)[0], str):
            filters = [ast.literal_eval(filter_str)]
        else:
            filters = list(ast.literal_eval(filter_str))
        try:
            utils.drop_rows(
                database=self.current_db,
                table_name=query_dict['from'],
                filters=filters
            )
            print('Rows successfully removed')
        except Exception as e:
            print(f'An exception ocurred: {e}')


    def do_amend(self, arg):
        '''
        argument parser to modify values in a table
        supports only 1 condition for now
        '''
        keywords = ['amend', 'filter', 'set', 'to']
        pattern = r'({})'.format('|'.join(map(re.escape, keywords)))
        result = ['amend'] + [s.strip() for s in re.split(pattern, arg) if s]
        query_dict, key = {}, None
        for item in result:
            if item in keywords:
                key = item
                query_dict[key] = ''
            else:
                query_dict[key] += item.strip()

        # parse update value
        update_val = query_dict['to']
        if update_val.replace('.', '').isnumeric():
            if "'" not in update_val:
                update_val = ast.literal_eval(update_val)
        else:
            update_val = str(update_val)

        # parse filters       
        filter_str = query_dict['filter']
        filter_str = filter_str.replace(' gte', "', '>=',")\
            .replace(' eq', "', '=',")\
            .replace(' in', "', 'in',")\
            .replace(' nin', "', 'not in',")\
            .replace(' lte', "', '<=',")\
            .replace(' lt', "', '<',")\
            .replace(' gt', "', '>',")\
            .replace(' ne', "', '!=',")
        # replace all instance of '(' followed by letter to "'" separated
        filter_str = re.sub(r'\((\w)', r"('\1", filter_str)
        if isinstance(ast.literal_eval(filter_str)[0], str):
            filters = [ast.literal_eval(filter_str)]
        else:
            filters = list(ast.literal_eval(filter_str))

        try:
            utils.modify(
                database=self.current_db,
                table_name=query_dict['amend'],
                filters=filters,
                update_col=query_dict['set'],
                update_val=update_val
            )
            print(f'{query_dict["amend"]} successfully updated')
        except Exception as e:
            print(f'An exception occurred: {e}')

    def parse_from_join(self, query_dict):
        table_name = query_dict['from'] if '+' not in query_dict['from'] else query_dict['from'].split(' + ')[0]
        join_table_name = query_dict['from'].split(' + ')[1].split('by')[0].strip() if '+' in query_dict['from'] else None
        if join_table_name:
            join_col = query_dict['from'].split(' + ')[1].split('by')[1].strip()
        else:
            join_col = None
        return table_name, join_table_name, join_col
    
    def parse_filters(self, query_dict, group=False):
        if not group:
            if 'filter' not in query_dict.keys():
                return []
            filter_str = query_dict['filter']
        else:
            if 'grpfilt' not in query_dict.keys():
                return []
            filter_str = query_dict['grpfilt']
        # replace all comparison operators with ', <op>',
        filter_str = filter_str.replace(' gte', "', '>=',")\
            .replace(' eq', "', '=',")\
            .replace(' in', "', 'in',")\
            .replace(' nin', "', 'not in',")\
            .replace(' lte', "', '<=',")\
            .replace(' lt', "', '<',")\
            .replace(' gt', "', '>',")\
            .replace(' ne', "', '!=',")
        # replace all instance of '(' followed by letter to "'" separated
        if not group:
            filter_str = re.sub(r'\((\w)', r"('\1", filter_str)
        else:
            filter_str = filter_str.replace("(", "('", 1)
        if isinstance(ast.literal_eval(filter_str)[0], str):
            return [ast.literal_eval(filter_str)]
        else:
            return list(ast.literal_eval(filter_str))
    
    def parse_group_agg(self, query_dict):
        group_col = query_dict['group'] if 'group' in query_dict.keys() else None
        if 'agg' in query_dict.keys():
            agg_vals = re.findall(r'([a-zA-Z]+)\((.*)\)', query_dict['agg'])[0]
            agg_col = agg_vals[1]
            agg_func = agg_vals[0]
        else:
            agg_col, agg_func = None, None
        return group_col, agg_col, agg_func
    
    def parse_sort(self, query_dict):
        if 'sort' not in query_dict.keys():
            return None, False
        else:
            if ('rev' or 'reverse') in query_dict['sort']:
                return query_dict['sort'].replace('reverse', '').replace('rev', '').strip(), True
            else:
                return query_dict['sort'], False
        
    
    def parse_projection(self, query_dict):
        if 'gimme' not in query_dict.keys():
            return []
        else:
            selected_cols, new_col_names = [], []
            for col in query_dict['gimme'].split(', '):
                s = col.split(':')
                if len(s) == 2:
                    selected_cols.append(s[0])
                    new_col_names.append(s[1])
                else:
                    selected_cols.append(s[0])
                    new_col_names.append(s[0])
            return [selected_cols, new_col_names]
            
    def do_query(self, arg):
        '''
        Example:
        gimme <col a>, <col b>, <col c>
        from <table> <+ join table by col a> e.g. employees + departments by id
        filter <expr> e.g. (age gt 10), (name eq 'john')
        group <col>
        agg <expr> count(age)
        grpfilt <expr> e.g. (count(age) > 10)
        sort <col>
        trunc <int>
        skip <int>
        '''
        start_time = time.time()
        if self.current_db is None:
            print('No database selected. Please use the "use" command to select a database')
            return
        keywords = [
            'gimme',
            'from',
            'filter',
            'group',
            'agg',
            'grpfilt',
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
        group_col, agg_col, agg_func = self.parse_group_agg(query_dict)
        group_filter = self.parse_filters(query_dict, group=True)
        sort_col, reverse = self.parse_sort(query_dict)
        columns = self.parse_projection(query_dict)

        # checks
        if table_name not in self.tables:
            print(f'Error: {table_name} not found')
            return
        if join_table_name is not None and join_table_name not in self.tables:
            print(f'Error: {join_table_name} not found')
            return

        valid_agg_funcs = ['sum', 'min', 'max', 'count', 'average']
        if agg_func is not None and agg_func not in valid_agg_funcs:
            print(f'Error: {agg_func} is not a valid aggregate function. Valid aggregate functions are: {valid_agg_funcs}')
            return
        
        if group_col and agg_col and agg_func:
            if len(group_filter):
                if group_filter[0][0] not in [group_col, f'{agg_func}({agg_col})']:
                    print('Group filter must be applied to grouping column or aggregate column.')
                    return
            if len(columns):
                for col in columns[0]:
                    if col not in [group_col, f'{agg_func}({agg_col})']:
                        print('Error: columns in projection must either be the grouping column or the aggregated column')
                        return
            if sort_col is not None and sort_col not in columns[1]:
                print('Error: if sort column is given an alias, must pass the alias to the sort clause')
                return
        
        try:
            query_id = 'query_' + datetime.datetime.now().strftime("%y%m%d_%H%M%S") 
            query_path = Path(TEMP_DB_PATH / query_id)

            result_dataset = utils.execute_query(
                database=self.current_db,
                table_name=table_name,
                query_path=query_path,
                query_id=query_id,
                join_table_name=join_table_name,
                join_col=join_col,
                filters=filters,
                group_col=group_col,
                columns=columns,
                agg_col=agg_col,
                agg_func=agg_func,
                group_filter=group_filter,
                sort_col=sort_col,
                reverse=reverse
            )
            n_rows = result_dataset.count_rows()
            n_cols = len(result_dataset.schema.names)

            print(f'{n_rows} rows\t{n_cols} columns')
            # if dataset is only 1 file, read first partition
            if len(result_dataset.files) == 1:
                print(pl.read_parquet(result_dataset.files[0]))
            else:
                tail_nrows = pq.ParquetFile(result_dataset.files[-1]).metadata.num_rows
                data_tail = pl.read_parquet(result_dataset.files[-1]).tail(-tail_nrows//2)

                head_nrows = pq.ParquetFile(result_dataset.files[0]).metadata.num_rows
                data_head = pl.read_parquet(result_dataset.files[0], n_rows=head_nrows)

                print(pl.concat([data_head, data_tail]))
        finally:
            shutil.rmtree(query_path, ignore_errors=True)
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f'Elapsed time: {elapsed_time:.4f} seconds')

    def do_copy(self, arg):
        """Usage: copy (query ...) to 'output.csv'"""
        if self.current_db is None:
            print('No database selected. Please use the "use" command to select a database')
            return

        query_pattern = re.compile(r'\((.*)\)')
        query_str = query_pattern.search(arg).group(1).strip()
        query_str = query_str.replace('query ', '')
        output_pattern = re.compile(r"to '([^']+\.csv)'")
        output_path = Path(output_pattern.search(arg).group(1))

        keywords = [
            'gimme',
            'from',
            'filter',
            'group',
            'agg',
            'grpfilt',
            'sort',
            'trunc',
            'skip'
        ]
        pattern = r'({})'.format('|'.join(map(re.escape, keywords)))
        result = list(filter(None, re.split(pattern, query_str)))
        query_dict, key = {}, None
        for item in result:
            if item in keywords:
                key = item
                query_dict[key] = ''
            else:
                query_dict[key] += item.strip()
        table_name, join_table_name, join_col = self.parse_from_join(query_dict)
        filters = self.parse_filters(query_dict)
        group_col, agg_col, agg_func = self.parse_group_agg(query_dict)
        sort_col, reverse = self.parse_sort(query_dict)
        columns = self.parse_projection(query_dict)
        try:
            query_id = 'query_' + datetime.datetime.now().strftime("%y%m%d_%H%M%S") 
            query_path = Path(TEMP_DB_PATH / query_id)

            result_dataset = utils.execute_query(
                database=self.current_db,
                table_name=table_name,
                query_path=query_path,
                query_id=query_id,
                join_table_name=join_table_name,
                join_col=join_col,
                filters=filters,
                group_col=group_col,
                columns=columns,
                agg_col=agg_col,
                agg_func=agg_func,
                sort_col=sort_col,
                reverse=reverse
            )
            if not output_path.exists():
                output_path.touch()
            else:
                print(f'File {output_path} already exists.')
                return
            with csv.CSVWriter(output_path, result_dataset.schema) as writer:
                for file in result_dataset.files:
                        table = pq.read_table(file)
                        writer.write_table(table)
        finally:
            shutil.rmtree(query_path, ignore_errors=True)



    def do_exit(self, arg):
        """Exit the CLI."""
        temp_dir = DATA_PATH / 'temp'
        try:
            # Clear the contents of the directory
            for filename in temp_dir.glob('*'):
                if filename.is_file():
                    filename.unlink()
                else:
                    shutil.rmtree(filename)
        except Exception as e:
            print(f'Error: {e}')
        finally:
            print('Bye!', '\U0001F923')
            return True

def main():
    cli = DatabaseCLI()
    cli.cmdloop('Welcome to MiggyDB. \U0001F60E Type "exit" to exit.')

if __name__ == '__main__':
    main()