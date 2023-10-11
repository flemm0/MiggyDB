import cmd
import os
from pathlib import Path

import utils

data_dir = Path('/home/flemm0/school_stuff/USC_Fall_2023/DSCI551-Final_Project/data/')

class DatabaseCLI(cmd.Cmd):
    prompt = '\U0001F911 > '

    def __init__(self):
        super().__init__()
        self.dbs = [dir.stem for dir in list(data_dir.iterdir())]
        self.current_db = None
        self.tables = None

    def do_show_dbs(self, arg):
        '''Prints all available databases under the data directory'''
        print('Existing databases:\n')
        print(*self.dbs, sep='\n')

    def do_use_db(self, arg):
        '''Sets the path to the database to be queried. Loads tables names from database into memory.'''
        db_name = arg.strip()
        db_path = data_dir / db_name

        if os.path.exists(db_path):
            self.current_db = db_path
            self.tables = [dir.stem for dir in list(db_path.iterdir())]
            print(f'Using database: {db_name}')
        else:
            print(f'Database {db_name} not found')

    def do_show_existing_tables(self, arg):
        if self.current_db is not None:
            print('Existing tables:\n')
            print(*self.tables, sep='\n')
        else:
            print(f'Database not set. Please set database to use to view existing tables.')


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

if __name__ == '__main__':
    cli = DatabaseCLI()
    cli.cmdloop('Welcome to Flemming\'s Database CLI. \U0001F60E Type "exit" to exit.')