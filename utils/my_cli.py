import cmd
import os
import polars as pl

data_dir = '/home/flemm0/school_stuff/USC_Fall_2023/DSCI551/Final_Project/data/'

class DatabaseCLI(cmd.Cmd):
    prompt = 'PQL> '

    def __init__(self):
        super().__init__()
        self.dbs = os.listdir(data_dir)
        self.current_db = None
        self.tables = None

    def do_show_dbs(self, arg):
        '''Prints all available databases under the data directory'''
        print('Existing databases:\n')
        print(*self.dbs, sep='\n')

    def do_use_db(self, arg):
        '''Sets the path to the database to be queried. Loads tables names from database into memory.'''
        db_name = arg.strip()
        db_path = os.path.join(data_dir, db_name)

        if os.path.exists(db_path):
            self.current_db = db_path
            self.tables = [os.path.splitext(fname)[0] for fname in os.listdir(db_path) if fname.endswith('parquet')]
            print(f'Using database: {db_name}')
        else:
            print(f'Database {db_name} not found')

    def do_show_existing_tables(self, arg):
        if self.current_db is not None:
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

        def infer_datatypes(type):
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
        
        args = arg.split(',')
        if len(args) < 3:
            print('Invalid input. Please provide a table name, and at least one column name followed by its data type')
            return
        table_name = args[0].strip()
        schema_info = args[1:]
        schema = [(schema_info[i], infer_datatypes(schema_info[i + 1])) for i in range(0, len(schema_info), 2)]
        data = pl.DataFrame([], schema=schema)
        print(data)

    def do_read_table(self, arg):
        '''
        Reads in parquet table from disk and displays to console
        '''
        table_name = arg.split()[0]
        data = pl.read_parquet(os.path.join(data_dir, self.current_db, table_name + '.parquet'))
        print(data.head())

    def do_exit(self, arg):
        """Exit the CLI."""
        print('Bye!', '\U0001F923')
        return True

if __name__ == '__main__':
    cli = DatabaseCLI()
    cli.cmdloop('Welcome to the Custom CLI. Type "exit" to exit.')