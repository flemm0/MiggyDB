# MiggyDB Documentation

# About

`MiggyDB` is a memory-efficient relational database management system with its own query language. It supports essential functionalities such as projection, filtering, joins, grouping, and more. With an emphasis on scalability, it efficiently manages large datasets without loading the entire database into memory. The memory limit of `MiggyDB` is set to 100 MB.

## Table of Contents
- [Installation](#installation)
- [Meta queries](#meta-queries)
- [Inserting data](#inserting-data)
- [Querying data](#querying-data)
    - [Projection](#projection)
    - [Filtering](#filtering)
    - [Joining](#joining)
    - [Grouping and aggregation](#grouping-and-aggregation)
    - [Filtering aggregations](#filtering-aggregations)
    - [Sorting](#sorting)
    - [Limit and Offset](#limit-and-offset)
    - [Copying query results to file](#copying-query-results-to-csv-file)
- [Modifying data](#modifying-data)
- [Deleting data](#deleting-data)

# Installation

### Method 1. Install from GitHub

- it is recommended to create a virtual environment and install `MiggyDB` to the environment

```bash
mamba create -n miggydb python=3.11.5
mamba activate miggydb

# or if you use conda
conda create --name miggydb python=3.11.5
conda activate miggydb

# or python venv
python3.11.5 -m venv miggydb
source miggydb/bin/activate
```

- then clone the repository

```bash
git clone git@github.com:flemm0/MiggyDB.git
```

- then navigate to the cloned repository (assuming you are in the same directory that you cloned to)

```bash
cd ./MiggyDB
```

- install dependencies

```bash
pip install .
```

- access the CLI

```bash
miggydb
```

### Method 2. Install source package

- follow instructions above to create virtual environment
- download `.zip` or `tar.gz` file

```bash
# zip file
wget https://github.com/flemm0/MiggyDB/archive/refs/tags/v0.2.0.zip

# or tar file
wget https://github.com/flemm0/MiggyDB/archive/refs/tags/v0.2.0.tar.gz
```

- extract contents

```bash
# zip file
unzip v0.2.0.zip

# or tar file
tar -xzvf v0.2.0.tar.gz
```

- navigate to the new directory

```bash
cd ./MiggyDB-0.2.0
```

- install dependencies

```bash
pip install .
```

- access the CLI

```bash
miggydb
```

# Meta queries

### Show databases

```
🤑> show dbs

# or
🤑> show databases
```

### Create new database

```
new db <database name>
new database <database name>
```

```
# e.g. creating a new database called 'demo' 
🤑> new database demo
```

### Switch to database

```
use db <database name>
use database <database name>
```

```
# e.g. switching to 'demo' database 
🤑> use database demo
```

### Show existing table under current database

```
🤑> show tables
```

### Create new table under current database

```
new table <table name> <column> <data type> <column> <data type> ...
```

```
# e.g. create table 'KrustyKrabEmployees' with employee_id, age, first_name, last_name, salary, and is_manager

🤑> new table KrustyKrabEmployees employee_id int age int first_name str last_name str salary float is_manager bool
```

- list the column names followed by the datatype
- allowable datatypes are `int`, `str`, `float`, and `bool`
- no need to separate the columns with commas

### Create new table from existing csv file

```
🤑> new table from csv '/path/to/file.csv' <optional table name>
```

- please specify the full path to the csv file, and put the path in single quotes
- you can optionally specify a table name, by default it will use the name of the file provided (minus the file extension)

### Create new table from JSON file

```
new table from csv '/path/to/file.json' <optional table name>
```

- similar to creating a csv file from JSON, specify the full path to the csv file, and put the path in single quotes
- if the file is not newline-delimited, `MiggyDB` will `jc` to convert it
- ****************************warning:**************************** this feature uses the fields from the first line of the JSON file as the headers. If the data is expected to have varying fields try converting the file to csv before creating the table

### Dropping a database or table

```
🤑> obliterate <table name>
🤑> obliterate <database name>
```

# Inserting data

```
add rows to <table> (<value>, <value>, <value>, ...)
```

```
# add one row
🤑> add rows to KrustyKrabEmployees (100, 27, 'Spongebob', 'Squarepants', 27.80, False) 

# add multiple rows
🤑> add rows to KrustyKrabEmployees (101, 45, 'Eugene', 'Krabs', 145.45, True) (102, 33, 'Squidward', 'Tentacles', 30.00, False) (202, 5, 'Gary', 'Squarepants', 90.00, False) (303, 29, 'Patrick', 'Star', 70.25, True) (550, 46, 'Sheldon', 'Plankton', 1.00, True) (634, 19, 'Pearl', 'Krabs', 12.75, False) (400, 35, 'Sandy', 'Cheeks', 18.00, False) (90, 90, 'Old Man', 'Jenkins', 60.45, False)
```

- Specify rows to add inside of a tuple, separated by comma
- There is no need to specify column name but a value for every column must be provided
- `MiggyDB` uses single quotes for strings

# Querying data

```
query from <table>
```

```
🤑> query from KrustyKrabEmployees

9 rows  6 columns
┌─────┬─────┬────────────┬─────────────┬────────┬────────────┐
│ id  ┆ age ┆ first_name ┆ last_name   ┆ salary ┆ is_manager │
│ --- ┆ --- ┆ ---        ┆ ---         ┆ ---    ┆ ---        │
│ i64 ┆ i64 ┆ str        ┆ str         ┆ f64    ┆ bool       │
╞═════╪═════╪════════════╪═════════════╪════════╪════════════╡
│ 100 ┆ 27  ┆ Spongebob  ┆ Squarepants ┆ 27.8   ┆ false      │
│ 101 ┆ 45  ┆ Eugene     ┆ Krabs       ┆ 145.45 ┆ true       │
│ 102 ┆ 33  ┆ Squidward  ┆ Tentacles   ┆ 30.0   ┆ false      │
│ 202 ┆ 5   ┆ Gary       ┆ Squarepants ┆ 90.0   ┆ false      │
│ 303 ┆ 29  ┆ Patrick    ┆ Star        ┆ 70.25  ┆ true       │
│ 550 ┆ 46  ┆ Sheldon    ┆ Plankton    ┆ 1.0    ┆ true       │
│ 634 ┆ 19  ┆ Pearl      ┆ Krabs       ┆ 12.75  ┆ false      │
│ 400 ┆ 35  ┆ Sandy      ┆ Cheeks      ┆ 18.0   ┆ false      │
│ 90  ┆ 90  ┆ Old Man    ┆ Jenkins     ┆ 60.45  ┆ false      │
└─────┴─────┴────────────┴─────────────┴────────┴────────────┘
Elapsed time: 0.0120 seconds
```

### Projection

```
query gimme <column>, <column>, <column> from <table>
```

```
🤑> query gimme id, first_name, is_manager from KrustyKrabEmployees

9 rows  3 columns
┌─────┬────────────┬────────────┐
│ id  ┆ first_name ┆ is_manager │
│ --- ┆ ---        ┆ ---        │
│ i64 ┆ str        ┆ bool       │
╞═════╪════════════╪════════════╡
│ 100 ┆ Spongebob  ┆ false      │
│ 101 ┆ Eugene     ┆ true       │
│ 102 ┆ Squidward  ┆ false      │
│ 202 ┆ Gary       ┆ false      │
│ 303 ┆ Patrick    ┆ true       │
│ 550 ┆ Sheldon    ┆ true       │
│ 634 ┆ Pearl      ┆ false      │
│ 400 ┆ Sandy      ┆ false      │
│ 90  ┆ Old Man    ┆ false      │
└─────┴────────────┴────────────┘
Elapsed time: 0.0231 seconds
```

You can also rename columns with syntax `<old>:<new>`

```
🤑> query gimme id:employee_id, first_name:name, is_manager from KrustyKrabEmployees

9 rows  3 columns
┌─────────────┬───────────┬────────────┐
│ employee_id ┆ name      ┆ is_manager │
│ ---         ┆ ---       ┆ ---        │
│ i64         ┆ str       ┆ bool       │
╞═════════════╪═══════════╪════════════╡
│ 100         ┆ Spongebob ┆ false      │
│ 101         ┆ Eugene    ┆ true       │
│ 102         ┆ Squidward ┆ false      │
│ 202         ┆ Gary      ┆ false      │
│ 303         ┆ Patrick   ┆ true       │
│ 550         ┆ Sheldon   ┆ true       │
│ 634         ┆ Pearl     ┆ false      │
│ 400         ┆ Sandy     ┆ false      │
│ 90          ┆ Old Man   ┆ false      │
└─────────────┴───────────┴────────────┘
Elapsed time: 0.0173 seconds
```

### Filtering

```
query from <table> filter (<column>, <operator>, <value>)
```

- operators are: `lt`, `lte`, `gt`, `gte`, `eq`, `ne`, `in`, `nin` (not in)
    - if using `in` or `nin` operators, specify a set with curly brackets `{...}` for the matches desired

```
🤑> query from KrustyKrabEmployees filter (last_name eq 'Squarepants')

2 rows  6 columns
┌─────┬─────┬────────────┬─────────────┬────────┬────────────┐
│ id  ┆ age ┆ first_name ┆ last_name   ┆ salary ┆ is_manager │
│ --- ┆ --- ┆ ---        ┆ ---         ┆ ---    ┆ ---        │
│ i64 ┆ i64 ┆ str        ┆ str         ┆ f64    ┆ bool       │
╞═════╪═════╪════════════╪═════════════╪════════╪════════════╡
│ 100 ┆ 27  ┆ Spongebob  ┆ Squarepants ┆ 27.8   ┆ false      │
│ 202 ┆ 5   ┆ Gary       ┆ Squarepants ┆ 90.0   ┆ false      │
└─────┴─────┴────────────┴─────────────┴────────┴────────────┘
Elapsed time: 0.0686 seconds
```

You can filter on multiple conditions by adding another tuple after `filter`

```
🤑> query from KrustyKrabEmployees filter (id gte 102), (last_name in {'Squarepants', 'Krabs', 'Star'})

3 rows  6 columns
┌─────┬─────┬────────────┬─────────────┬────────┬────────────┐
│ id  ┆ age ┆ first_name ┆ last_name   ┆ salary ┆ is_manager │
│ --- ┆ --- ┆ ---        ┆ ---         ┆ ---    ┆ ---        │
│ i64 ┆ i64 ┆ str        ┆ str         ┆ f64    ┆ bool       │
╞═════╪═════╪════════════╪═════════════╪════════╪════════════╡
│ 202 ┆ 5   ┆ Gary       ┆ Squarepants ┆ 90.0   ┆ false      │
│ 303 ┆ 29  ┆ Patrick    ┆ Star        ┆ 70.25  ┆ true       │
│ 634 ┆ 19  ┆ Pearl      ┆ Krabs       ┆ 12.75  ┆ false      │
└─────┴─────┴────────────┴─────────────┴────────┴────────────┘
Elapsed time: 0.0501 seconds
```

To use `or` logic for filtering, pass in the two conditions inside of square brackets `[(…)], [(…)]`

```
🤑> query from KrustyKrabEmployees filter [(id gte 102)], [(is_manager eq True)]

7 rows  6 columns
┌─────┬─────┬────────────┬─────────────┬────────┬────────────┐
│ id  ┆ age ┆ first_name ┆ last_name   ┆ salary ┆ is_manager │
│ --- ┆ --- ┆ ---        ┆ ---         ┆ ---    ┆ ---        │
│ i64 ┆ i64 ┆ str        ┆ str         ┆ f64    ┆ bool       │
╞═════╪═════╪════════════╪═════════════╪════════╪════════════╡
│ 101 ┆ 45  ┆ Eugene     ┆ Krabs       ┆ 145.45 ┆ true       │
│ 102 ┆ 33  ┆ Squidward  ┆ Tentacles   ┆ 30.0   ┆ false      │
│ 202 ┆ 5   ┆ Gary       ┆ Squarepants ┆ 90.0   ┆ false      │
│ 303 ┆ 29  ┆ Patrick    ┆ Star        ┆ 70.25  ┆ true       │
│ 550 ┆ 46  ┆ Sheldon    ┆ Plankton    ┆ 1.0    ┆ true       │
│ 634 ┆ 19  ┆ Pearl      ┆ Krabs       ┆ 12.75  ┆ false      │
│ 400 ┆ 35  ┆ Sandy      ┆ Cheeks      ┆ 18.0   ┆ false      │
└─────┴─────┴────────────┴─────────────┴────────┴────────────┘
Elapsed time: 0.0201 seconds
```

You can put `and` conditions inside of the square brackets `[(a), (b)], [(c)]` which translates to ((a and b) or c)

```
# (('id' > 102 and 'age' < 30) or (is_manager == True))

🤑> query from KrustyKrabEmployees filter [(id gt 102), (age lt 30)], [(is_manager eq True)]

5 rows  6 columns
┌─────┬─────┬────────────┬─────────────┬────────┬────────────┐
│ id  ┆ age ┆ first_name ┆ last_name   ┆ salary ┆ is_manager │
│ --- ┆ --- ┆ ---        ┆ ---         ┆ ---    ┆ ---        │
│ i64 ┆ i64 ┆ str        ┆ str         ┆ f64    ┆ bool       │
╞═════╪═════╪════════════╪═════════════╪════════╪════════════╡
│ 101 ┆ 45  ┆ Eugene     ┆ Krabs       ┆ 145.45 ┆ true       │
│ 202 ┆ 5   ┆ Gary       ┆ Squarepants ┆ 90.0   ┆ false      │
│ 303 ┆ 29  ┆ Patrick    ┆ Star        ┆ 70.25  ┆ true       │
│ 550 ┆ 46  ┆ Sheldon    ┆ Plankton    ┆ 1.0    ┆ true       │
│ 634 ┆ 19  ┆ Pearl      ┆ Krabs       ┆ 12.75  ┆ false      │
└─────┴─────┴────────────┴─────────────┴────────┴────────────┘
Elapsed time: 0.0111 seconds
```

### Joining

```
query from <table 1> + <table 2> by <join column>
```

- note: `MiggyDB` currently only supports inner joins

```
# create joining table

🤑> new table KrustyKrabDepartments department_id int employee_id int department_name str

🤑> add rows to KrustyKrabDepartments (1, 100, 'Kitchen') (2, 101, 'Management') (3, 102, 'Service') (1, 202, 'Kitchen') (2, 303, 'Management') (2, 550, 'Management') (1, 634, 'Kitchen') (3, 400, 'Service')
```

```
🤑> query from KrustyKrabEmployees + KrustyKrabDepartments by employee_id

8 rows  9 columns
┌───────────────┬─────┬────────────┬─────────────┬────────┬────────────┬───────────────┬───────────────┬─────────────────┐
│ employee_id_x ┆ age ┆ first_name ┆ last_name   ┆ salary ┆ is_manager ┆ department_id ┆ employee_id_y ┆ department_name │
│ ---           ┆ --- ┆ ---        ┆ ---         ┆ ---    ┆ ---        ┆ ---           ┆ ---           ┆ ---             │
│ i64           ┆ i64 ┆ str        ┆ str         ┆ f64    ┆ bool       ┆ i64           ┆ i64           ┆ str             │
╞═══════════════╪═════╪════════════╪═════════════╪════════╪════════════╪═══════════════╪═══════════════╪═════════════════╡
│ 100           ┆ 27  ┆ Spongebob  ┆ Squarepants ┆ 27.8   ┆ false      ┆ 1             ┆ 100           ┆ Kitchen         │
│ 101           ┆ 45  ┆ Eugene     ┆ Krabs       ┆ 145.45 ┆ true       ┆ 2             ┆ 101           ┆ Management      │
│ 102           ┆ 33  ┆ Squidward  ┆ Tentacles   ┆ 30.0   ┆ false      ┆ 3             ┆ 102           ┆ Service         │
│ 202           ┆ 5   ┆ Gary       ┆ Squarepants ┆ 90.0   ┆ false      ┆ 1             ┆ 202           ┆ Kitchen         │
│ 303           ┆ 29  ┆ Patrick    ┆ Star        ┆ 70.25  ┆ true       ┆ 2             ┆ 303           ┆ Management      │
│ 400           ┆ 35  ┆ Sandy      ┆ Cheeks      ┆ 18.0   ┆ false      ┆ 3             ┆ 400           ┆ Service         │
│ 550           ┆ 46  ┆ Sheldon    ┆ Plankton    ┆ 1.0    ┆ true       ┆ 2             ┆ 550           ┆ Management      │
│ 634           ┆ 19  ┆ Pearl      ┆ Krabs       ┆ 12.75  ┆ false      ┆ 1             ┆ 634           ┆ Kitchen         │
└───────────────┴─────┴────────────┴─────────────┴────────┴────────────┴───────────────┴───────────────┴─────────────────┘
Elapsed time: 0.0785 seconds
```

### Grouping and aggregation

```
query from <table> group <grouping column> agg <function(aggregate column)>
```

```
🤑> query from KrustyKrabEmployees group is_manager agg average(salary)

2 rows  2 columns
┌────────────┬─────────────────┐
│ is_manager ┆ average(salary) │
│ ---        ┆ ---             │
│ bool       ┆ f64             │
╞════════════╪═════════════════╡
│ false      ┆ 39.833333       │
│ true       ┆ 72.233333       │
└────────────┴─────────────────┘
Elapsed time: 0.0198 seconds
```

- available aggregate functions are `average()`, `min()`, `max()`, `sum()`, `count()`

### Filtering aggregations

```
query from <table> group <grouping column> agg <function(aggregate column)> grpfilt (<column>, <operator>, <value>)
```

```
🤑> query from KrustyKrabEmployees + KrustyKrabDepartments by employee_id group department_name agg average(age) grpfilt (average(age) gt 35)

1 rows  2 columns
┌─────────────────┬──────────────┐
│ department_name ┆ average(age) │
│ ---             ┆ ---          │
│ str             ┆ f64          │
╞═════════════════╪══════════════╡
│ Management      ┆ 40.0         │
└─────────────────┴──────────────┘
Elapsed time: 0.0328 seconds
```

### Sorting

```
query from <table> sort <column>
```

```
🤑> query from KrustyKrabEmployees sort age

9 rows  6 columns
┌─────────────┬─────┬────────────┬─────────────┬────────┬────────────┐
│ employee_id ┆ age ┆ first_name ┆ last_name   ┆ salary ┆ is_manager │
│ ---         ┆ --- ┆ ---        ┆ ---         ┆ ---    ┆ ---        │
│ i64         ┆ i64 ┆ str        ┆ str         ┆ f64    ┆ bool       │
╞═════════════╪═════╪════════════╪═════════════╪════════╪════════════╡
│ 202         ┆ 5   ┆ Gary       ┆ Squarepants ┆ 90.0   ┆ false      │
│ 634         ┆ 19  ┆ Pearl      ┆ Krabs       ┆ 12.75  ┆ false      │
│ 100         ┆ 27  ┆ Spongebob  ┆ Squarepants ┆ 27.8   ┆ false      │
│ 303         ┆ 29  ┆ Patrick    ┆ Star        ┆ 70.25  ┆ true       │
│ 102         ┆ 33  ┆ Squidward  ┆ Tentacles   ┆ 30.0   ┆ false      │
│ 400         ┆ 35  ┆ Sandy      ┆ Cheeks      ┆ 18.0   ┆ false      │
│ 101         ┆ 45  ┆ Eugene     ┆ Krabs       ┆ 145.45 ┆ true       │
│ 550         ┆ 46  ┆ Sheldon    ┆ Plankton    ┆ 1.0    ┆ true       │
│ 90          ┆ 90  ┆ Old Man    ┆ Jenkins     ┆ 60.45  ┆ false      │
└─────────────┴─────┴────────────┴─────────────┴────────┴────────────┘
Elapsed time: 0.0151 seconds
```

To sort in reverse, put the keyword `rev` or `reverse` after the `sort` clause

```
🤑> query from KrustyKrabEmployees sort age reverse

9 rows  6 columns
┌─────────────┬─────┬────────────┬─────────────┬────────┬────────────┐
│ employee_id ┆ age ┆ first_name ┆ last_name   ┆ salary ┆ is_manager │
│ ---         ┆ --- ┆ ---        ┆ ---         ┆ ---    ┆ ---        │
│ i64         ┆ i64 ┆ str        ┆ str         ┆ f64    ┆ bool       │
╞═════════════╪═════╪════════════╪═════════════╪════════╪════════════╡
│ 90          ┆ 90  ┆ Old Man    ┆ Jenkins     ┆ 60.45  ┆ false      │
│ 550         ┆ 46  ┆ Sheldon    ┆ Plankton    ┆ 1.0    ┆ true       │
│ 101         ┆ 45  ┆ Eugene     ┆ Krabs       ┆ 145.45 ┆ true       │
│ 400         ┆ 35  ┆ Sandy      ┆ Cheeks      ┆ 18.0   ┆ false      │
│ 102         ┆ 33  ┆ Squidward  ┆ Tentacles   ┆ 30.0   ┆ false      │
│ 303         ┆ 29  ┆ Patrick    ┆ Star        ┆ 70.25  ┆ true       │
│ 100         ┆ 27  ┆ Spongebob  ┆ Squarepants ┆ 27.8   ┆ false      │
│ 634         ┆ 19  ┆ Pearl      ┆ Krabs       ┆ 12.75  ┆ false      │
│ 202         ┆ 5   ┆ Gary       ┆ Squarepants ┆ 90.0   ┆ false      │
└─────────────┴─────┴────────────┴─────────────┴────────┴────────────┘
Elapsed time: 0.0118 seconds

# also valid
🤑> query from KrustyKrabEmployees sort rev age

9 rows  6 columns
┌─────────────┬─────┬────────────┬─────────────┬────────┬────────────┐
│ employee_id ┆ age ┆ first_name ┆ last_name   ┆ salary ┆ is_manager │
│ ---         ┆ --- ┆ ---        ┆ ---         ┆ ---    ┆ ---        │
│ i64         ┆ i64 ┆ str        ┆ str         ┆ f64    ┆ bool       │
╞═════════════╪═════╪════════════╪═════════════╪════════╪════════════╡
│ 90          ┆ 90  ┆ Old Man    ┆ Jenkins     ┆ 60.45  ┆ false      │
│ 550         ┆ 46  ┆ Sheldon    ┆ Plankton    ┆ 1.0    ┆ true       │
│ 101         ┆ 45  ┆ Eugene     ┆ Krabs       ┆ 145.45 ┆ true       │
│ 400         ┆ 35  ┆ Sandy      ┆ Cheeks      ┆ 18.0   ┆ false      │
│ 102         ┆ 33  ┆ Squidward  ┆ Tentacles   ┆ 30.0   ┆ false      │
│ 303         ┆ 29  ┆ Patrick    ┆ Star        ┆ 70.25  ┆ true       │
│ 100         ┆ 27  ┆ Spongebob  ┆ Squarepants ┆ 27.8   ┆ false      │
│ 634         ┆ 19  ┆ Pearl      ┆ Krabs       ┆ 12.75  ┆ false      │
│ 202         ┆ 5   ┆ Gary       ┆ Squarepants ┆ 90.0   ┆ false      │
└─────────────┴─────┴────────────┴─────────────┴────────┴────────────┘
Elapsed time: 0.0151 seconds
```

- sorting currently only supported on one column

### Limit and offset

```
query from <table> skip <number> trunc <number>
```

```
# return 5 highest salaries

🤑> query from KrustyKrabEmployees sort rev salary trunc 5

5 rows  6 columns
┌─────────────┬─────┬────────────┬─────────────┬────────┬────────────┐
│ employee_id ┆ age ┆ first_name ┆ last_name   ┆ salary ┆ is_manager │
│ ---         ┆ --- ┆ ---        ┆ ---         ┆ ---    ┆ ---        │
│ i64         ┆ i64 ┆ str        ┆ str         ┆ f64    ┆ bool       │
╞═════════════╪═════╪════════════╪═════════════╪════════╪════════════╡
│ 101         ┆ 45  ┆ Eugene     ┆ Krabs       ┆ 145.45 ┆ true       │
│ 202         ┆ 5   ┆ Gary       ┆ Squarepants ┆ 90.0   ┆ false      │
│ 303         ┆ 29  ┆ Patrick    ┆ Star        ┆ 70.25  ┆ true       │
│ 90          ┆ 90  ┆ Old Man    ┆ Jenkins     ┆ 60.45  ┆ false      │
│ 102         ┆ 33  ┆ Squidward  ┆ Tentacles   ┆ 30.0   ┆ false      │
└─────────────┴─────┴────────────┴─────────────┴────────┴────────────┘
Elapsed time: 0.0171 seconds
```

```
# return 5 oldest people, skipping the oldest

🤑> query from KrustyKrabEmployees sort rev age skip 1 trunc 5

6 rows  6 columns
┌─────────────┬─────┬────────────┬───────────┬────────┬────────────┐
│ employee_id ┆ age ┆ first_name ┆ last_name ┆ salary ┆ is_manager │
│ ---         ┆ --- ┆ ---        ┆ ---       ┆ ---    ┆ ---        │
│ i64         ┆ i64 ┆ str        ┆ str       ┆ f64    ┆ bool       │
╞═════════════╪═════╪════════════╪═══════════╪════════╪════════════╡
│ 550         ┆ 46  ┆ Sheldon    ┆ Plankton  ┆ 1.0    ┆ true       │
│ 101         ┆ 45  ┆ Eugene     ┆ Krabs     ┆ 145.45 ┆ true       │
│ 400         ┆ 35  ┆ Sandy      ┆ Cheeks    ┆ 18.0   ┆ false      │
│ 102         ┆ 33  ┆ Squidward  ┆ Tentacles ┆ 30.0   ┆ false      │
│ 303         ┆ 29  ┆ Patrick    ┆ Star      ┆ 70.25  ┆ true       │
└─────────────┴─────┴────────────┴───────────┴────────┴────────────┘
Elapsed time: 0.0126 seconds
```

### Copying query results to csv file

```
copy (query from ...) to '/path/to/file.csv'

# e.g.
🤑> copy (query from KrustyKrabEmployees filter (is_manager eq true)) to '/home/user/output.csv'
```

```bash
$ cat /home/user/output.csv

"employee_id","age","first_name","last_name","salary","is_manager"
101,45,"Eugene","Krabs",145.45,true
303,29,"Patrick","Star",70.25,true
550,46,"Sheldon","Plankton",1,true
```

- use absolute path for desired output file

# Modifying data

```
amend <table> filter (<column>, <operator>, <value>) set <column> to <value>
```

- uses the same filtering syntax as when filtering query results
- however, `amend` currently doesn’t support `or` logic, so to modify results when you have an `or` condition, just write two separate `amend` commands

```
🤑> amend KrustyKrabEmployees filter (employee_id eq 100) set salary to 29.00
KrustyKrabEmployees successfully updated
🤑> query from KrustyKrabEmployees

9 rows  6 columns
┌─────────────┬─────┬────────────┬─────────────┬────────┬────────────┐
│ employee_id ┆ age ┆ first_name ┆ last_name   ┆ salary ┆ is_manager │
│ ---         ┆ --- ┆ ---        ┆ ---         ┆ ---    ┆ ---        │
│ i64         ┆ i64 ┆ str        ┆ str         ┆ f64    ┆ bool       │
╞═════════════╪═════╪════════════╪═════════════╪════════╪════════════╡
│ 100         ┆ 27  ┆ Spongebob  ┆ Squarepants ┆ 29.0   ┆ false      │
│ 101         ┆ 45  ┆ Eugene     ┆ Krabs       ┆ 145.45 ┆ true       │
│ 102         ┆ 33  ┆ Squidward  ┆ Tentacles   ┆ 30.0   ┆ false      │
│ 202         ┆ 5   ┆ Gary       ┆ Squarepants ┆ 90.0   ┆ false      │
│ 303         ┆ 29  ┆ Patrick    ┆ Star        ┆ 70.25  ┆ true       │
│ 550         ┆ 46  ┆ Sheldon    ┆ Plankton    ┆ 1.0    ┆ true       │
│ 634         ┆ 19  ┆ Pearl      ┆ Krabs       ┆ 12.75  ┆ false      │
│ 400         ┆ 35  ┆ Sandy      ┆ Cheeks      ┆ 18.0   ┆ false      │
│ 90          ┆ 90  ┆ Old Man    ┆ Jenkins     ┆ 60.45  ┆ false      │
└─────────────┴─────┴────────────┴─────────────┴────────┴────────────┘
Elapsed time: 0.0063 seconds
```

# Deleting data

```
remove rows from <table> filter (<column>, <operator>, <value>)
```

- again, uses the same logic for filtering as when querying the data
- `or` conditions are allowed

```
🤑> remove rows from KrustyKrabEmployees filter (age gt 80)
Rows successfully removed
🤑> query from KrustyKrabEmployees

8 rows  6 columns
┌─────────────┬─────┬────────────┬─────────────┬────────┬────────────┐
│ employee_id ┆ age ┆ first_name ┆ last_name   ┆ salary ┆ is_manager │
│ ---         ┆ --- ┆ ---        ┆ ---         ┆ ---    ┆ ---        │
│ i64         ┆ i64 ┆ str        ┆ str         ┆ f64    ┆ bool       │
╞═════════════╪═════╪════════════╪═════════════╪════════╪════════════╡
│ 100         ┆ 27  ┆ Spongebob  ┆ Squarepants ┆ 27.8   ┆ false      │
│ 101         ┆ 45  ┆ Eugene     ┆ Krabs       ┆ 145.45 ┆ true       │
│ 102         ┆ 33  ┆ Squidward  ┆ Tentacles   ┆ 30.0   ┆ false      │
│ 202         ┆ 5   ┆ Gary       ┆ Squarepants ┆ 90.0   ┆ false      │
│ 303         ┆ 29  ┆ Patrick    ┆ Star        ┆ 70.25  ┆ true       │
│ 550         ┆ 46  ┆ Sheldon    ┆ Plankton    ┆ 1.0    ┆ true       │
│ 634         ┆ 19  ┆ Pearl      ┆ Krabs       ┆ 12.75  ┆ false      │
│ 400         ┆ 35  ┆ Sandy      ┆ Cheeks      ┆ 18.0   ┆ false      │
└─────────────┴─────┴────────────┴─────────────┴────────┴────────────┘
Elapsed time: 0.0055 seconds
```