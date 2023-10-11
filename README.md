# Project Proposal

## Project outline:

For my project, I will implement a relational database system that can be interacted with in the terminal similar to MySQL. The database system will utilize local disk storage, and when first initialized, will create a data directory under the user’s home directory. In order to start interacting with data stored in the database, the user will be prompted to create a new database. This will create a database directory with the name of the database under the data directory, if it doesn’t already exist. Underneath the database directory, will live the files that store all the information contained in the tables. The hierarchy is: data directory —> database —> tables. An example of what the data directory may look like is as follows:

```bash
data_dir
├── database_1
│   ├── table1_data0.parquet
│   └── table1_data1.parquet
└── database_2
    ├── table1_data0.parquet
    ├── table1_data1.parquet
    ├── table1_data2.parquet
    ├── table2_data0.parquet
    ├── table2_data1.parquet
    ├── table3_data0.parquet
    ├── table3_data1.parquet
    └── table3_data2.parquet
```

My database system will also partition large data sets into more manageable sizes. I am setting the memory limit to 100 MB per file, so if an insert statement is made to a table that is at 100 MB, it will create a new parquet file to hold the new data. Similarly, if a large CSV or JSON file is uploaded to a database table, it will be separated into partitions to meet the memory requirements. This will keep data organized and allow for database operations to be performed on partitions of the data, eliminating the need to read the entire data set into memory. 

I will store the tables data in Apache Parquet format, which is columnar (technically hybrid columnar). The parquet format will provide advantages for the database system including faster read times, and better compression when stored on disk. Parquet files also allow storage of metadata, which can be read instead of the whole file. I plan to utilize this aspect of the files to store information that can help speed up database operations. The only downside of parquet files is that they are not human-readable, so I will be using the `pyarrow` and `polars` libraries for reading these files from disk. When query results are returned and printed to console, it will be in a polars data frame format.

As for the query language, I will structure it similar to MySQL queries, but use different syntax. There will be commands such as: `use_db`, `new_table`, `show_tables`, etc. and ones for more complex database operations such as joining and aggregation. I will also include options for projection and filtering on conditions. The more complex operations will be implemented from scratch (i.e. not use existing join or aggregate functions), using data structures and compute functions provided by base Python and `pyarrow`.

## My Background:

I majored in biochemistry as an undergraduate at UC San Diego, but took an interest in programming and data during my senior year of college, when I took bioinformatics and biostatistics courses, learning programming in Python and R. During the year I took between undergraduate and graduate school, I continued to learn while applying to graduate school, getting better with Python and learning how to use the Linux command line. I started my M.S. in bioinformatics at USC last fall, and started research work in a lab a couple months after I started, where I used R and more bash (a lot of my work was done on the USC Discovery HPC). 

This past summer, I worked as a bioinformatics intern for a biotech company in San Diego. At my internship I performed some analysis and machine learning work, but my focal project was to help develop a data lake to help the company organize their large amount of data. I developed a data catalog using MongoDB which would store metadata about the files in the data lake to make them easy to query for the bioinformatics team. Additionally, I developed an in-house Python library for the bioinformatics team to perform CRUD operations on the data catalog, eliminating the need for everyone to learn the MongoDB Query Language. I also developed an application using Streamlit to summarize key files in the data lake that non-technical people are able to use as well.


---

# Midterm Progress Report

## Changes

I have changed how I will store data partitions for each table. Originally, each database was a directory under the data directory, and data partitions for all tables in that database were stored as files under the database directory like so:
```bash
data_dir
├── database_1
│   ├── table1_data_0.parquet
│   └── table1_data_1.parquet
└── database_2
    ├── table1_data_0.parquet
    ├── table1_data_1.parquet
    ├── table2_data_0.parquet
    └── table2_data_1.parquet
```

I have changed it so that tables are now directories under their respective database. Within the table directory, will live all of the partitions of the datasets:
```bash
data_dir
├── database_1
│   └── table1
│       ├── data_0.parquet
│       └── data_1.parquet
└── database_2
    ├── table2
    │   ├── data_0.parquet
    │   ├── data_1.parquet
    │   └── data_2.parquet
    └── table3
        ├── data_0.parquet
        └── data_1.parquet
```
This will help to keep tables within a database more organized and make it easier to manage.

As for the rest of the planning I stated in the project outline, not much else has changed. I am still using parquet files for the physical data storage, and using the `pyarrow` library for reading and writing data to and from disk. When tables are created or added to in the database, the memory limit of 100 MB is checked whenever writing data, so operations that read in the data can read in entire files (partitions) without going over the memory limit. I also use the `polars` library mainly for printing data to the console (such as for query results). When printing query results to console, `polars` will not read the enitre table, but rather reads the first few rows of the first data partition and last few rows of the last data partition, and then prints that dataframe to the console. I also included logic to print the total number of rows and columns of the entire table.

## New Things

I started the project with writing functions that allow the user to create tables and insert data to tables. Creating a table will create a new directory under the specified database and initialize an empty parquet file with partition suffix "_0.parquet". The process of inserting data into the table involves checking the latest partition size to see if it is at the memory limit, and if so it will create a new partition under the table directory, and write data to that file. If the memory limit has not been reached, the data will be added to the latest partition. I also included a feature that allows users to specify a path to a csv or JSON file, for which my database system will create tables from the data in the files. My program checks the size of the specified file, and then determines how many partitions to create. For example, if the csv file is 500 MB, it will create 5 partitions. Since parquet offers better compression than csv or JSON, the files are guaranteed to meet the memory requirement. The database will use partition size and calculate how many rows to read at a time for the `n_rows` argument in `polars.read_csv` or `polars.read_json` to stay within the memory limit of my database. 

I have also started writing some functions for querying the data. The operations of selection, filtering, projection, etc. are not too difficult, they just apply whatever function is needed to each partition of the data using a loop. The challenge I am facing here is writing the functions so that they can support queries with multiple steps (e.g. selecting, joining, filtering, then projection). What I am thinking so far is to create a temporary database under the root data directory to store intermediate query results. I will generate a random query id, using the current date and time, and create a directory under the temporary database for each step of the query as follows:
```bash
.
└── temp_db
    ├── query_step_0
    │   ├── intermediate_data_0.parquet
    │   ├── intermediate_data_1.parquet
    │   ├── intermediate_data_2.parquet
    │   └── intermediate_data_3.parquet
    ├── query_step_1
    │   ├── intermediate_data_0.parquet
    │   ├── intermediate_data_1.parquet
    │   ├── intermediate_data_2.parquet
    │   └── intermediate_data_3.parquet
    └── query_step_2
        ├── intermediate_data_0.parquet
        ├── intermediate_data_1.parquet
        ├── intermediate_data_2.parquet
        └── intermediate_data_3.parquet
```

The first step of any query is the select and joining portion, so this function will perform the select or join on each partition of the table(s) and flush the table(s) to a directory under the temporary database. This will be step "0" of the query. Each subsequent step will read in intermediate data files from the previous step (e.g. if the query is on step "2", it reads files under "query_step_1" as input). Once the end of the query is reached, the results will be printed to the console. I plan to also allow users to write query results to a file. Once the query is completed, the temporary database directory will be cleared. 

As for update and delete commands for the tables, I will implement them once I have the filtering step completed, since this is required before updates and deletes can be performed.

Additionally, I have started writing the CLI functions for interacting with the database. It currently functions similar to MySQL CLI, where the user must first select a database to use, and can view what tables are available for the currently selected database. I will develop this further once I have the query functionality working.