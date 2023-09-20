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