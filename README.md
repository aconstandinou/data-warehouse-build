# Building an Equity Data Warehouse in PostgreSQL with Python

###### This project is meant to use python and SQL in building out an equity data warehouse that will be used for future quantitative research projects.

### Getting Started
This code repository will build a Postgres database on your local machine.
The code is meant to build out the database, SQL schema, scrape the internet for our list of stocks, load their data into the PostgreSQL database and retrieve data as a test.

### Prerequisites
You need to have PostgreSQL and Python installed.

Here are the python libraries used in this database builder. Version numbers are included as well.

* psycopg2 version 2.7.5 (dt dec pq3 ext)
* requests version 2.7.0
* bs4 version 4.3.2
* os
* datetime

### Steps to build this database locally

1. Download this repository into it's own folder.
2. Edit the second line of the `database_info.txt` with your personal credentials needed to connect
   to a PostgreSQL database. There are 3 required credential details that you need to edit.
      1. localhost name (your local host name on your machine)
      2. username (the user name associated with connecting to a PostgreSQL database)
      3. connection password (the password associated with connecting to a PostgreSQL database)
      4. Ignore the last piece of information as this will be the name of the database and **cannot be changed**.
3. Run `db_part_one_schema_builder.py`
4. Run `db_part_two_sp500_stock_builder.py`
5. to do
6. to do

### Development Environment
* Spyder IDE version 2.3.5.2
* Python 2.7.10
* PostgreSQL 9.5.9

### Authors
Antonio Constandinou
