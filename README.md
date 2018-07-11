# Building an Equity Data Warehouse in PostgreSQL with Python

###### This project is meant to use python and SQL in building out an equity data
###### warehouse that will be used for future quantitative research projects.

### Getting Started
This code repository will build a Postgres database on your local machine.
The code is meant to build out the database, SQL schema, scrape the internet for our
list of stocks, load their data into the PostgreSQL database and retrieve data as a test.

### Prerequisites
You need to have PostgreSQL and Python installed. I used the spyder IDE for code development.

In addition, here is a list of specific python libraries used (and needed) in this project:

* psycopg2
* requests
* bs4
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

### Built With
"FINISH THIS SECTION"

### Authors
Antonio Constandinou
