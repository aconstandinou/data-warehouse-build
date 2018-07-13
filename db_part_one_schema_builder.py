# -*- coding: utf-8 -*-
"""
Created on Tue Jul 10 14:32:51 2018

@author: antonio constandinou
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

   
#def table_exists(credentials):
#    """
#    check to see if table exists PostgreSQL database
#    args:
#        credentials: database credentials including host, user, password and db name, type array
#    returns:
#        boolean value (True or False)
#    """
#    db_host, db_user, db_password, db_name = credentials
#    conn = psycopg2.connect(host=db_host,database=db_name, user=db_user, password=db_password)
#    cur = conn.cursor()
#    cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", (table_name,))
#    return cur.fetchone()[0]


def create_db(db_credential_info):
    """
    create a new database if it does not exist in the PostgreSQL database
    will use method 'check_db_exists' before creating a new database
    args:
        db_credential_info: database credentials including host, user, password and db name, type array
    returns:
        NoneType
    """
    db_host, db_user, db_password, db_name = db_credential_info
    
    if check_db_exists(db_credential_info):
        pass
    else:
        print('Creating new database.')
        conn = psycopg2.connect(host=db_host, database='postgres', user=db_user, password=db_password)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("CREATE DATABASE %s  ;" % db_name)
        cur.close()

        
def check_db_exists(db_credential_info):
    """
    checks to see if a database already exists in the PostgreSQL database
    args:
        db_credential_info: database credentials including host, user, password and db name, type array
    returns:
        boolean value (True or False)
    """
    db_host, db_user, db_password, db_name = db_credential_info
    try:
        conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.close()
        print('Database exists.')
        return True
    except:
        print("Database does not exist.")
        return False

       
def create_mkt_tables(db_credential_info):
    """
    create table in designated PostgreSQL database
    will use method 'check_db_exists' before creating table
    args:
        db_credential_info: database credentials including host, user, password and db name, type array
    returns:
        NoneType
    """
    db_host, db_user, db_password, db_name = db_credential_info
    conn = None
    
    if check_db_exists(db_credential_info):
        commands = (
                    """
                    CREATE TABLE exchange (
                        id SERIAL PRIMARY KEY,
                        abbrev TEXT NOT NULL,
                        name TEXT NOT NULL,
                        currency VARCHAR(64) NULL,
                        created_date TIMESTAMP NOT NULL,
                        last_updated_date TIMESTAMP NOT NULL
                        )
                    """,
                    """
                    CREATE TABLE data_vendor (
                        id SERIAL PRIMARY KEY,
                        name TEXT UNIQUE NOT NULL,
                        website_url VARCHAR(255) NULL,
                        created_date TIMESTAMP NOT NULL,
                        last_updated_date TIMESTAMP NOT NULL
                        )
                    """,
                    """
                    CREATE TABLE symbol (
                        id SERIAL PRIMARY KEY,
                        exchange_id integer NULL,
                        ticker TEXT NOT NULL,
                        instrument TEXT NOT NULL,
                        name TEXT NOT NULL,
                        sector TEXT NOT NULL,
                        currency VARCHAR(64) NULL,
                        created_date TIMESTAMP NOT NULL,
                        last_updated_date TIMESTAMP NOT NULL,
                        FOREIGN KEY (exchange_id) REFERENCES exchange(id)
                        )
                    """,
                    """
                    CREATE TABLE daily_data (
                        id SERIAL PRIMARY KEY,
                        data_vendor_id INTEGER NOT NULL,
                        stock_id INTEGER NOT NULL,
                        created_date TIMESTAMP NOT NULL,
                        last_updated_date TIMESTAMP NOT NULL,
                        date_price DATE,
                        open_price NUMERIC,
                        high_price NUMERIC,
                        low_price NUMERIC,
                        close_price NUMERIC,
                        adj_close_price NUMERIC,
                        volume BIGINT,
                        FOREIGN KEY (data_vendor_id) REFERENCES data_vendor(id),
                        FOREIGN KEY (stock_id) REFERENCES symbol(id)
                        )                    
                    """)
        try:
            for command in commands:
                print('Building tables.')
                conn = psycopg2.connect(host=db_host,database=db_name, user=db_user, password=db_password)
                cur = conn.cursor()
                cur.execute(command)
                # need to commit this change
                conn.commit()
                cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cur.close()
        finally:
            if conn:
                conn.close()
    else:
        pass

    
def load_db_credential_info(f_name_path):
    """
    load text file holding our database credential info and the database name
    args:
        f_name_path: name of file preceded with "\\", type string
    returns:
        array of 4 values that should match text file info
    """
    cur_path = os.getcwd()
    # lets load our database credentials and info
    f = open(cur_path + f_name_path, 'r')
    lines = f.readlines()[1:]
    lines = lines[0].split(',')
    return lines


def main():
    # name of our database credential files (.txt)
    db_credential_info = "database_info.txt"
    # create a path version of our text file
    db_credential_info_p = "\\" + db_credential_info
    
    # create our instance variables for host, username, password and database name
    db_host, db_user, db_password, db_name = load_db_credential_info(db_credential_info_p)
    
    # first lets create our database from postgres
    create_db([db_host, db_user, db_password, db_name])
    
    # second lets create our tables for our new database
    create_mkt_tables([db_host, db_user, db_password, db_name])

    
if __name__ == "__main__":
    main()