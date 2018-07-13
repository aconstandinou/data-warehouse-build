# -*- coding: utf-8 -*-
"""
Created on Thu Jul 12 13:37:39 2018

@author: antonio constandinou
"""

from __future__ import print_function

import psycopg2
import os

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

   
def obtain_list_db_tickers(conn):
    """
    query our Postgres database table 'symbol' for a list of all tickers in our symbol table
    args:
        conn: a Postgres DB connection object
    returns: 
        list of tuples
    """
    with conn:
        cur = conn.cursor()
        cur.execute("SELECT id, ticker FROM symbol")
        data = cur.fetchall()
        return [(d[0], d[1]) for d in data]


def select_first_last_dates(symbol_id, symbol, conn):
    """
    query our Postgres database for the first date and last date of each stocks historical data
    args:
        symbol_id: integer 
        symbol: string
        conn: a Postgres DB connection object     
    returns: 
        string including symbol, first_date, last_date
    """
    cur = conn.cursor()
    cur.execute(
                """
                SELECT MIN(daily_data.date_price), MAX(daily_data.date_price) FROM daily_data
                JOIN symbol ON symbol.id = daily_data.stock_id
                WHERE symbol.id = %s
                """,
                (symbol_id,)
                )
    data = cur.fetchall()
    first_date = data[0][0].strftime('%m/%d/%Y')
    last_date = data[0][1].strftime('%m/%d/%Y')
    return str.join(',', (symbol, first_date, last_date))

  
def main():
    # Connect to our Postgres database 'securities_master'
    db_info_file = "database_info.txt"
    db_info_file_p = "\\" + db_info_file
    # necessary database info to connect
    db_host, db_user, db_password, db_name = load_db_credential_info(db_info_file_p)
    
    # connect to our securities_master database
    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
    
    # list of tuples: stock data pulled from our DB securities_master, table symbol
    # stock_data[0] = table id
    # stock_data[1] = ticker
    stock_data = obtain_list_db_tickers(conn)
    
    # load in our 'failed' stocks to ensure we skip these
    failed_symbols_file = '\\failed_symbols.txt'
    cur_path = os.getcwd()
    failed_symbols_file_path = cur_path + failed_symbols_file
    
    with open(failed_symbols_file_path) as f:
        failed_symbols = f.readlines()
    
    # create a list of failed symbols    
    failed_symbols = [x.strip('\n') for x in failed_symbols]
    
    # our collection of date strings that will include 'symbol,first_date,last_date'
    collect_date_array = []
    
    for stock in stock_data:
        # get the earliest and last market data 'date' for a given stock
        symbol_id = stock[0]
        symbol = stock[1]
        if symbol in failed_symbols:
            continue
        else:
            print('Fetching first date and last date for {}'.format(symbol))
            dates_data_string = select_first_last_dates(symbol_id, symbol, conn)
            collect_date_array.append(dates_data_string)
        
    # lets write our first_date and last_date for each ticker
    file_to_write = open('stock_dates.txt', 'w')

    for date_data in collect_date_array:
        file_to_write.write("%s\n" % date_data)


if __name__ == "__main__":
    main()
    