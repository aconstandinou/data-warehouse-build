# -*- coding: utf-8 -*-
"""
Created on Tue Jul 10 21:11:09 2018

@author: antonio constandinou
"""

from __future__ import print_function

import datetime
import psycopg2
import fix_yahoo_finance as yf
import pandas as pd
import os

MASTER_LIST_FAILED_SYMBOLS = []

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


def insert_new_vendor(vendor, conn):
    """
    Create a new vendor in data_vendor table.
    args:
        vendor: name of our vendor, type string.
        conn: a Postgres DB connection object
    return:
        None
    """
    todays_date = datetime.datetime.utcnow()
    cur = conn.cursor()
    cur.execute(
                "INSERT INTO data_vendor(name, created_date, last_updated_date) VALUES (%s, %s, %s)",
                (vendor, todays_date, todays_date)
                )
    conn.commit()
    
    
def fetch_vendor_id(vendor_name, conn):
    """
    Retrieve our vendor id from our PostgreSQL DB, table data_vendor.
    args:
        vendor_name: name of our vendor, type string.
        conn: a Postgres DB connection object
    return:
        vendor id as integer
    """
    cur = conn.cursor()
    cur.execute("SELECT id FROM data_vendor WHERE name = %s", (vendor_name,))
    # will return a list of tuples
    vendor_id = cur.fetchall()
    # index to our first tuple and our first value
    vendor_id = vendor_id[0][0]
    return vendor_id


def load_yhoo_data(symbol, symbol_id, vendor_id, conn):
    """
    This will load stock data (date+OHLCV) and additional info to our daily_data table.
    args:
        symbol: stock ticker, type string.
        symbol_id: stock id referenced in symbol(id) column, type integer.
        vendor_id: data vendor id referenced in data_vendor(id) column, type integer.
        conn: a Postgres DB connection object
    return:
        None
    """
    
    cur = conn.cursor()
    # generic start date should pull all data for a given symbol
    start_dt = datetime.datetime(2004,12,30)
    end_dt = datetime.datetime(2017,12,1)
    
    yf.pdr_override()
    
    try:
        data = yf.download(symbol, start=start_dt, end=end_dt)
    except:
        MASTER_LIST_FAILED_SYMBOLS.append(symbol)
        raise Exception('Failed to load {}'.format(symbol))
        
    data['Date'] = data.index
    
    # create new dataframe matching our table schema
    # and re-arrange our dataframe to match our database table
    columns_table_order = ['data_vendor_id', 'stock_id', 'created_date', 
                           'last_updated_date', 'date_price', 'open_price',
                           'high_price', 'low_price', 'close_price',
                           'adj_close_price', 'volume']
    
    newDF = pd.DataFrame()
    newDF['date_price'] = data['Date']
    newDF['open_price'] = data['Open']
    newDF['high_price'] = data['High']
    newDF['low_price'] = data['Low']
    newDF['close_price'] = data['Close']
    newDF['adj_close_price'] = data['Adj Close']
    newDF['volume'] = data['Volume']
    newDF['stock_id'] = symbol_id
    newDF['data_vendor_id'] = vendor_id
    newDF['created_date'] = datetime.datetime.utcnow()
    newDF['last_updated_date'] = datetime.datetime.utcnow()
    newDF = newDF[columns_table_order]
    
    # convert our dataframe to a list
    list_of_lists = newDF.values.tolist()
    # convert our list to a list of tuples       
    tuples_mkt_data = [tuple(x) for x in list_of_lists]
    
    # WRITE DATA TO DB
    insert_query =  """
                    INSERT INTO daily_data (data_vendor_id, stock_id, created_date,
                    last_updated_date, date_price, open_price, high_price, low_price, close_price, 
                    adj_close_price, volume) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
    cur.executemany(insert_query, tuples_mkt_data)
    conn.commit()    
    print('{} complete!'.format(symbol))
   

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
    
    # vendor name for Yahoo
    vendor = 'Yahoo Finance'
    # insert new vendor to data_vendor table and fetch its id needed for stock data dump
    
    insert_new_vendor(vendor, conn)
    
    vendor_id = fetch_vendor_id(vendor, conn)
    
    for stock in stock_data:
        # download stock data and dump into daily_data table in our Postgres DB
        symbol_id = stock[0]
        symbol = stock[1]
        print('Currently loading {}'.format(symbol))
        try:
            load_yhoo_data(symbol, symbol_id, vendor_id, conn)
        except:
            continue
        
    # lets write our failed stock list to text file for reference
    file_to_write = open('failed_symbols.txt', 'w')

    for symbol in MASTER_LIST_FAILED_SYMBOLS:
        file_to_write.write("%s\n" % symbol)
        
if __name__ == "__main__":
    main()