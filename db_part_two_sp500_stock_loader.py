# -*- coding: utf-8 -*-
"""
Created on Tue Jul 10 11:16:08 2018

@author: antonio constandinou
"""
from __future__ import print_function

import datetime
import bs4
import psycopg2
import requests
import os

def parse_wiki_snp500():
    """
    Download and parse Wikipedia for the current list of S&P500 companies.
    return:
        list of tuples to add to PostgreSQL.
    """
    now = datetime.datetime.utcnow()
    
    # return html of our desired S&P 500 webapge on wikipedia
    response = requests.get("http://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    # soup object
    soup = bs4.BeautifulSoup(response.text)
    
    # CSS Selector syntax: find first table, select all rows and skip headers row 
    symbols_list = soup.select('table')[0].select('tr')[1:]
    
    symbols = []
    for i, symbol in enumerate(symbols_list):
        # standard cell containing our data: 'td'
        tds = symbol.select('td')
        symbols.append(
                        (tds[0].select('a')[0].text,'equity',
                         tds[1].select('a')[0].text,
                         tds[3].text, 'USD', now, now)
                      )
    return symbols

def insert_snp500_symbols_postgres(symbols, db_host, db_user, db_password, db_name):
    """
    Load S&P500 symbols into our PostgreSQL database.
    """
    # Connect to our PostgreSQL database
    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
    
    column_str = """
                 ticker, instrument, name, sector, currency, created_date, last_updated_date
                 """
    insert_str = ("%s, " * 7)[:-2]
    final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
    with conn:
        cur = conn.cursor()
        cur.executemany(final_str, symbols)
        
def load_db_info(f_name_path):
    cur_path = os.getcwd()
    # lets load our database credentials and info
    f = open(cur_path + f_name_path, 'r')
    lines = f.readlines()[1:]
    lines = lines[0].split(',')
    return lines

def main():
    db_info_file = "database_info.txt"
    db_info_file_p = "\\" + db_info_file
    # necessary database info to connect and load our symbols further below
    db_host, db_user, db_password, db_name = load_db_info(db_info_file_p)
    
    symbols = parse_wiki_snp500()
    insert_snp500_symbols_postgres(symbols, db_host, db_user, db_password, db_name)
    print("%s symbols were successfully added." % len(symbols))  
    
if __name__ == "__main__":
    main()
      