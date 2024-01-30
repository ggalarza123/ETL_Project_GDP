# objectives:

# Write a data extraction function to retrieve the relevant information from the required URL.
# Transform the available GDP information into 'Billion USD' from 'Million USD'.
# Load the transformed information to the required CSV file and as a database file.
# Run the required query on the database.
# Log the progress of the code with appropriate timestamps.
# Project idea from IBM data engineer certification 

import sqlite3
import pandas as pd
import numpy
from datetime import datetime 
import requests
from bs4 import BeautifulSoup

# Code for ETL operations on Country-GDP data

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    page = requests.get(url)
    data = BeautifulSoup(page.text, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0 :
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {'Country': col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    # first convert the colum into a list
    df_GDP_list = df['GDP_USD_millions'].tolist()
    cleaned_GDP_list = []
    for item in df_GDP_list:
        # for every item in the list remove the comma
        cleaned_value = float(item.replace(',',''))
        # divide by 1000 to convert millions to billions and leave to decimal places
        cleaned_value = round(cleaned_value /1000,2)
        # append to the empty modified list
        cleaned_GDP_list.append(cleaned_value)
    # assign a new pointer from gdp list to cleaned list
    df_GDP_list = cleaned_GDP_list

    # Assign the list to the specific column
    # rename the colum
    df['GDP_USD_millions'] = df_GDP_list
    df = df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(pd.read_sql(query_statement,sql_connection))

def log_progress(message, log_file):
    ''' This function logs the mentioned message at a given stage of the code 
    execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

''' Here is defined the required entities and the relevant functions 
are called in the correct order to complete the project. '''

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

table_attribs = ['Country', 'GDP_USD_millions',]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'
log_file = "./log_file.txt"


log_progress("Starting ETL from online source", log_file)

log_progress("Starting Extraction", log_file)
df = extract(url, table_attribs)
log_progress("Extraction Ended", log_file)

log_progress("Starting Tranformation", log_file)
df= transform(df)
log_progress( "Tranformation Ended", log_file)

log_progress("Starting Load to CSV", log_file)
load_to_csv(df, csv_path)
log_progress("Load to CSV Ended", log_file)

conn = sqlite3.connect(db_name)
log_progress("Starting Load to DB", log_file)
load_to_db(df,conn,table_name)
log_progress("Load to DB Ended", log_file)

log_progress("Starting Query", log_file)
query_stmt = f"SELECT * FROM {table_name} WHERE GDP_USD_billions >=100"
run_query(query_stmt, conn)
conn.close()
log_progress("Query Ended", log_file)
log_progress("ETL process complete", log_file)

# successful output looks like:

''' 
        Country  GDP_USD_billions
0   United States          26854.60
1           China          19373.59
2           Japan           4409.74
3         Germany           4308.85
4           India           3736.88
..            ...               ...
64          Kenya            118.13
65         Angola            117.88
66           Oman            104.90
67      Guatemala            102.31
68       Bulgaria            100.64
'''

