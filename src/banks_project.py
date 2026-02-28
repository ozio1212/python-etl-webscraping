from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

# 1. Logging Function
def log_progress(message):
    ''' This function logs the progress of the ETL process by printing
    the message with the current timestamp. Function returns nothing.'''
    
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format)
    
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

# 2. Extract Function
def extract(url, table_attribs):
    ''' This function extracts the required information from the website 
    and saves it to a dataframe. The function returns the dataframe for 
    further processing. '''
    
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    
    # Locate the table. For this specific archive, it is the first table (index 0)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    data_list = []
    
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            # Name extraction
            anchors = col[1].find_all('a')
            if len(anchors) > 1:
                bank_name = anchors[1].text.strip()
            else:
                bank_name = col[1].text.strip()

            # Market Cap extraction
            # Using strip() is safer than [:-1] to remove newlines/spaces
            market_cap_str = str(col[2].contents[0]).strip()
            market_cap = float(market_cap_str)
            
            # Append to list
            data_dict = {
                "Name": bank_name,
                "MC_USD_Billion": market_cap
            }
            data_list.append(data_dict)
    
    df = pd.DataFrame(data_list, columns=table_attribs)
    
    return df

# 3. Transform Function
def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    # Read the exchange rate CSV file
    exchange_rate_df = pd.read_csv(csv_path)
    
    # Convert to dictionary: keys are Currency, values are Rate
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    
    # Add MC_GBP_Billion
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    
    # Add MC_EUR_Billion
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    
    # Add MC_INR_Billion
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    
    return df

# 4. Load to CSV Function
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path, index=False)
    
    # CORRECTION: Log progress INSIDE the function
    log_progress('Data saved to CSV file')

# 5. Load to Database Function
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    
    # CORRECTION: Log progress INSIDE the function
    log_progress('Data loaded to Database as table. Running the query')

# 6. Run Query Function
def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    
    # CORRECTION: Log progress INSIDE the function
    log_progress(f'Query executed: {query_statement}')

# --- Main Execution Block ---

# CORRECTION: Removed space in URL
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

# 1. Log initialization
log_progress('Preliminaries complete. Initiating ETL process')

# 2. Extract
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

# 3. Transform
# Note: Ensure exchange_rate.csv is in the same directory
df = transform(df, "./exchange_rate.csv") 
log_progress('Data transformation complete. Initiating Loading process')

# 4. Load to CSV
load_to_csv(df, csv_path)
# Note: log_progress is now inside the function

# 5. SQL Connection
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')

# 6. Load to DB
load_to_db(df, sql_connection, table_name)
# Note: log_progress is now inside the function

# 7. Run Queries
query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name FROM {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

# 8. Close Connection
log_progress('Process Complete.')
sql_connection.close()