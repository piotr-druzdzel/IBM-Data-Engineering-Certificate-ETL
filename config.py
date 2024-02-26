"""
Configuration variables for the project from IBM Certificate
"""

# Archive URL to extract the data using webscraping
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'

table_attribs = ['Name', 'MC_USD_Billion']   # Required attributes

csv_file = 'Largest_banks_data.csv'             # Required file name
db_file = 'Banks.db'                            # Required database name
log_file = 'ETL.log'                            # Log history

table_name = 'Largest_banks'                    # Required SQL table name 
