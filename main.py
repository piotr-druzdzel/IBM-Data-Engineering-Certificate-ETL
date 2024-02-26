"""
Main script to perform the complete ETL (Extract, Transform, Load) process 
for a webscraped data as a project for the IBM Data Engineergin Certificate.

1. Extracts data:
    - Fetches HTML data from a webpage using the `extract` function.

2. Transforms data:
    - Cleans and manipulates the extracted data using the `transform` function.

3. Loads data:
    - Saves the transformed data to a CSV file using the `load_to_csv` function.
    - Saves the data to a SQLite database using the `load_to_db` function.

4. Queries data:
    - Executes predefined queries on the database table using the `run_query` function.

5. Logs progress:
    - Utilises a decorator to extend the functionalities of chosen functions.
"""

import sqlite3
import logging

from config import url, table_attribs, csv_file, db_file, log_file, table_name

from extract import extract
from transform import transform
from load import load_to_csv, load_to_db

from queries import run_query


if __name__ == "__main__":

    # Establish SQL connection
    connection = sqlite3.connect(db_file)

    # Save logs to log_file and define loggging level
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        handlers=[logging.FileHandler(log_file, 'a'),   # log to file
                                  logging.StreamHandler()])             # log to console

    # Mark a new run in .log file
    logging.info("Beginning the ETL process...")

    # Extract data from the website
    df = extract(url, table_attribs)

    # Transform data according to requirements
    df = transform(df)

    # Load the data
    load_to_csv(df, csv_file)
    load_to_db(df, connection, table_name, db_file)

    # Run the following queries on the database table:
    query_a = f"SELECT Name, MC_GBP_Billion FROM {table_name}"
    query_b = f"SELECT Name, MC_EUR_Billion FROM {table_name}"
    query_c = f"SELECT Name, MC_INR_Billion FROM {table_name}"
    
    run_query(query_a, connection)
    run_query(query_b, connection)
    run_query(query_c, connection)

    # Mark the end of the process
    logging.info("Completed the ETL process.\n")
