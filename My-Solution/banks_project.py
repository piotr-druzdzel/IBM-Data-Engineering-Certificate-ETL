import sqlite3
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from functools import wraps


def log_progress(message: str) -> None:
    """Logs a message with a timestamp to both the console and a log file.

    Args:
        message (str): The message to be logged.

    Returns:
        None
    """
    
    print(message)
    logging.info(message)

def log(func: callable) -> callable:
    """Decorator that logs the start and end of a function call,
    along with exception handling and re-raising.

    Args:
        func (callable): The function to be decorated.

    Returns:
        function: The decorated wrapper function.

    Raises:
        Exception: Any exception raised within the decorated function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Log start with message and function name
            log_progress(f"Calling {func.__name__} ...")
            result = func(*args, **kwargs)
            # Log end with message and function name
            log_progress(f"Finished {func.__name__}.")
            
            return result
        
        except Exception as e:
            logging.exception(f"Exception raised in {func.__name__}. exception: {str(e)}.")
            raise e

    return wrapper


@log
def extract(url: str, table_attributes: list[str]) -> pd.DataFrame:

    """
    Extracts tabular information from a given URL under the heading 
    "By Market Capitalization" and saves it to a pandas DataFrame.

    Args:
        url (str): The URL of the webpage containing the table.
        table_attributes (list): A list of column names for the DataFrame.

    Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.

    Raises:
        ValueError: If the table is not found.
    """

    # Get URL content
    response = requests.get(url)

    # Parse HTML content to Beautiful Soup object
    soup = BeautifulSoup(response.text, "html.parser")

    # Find the right table in the Soup object 
    tables = soup.find_all("table", class_="wikitable")

    # Choose the first table, called: "By market capitalization"
    table = tables[0]

    # Identify rows (containing many columns) in the table
    rows = table.findAll('tr')

    # Extract data from the table row by row
    # Initialize an empty list to store Wikitable data
    data = []
    for row in rows[1:]:

        # Create a list fo columns in each row
        columns = row.find_all('td')

        # Extract text from specific cell and strip
        bank_name = columns[1].text.strip()
        market_cap = float(columns[2].text.strip())

        # Append rows as lists to list
        data.append([bank_name, market_cap])

        # Create the dataframe from the appended data list
        df = pd.DataFrame(data, columns=table_attributes)

    return df


@log
def rate(currency: str) -> float:

    try:
        # Load the CSV data into a DataFrame
        df = pd.read_csv("exchange_rate.csv")

    except FileNotFoundError:
        print("Error: exchange_rate.csv file not found.")
        return None

    try:
        # Extract exchange rates into variables
        eur_rate =df[df['Currency'] == currency]['Rate'].values[0]
        gbp_rate = df[df['Currency'] == currency]['Rate'].values[0]
        inr_rate = df[df['Currency'] == currency]['Rate'].values[0]

        if currency == 'EUR':
            return eur_rate
        elif currency == 'GBP':
            return gbp_rate
        elif currency == 'INR':
            return inr_rate
        else:
            print('\nWrong currency specified.\n')

    except Exception as e:
        print(f"Error: Invalid data provided: \n{e}")
        return None
    

@log
def transform(df: pd.DataFrame) -> pd.DataFrame:

    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * rate('GBP'), 2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * rate('EUR'), 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * rate('INR'), 2)

    return df


@log
def load_to_csv(df: pd.DataFrame, csv_path: str) -> None:
    """
    Saves a pandas DataFrame to a CSV file at the specified path.

    Args:
        df (pandas.DataFrame): The DataFrame to save as a CSV file.
        csv_path (str): The path to the output CSV file.

    Raises:
        ValueError: If `csv_path` is not a valid string representing a file path.
        IOError: If there are errors opening or writing to the CSV file.
        TypeError: If `df` is not a pandas DataFrame.
        IndexError: If the DataFrame has a multi-index and `index=True` is not specified in `to_csv`.
        UnicodeEncodeError: If there are issues encoding strings in the DataFrame.

    Returns:
        None
    """

    with open(csv_path, 'w') as file:
        try:
            df.to_csv(file, index=False)
            print(f"Saved dataframe to the CSV file: {csv_path}.")

        except (IOError, ValueError, IndexError, UnicodeEncodeError) as e:
            # Handle specific exceptions with tailored messages
            print(f"Specific error encountered: \n{e}")
        
        except Exception as e:
            # Handle any other unexpected errors
            logging.error("General error writing to CSV:", exc_info=True)


@log
def load_to_db(df: pd.DataFrame, sql_connection: object, table_name: str) -> None:
    """
    Saves a DataFrame to a SQL database as a table.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        sql_connection: An open SQL connection object.
        table_name (str): The name of the table to create or replace.

    Raises:
        ValueError: If the DataFrame is empty.
        Exception: For any other errors encountered during saving.
    """

    if df.empty:
        raise ValueError("DataFrame is empty. Cannot load to database.")

    try:
        with sql_connection:
            df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
            print(f"Saved dataframe to the database: {db_file}.")

    except Exception as e:
        raise Exception(f"Error saving DataFrame to database: {e}")
    

@log
def run_query(query_statement: str, sql_connection: object) -> None:
    """
    Executes a given SQL query on a database table using a provided sql_connection object.
    Prints the executed query and resulting DataFrame to the console.

    Args:
        query_statement: The SQL query string to execute.
        sql_connection: A valid sql_connection object used to connect to the database.

    Returns:
        None

    Raises:
        Exception: If an error occurs during query execution.
    """

    try:
        with sql_connection:
            print(f"\nExecuted query:\n{query_statement}\n")
            print(pd.read_sql(query_statement, sql_connection))

    except Exception as e:
        print(f"Error running query: {e}")


if __name__ == "__main__":

    # Define variables
    url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'

    table_attribs_in = ['Name', 'MC_USD_Billion']
    csv_file = 'Largest_banks_data.csv'
    db_file = 'Banks.db'
    log_file = 'ETL.log'
    table_name = 'Largest_banks'

    # Establish SQL connection
    connection = sqlite3.connect(db_file)

    # Save logs to log_file and define loggging level
    #logging.basicConfig(filename=log_file, level=logging.INFO)
    logging.basicConfig(filename=log_file,
                        format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')

    # Mark a new run in .log file
    logging.info("Beginning the ETL process...")

    # Extract data from the website
    df = extract(url, table_attribs_in)

    # Transform data according to requirements
    df = transform(df)

    # Load the data
    load_to_csv(df, csv_file)
    load_to_db(df, connection, table_name)

    # Run the following queries on the database table:
    query_a = f"SELECT Name, MC_GBP_Billion FROM {table_name}"
    query_b = f"SELECT Name, MC_EUR_Billion FROM {table_name}"
    query_c = f"SELECT Name, MC_INR_Billion FROM {table_name}"
    
    run_query(query_a, connection)
    run_query(query_b, connection)
    run_query(query_c, connection)

    # Mark the end of the process
    logging.info("Completed the ETL process.\n")
    print("\nCompleted the ETL process.\n")
