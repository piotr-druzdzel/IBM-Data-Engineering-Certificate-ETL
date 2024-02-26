"""
Load functions of the ETL process for CSV and SQL 
with added logging functionality via the decorator.
"""

import logging
import pandas as pd
from progress_logging import log

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
def load_to_db(df: pd.DataFrame, sql_connection: object, table_name: str, db_file: str) -> None:
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
    