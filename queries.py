"""
SQL queries to be executed on the loaded database 
with added logging functionality via the decorator.
"""

import pandas as pd
from progress_logging import log

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
