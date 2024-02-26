"""
Extracting module for the ETL process with added logging functionality via the decorator.
"""

import requests
import pandas as pd
from progress_logging import log
from bs4 import BeautifulSoup

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
