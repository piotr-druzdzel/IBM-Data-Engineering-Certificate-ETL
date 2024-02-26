"""
Transform function of the ETL process 
with added logging functionality via the decorator.
"""

import pandas as pd
from progress_logging import log

@log
def rate(currency: str) -> float:
    """
    Fetches the exchange rate for a given currency from a CSV file.

    Args:
        currency (str): The currency code for which to find the exchange rate.
        Supported currencies: EUR, GBP, INR.

    Returns:
        float: The exchange rate for the specified currency, or `None` on error.

    Raises:
        FileNotFoundError: If the "exchange_rate.csv" file is not found.
        Exception: If there is an error parsing the CSV data or extracting the rate.

    Note:
        Assumes"exchange_rate.csv" file has columns: "Currency" and "Rate".s
    """

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
    """
    Transform the input DataFrame by adding new columns 
    representing market capitalization in different currencies.

    Parameters:
    - df (pd.DataFrame): Input DataFrame with a 'MC_USD_Billion' column.

    Returns:
    - pd.DataFrame: Transformed DataFrame with additional columns:
        - 'MC_GBP_Billion': Market capitalization in GBP (British Pounds) rounded to 2 decimal places.
        - 'MC_EUR_Billion': Market capitalization in EUR (Euros) rounded to 2 decimal places.
        - 'MC_INR_Billion': Market capitalization in INR (Indian Rupees) rounded to 2 decimal places.
    """

    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * rate('GBP'), 2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * rate('EUR'), 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * rate('INR'), 2)

    return df
