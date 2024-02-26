# Project Overview

This Python project automates the generation of a report on the top 10 largest banks in the world, ranked by market capitalization in various currencies. It retrieves data from a webpage, transforms it based on exchange rates, and stores it in both a CSV file and an SQL database. The script is designed to be executed quarterly for updated reports.

## Project Setup

**Data sources:**

* Bank data URL: https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks
* Exchange rate CSV: https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv

**Output:**

* CSV file: Largest_banks_data.csv
* Database: Banks.db, table: Largest_banks
* Log file: ETL.log

## Project Tasks

1. **Logging:** Implement a `log_progress()` function to record script progress in `code_log.txt`.
2. **Data Extraction:** Develop an `extract()` function to fetch data from the provided URL, targetting the table under "By market capitalization", and store it as a pandas DataFrame.
3. **Data Transformation:** Create a `transform()` function to add Market Capitalization columns in GBP, EUR, and INR based on the exchange rate CSV, rounding to 2 decimal places.
4. **CSV Output:** Build a `load_to_csv()` function to save the transformed DataFrame as a CSV file at the specified path.
5. **Database Output:** Implement a `load_to_db()` function to insert the DataFrame into the specified database table.
6. **SQL Queries:** Execute predefined queries on the database table using a `run_queries()` function and verify the results.
7. **Log Verification:** Ensure all stages are logged by checking `ETL.log`.

## Script Execution

1. Ensure necessary libraries are installed (pandas, requests, etc.).
2. Customize parameters like URLs and paths if needed.
3. Run `main.py`.

## Additional Notes

* This script can be easily scheduled to run quarterly using cron jobs or similar tools for automated report generation.
* Further improvements could include error handling, input validation, and more comprehensive logging.

## Author

Piotr Drużdżel
