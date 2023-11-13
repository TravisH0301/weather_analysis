###############################################################################
# Name: reconcile_data.py
# Description: This script reconciles row count between staging schema and
#              weather schemas in Snowflake to ensure data integrity.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/weather_analysis
###############################################################################
import os

import snowflake.connector
from airflow.utils.log.logging_mixin import LoggingMixin


def extract_row_count(schema, start_year, end_year):
    """
    This function extracts the total row count of the given weather
    schema tables.

    Parameters
    ----------
    schema: str
        Weather schema.
    start_year: int/str
        Starting year to start count.
    end_year: int/str
        Ending year to stop count.

    Returns
    -------
    int
        Total row count of the schema.
    """
    # Build query to count rows of the schema
    query_count = f"WITH UNION_CTE AS\n(SELECT * FROM {schema}.{schema}_{start_year}"
    for year in range(start_year, end_year, 1):
        query_count += f"\nUNION\nSELECT * FROM {schema}.{schema}_{year+1}"
        if year + 1 == end_year:
            query_count += "\n)\nSELECT COUNT(*) FROM UNION_CTE"

    # Execute query
    cur.execute(query_count)
    result = cur.fetchall()
    row_count = result[0][0]
    return row_count


def main():
    LoggingMixin().log.info("Process has started")

    # Extract row count from staging schema
    LoggingMixin().log.info("Extracting row count from staging schema...")
    cur.execute(query_count_staging)
    result = cur.fetchall()
    row_count_stg = result[0][0]
    LoggingMixin().log.info("Row count has been extracted")

    # Extract row count from weather schemas
    LoggingMixin().log.info("Extracting row counts from weather scheams")
    weather_schema_counts = {}
    for schema in weather_schema_names:
        row_count = extract_row_count(weather_schema_names[0], 2012, 2023)
        weather_schema_counts[schema] = row_count
    LoggingMixin().log.info("Row counts have been extracted")

    # Reconcile row counts
    LoggingMixin().log.info("Reconciling row counts...")
    for schema, row_count_weather in weather_schema_counts.items():
        LoggingMixin().log.info(f"Staging schame row count: {row_count_stg}")
        LoggingMixin().log.info(f"{schema} schema row count: {row_count_weather}")
        if row_count_stg != row_count_weather:
            LoggingMixin().log.error("Reconciliation has failed")
            raise Exception("Reconciliation failure")
    LoggingMixin().log.info("Reconciliation has been successful")

    LoggingMixin().log.info("Process has completed")


if __name__ == "__main__":     
    # Define Snowflake connection
    snowflake_user = os.environ["SNOWFLAKE_USER"]
    snowflake_pwd = os.environ["SNOWFLAKE_PWD"]
    snowflake_acct = os.environ["SNOWFLAKE_ACCT"]
    snowflake_wh = "COMPUTE_WH"
    snowflake_db = "WEATHER_ANALYSIS"
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_pwd,
        account=snowflake_acct,
        warehouse=snowflake_wh,
        database=snowflake_db,
    )
    cur = conn.cursor()

    # Define schema names
    weather_schema_names = [
        "EVAPO_TRANSPIRATION",
        "RAIN",
        "PAN_EVAPORATION",
        "TEMPERATURE",
        "RELATIVE_HUMIDITY",
        "WIND_SPEED",
        "SOLAR_RADIATION"
    ]

    # Define Snowflake query
    query_count_staging = "SELECT COUNT(*) FROM STAGING.WEATHER_PREPROCESSED"

    try:
        # Start process
        main()
    finally:
        # Close cursor and connection
        cur.close()
        conn.close()
