###############################################################################
# Name: generate_partition.py
# Description: 
#
# 
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/weather_analysis
###############################################################################
import os
import logging
from datetime import datetime
import pytz
import pandas as pd

import snowflake.connector


def make_col_query_str(cols):
    """
    This function returns a query string that contains
    given columns and their data type.
    This is to be used in the query for creating year
    partition tables for each weather measurement schemas.
    
    Parameters
    ----------
    cols: list
        list of columns to be added in the query.

    Returns
    -------
    string
        Query string for columns and their data types.
    """
    query_str = ""
    for col in cols:
        query_str += col + " FLOAT, "
    return query_str


def main():
    logging.info("Process has started")

    # Fetch years from preprocessed weather table in staging schema
    logging.info("Fetching years from preprocessed weather table...")
    cur.execute(query_fetch_weather_years)
    result = cur.fetchall()
    year_li = [year[0] for year in result]
    logging.info("Years have been fetched")

    # For each weather schema, create year partition tables if not existing
    logging.info("Creating year partition tables for weather schemas...")
    for schema, cols in weather_schema_dict.items():
        cols_query_str = make_col_query_str(cols)
        for year in year_li:
            cur.execute(query_create_year_partition.format(schema, year, cols_query_str))
            response = cur.fetchall()[0][0]
            logging.info(response)
    
    logging.info("Process has completed")


if __name__ == "__main__":
    # Define logger
    logging.basicConfig(
        filename = "./log/generate_partition_log.txt",
        filemode="w",
        level=logging.INFO,
        format = "%(asctime)s; %(levelname)s; %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p %Z"
    )

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

    # Define Snowflake weather measurement schemas and their attributes
    weather_schema_dict = {
        "EVAPO_TRANSPIRATION": ["EVAPO_TRANSPIRATION"],
        "RAIN": ["RAIN"],
        "PAN_EVAPORATION": ["PAN_EVAPORATION"],
        "TEMPERATURE": ["MAX_TEMPERATURE", "MIN_TEMPERATURE"],
        "RELATIVE_HUMIDITY": ["MAX_RELATIVE_HUMIDITY", "MIN_RELATIVE_HUMIDITY"],
        "WIND_SPEED": ["AVG_10M_WIND_SPEED"],
        "SOLAR_RADIATION": ["SOLAR_RADIATION"]
    }

    # Define Snowflake queries
    query_fetch_weather_years = """
        SELECT DISTINCT EXTRACT(YEAR FROM DATE)
        FROM STAGING.WEATHER_PREPROCESSED
    """
    query_create_year_partition = """
        CREATE TABLE {0}.{0}_{1} IF NOT EXISTS (
            RECORD_ID VARCHAR(100),
            {2}
            STATE VARCHAR(3),
            LOAD_DATE DATE
        );
    """

    try:
        main()
    except Exception:
        logging.error("Process has failed:", exc_info=True)
        raise
    finally:
        cur.close()
        conn.close()
