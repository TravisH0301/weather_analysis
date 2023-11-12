###############################################################################
# Name: generate_partition.py
# Description: This script automatically generates the followings based on the
#              available years in the preprocessed weather table in Snowflake
#              staging schema:
#              - Year partition table for weather measurement schemas 
#                (e.g., rain_2023)
#              - dbt data model for the created partition table
#                (e.g., rain_2023.sql)
#              This script allows the dataset to grow incrementally without
#              having to manually create new table nor dbt data model.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/weather_analysis
###############################################################################
import os
import logging

import snowflake.connector


def make_col_query_str(cols, purpose):
    """
    This function serves two purposes.
    1. Returning a partial query string that contains
    given columns and their data type.
    E.g., "MAXIMUM_TEMPERATURE FLOAT, MINIMUM_TEMPERATURE FLAT, "
    This goes into the variable `query_create_year_partition` for
    creating year partition tables for each weather measurement schemas.
    
    2. Returning a partial dbt model script query that
    contains given columns.
    E.g., "MAXIMUM_TEMPERATURE, MINIMUM_TEMPERATURE, "
    This goes into the variable `dbt_script_str` for
    generating dbt data model scripts for the created year partition tables.
    
    Parameters
    ----------
    cols: list
        List of columns to be added in the query.
    purpose: str
        Purpose for ethier year_partition_table or dbt_model_script

    Returns
    -------
    string
        Query string for columns and their data types.
    """
    query_str = ""
    for col in cols:
        if purpose == "year_partition_table":
            query_str += col + " FLOAT, "
        elif purpose == "dbt_model_script":
            query_str += col +", "
    return query_str


def generate_dbt_model_script(schema, year, script_name, target_location):
    """
    This function automatically generates a dbt data model script
    of the year partition table for the given schema and year 
    in the target location.

    Parameters
    ----------
    schema: str
        Name of schema.
    year: int/str
        Year for partition table.
    target_location: str
        Location for dbt data model script.
    """
    schema_lower = schema.lower()
    attribute_li = weather_schema_dict[schema]
    attribute_query_str = make_col_query_str(attribute_li, purpose="dbt_model_script")

    with open(target_location.format(schema_lower, script_name), "w") as f:
        f.write(dbt_script_str.format(attribute_query_str))


def main():
    logging.info("Process has started")

    # Fetch years from preprocessed weather table in staging schema
    logging.info("Fetching years from preprocessed weather table...")
    cur.execute(query_fetch_weather_years)
    result = cur.fetchall()
    year_li = [year[0] for year in result]
    logging.info("Years have been fetched")

    # For each weather schema, create year partition tables if not existing
    # and generate dbt model scripts for the created year partition tables
    logging.info("Creating year partition tables & dbt model scripts for weather schemas...")
    for schema, cols in weather_schema_dict.items():
        cols_query_str = make_col_query_str(cols, purpose="year_partition_table")
        for year in year_li:
            # Create year partition table
            cur.execute(query_create_year_partition.format(schema, year, cols_query_str))
            response = cur.fetchall()[0][0]
            logging.info(response)
            
            if "successfully created" in response:
                # Generate dbt model script for year partition table
                script_name = f"{schema.lower()}_{year}.sql" 
                generate_dbt_model_script(schema, year, script_name, target_location)
                logging.info(f"dbt model script {script_name} has been created")
    
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

    # Define dbt data model script
    target_location = "./dbt/models/{}/{}"
    dbt_script_str_1 = "{{{{\n    config(\n        materialized='incremental'\n    )\n}}}}"
    dbt_script_str_2 = "\n\n{{{{\n    generate_year_partition_model(\n        \"{}\"\n    )\n}}}}"
    dbt_script_str = dbt_script_str_1 + dbt_script_str_2

    try:
        main()
    except Exception:
        logging.error("Process has failed:", exc_info=True)
        raise
    finally:
        cur.close()
        conn.close()
