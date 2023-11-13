###############################################################################
# Name: generate_partition.py
# Description: This script automatically generates the followings based on the
#              available years in the preprocessed weather table in Snowflake
#              staging schema:
#              - Year partition table for weather measurement schemas 
#                (e.g., rain_2023)
#              - dbt data model & schema file for the created partition table
#                (e.g., rain_2023.sql, rain_2023.yml)
#              This script allows the dataset to grow incrementally without
#              having to manually create new table nor dbt data model.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/weather_analysis
###############################################################################
import os
import yaml

import snowflake.connector
from airflow.utils.log.logging_mixin import LoggingMixin


def make_col_query_str(cols, purpose):
    """
    This function serves two purposes.
    1. Returning a partial query string that contains
    given columns and their data type.
    E.g., Input: ["MAXIMUM_TEMPERATURE", "MINIMUM_TEMPERATURE"]
          Output: "MAXIMUM_TEMPERATURE FLOAT, MINIMUM_TEMPERATURE FLAT, "
    This goes into the variable `query_create_year_partition` for
    creating year partition tables for each weather measurement schemas.
    
    2. Returning a partial dbt model script query that
    contains given columns.
    E.g., Input: ["MAXIMUM_TEMPERATURE", "MINIMUM_TEMPERATURE"]
          Output: "MAXIMUM_TEMPERATURE, MINIMUM_TEMPERATURE, "
    This goes into the variable `dbt_script_str` for
    generating dbt data model scripts for the created year partition tables.
    
    Parameters
    ----------
    cols: list
        List of columns to be added in the query.
    purpose: str
        Purpose for ethier "year_partition_table" or "dbt_model_script"

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

    The dbt model script is designed to call the dbt macro 
    `generate_year_partition_model` to create a year partition data model
    with the given year and attributes. This macro is universal across
    all weather schemas.
    
    Parameters
    ----------
    schema: str
        Name of schema.
    year: int/str
        Year for partition table.
    script_name: str
        Name of dbt script.
    target_location: str
        Location for dbt data model script.
    """
    schema_lower = schema.lower()
    attribute_li = weather_schema_dict_model[schema]
    attribute_query_str = make_col_query_str(attribute_li, purpose="dbt_model_script")

    with open(target_location.format(schema_lower, script_name), "w") as f:
        f.write(dbt_script_str.format(attribute_query_str, year))


def generate_schema_yml(schema, year, col_schema):
    """
    This function creates a schame yaml file for the year partition tables.
    Schema-specific columns, their descriptions and test cases (optional) 
    can be added dynamically by passing them in a dictionary.

    Parameters
    ----------
    schema: str
        Name of schema.
    year: int/str
        Year for partition table.
    col_schema: dict
        Dictionary of schema-specific columns details.
    """
    # Define base schame
    schema_dict = {
        "version": 2,
        "models": [{
            "name": f"{schema}_{year}",
            "columns": [
                {
                    "name": "record_id",
                    "description": "Synthetic key consisted of station name and date",
                    "tests": ["not_null", "unique"]
                },
                {
                    "name": "station_name",
                    "description": "Weather station name",
                    "tests": ["not_null"]
                },
                {
                    "name": "date",
                    "description": "Measurement date",
                    "tests": ["not_null"]
                }
            ]
        }]
    }
    
    # Dynamically add schema specifc columns and their tests to schema
    for col_name, col_detail in col_schema.items():
        if len(col_detail) > 1:
            column_entry = {
                "name": col_name,
                "description": col_detail[0],
                "tests": col_detail[1]
            }
        else:
            column_entry = {
                "name": col_name,
                "description": col_detail[0],
            }
        schema_dict["models"][0]["columns"].append(column_entry)

    # Add last 2 static columns to schema
    schema_dict["models"][0]["columns"].append({
        "name": "state",
        "description": "Address state",
        "tests": [
            {
                "dbt_expectations.expect_column_values_to_be_in_set": {
                    "value_set": "['NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']"
                }
            }
        ]
    })
    schema_dict["models"][0]["columns"].append({
        "name": "load_date",
        "description": "Date of data load from staging schema",
    })

    # Write yaml file
    file_path = f"/opt/airflow/dags/dbt/models/{schema}/{schema}_{year}.yml"
    with open(file_path, "w") as f:
        yaml.dump(schema_dict, f, sort_keys=False)

    # Fix formatting from yaml dump and rewrite yaml file
    with open(file_path, "r") as f:
        schema_str = "".join(f.readlines())
        schema_str_formatted = schema_str.replace("''", "'").replace("'[", "[").replace("]'", "]")
        with open(file_path, "w") as f:
            f.write(schema_str_formatted)


def main():
    LoggingMixin().log.info("Process has started")

    # Fetch years from preprocessed weather table in staging schema
    LoggingMixin().log.info("Fetching years from preprocessed weather table...")
    cur.execute(query_fetch_weather_years)
    result = cur.fetchall()
    year_li = [year[0] for year in result]
    LoggingMixin().log.info("Years have been fetched")

    # For each weather schema, create year partition tables if not existing
    # and generate dbt model scripts & respective schema files for the 
    # created year partition tables
    LoggingMixin().log.info("Creating year partition tables & dbt model scripts for weather schemas...")
    for schema, cols in weather_schema_dict_table.items():
        cols_query_str = make_col_query_str(cols, purpose="year_partition_table")
        for year in year_li:
            # Create year partition table
            cur.execute(query_create_year_partition.format(schema, year, cols_query_str))
            response = cur.fetchall()[0][0]
            LoggingMixin().log.info(response)

            # Only generate dbt objects when new year partition table is created
            if "successfully created" in response:
                # Generate dbt model script for year partition table
                schema_lower = schema.lower()
                script_name = f"{schema_lower}_{year}.sql" 
                generate_dbt_model_script(schema, year, script_name, target_location)
                LoggingMixin().log.info(f"dbt model script {script_name} has been created")

                # Generate respective schema file for the above model script
                col_schema = weather_schema_dict_yaml[schema]
                generate_schema_yml(schema_lower, year, col_schema)
                LoggingMixin().log.info(f"dbt model schema file {schema}_{year}.yml has been created")
    
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

    # Define Snowflake weather measurement schemas and their attributes
    ## For year partition tables
    """
    E.g., { Weather schema: Attributes of respective year partition table }
    """
    weather_schema_dict_table = {
        "EVAPO_TRANSPIRATION": ["EVAPO_TRANSPIRATION"],
        "RAIN": ["RAIN"],
        "PAN_EVAPORATION": ["PAN_EVAPORATION"],
        "TEMPERATURE": [
            "MAXIMUM_TEMPERATURE",
            "MINIMUM_TEMPERATURE",
            "VARIANCE_TEMPERATURE"
        ],
        "RELATIVE_HUMIDITY": [
            "MAXIMUM_RELATIVE_HUMIDITY",
            "MINIMUM_RELATIVE_HUMIDITY"
        ],
        "WIND_SPEED": ["AVERAGE_10M_WIND_SPEED"],
        "SOLAR_RADIATION": ["SOLAR_RADIATION"]
    }
    ## For dbt data model scripts
    """
    E.g., { Weather schema: Partial query to model the attributes 
                            of the respective year partition table }
    """
    weather_schema_dict_model = {
        "EVAPO_TRANSPIRATION": ["EVAPO_TRANSPIRATION"],
        "RAIN": ["RAIN"],
        "PAN_EVAPORATION": ["PAN_EVAPORATION"],
        "TEMPERATURE": [
            "MAXIMUM_TEMPERATURE",
            "MINIMUM_TEMPERATURE",
            "MAXIMUM_TEMPERATURE - MINIMUM_TEMPERATURE AS VARIANCE_TEMPERATURE"
        ],
        "RELATIVE_HUMIDITY": [
            "MAXIMUM_RELATIVE_HUMIDITY",
            "MINIMUM_RELATIVE_HUMIDITY"
        ],
        "WIND_SPEED": ["AVERAGE_10M_WIND_SPEED"],
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
            STATION_NAME VARCHAR(100),
            DATE DATE,
            {2}
            STATE VARCHAR(3),
            LOAD_DATE DATE
        );
    """

    # Define dbt data model script
    """
    This script uses macro to generate a data model for year partition tables with
    the passed year and schema-specific attributes.
    """
    target_location = "/opt/airflow/dags/dbt/models/{}/{}"
    dbt_script_str_1 = "{{{{\n    config(\n        materialized='incremental'\n    )\n}}}}"
    dbt_script_str_2 = "\n\n{{{{\n    generate_year_partition_model_macro(\n        \"{}\", {}\n    )\n}}}}"
    dbt_script_str = dbt_script_str_1 + dbt_script_str_2

    # Define dictionary of schema-specific columns details
    """
    This dictionary holds schema-specific columns details including
    test cases (optional).
    E.g., 
    { 
        Weather schema: {
            Column name: [
                Column description,
                [ Test case (optional) ]
            ]
        }
    }
    """
    weather_schema_dict_yaml = {
        "EVAPO_TRANSPIRATION": {
            "evapo_transpiration": [
                "Evapo transpiration (mm)",
                [{
                    "dbt_expectations.expect_column_values_to_be_between": {
                        "min_value": "0"
                    }
                }]
            ],
        },
        "RAIN": {
            "rain": [
                "Rain fall (mm)",
                [{
                    "dbt_expectations.expect_column_values_to_be_between": {
                        "min_value": "0"
                    }
                }]
            ]
        },
        "PAN_EVAPORATION": {
            "pan_evaporation": [
                "Pan evaporation (mm)",
                [{
                    "dbt_expectations.expect_column_values_to_be_between": {
                        "min_value": "0"
                    }
                }]
            ]
        },
        "TEMPERATURE": {
            "maximum_temperature": ["Maximum temperature ('C)"],
            "minimum_temperature": ["Minimum temperature ('C)"],
            "variance_temperature": [
                "Temperature variance ('C)",
                [{
                    "dbt_expectations.expect_column_values_to_be_between": {
                        "min_value": "0"
                    }
                }]
            ]
        },
        "RELATIVE_HUMIDITY": {
            "maximum_relative_humidity": [
                "Maximum_relative_humidity(%)",
                [{
                    "dbt_expectations.expect_column_values_to_be_between": {
                        "min_value": "0"
                    }
                }]
            ],
            "minimum_relative_humidity": [
                "Minimum_relative_humidity(%)",
                [{
                    "dbt_expectations.expect_column_values_to_be_between": {
                        "min_value": "0"
                    }
                }]
            ]
        },
        "WIND_SPEED": {
            "average_10m_wind_speed": [
                "Average 10m wind speed (m/sec)",
                [{
                    "dbt_expectations.expect_column_values_to_be_between": {
                        "min_value": "0"
                    }
                }]
            ]
        },
        "SOLAR_RADIATION": {
            "solar_radiation": [
                "Solar radiation (MJ/sq m)",
                [{
                    "dbt_expectations.expect_column_values_to_be_between": {
                        "min_value": "0"
                    }
                }]
            ]
        }
    }

    try:
        # Start process
        main()
    finally:
        # Close cursor and connection
        cur.close()
        conn.close()
