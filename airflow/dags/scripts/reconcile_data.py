###############################################################################
# Name: reconcile_data.py
# Description: 
#
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/weather_analysis
###############################################################################
import os

import snowflake.connector
from airflow.utils.log.logging_mixin import LoggingMixin


def main():
    LoggingMixin().log.info("Process has started")

    

    LoggingMixin().log.info("Process has completed")



if __name__ == "__main__":    
    # Define Snowflake connection
    snowflake_user = os.environ["SNOWFLAKE_USER"]
    snowflake_pwd = os.environ["SNOWFLAKE_PWD"]
    snowflake_acct = os.environ["SNOWFLAKE_ACCT"]
    snowflake_wh = "COMPUTE_WH"
    snowflake_db = "WEATHER_ANALYSIS"
    snowflake_schema = "STAGING"
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_pwd,
        account=snowflake_acct,
        warehouse=snowflake_wh,
        database=snowflake_db,
        schema=snowflake_schema
    )
    cur = conn.cursor()