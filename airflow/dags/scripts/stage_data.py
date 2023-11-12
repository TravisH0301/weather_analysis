###############################################################################
# Name: stage_data.py
# Description: This script extracts weather and station datasets from the
#              compressed BOM dataset file in the object storage. And this 
#              pre-processes and loads the datasets into the Snowflake
#              staging schema.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/weather_analysis
###############################################################################
import os
import io
import tarfile
from datetime import datetime
import pytz
import pandas as pd
import numpy as np

import boto3
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow.utils.log.logging_mixin import LoggingMixin


def find_latest_file(s3_client, bucket_name):
    """
    This function looks up the object storage
    bucket to find the latest file.

    Parameters
    ----------
    s3_client: object
        boto3 S3 client.
    bucket_name: str
        Name of source bucket.
    
    Returns
    -------
    latest_obj_name: str
        Name of the latest compressed BOM dataset file.
    """
    # Retrieve files and their dates
    obj_name_date_dict = dict()
    response = s3_client.list_objects(Bucket=bucket_name)
    for obj in response["Contents"]:
        obj_name = obj["Key"]
        obj_date = obj_name[-14:-4]  #YYYY-MM-DD
        obj_name_date_dict[obj_name] = obj_date
    
    # Get the latest file with its latest date
    latest_obj_name = max(
        obj_name_date_dict, 
        key=lambda k: datetime.strptime(obj_name_date_dict[k], "%Y-%m-%d")
    )
    
    return latest_obj_name


def check_dataset_date_condition(file_name):
    """
    This function checks if the weather dataset is
    created in or after 2012. This is to limit the 
    datasets based on the case study section 3.

    Parameters
    ----------
    file_name: str
        Name of weather dataset

    Returns
    -------
    Boolean
    """

    create_year = int(file_name[-10:-6])
    return create_year >= 2012


def pre_process_csv(file_obj, state):
    """
    This function pre-processes CSV file object
    to refine columns with additional attributes.

    Parameters
    ----------
    file_obj: object
        CSV file object in Byte.
    state: str
        State the CSV dataset is from.

    Returns
    -------
    df: pd.DataFrame
        Pre-processed dataset.
    """
    # Define columns
    columns = [
        "STATION_NAME",
        "DATE",
        "EVAPO_TRANSPIRATION",
        "RAIN",
        "PAN_EVAPORATION",
        "MAXIMUM_TEMPERATURE",
        "MINIMUM_TEMPERATURE",
        "MAXIMUM_RELATIVE_HUMIDITY",
        "MINIMUM_RELATIVE_HUMIDITY",
        "AVERAGE_10M_WIND_SPEED",
        "SOLAR_RADIATION"
    ]

    # Load to dataframe
    df = pd.read_csv(
        file_obj,
        encoding="ISO-8859-1",
        engine="python",
        skiprows=12,
        skipfooter=1,
        skip_blank_lines=True
    )

    # Pre-process dataset
    ## Redefine columns
    df.columns = columns
    ## Nulliyfy empty strings
    df = df.replace("", None).replace(" ", None)
    ## Convert measurement attributes into float data type
    for float_col in columns[2:]:
        df[float_col] = df[float_col].astype(np.float64)
    ## Convert DATE column into date data type
    df["DATE"] = pd.to_datetime(df["DATE"], format="%d/%m/%Y").dt.date
    ## Add additional attributes
    df["STATE"] = state
    df["LOAD_DATE"] = date_today

    return df


def pre_process_fwf(file_obj):
    """
    This function pre-processes FWF (fixed width format) file object
    to define columns with additional attribute.

    Parameters
    ----------
    file_obj: object
        FWF file object in Byte.

    Returns
    -------
    df: pd.DataFrame
        Pre-processed dataset.
    """
    # Define column width specifications
    col_width_specs = [
        (0, 8),
        (8, 12),
        (12, 18),
        (18, 59),
        (59, 75),
        (75, 84),
        (84, 94)
    ]

    # Define columns
    columns = [
        "STATION_ID",
        "STATE",
        "DISTRICT_CODE",
        "STATION_NAME",
        "STATION_SINCE",
        "LATITUDE",
        "LONGITUDE"
    ]

    # Load to dataframe
    df = pd.read_fwf(
        file_obj,
        colspecs=col_width_specs,
        header=None,
        names=columns
    )

    # Pre-process dataset
    # Strip empty spaces
    for col in columns[1:-2]:
        df[col] = df[col].str.strip()
    # Convert STATION_ID column into string data type & fill zero upto 6 characters
    df["STATION_ID"] = df["STATION_ID"].astype(str).str.zfill(6)
    # Convert STATION_SINCE column into date data type
    df["STATION_SINCE"] = pd.to_datetime(df["STATION_SINCE"], format="%Y%m%d..").dt.date
    # Convert coordinate attributes into float data type
    df["LATITUDE"] = df["LATITUDE"].astype(np.float64)
    df["LONGITUDE"] = df["LONGITUDE"].astype(np.float64)
    # Add additional attribute
    df["LOAD_DATE"] = date_today

    return df


def main():
    LoggingMixin().log.info("Process has started")

    # Load latest compressed file as byte stream object
    LoggingMixin().log.info("Retrieving latest compressed file...")
    latest_file_name = find_latest_file(s3, bucket_name)
    latest_file = io.BytesIO()
    s3.download_fileobj(
        Bucket=bucket_name,
        Key=latest_file_name,
        Fileobj=latest_file
    )
    LoggingMixin().log.info("Compressed file has been retrieved")

    # Create Snowflake tables if not existing
    LoggingMixin().log.info("Creating Snowflake tables...")
    ## Weather dataset
    cur.execute(query_create_tgt_weather)
    cur.execute(query_create_temp_table.format(table_temp_weather, table_tgt_weather))
    ## Station dataset
    cur.execute(query_create_tgt_station)
    cur.execute(query_create_temp_table.format(table_temp_station, table_tgt_station))
    LoggingMixin().log.info("Snowflake tables have been created")

    # Pre-process weather and station datasets from byte stream object
    LoggingMixin().log.info("Pre-processing weather and station datasets...")
    latest_file.seek(0)
    df_weather_li = []
    with tarfile.open(fileobj=latest_file) as tar_file:
        for member in tar_file.getmembers():
            # Process and load csv files for weather datasets
            if member.isfile() and member.name.endswith(".csv"):
                # Process only if dataset is created in or after 2012
                is_valid = check_dataset_date_condition(member.name)
                if not is_valid:
                    continue
                # Convert csv file object to dataframe
                state = member.name.split("/")[1].upper()
                csv_obj = tar_file.extractfile(member)
                df_weather = pre_process_csv(csv_obj, state)
                df_weather_li.append(df_weather)

            # Process and load text file for station dataset
            elif member.isfile() and member.name.endswith(".txt"):
                # Convert fwf text file object to dataframe
                fwf_obj = tar_file.extractfile(member)
                df_station = pre_process_fwf(fwf_obj)  
    LoggingMixin().log.info("Datasets have been pre-processed")

    # Load pre-processed datasets into Snowflake staging schema
    """The use of temp tables and merge statements ensures
    the idempotency of this process.
    """
    LoggingMixin().log.info("Loading datasets into Snowflake staging schema...")
    ## Weather dataset 
    ### Combine weather datasets and load into temp weather table
    df_weather_combine = pd.concat(df_weather_li, ignore_index=True)
    df_weather_combine = df_weather_combine.drop_duplicates()
    write_pandas(conn, df_weather_combine, table_temp_weather)
    ### Merge from temp weather table to target weather table
    cur.execute(query_merge_weather)
    ## State dataset
    ### Load station dataset into temp station table
    write_pandas(conn, df_station, table_temp_station)
    ### Merge from temp station table to target station table
    cur.execute(query_merge_station)
    LoggingMixin().log.info("Datasets have been loaded to Snowflake")

    LoggingMixin().log.info("Process has completed")


if __name__ == "__main__":
    # Define date variables
    melb_tz = pytz.timezone("Australia/Melbourne")
    datetime_now = datetime.now(melb_tz)
    date_today = datetime_now.date()
    
    # Define S3-compatible object storage client via MinIO
    minio_endpoint = "http://host.docker.internal:9000"
    minio_access_key = os.environ["MINIO_ACCESS_KEY"]
    minio_secret_key = os.environ["MINIO_SECRET_KEY"]
    bucket_name = "bom-landing"
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
    )

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

    # Define Snowflake tables
    ## Weather dataset
    table_tgt_weather = "WEATHER_PREPROCESSED"
    table_temp_weather = "WEATHER_PREPROCESSED_TEMP"
    ## Station dataset
    table_tgt_station = "STATION_PREPROCESSED"
    table_temp_station = "STATION_PREPROCESSED_TEMP"

    # Define Snowflake queries
    query_create_temp_table = """
        CREATE TEMPORARY TABLE {} LIKE {};
    """
    ## Weather dataset
    query_create_tgt_weather = f"""
        CREATE TABLE IF NOT EXISTS {table_tgt_weather} (
            STATION_NAME VARCHAR(100),
            DATE DATE,
            EVAPO_TRANSPIRATION FLOAT,
            RAIN FLOAT,
            PAN_EVAPORATION FLOAT,
            MAXIMUM_TEMPERATURE FLOAT,
            MINIMUM_TEMPERATURE FLOAT,
            MAXIMUM_RELATIVE_HUMIDITY FLOAT,
            MINIMUM_RELATIVE_HUMIDITY FLOAT,
            AVERAGE_10M_WIND_SPEED FLOAT,
            SOLAR_RADIATION FLOAT,
            STATE VARCHAR(100),
            LOAD_DATE DATE
        );
    """
    query_merge_weather = f"""
        MERGE INTO {table_tgt_weather} AS TARGET 
        USING {table_temp_weather} AS SOURCE
            ON TARGET.STATION_NAME = SOURCE.STATION_NAME
                AND TARGET.DATE = SOURCE.DATE
            WHEN NOT MATCHED THEN INSERT (
                STATION_NAME,
                DATE,
                EVAPO_TRANSPIRATION,
                RAIN,
                PAN_EVAPORATION,
                MAXIMUM_TEMPERATURE,
                MINIMUM_TEMPERATURE,
                MAXIMUM_RELATIVE_HUMIDITY,
                MINIMUM_RELATIVE_HUMIDITY,
                AVERAGE_10M_WIND_SPEED,
                SOLAR_RADIATION,
                STATE,
                LOAD_DATE
            ) VALUES (
                SOURCE.STATION_NAME,
                SOURCE.DATE,
                SOURCE.EVAPO_TRANSPIRATION,
                SOURCE.RAIN,
                SOURCE.PAN_EVAPORATION,
                SOURCE.MAXIMUM_TEMPERATURE,
                SOURCE.MINIMUM_TEMPERATURE,
                SOURCE.MAXIMUM_RELATIVE_HUMIDITY,
                SOURCE.MINIMUM_RELATIVE_HUMIDITY,
                SOURCE.AVERAGE_10M_WIND_SPEED,
                SOURCE.SOLAR_RADIATION,
                SOURCE.STATE,
                SOURCE.LOAD_DATE
            );
    """
    ## Station dataset
    query_create_tgt_station = f"""
        CREATE TABLE IF NOT EXISTS {table_tgt_station} (
            STATION_ID VARCHAR(6),
            STATE VARCHAR(3),
            DISTRICT_CODE VARCHAR(5),
            STATION_NAME VARCHAR(40),
            STATION_SINCE DATE,
            LATITUDE FLOAT,
            LONGITUDE FLOAT,
            LOAD_DATE DATE
        );
    """
    query_merge_station = f"""
        MERGE INTO {table_tgt_station} AS TARGET 
        USING {table_temp_station} AS SOURCE
            ON TARGET.STATION_ID = SOURCE.STATION_ID
            WHEN NOT MATCHED THEN INSERT (
                STATION_ID,
                STATE,
                DISTRICT_CODE,
                STATION_NAME,
                STATION_SINCE,
                LATITUDE,
                LONGITUDE,
                LOAD_DATE
            ) VALUES (
                SOURCE.STATION_ID,
                SOURCE.STATE,
                SOURCE.DISTRICT_CODE,
                SOURCE.STATION_NAME,
                SOURCE.STATION_SINCE,
                SOURCE.LATITUDE,
                SOURCE.LONGITUDE,
                SOURCE.LOAD_DATE
            );
    """

    try:
        # Start process
        main()
    finally:
        # Close cursor and connection
        cur.close()
        conn.close()
