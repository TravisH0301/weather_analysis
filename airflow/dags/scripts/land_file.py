###############################################################################
# Name: land_file.py
# Description: This script retrieves the compressed BOM dataset
#              via FTP and loads into the S3-compatible object storage.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/weather_analysis
###############################################################################
import os
import io
from datetime import datetime
import pytz
from urllib.request import urlopen

import boto3
from botocore.exceptions import ClientError
from airflow.utils.log.logging_mixin import LoggingMixin


def retrieve_ftp_file(ftp_file_path):
    """
    This function retrieves the file via FTP
    as a byte stream object.

    Parameters
    ----------
    ftp_file_path: str
        FTP file path of the compressed BOM dataset file.

    Returns
    -------
    object
        Byte stream of the compressed BOM dataset file.
    """
    response = urlopen(ftp_file_path)
    return io.BytesIO(response.read())


def main():
    LoggingMixin().log.info("Process has started")

    # Retrieve compressed file as byte stream object
    LoggingMixin().log.info("Retrieving compressed file...")
    comp_file = retrieve_ftp_file(ftp_file_path)
    LoggingMixin().log.info("Compressed file has been retrieved")

    # Load compressed file into object storage
    """Current date is added to the file name to keep track of 
    BOM dataset version within the compressed file.
    """
    LoggingMixin().log.info("Loading compressed file into object storage...")
    file_name = os.path.basename(ftp_file_path)
    file_name_date = file_name[:-4] + f"_{date_today_str}" + file_name[-4:]
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name_date,
            Body=comp_file
        )
    except ClientError as e:
        LoggingMixin().log.error("File load has failed with an error: {e}")    
    LoggingMixin().log.info("Compressed file has been loaded to object storage")
    
    LoggingMixin().log.info("Process has completed")


if __name__ == "__main__":
    # Define date variables
    melb_tz = pytz.timezone('Australia/Melbourne')
    datetime_now = datetime.now(melb_tz)
    date_today_str = datetime_now.date().strftime("%Y-%m-%d")

    # Define FTP compressed file source
    ftp_file_path = "ftp://ftp2.bom.gov.au/anon/gen/clim_data/IDCKWCDEA0.tgz"

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

    # Start Process
    main()
    

