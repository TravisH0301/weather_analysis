###############################################################################
# Name: land_file.py
# Description: This script retrieves the compressed BOM dataset
#              via FTP and loads into the S3-compatible object storage.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/weather_analysis
###############################################################################
import os
import io
import logging
from datetime import datetime
import pytz
from urllib.request import urlopen

import boto3


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
    logging.info("Process has started")

    # Retrieve compressed file as byte stream object
    logging.info("Retrieving compressed file...")
    comp_file = retrieve_ftp_file(ftp_file_path)
    logging.info("Compressed file has been retrieved")

    # Load compressed file into object storage
    """Current date is added to the file name to keep track of 
    BOM dataset version within the compressed file.
    """
    logging.info("Loading compressed file into object storage...")
    file_name = os.path.basename(ftp_file_path)
    file_name_date = file_name[:-4] + f"_{date_today_str}" + file_name[-4:]
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name_date,
            Body=comp_file
        )
        logging.info("Compressed file has been loaded to object storage")
    except Exception:
        logging.error(f"Error occurred during file load:", exc_info=True)

    logging.info("Process has completed")


if __name__ == "__main__":
    # Define variables
    ## Logger
    logging.basicConfig(
        filename = "./log/land_file_log.txt",
        filemode="w",
        level=logging.INFO,
        format = "%(asctime)s; %(levelname)s; %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p %Z"
    )
    ## Date
    melb_tz = pytz.timezone('Australia/Melbourne')
    datetime_now = datetime.now(melb_tz)
    date_today_str = datetime_now.date().strftime("%Y-%m-%d")
    ## FTP compressed file source
    ftp_file_path = "ftp://ftp2.bom.gov.au/anon/gen/clim_data/IDCKWCDEA0.tgz"
    ## S3-compatible object storage via MinIO
    minio_endpoint = os.environ["MINIO_ENDPOINT"]
    minio_access_key = os.environ["MINIO_ACCESS_KEY"]
    minio_secret_key = os.environ["MINIO_SECRET_KEY"]
    bucket_name = "bom-landing"
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
    )

    try:
        main()
    except Exception:
        logging.error("Process has failed:", exc_info=True)
        raise
