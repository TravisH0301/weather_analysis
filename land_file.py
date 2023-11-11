#####################################################################
# Name: land_file.py
# Description: This file extract the compressed BOM dataset via FTP
#              and loads into the S3-compatible object storage.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/weather_analysis
#####################################################################
import os
import io
import tarfile
import logging
from datetime import datetime
import pytz
from urllib.request import urlopen

import boto3


def download_ftp(ftp_file_path):
    response = urlopen(ftp_file_path)
    return io.BytesIO(response.read())


def main():
    logging.info("Process has started")

    # Download compressed file into memory
    logging.info("Downloading compressed file...")
    comp_file_in_memory = download_ftp(ftp_file_path)
    logging.info("Compressed file has been downloaded")

    # Load compressed file into object storage
    logging.info("Loading compressed file into object storage...")
    file_name = os.path.basename(ftp_file_path)
    file_name_date = file_name[:-4] + f"_{date_today}" + file_name[-4:]
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name_date,
            Body=comp_file_in_memory
        )
        logging.info("Compressed file has been loaded to object storage")
    except Exception:
        logging.error(f"Error occurred during file load:", exc_info=True)

    logging.info("Process has completed")


if __name__ == "__main__":
    # Define configurations
    ## Logger
    logging.basicConfig(
        filename = "./log/land_file_log.txt",
        filemode="w",
        level=logging.INFO,
        format = "%(asctime)s; %(levelname)s; %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p %Z"
    )
    ## Date
    tz = pytz.timezone('Australia/Melbourne')
    datetime_now = datetime.now(tz)
    date_today = datetime_now.date().strftime("%Y-%m-%d")
    ## FTP
    ftp_file_path = "ftp://ftp2.bom.gov.au/anon/gen/clim_data/IDCKWCDEA0.tgz"
    ## S3-compatible object storage via MinIO
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]
    s3 = boto3.client(
        "s3",
        endpoint_url="http://172.17.0.1:9000",
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key
    )
    bucket_name = "bom-landing"

    try:
        main()
    except Exception:
        logging.error("Process has failed:", exc_info=True)
        raise