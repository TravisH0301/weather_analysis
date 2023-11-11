import os
import io
import tarfile
import logging
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

    # Extract file and load to object storage
    logging.info("Extracting and loading files to object storage...")
    with tarfile.open(fileobj=comp_file_in_memory) as tar:
        for member in tar.getmembers():
            f = tar.extractfile(member)
            if f is not None:
                try:
                    file_in_memory = io.BytesIO(f.read())
                    s3.put_object(
                        Bucket=bucket_name,
                        Key=member.name,
                        Body=file_in_memory
                    )
                except Exception:
                    logging.error(f"Error occurred while loading {member.name} to object storage:", exc_info=True)
    logging.info("files have been extracted and loaded to object storage")

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