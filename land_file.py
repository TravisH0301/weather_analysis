import os
import io
import tarfile
import logging
from urllib.request import urlopen

from minio import Minio


def download_ftp(ftp_file_path):
    response = urlopen(ftp_file_path)
    return io.BytesIO(response.read())


def upload_to_minio(client, bucket_name, object_name, data):
    client.put_object(bucket_name, object_name, data, length=-1, part_size=10*1024*1024)


def main():
    logging.info("Process has started")

    # Download compressed file into memory
    logging.info("Downloading compressed file...")
    comp_file_in_memory = download_ftp(ftp_file_path)
    logging.info("Compressed file has been downloaded")

    # Extract file and load to MinIO object storage
    logging.info("Extracting and loading files to object storage...")
    with tarfile.open(fileobj=comp_file_in_memory) as tar:
        for member in tar.getmembers():
            f = tar.extractfile(member)
            if f is not None:
                try:
                    file_in_memory = io.BytesIO(f.read())
                    upload_to_minio(minioClient, bucket_name, member.name, file_in_memory)
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
    ## MinIO
    object_storage_access_key = os.environ["OBJECT_STORAGE_ACCESS_KEY"]
    object_storage_secret_key = os.environ["OBJECT_STORAGE_SECRET_KEY"]
    minioClient = Minio(
        endpoint="http://172.17.0.2:9000",
        access_key=object_storage_access_key,
        secret_key=object_storage_secret_key,
        secure=False
    )
    bucket_name = "bom-landing"

    try:
        main()
    except Exception:
        logging.error("Process has failed:", exc_info=True)
        raise