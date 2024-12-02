from minio import Minio
from minio.error import S3Error
from s3fs import S3FileSystem
import pandas as pd
import os
import itertools
from glob import glob

bucket_name     = "warehouse/"
key             = "d3eAQ929RaBqS2zPn0JY" #os.environ['MINIO_ACCESS_KEY']
secret          = "X64cBl8JTlAQJOh3aQsho59kHtwGk05RclgWiwwA" #os.environ['MINIO_SECRET_ACCESS_KEY']
endpoint_url    = "http://10.88.0.16:9000" #os.environ['MINIO_URL']
download_dir = f"/app/data"

#"accessKey":"d3eAQ929RaBqS2zPn0JY","secretKey":"X64cBl8JTlAQJOh3aQsho59kHtwGk05RclgWiwwA","api":"s3v4","path":"auto"

s3 = S3FileSystem(anon=False,
                 endpoint_url=endpoint_url,
                 key=key,
                 secret=secret,
                 use_ssl=False)

storage_options={
        'key': key,
        'secret': secret,
        'endpoint_url': endpoint_url,
    }


file_pattern = 'fhvhv_*.parquet'

def find_raw_files(pattern:str) -> list:
    return(glob(f'{download_dir}/{pattern}'))

def upload_to_datalake(where_upload_to:str,
                       pattern:str):
    """
    where_to_upload_to  : place where to upload files. Bronce/{prefix}, Silver, Gold.
    pattern             : file pattern to found into download directory.
    """
    try:
        # Uploading Market Data    
        for file in find_raw_files(pattern=pattern):
            file_name = file.rsplit("/", 1)[-1] #/app/data/yellow_tripdata_2023-03.parquet
            remote_object_path = f'{where_upload_to}/{file_name}'
            print('remote path',remote_object_path)
            s3.put_file(file, remote_object_path)
            print(f'{file_name} uploaded.')
            # Delete raw data once uploaded
            try:
                os.remove(file)
            except os.error as os_err:
                print(f"Error occurred: {os_err}")
    except S3Error as err:
        # Print the error message
        print(f"Error occurred: {err}")

def get_data_from_object(url: str, storage_options:dict) -> pd.DataFrame:
    
    # Get the name of the file from url
    file_name = url.rsplit('/', 1)[-1].strip()
    
    file_data = pd.read_csv(url, storage_options=storage_options)
    return file_data

def main():
    #file_name = 'taxi_zone_lookup.csv'
    #url = f's3://{bucket_name}/{file_name}'
    #file_data = get_data_from_object(url = url,
    #                                storage_options=storage_options)
    
    upload_to_datalake(where_upload_to='s3://bronce',
                       pattern=file_pattern)

if __name__ == "__main__":
    main()