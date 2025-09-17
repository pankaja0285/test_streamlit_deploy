import os
import boto3
import pickle
import yaml
import json
import pandas as pd 
import logging
# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from io import StringIO, BytesIO

from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
# ******************************************************************************************

def get_s3_settings():
    s3_settings = {}

    print(f"Source of access data: env")
    s3_region = os.getenv("S3_REGION")
    # bucket_name --> this would also be set in env, especially we have access to only one bucket
    # bucket_name = "bhutan-climatesense" 
    bucket_name = os.getenv('S3_BUCKET_NAME')
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_settings = {
        "S3_BUCKET_NAME": bucket_name,
        "S3_REGION": s3_region,
        "AWS_ACCESS_KEY_ID": aws_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret        
    }
        
    return s3_settings


def get_s3_client():
    s3_settings = get_s3_settings()
    bucket = s3_settings['S3_BUCKET_NAME']
    s3_client = boto3.client(
        's3',
        aws_access_key_id=s3_settings["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=s3_settings['AWS_SECRET_ACCESS_KEY'],
        region_name=s3_settings['S3_REGION']  # e.g., 'us-east-2'
    )

    return s3_client, bucket


def connect_to_s3_resource(s3_settings=None):
    s3_settings = get_s3_settings()
    s3_client = boto3.client(
        's3',
        aws_access_key_id=s3_settings["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=s3_settings['AWS_SECRET_ACCESS_KEY'],
        region_name=s3_settings['S3_REGION']  # e.g., 'us-east-2'
    )
    s3_resource = boto3.resource(
        's3',
        aws_access_key_id=s3_settings["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=s3_settings['AWS_SECRET_ACCESS_KEY'],
        region_name=s3_settings['S3_REGION']  # e.g., 'us-east-2'
    )
    return s3_client, s3_resource, s3_settings


def verify_object_exists(bucket: str=None, key: str=None, s3_client=None) -> bool:
    """Verify object exists and return True (calls head_object)."""
    verify_status = False
    try:
        # NOTE: key is filename here...
        s3_client.head_object(Bucket=bucket, Key=key)
        logging.info(f"Verified: s3://{bucket}/{key} exists and is accessible.")
        verify_status = True
    except ClientError as e:
        # 404 or AccessDenied will raise ClientError
        logging.error(f"head_object failed: {e}")
    return verify_status

def list_bucket_objects(bucket:str="", s3_client=None, object_prefix="") -> list:
    lst_objects = []
    
    try:
        # response = s3_client.list_objects_v2(Bucket=bucket)
        if object_prefix != "":
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=object_prefix)
        else:
            response = s3_client.list_objects_v2(Bucket=bucket)

        if 'Contents' in response:
            for obj in response['Contents']:
                lst_objects.append(f"{obj['Key']}")
        else:
            raise Exception("Contents not found in the list_objects_v2")
    except Exception as ex:
        logging.error(f"Error with exception: {ex}")
    
    return lst_objects
    
def remove_files_on_s3(file_list=None, bucket:str="", s3_client=None):
    status = False

    try:
        files_to_delete = [{"Key":fl} for fl in file_list]
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={'Objects': files_to_delete, 'Quiet': False}
        )
        print(f"Files deleted successfully from bucket '{bucket}'.")
        if 'Errors' in response:
            print("Errors encountered during deletion:")
            for error in response['Errors']:
                print(f"  Code: {error['Code']}, Key: {error['Key']}, Message: {error['Message']}")
        else:
            status = True
    except Exception as e:
        print(f"Error deleting files: {e}")
    return status


def upload_as_file(local_path: str="", bucket: str="", key: str="", s3_client=None):
    """Upload a local file to s3://{bucket}/{key} using multipart upload (robust)."""
    upl_f_status = False

    try:
        # NOTE: key is filename here...
        logging.info(f"Uploading local file {local_path} -> s3://{bucket}/{key}")
        s3_client.upload_file(local_path, bucket, key)
        logging.info("Upload complete.")
        upl_f_status = True
    except (ClientError, FileNotFoundError) as e:
        logging.error(f"Upload failed: {e}")
    return upl_f_status


def upload_dataframe_as_csv(df: pd.DataFrame, bucket: str, key: str, s3_client=None):
    """Upload a DataFrame to S3 as CSV (in-memory)"""
    buf = StringIO()
    upl_d_status = False

    try:
        df.to_csv(buf, index=False)
        buf.seek(0)
        # NOTE: key is filename here...
        logging.info(f"Uploading DataFrame as CSV -> s3://{bucket}/{key}")
        # put_object is fine for reasonably sized CSVs
        s3_client.put_object(Body=buf.getvalue().encode("utf-8"), Bucket=bucket, Key=key)
        logging.info("CSV upload complete.")
        upl_d_status = True
    except ClientError as e:
        logging.error(f"CSV upload failed: {e}")
    return upl_d_status


def upload_dataframe_as_parquet(df: pd.DataFrame, bucket: str="", key: str="", s3_client=None):
    """Upload a DataFrame to S3 as Parquet (in-memory). Requires pyarrow or fastparquet."""
    buf = BytesIO()
    upl_p_status = False
    upl_d_status = False

    try:
        # pandas will pick pyarrow or fastparquet if installed
        df.to_parquet(buf, index=False)
        upl_d_status = True
    except Exception as e:
        logging.error(f"Failed to convert DataFrame to parquet: {e}")
        return upl_d_status
    
    # If no error above, then proceed 
    buf.seek(0)
    try:
        # NOTE: key is filename here...
        logging.info(f"Uploading DataFrame as Parquet -> s3://{bucket}/{key}")
        # Use upload_fileobj for streaming bytes
        s3_client.upload_fileobj(buf, bucket, key)
        logging.info("Parquet upload complete.")
        upl_p_status = True
    except ClientError as e:
        logging.error(f"Parquet upload failed: {e}")
    return upl_p_status


def download_file(file_type:str="csv", download_path:str="",
                   bucket:str="", key:str="", s3_client=None):
    dwl_f_status= False

    try:
        match file_type:            
            case "csv" | "parquet" | "pickle":
                s3_client.download_file(bucket, key, download_path)
                print(f"{file_type.capitalize()} file '{key}' downloaded to '{download_path}' successfully.")
                dwl_f_status = True
            case _:
                s3_client.download_file(bucket, key, download_path)
                print(f"{file_type.capitalize()} file '{key}' downloaded to '{download_path}' successfully.")
                dwl_f_status = True
    except FileNotFoundError:
        print(f"Error: File '{key}' not found in bucket '{bucket}'. Download failed.")
    except Exception as e:
        print(f"Error downloading {file_type} file: {e}")
    return dwl_f_status


def load_csv_from_s3_to_dataframe(s3_file_key="", bucket="", s3_client=None):
    try:
        # get the s3 - stored csv file as an object 
        s3_file = s3_client.get_object(Bucket=bucket, Key=s3_file_key)

        # load the object's body (CSV content), decoded as utf-8
        csv_data = s3_file['Body'].read().decode('utf-8')

        # load to a pandas dataframe, the io.StringIO contents of csv_data file-like object 
        # and read into pandas DataFrame
        df = pd.read_csv(StringIO(csv_data)) 
    except s3_client.exceptions.NoSuchKey:
        print(f"Error: The object '{s3_file_key}' was not found in bucket '{bucket}'.")
    except Exception as e:
        print(f"Error loading to dataframe from csv file on s3: {e}")
    return df
