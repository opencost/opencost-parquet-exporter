import pandas as pd
from .base_storage import BaseStorage
import boto3
from botocore.exceptions import ClientError, PartialCredentialsError, NoCredentialsError

class S3Storage(BaseStorage):
    def save_data(self, data, config) -> str | None:
        s3_client = boto3.client('s3')
        file_name = 'k8s_opencost.parquet'
        window = pd.to_datetime(config['window_start'])
        parquet_prefix = f"{config['file_key_prefix']}/year={window.year}/month={window.month}/day={window.day}"
        key = f"{parquet_prefix}/{file_name}"
        try:
            s3_client.upload_fileobj(data, config['s3_bucket'], key)
            return f"s3://{config['s3_bucket']}/{key}"
        except NoCredentialsError:
            print("Error: No AWS credentials found to access S3")
        except PartialCredentialsError:
            print("Error: Incomplete AWS credentials provided for accessing S3")
        except ClientError as ce:
            print(f"AWS Client Error: {ce}")
            
        return None