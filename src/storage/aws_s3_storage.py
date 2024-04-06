"""
This module provides an implementation of the BaseStorage class for Amazon S3.
"""

import pandas as pd
import boto3
from botocore.exceptions import ClientError, PartialCredentialsError, NoCredentialsError
from .base_storage import BaseStorage

# pylint: disable=R0903


class S3Storage(BaseStorage):
    """
    A class that extends the BaseStorage abstract class to provide functionality
    for saving data to Amazon S3.

    """

    def save_data(self, data, config) -> str | None:
        """
        Uploads the provided data to an Amazon S3 bucket using the specified configuration.

        Parameters:
            data: The data to be uploaded. Should be a file-like object (BytesIO, for example).
            config (dict): Configuration information including the S3 bucket name, object key 
                           prefix,and the 'window_start' datetime that influences the object's 
                           key structure.

        Returns:
            str | None: The full S3 object path if the upload is successful, None otherwise.

        """
        s3_client = boto3.client('s3')
        file_name = 'k8s_opencost.parquet'
        window = pd.to_datetime(config['window_start'])
        # pylint: disable=C0301
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
