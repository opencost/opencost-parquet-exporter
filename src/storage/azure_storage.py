# pylint: disable=W0511

"""
This module provides an implementation of the BaseStorage class for Azure Blob Storage 
with authentication via client secret credentials.
"""

from io import BytesIO
import logging
import sys
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, BlobType
from .base_storage import BaseStorage

logger = logging.getLogger('azure.storage.blob')
logger.setLevel(logging.INFO)  # TODO: Make ENV var
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)


# pylint: disable=R0903
class AzureStorage(BaseStorage):
    """
    A class to handle data storage in Azure Blob Storage.

    """

    def save_data(self, data: pd.core.frame.DataFrame, config) -> str | None:
        """
        Saves a DataFrame to Azure Blob Storage.

        Parameters:
            data (pd.core.frame.DataFrame): The DataFrame to be saved.
            config (dict): Configuration dictionary containing necessary information for storage.
                           Expected keys include 'azure_tenant', 'azure_application_id', 
                           'azure_application_secret', 'azure_storage_account_name', 
                           'azure_container_name', and 'file_key_prefix'.

        Returns:
            str | None: The URL of the saved blob if successful, None otherwise.

        """
        credentials = ClientSecretCredential(
            config['azure_tenant'],
            config['azure_application_id'],
            config['azure_application_secret']
        )
        blob_service_client = BlobServiceClient(
            f"https://{config['azure_storage_account_name']}.blob.core.windows.net",
            logging_enable=True,
            credential=credentials
        )

        # TODO: Force overwrite? As of now upload would fail since key is the same.
        # blob_client provides an option for this
        file_name = 'k8s_opencost.parquet'
        window = pd.to_datetime(config['window_start'])
        parquet_prefix = f"{config['file_key_prefix']}{window.year}/{window.month}/{window.day}"
        key = f"{parquet_prefix}/{file_name}"
        blob_client = blob_service_client.get_blob_client(
            container=config['azure_container_name'], blob=key)
        parquet_file = BytesIO()
        data.to_parquet(parquet_file, engine='pyarrow', index=False)
        parquet_file.seek(0)

        try:
            response = blob_client.upload_blob(
                data=parquet_file, blob_type=BlobType.BlockBlob)
            if response:
                return f"{blob_client.url}"
        # pylint: disable=W0718
        except Exception as e:
            logger.error(e)

        return None
