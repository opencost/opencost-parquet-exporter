from io import BytesIO
import logging
import sys
import pandas as pd
from .base_storage import BaseStorage
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, BlobType

logger = logging.getLogger('azure.storage.blob')
logger.setLevel(logging.INFO) # TODO: Make ENV var
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)

class AzureStorage(BaseStorage):
    def save_data(self, data: pd.core.frame.DataFrame, config) -> str | None:
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
        
        file_name = 'k8s_opencost.parquet' # TODO: Force overwrite? As of now upload would fail since key is the same 
        window = pd.to_datetime(config['window_start'])
        parquet_prefix = f"{config['file_key_prefix']}{window.year}/{window.month}/{window.day}"
        key = f"{parquet_prefix}/{file_name}"
        blob_client = blob_service_client.get_blob_client(container=config['azure_container_name'], blob=key)
        output = BytesIO()
        data.to_csv(output, index=False)
        output.seek(0)

        try:
            response = blob_client.upload_blob(data=output, blob_type=BlobType.BlockBlob)
            if response:
                return f"{blob_client.url}"
        except Exception as e:
            print(e) 
        
        return None