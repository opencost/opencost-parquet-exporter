# pylint: disable=W0511

"""
This module provides an implementation of the BaseStorage class for Azure Blob Storage.

Authentication supports both traditional client-secret credentials and Azure Workload
Identity / managed identity. Client secret remains the default for backwards
compatibility, but the credential strategy can be controlled via configuration.
"""

from io import BytesIO
import logging
import os
import sys
import pandas as pd
from azure.identity import (  # type: ignore[import]
    ClientSecretCredential,
    CredentialUnavailableError,
    DefaultAzureCredential,
    WorkloadIdentityCredential,
)
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

    def _build_credentials(self, config):
        """
        Build the Azure credential object based on configuration/environment.

        Priority:
        1. Explicit client secret (default behaviour / backwards compatible)
        2. Workload identity credential (either when requested or when no secret provided)
        3. DefaultAzureCredential fallback (covers managed identity, CLI, etc.)
        """
        auth_mode = (config.get('azure_auth_mode') or 'auto').lower()
        client_id = config.get('azure_application_id') or os.environ.get('AZURE_CLIENT_ID')
        tenant_id = config.get('azure_tenant') or os.environ.get('AZURE_TENANT_ID')
        secret = config.get('azure_application_secret')
        token_file = config.get('azure_federated_token_file') or os.environ.get('AZURE_FEDERATED_TOKEN_FILE')

        def build_client_secret_credential():
            missing = [name for name in (
                'azure_tenant',
                'azure_application_id',
                'azure_application_secret',
            ) if not config.get(name)]
            if missing:
                raise ValueError(
                    f"Missing Azure client secret configuration values: {', '.join(missing)}")
            return ClientSecretCredential(
                config['azure_tenant'],
                config['azure_application_id'],
                config['azure_application_secret']
            )

        def build_workload_identity_credential():
            if not client_id or not tenant_id or not token_file:
                missing_env = [
                    name for name, value in (
                        ('AZURE_CLIENT_ID', client_id),
                        ('AZURE_TENANT_ID', tenant_id),
                        ('AZURE_FEDERATED_TOKEN_FILE', token_file),
                    ) if not value
                ]
                raise CredentialUnavailableError(
                    f"Workload identity environment variables missing: {', '.join(missing_env)}")
            return WorkloadIdentityCredential(
                client_id=client_id,
                tenant_id=tenant_id,
                token_file_path=token_file,
            )

        # Explicit client-secret mode or auto-detect with secret present
        if auth_mode == 'client-secret' or (auth_mode == 'auto' and secret):
            return build_client_secret_credential()

        # Explicit workload-identity mode or auto-detect without secret
        if auth_mode in ('workload-identity', 'auto'):
            try:
                return build_workload_identity_credential()
            except CredentialUnavailableError as err:
                logger.info("Workload identity credential unavailable: %s", err)
                if auth_mode == 'workload-identity':
                    raise

        # Final fallback allows other DefaultAzureCredential sources (managed identity, CLI, etc.)
        logger.info("Falling back to DefaultAzureCredential for Azure authentication")
        return DefaultAzureCredential(
            exclude_interactive_browser_credential=True
        )

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
        try:
            credentials = self._build_credentials(config)
        except (ValueError, CredentialUnavailableError) as err:
            logger.error("Failed to build Azure credential: %s", err)
            return None

        missing_required = [
            field for field in ('azure_storage_account_name', 'azure_container_name')
            if not config.get(field)
        ]
        if missing_required:
            logger.error(
                "Missing required Azure storage configuration values: %s",
                ', '.join(missing_required)
            )
            return None

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
