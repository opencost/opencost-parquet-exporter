"""
This module provides an implementation of the BaseStorage class for Google Cloud Storage.
"""

from io import BytesIO
import logging
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core import exceptions as gcp_exceptions
import pandas as pd
from .base_storage import BaseStorage

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# pylint: disable=R0903
class GCPStorage(BaseStorage):
    """
    A class to handle data storage in Google Cloud Storage.
    """

    def _get_client(self, config) -> storage.Client:
        """
        Returns a Google Cloud Storage client using credentials provided in the config.

        Parameters:
            config (dict): Configuration dictionary that may contain 'gcp_credentials'
                           for service account keys and other authentication-related keys.

        Returns:
            storage.Client: An authenticated Google Cloud Storage client.
        """
        if "gcp_credentials" in config:
            credentials_info = config["gcp_credentials"]
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info
            )
            client = storage.Client(credentials=credentials)
        else:
            # Use default credentials
            client = storage.Client()

        return client

    def save_data(self, data: pd.DataFrame, config) -> str | None:
        """
        Saves a DataFrame to Google Cloud Storage.

        Parameters:
            data (pd.core.frame.DataFrame): The DataFrame to be saved.
            config (dict): Configuration dictionary containing necessary information for storage.
                           Expected keys include 'gcp_bucket_name',
                           'file_key_prefix', and 'window_start'.

        Returns:
            str | None: The URL of the saved object if successful, None otherwise.
        """
        client = self._get_client(config)

        file_name = "k8s_opencost.parquet"
        window = pd.to_datetime(config["window_start"])
        blob_prefix = (
            f"{config['file_key_prefix']}/{window.year}/{window.month}/{window.day}"
        )
        bucket_name = config["gcp_bucket_name"]
        blob_name = f"{blob_prefix}/{file_name}"

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        parquet_file = BytesIO()
        data.to_parquet(parquet_file, engine="pyarrow", index=False)
        parquet_file.seek(0)

        try:
            blob.upload_from_file(parquet_file, content_type="application/octet-stream")
            return blob.public_url
        except gcp_exceptions.BadRequest as e:
            logger.error("Bad Request Error: %s", e)
        except gcp_exceptions.Forbidden as e:
            logger.error("Forbidden Error: %s", e)
        except gcp_exceptions.NotFound as e:
            logger.error("Not Found Error: %s", e)
        except gcp_exceptions.TooManyRequests as e:
            logger.error("Too Many Requests Error: %s", e)
        except gcp_exceptions.InternalServerError as e:
            logger.error("Internal Server Error: %s", e)
        except gcp_exceptions.GoogleAPIError as e:
            logger.error("Google API Error: %s", e)

        return None
