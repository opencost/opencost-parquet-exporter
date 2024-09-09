"""
This module provides a factory function for creating storage objects based on
the specified backend.
"""

from storage.aws_s3_storage import S3Storage
from storage.azure_storage import AzureStorage
from storage.gcp_storage import GCPStorage  # New import


def get_storage(storage_backend):
    """
    Factory function to create and return a storage object based on the given backend.

    This function abstracts the creation of storage objects. It supports 'azure' for
    Azure Storage, 's3' for AWS S3 Storage, and 'gcp' for Google Cloud Storage.

    Parameters:
        storage_backend (str): The name of the storage backend. Supported: 'azure', 's3', 'gcp'.

    Returns:
        An instance of the specified storage backend class.

    Raises:
        ValueError: If the specified storage backend is not supported.
    """
    if storage_backend == 'azure':
        return AzureStorage()
    if storage_backend == 's3':
        return S3Storage()
    if storage_backend == 'gcp':
        return GCPStorage()

    raise ValueError("Unsupported storage backend")
