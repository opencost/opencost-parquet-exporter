"""
This module provides a factory function for creating storage objects based on
the specified backend.
"""

from storage.aws_s3_storage import S3Storage
from storage.azure_storage import AzureStorage


def get_storage(storage_backend):
    """
    Factory function to create and return a storage object based on the given backend.

    This function abstracts the creation of storage objectss. It supports 'azure' for 
    Azure Storage and 's3' for AWS S3 Storage.

    Parameters:
        storage_backend (str): The name of the storage backend. SUpported:'azure','s3'.

    Returns:
        An instance of the specified storage backend class.

    Raises:
        ValueError: If the specified storage backend is not supported.
    """
    if storage_backend == 'azure':
        return AzureStorage()
    if storage_backend in ['s3', 'aws']:
        return S3Storage()

    raise ValueError("Unsupported storage backend")
