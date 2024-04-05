from storage.base_storage import BaseStorage
from storage.aws_s3_storage import S3Storage
from storage.azure_storage import AzureStorage

def get_storage(storage_backend):
    if storage_backend == 'azure':
        return AzureStorage()
    elif storage_backend == 's3':
        return S3Storage()
    else:
        raise ValueError("Unsupported storage backend")
