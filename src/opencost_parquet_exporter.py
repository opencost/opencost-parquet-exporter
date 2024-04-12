# pylint: disable=W0511

"""
This module provides an implementation of the OpenCost storage exporter.
"""

import sys
from datetime import datetime, timedelta
import os
import json
import pandas as pd
import requests
from storage_factory import get_storage


def load_config_file(file_path: str):
    """
    Loads and returns the a JSON file specified by the file path.

    Parameters:
        file_path (str): The path to the JSON configuration file.

    Returns:
        dict: A dictionary containing the loaded JSON file.
    """
    with open(file_path, 'r', encoding="utf-8") as file:
        config = json.load(file)
    return config

# pylint: disable=R0912,R0913,R0914,R0915


def get_config(
        hostname=None,
        port=None,
        window_start=None,
        window_end=None,
        s3_bucket=None,
        file_key_prefix=None,
        aggregate_by=None,
        step=None,
        resolution=None,
        accumulate=None,
        storage_backend=None,
        include_idle=None,
        idle_by_node=None,
):
    """
    Get configuration for the parquet exporter based on either provided
    parameters or environment variables.

    Parameters:
    - hostname (str): Hostname for the OpenCost service,
                      defaults to the 'OPENCOST_PARQUET_SVC_HOSTNAME' environment variable,
                      or 'localhost' if the environment variable is not set.
    - port (int): Port number for the OpenCost service,
                  defaults to the 'OPENCOST_PARQUET_SVC_PORT' environment variable,
                  or 9003 if the environment variable is not set.
    - window_start (str): Start datetime window for fetching data, in ISO format,
                          defaults to the 'OPENCOST_PARQUET_WINDOW_START' environment variable,
                          or yesterday's date at 00:00:00 if not set.
    - window_end (str): End datetime window for fetching data, in ISO format,
                        defaults to the 'OPENCOST_PARQUET_WINDOW_END' environment variable,
                        or yesterday's date at 23:59:59 if not set.
    - s3_bucket (str): S3 bucket name to upload the parquet file,
                       defaults to the 'OPENCOST_PARQUET_S3_BUCKET' environment variable.
    - file_key_prefix (str): Prefix for file keys within the S3 bucket or local filesystem,
                             defaults to the 'OPENCOST_PARQUET_FILE_KEY_PREFIX' environment
                             variable, or '/tmp/' if not set.
    - aggregate_by (str): Criteria for aggregating data, separated by commas,
                          defaults to the 'OPENCOST_PARQUET_AGGREGATE' environment variable,
                          or 'namespace,pod,container' if not set.
    - step (str): Granularity for the data aggregation,
                  defaults to the 'OPENCOST_PARQUET_STEP' environment variable,
                  or is not used in query if not set.
    - resolution (str): Granularity for the PromQL queries in opencost,
                        defaults to the 'OPENCOST_PARQUET_RESOLUTION' environment variable,
                        or is not used in query if not set.
    - accumulate (str): Whether or not to accumulate aggregated cost,
                        defaults to the 'OPENCOST_PARQUET_ACCUMULATE' environment variable,
                        or is not used in query if not set.
    - storage_backend (str): Backend of the storage service (aws or azure), 
                             defaults to the 'OPENCOST_PARQUET_STORAGE_BACKEND' ENV var, 
                             or 'aws' if not set.
    - include_idle (str): Whether to return the calculated __idle__ field for the query,
                          defaults to the 'OPENCOST_PARQUET_INCLUDE_IDLE' environment 
                          variable, or 'false' if not set.
    - idle_by_node (str): If true, idle allocations are created on a per node basis,
                          defaults to the 'OPENCOST_PARQUET_IDLE_BY_NODE' environment 
                          variable, or 'false' if not set.

    Returns:
    - dict: Configuration dictionary with keys for 'url', 'params', 's3_bucket',
            'file_key_prefix' and 'window_start'.
    """
    config = {}

    # If function was called passing parameters the default value is ignored and environment
    # variable is also ignored.
    # This is done, so passing parameters have precedence to environment variables.
    if hostname is None:
        hostname = os.environ.get('OPENCOST_PARQUET_SVC_HOSTNAME', 'localhost')
    if port is None:
        port = int(os.environ.get('OPENCOST_PARQUET_SVC_PORT', 9003))
    if window_start is None:
        window_start = os.environ.get('OPENCOST_PARQUET_WINDOW_START', None)
    if window_end is None:
        window_end = os.environ.get('OPENCOST_PARQUET_WINDOW_END', None)
    if s3_bucket is None:
        s3_bucket = os.environ.get('OPENCOST_PARQUET_S3_BUCKET', None)
    if file_key_prefix is None:
        # TODO: Discuss: Format guideline?
        file_key_prefix = os.environ.get(
            'OPENCOST_PARQUET_FILE_KEY_PREFIX', '/tmp/')
    if aggregate_by is None:
        aggregate_by = os.environ.get(
            'OPENCOST_PARQUET_AGGREGATE', 'namespace,pod,container')
    if step is None:
        step = os.environ.get('OPENCOST_PARQUET_STEP', '1h')
    if resolution is None:
        resolution = os.environ.get('OPENCOST_PARQUET_RESOLUTION', None)
    if accumulate is None:
        accumulate = os.environ.get('OPENCOST_PARQUET_ACCUMULATE', None)
    if idle_by_node is None:
        idle_by_node = os.environ.get('OPENCOST_PARQUET_IDLE_BY_NODE', 'false')
    if include_idle is None:
        include_idle = os.environ.get('OPENCOST_PARQUET_INCLUDE_IDLE', 'false')
    if storage_backend is None:
        storage_backend = os.environ.get(
            'OPENCOST_PARQUET_STORAGE_BACKEND', 'aws')  # For backward compatibility

    if s3_bucket is not None:
        config['s3_bucket'] = s3_bucket
    config['storage_backend'] = storage_backend
    config['url'] = f"http://{hostname}:{port}/allocation/compute"
    config['file_key_prefix'] = file_key_prefix

    # Azure-specific configuration
    if config['storage_backend'] == 'azure':
        config.update({
            # pylint: disable=C0301
            'azure_storage_account_name': os.environ.get('OPENCOST_PARQUET_AZURE_STORAGE_ACCOUNT_NAME'),
            'azure_container_name': os.environ.get('OPENCOST_PARQUET_AZURE_CONTAINER_NAME'),
            'azure_tenant': os.environ.get('OPENCOST_PARQUET_AZURE_TENANT'),
            'azure_application_id': os.environ.get('OPENCOST_PARQUET_AZURE_APPLICATION_ID'),
            'azure_application_secret': os.environ.get('OPENCOST_PARQUET_AZURE_APPLICATION_SECRET'),
        })

    # If window is not specified assume we want yesterday data.
    if window_start is None or window_end is None:
        yesterday = datetime.strftime(
            datetime.now() - timedelta(1), '%Y-%m-%d')
        window_start = yesterday+'T00:00:00Z'
        window_end = yesterday+'T23:59:59Z'
    window = f"{window_start},{window_end}"
    config['window_start'] = window_start
    config['params'] = [
        ("window", window),
        ("includeIdle", include_idle),
        ("idleByNode", idle_by_node),
        ("includeProportionalAssetResourceCosts", "false"),
        ("format", "json")
    ]

    # Conditionally append query parameters
    if step is not None:
        config['params'].append(("step", step))
    if aggregate_by is not None:
        config['params'].append(("aggregate", aggregate_by))
    if resolution is not None:
        config['params'].append(("resolution", resolution))
    if accumulate is not None:
        config['params'].append(("accumulate", accumulate))
    config['params'] = tuple(config['params'])

    return config


def request_data(config):
    """
    Request data from the OpenCost service using the provided configuration.

    Parameters:
    - config (dict): Configuration dictionary with necessary URL and parameters for the API request.

    Returns:
    - dict or None: The response from the OpenCost API parsed as a dictionary, or None if an error
                    occurs.
    """
    url, params = config['url'], config['params']
    try:
        response = requests.get(
            url,
            params=params,
            # 15 seconds connect timeout
            # No read timeout, in case it takes a long
            timeout=(15, None)
        )
        response.raise_for_status()
        if 'application/json' in response.headers['content-type']:
            response_object = response.json()['data']
            return response_object
        print(f"Invalid content type: {response.headers['content-type']}")
        return None
    except (requests.exceptions.RequestException, requests.exceptions.Timeout,
            requests.exceptions.TooManyRedirects, ValueError, KeyError) as err:
        print(f"Request error: {err}")
        return None


def process_result(result, ignored_alloc_keys, rename_cols, data_types):
    """
    Process raw results from the OpenCost API data request.
    Parameters:
    - result (dict): Raw response data from the OpenCost API.
    - ignored_alloc_keys (dict): Allocation keys to ignore
    - rename_cols (dict): Key-value pairs for coloumns to rename
    - data_types (dict): Data types for properties of OpenCost response 

    Returns:
    - DataFrame or None: Processed data as a Pandas DataFrame, or None if an error occurs.
    """
    for split in result:
        # Remove entry for unmounted pv's .
        # this break the table schema in athena
        split.pop('__unmounted__/__unmounted__/__unmounted__', None)
    for split in result:
        for alloc_name in split.keys():
            for ignored_key in ignored_alloc_keys:
                split[alloc_name].pop(ignored_key, None)
    try:
        # TODO: make sep an ENV var with default '.'
        frames = [pd.json_normalize(split.values(), sep='_')
                  for split in result]
        processed_data = pd.concat(frames)
        processed_data.rename(columns=rename_cols, inplace=True)
        processed_data = processed_data.astype(data_types)
    except pd.errors.EmptyDataError as err:
        print(f"No data: {err}")
        return None
    except pd.errors.ParserError as err:
        print(f"Error parsing data: {err}")
        return None
    except pd.errors.MergeError as err:
        print(f"Data merge error: {err}")
        return None
    except ValueError as err:
        print(f"Value error: {err}")
        return None
    except KeyError as err:
        print(f"Key error: {err}")
        return None
    return processed_data


def save_result(processed_result, config):
    """
    Save the processed result either to the local filesystem or an S3 bucket
    in parquet file format.

    Parameters:
    - processed_result (DataFrame): The processed data to save.
    - config (dict): Configuration dictionary including keys for the S3 bucket,
                     file key prefix, and others.

    Returns:
    - uri : String with the path where the data was saved.
    """
    # TODO: Handle save to local file system. Make it default maybe?
    storage = get_storage(storage_backend=config['storage_backend'])
    uri = storage.save_data(data=processed_result, config=config)
    if uri:
        print(f"Data successfully saved at: {uri}")
    else:
        print("Failed to save data.")
        sys.exit(1)

# pylint: disable=C0116


def main():
    # TODO: Error handling when load fails
    print("Starting run")
    print("Load data types")
    data_types = load_config_file(
        file_path='./src/data_types.json')  # TODO: Make path ENV var
    print("Load renaming coloumns")
    rename_cols = load_config_file(
        file_path='./src/rename_cols.json')  # TODO: Make path ENV var
    print("Load allocation keys to ignore")
    ignore_alloc_keys = load_config_file(
        file_path='./src/ignore_alloc_keys.json')  # TODO: Make path ENV var

    print("Build config")
    config = get_config()
    print("Retrieving data from opencost api")
    result = request_data(config=config)
    if result is None:
        print("Result is None. Aborting execution")
        sys.exit(1)
    print("Opencost data retrieved successfully")

    print("Processing the data")
    processed_data = process_result(
        result=result,
        ignored_alloc_keys=ignore_alloc_keys,
        rename_cols=rename_cols,
        data_types=data_types)
    if processed_data is None:
        print("Processed data is None, aborting execution.")
        sys.exit(1)
    print("Data processed successfully")

    print("Saving data")
    save_result(processed_data, config)


if __name__ == "__main__":
    main()
