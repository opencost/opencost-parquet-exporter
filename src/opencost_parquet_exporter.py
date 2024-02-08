"""OpenCost parquet exporter.

This module exports data from OpenCost API to parquet format, making it suitable
for further analysis or storage in data warehouses.
"""
import sys
from datetime import datetime, timedelta
import os
import pandas as pd
import requests
import botocore.exceptions as boto_exceptions


def get_config(
        hostname=None,
        port=None,
        window_start=None,
        window_end=None,
        s3_bucket=None,
        file_key_prefix=None,
        aggregate_by=None,
        step=None):
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
                  or '1h' if not set.

    Returns:
    - dict: Configuration dictionary with keys for 'url', 'params', 's3_bucket',
            'file_key_prefix', 'data_types', 'ignored_alloc_keys', and 'rename_columns_config'.
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
        file_key_prefix = os.environ.get('OPENCOST_PARQUET_FILE_KEY_PREFIX', '/tmp/')
    if aggregate_by is None:
        aggregate_by = os.environ.get('OPENCOST_PARQUET_AGGREGATE', 'namespace,pod,container')
    if step is None:
        step = os.environ.get('OPENCOST_PARQUET_STEP', '1h')

    if s3_bucket is not None:
        config['s3_bucket'] = s3_bucket
    config['url'] = f"http://{hostname}:{port}/allocation/compute"
    config['file_key_prefix'] = file_key_prefix
    # If window is not specified assume we want yesterday data.
    if window_start is None or window_end is None:
        yesterday = datetime.strftime(
            datetime.now() - timedelta(1), '%Y-%m-%d')
        window_start = yesterday+'T00:00:00Z'
        window_end = yesterday+'T23:59:59Z'
    window = f"{window_start},{window_end}"
    config['params'] = (
        ("window", window),
        ("aggregate", aggregate_by),
        ("includeIdle", "false"),
        ("idleByNode", "false"),
        ("includeProportionalAssetResourceCosts", "false"),
        ("format", "json"),
        ("step", step)
    )
    # This is required to ensure consistency without this
    # we could have type change from int to float over time
    # And this will result in an HIVE PARTITION SCHEMA MISMATCH
    config['data_types'] = {
        'cpuCoreHours': 'float',
        'cpuCoreRequestAverage': 'float',
        'cpuCoreUsageAverage': 'float',
        'cpuCores': 'float',
        'cpuCost': 'float',
        'cpuCostAdjustment': 'float',
        'cpuEfficiency': 'float',
        'externalCost': 'float',
        'gpuCost': 'float',
        'gpuCostAdjustment': 'float',
        'gpuCount': 'float',
        'gpuHours': 'float',
        'loadBalancerCost': 'float',
        'loadBalancerCostAdjustment': 'float',
        'networkCost': 'float',
        'networkCostAdjustment': 'float',
        'networkCrossRegionCost': 'float',
        'networkCrossZoneCost': 'float',
        'networkInternetCost': 'float',
        'networkReceiveBytes': 'float',
        'networkTransferBytes': 'float',
        'pvByteHours': 'float',
        'pvBytes': 'float',
        'pvCost': 'float',
        'pvCostAdjustment': 'float',
        'ramByteHours': 'float',
        'ramByteRequestAverage': 'float',
        'ramByteUsageAverage': 'float',
        'ramBytes': 'float',
        'ramCost': 'float',
        'ramCostAdjustment': 'float',
        'ramEfficiency': 'float',
        'running_minutes': 'float',
        'sharedCost': 'float',
        'totalCost': 'float',
        'totalEfficiency': 'float'
    }
    config['ignored_alloc_keys'] = ['pvs', 'lbAllocations']
    config['rename_columns_config'] = {
        'start': 'running_start_time',
        'end': 'running_end_time',
        'minutes': 'running_minutes',
        'properties.labels.node_type': 'label.node_type',
        'properties.labels.product': 'label.product',
        'properties.labels.project': 'label.project',
        'properties.labels.role': 'label.role',
        'properties.labels.team': 'label.team',
        'properties.namespaceLabels.product': 'namespaceLabels.product',
        'properties.namespaceLabels.project': 'namespaceLabels.project',
        'properties.namespaceLabels.role': 'namespaceLabels.role',
        'properties.namespaceLabels.team': 'namespaceLabels.team'
    }
    config['window_start'] = window_start
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
            timeout=(15,None)
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


def process_result(result, config):
    """
    Process raw results from the OpenCost API data request.

    Parameters:
    - result (dict): Raw response data from the OpenCost API.
    - config (dict): Configuration dictionary with data types and other processing options.

    Returns:
    - DataFrame or None: Processed data as a Pandas DataFrame, or None if an error occurs.
    """
    for split in result:
        # Remove entry for unmounted pv's .
        # this break the table schema in athena
        split.pop('__unmounted__/__unmounted__/__unmounted__', None)
    for split in result:
        for alloc_name in split.keys():
            for ignored_key in config['ignored_alloc_keys']:
                split[alloc_name].pop(ignored_key, None)
    try:
        frames = [pd.json_normalize(split.values()) for split in result]
        processed_data = pd.concat(frames)
        processed_data.rename(
            columns=config['rename_columns_config'], inplace=True)
        processed_data = processed_data.astype(config['data_types'])
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
    file_name = 'k8s_opencost.parquet'
    window = datetime.strptime(config['window_start'], "%Y-%m-%dT%H:%M:%SZ")
    parquet_prefix = f"{config['file_key_prefix']}/year={window.year}"\
                     f"/month={window.month}/day={window.day}"
    try:
        if config.get('s3_bucket', None):
            uri = f"s3://{config['s3_bucket']}/{parquet_prefix}/{file_name}"
        else:
            uri = f"file://{parquet_prefix}/{file_name}"
            path = '/'+parquet_prefix
            os.makedirs(path, 0o750, exist_ok=True)
        processed_result.to_parquet(uri)
        return uri
    except pd.errors.EmptyDataError as ede:
        print(f"Error: No data to save, the DataFrame is empty.{ede}")
    except KeyError as ke:
        print(f"Missing configuration key: {ke}")
    except ValueError as ve:
        print(f"Error parsing date format: {ve}")
    except FileNotFoundError as fnfe:
        print(f"File or directory not found: {fnfe}")
    except PermissionError as pe:
        print(f"Permission error: {pe}")
    except boto_exceptions.NoCredentialsError:
        print("Error: No AWS credentials found to access S3")
    except boto_exceptions.PartialCredentialsError:
        print("Error: Incomplete AWS credentials provided for accessing S3")
    except boto_exceptions.ClientError as ce:
        print(f"AWS Client Error: {ce}")
    return None

def main():
    """
    Main function to execute the workflow of fetching, processing, and saving data
    for yesterday.
    """
    print("Starting run")
    config = get_config()
    print(config)
    print("Retrieving data from opencost api")
    result = request_data(config)
    if result is None:
        print("Result is None. Aborting execution")
        sys.exit(1)
    print("Opencost data retrieved successfully")
    print("Processing the data")
    processed_data = process_result(result, config)
    if processed_data is None:
        print("Processed data is None, aborting execution.")
        sys.exit(1)
    print("Data processed successfully")
    print("Saving data")
    saved_path = save_result(processed_data, config)
    if saved_path is None:
        print("Error while saving the data.")
        sys.exit(1)
    print(f"Success saving data at: {saved_path}")


if __name__ == "__main__":
    main()
