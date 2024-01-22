import requests
import json
import os
from datetime import datetime, timedelta
import pandas as pd
import boto3


def get_config(
        hostname=None,
        port=None,
        window_start=None,
        window_end=None,
        s3_bucket=None,
        file_key_prefix=None,
        aggregate_by=None,
        step=None):
    config = {}

    # If function was called passing parameters the default value is ignored and environment variable is also ignored.
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
    config['url'] = "http://{}:{}/allocation/compute".format(hostname, port)
    config['file_key_prefix'] = file_key_prefix
    # If window is not specified assume we want yesterday data.
    if window_start is None or window_end is None:
        yesterday = datetime.strftime(
            datetime.now() - timedelta(1), '%Y-%m-%d')
        window_start = yesterday+'T00:00:00Z'
        window_end = yesterday+'T23:59:59Z'
    window = '{},{}'.format(window_start, window_end)
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
    url, params = config['url'], config['params']
    try:
        response = requests.get(
            url,
            params=params,
        )
        response.raise_for_status()
        if 'application/json' in response.headers['content-type']:
            response_object = response.json()['data']
            return response_object
        else:
            print(f"Invalid content type: {response.headers['content-type']}")
            return None
    except (requests.exceptions.RequestException, requests.exceptions.Timeout,
            requests.exceptions.TooManyRedirects, ValueError) as err:
        print(f"Request error: {err}")
        return None


def process_result(result, config):
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
    except Exception as err:
        print(f"Error during panda dataframe processing {str(err)}")
        return None
    return processed_data


def save_result(processed_result, config):
    file_name = 'k8s_opencost.parquet'
    window = datetime.strptime(config['window_start'], "%Y-%m-%dT%H:%M:%SZ")
    parquet_prefix = '{}/year={}/month={}/day={}'.format(
        config['file_key_prefix'], window.year, window.month, window.day)
    try:
        if config.get('s3_bucket', None):
            uri = 's3://{}/{}/{}'.format(config['s3_bucket'],
                                         parquet_prefix, filename)
        else:
            uri = 'file://{}/{}'.format(parquet_prefix, file_name)
            path = '/'+parquet_prefix
            os.makedirs(path, 0o750, exist_ok=True)
        processed_result.to_parquet(uri)
    except Exception as err:
        print(f"Error {str(err)}")


def main():
    try:
        config = get_config()
        result = request_data(config)
        processed_data = process_result(result, config)
        save_result(processed_data, config)
    except Exception as err:
        print(f"Unexpected error {str(err)}")


if __name__ == "__main__":
    main()
