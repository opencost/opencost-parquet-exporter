# opencost-parquet-exporter
Export OpenCost data in parquet format

This script was created to export data from opencost in PARQUET format.

It supports exporting the data to S3 and local directory.

# Dependencies
This script depends on boto3, pandas, numpy and python-dateutil.

The file requirements.txt has all the dependencies specified.

# Configuration:
The script supports the following environment variables:
* OPENCOST_PARQUET_SVC_HOSTNAME: Hostname of the opencost service. By default it assume the opencost service is on localhost.
* OPENCOST_PARQUET_SVC_PORT: Port of the opencost service, by default it assume it is 9003
* OPENCOST_PARQUET_WINDOW_START: Start window for the export, by default it is None, which results in exporting the data for yesterday.
* OPENCOST_PARQUET_WINDOW_END: End of export window, by default it is None, which results in exporting the data for yesterday.
* OPENCOST_PARQUET_S3_BUCKET: S3 bucket that will be used to store the export. By default this is None, and S3 export is not done. If set to a bucket use s3://bucket-name and make sure there is an AWS Role  with access to the s3 bucket attached to the container that is running the export. This also respect the environment variables AWS_PROFILE, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY. see: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
* OPENCOST_PARQUET_FILE_KEY_PREFIX: This is the prefix used for the export, by default it is '/tmp'. The export is going to be saved inside this prefix, in the following structure: year=window_start.year/month=window_start.month/day=window_start.day , ex: tmp/year=2024/month=1/date=15
* OPENCOST_PARQUET_AGGREGATE: This is the dimentions used to aggregate the data. by default we use "namespace,pod,container" which is the same dimensions used for the CSV native export.
* OPENCOST_PARQUET_STEP: This is the Step for the export, by default we use 1h steps, which result in 24 steps in a day and make easier to match the exported data to AWS CUR, since cur also export on hourly base.

# Usage:

## Export yesterday data
```
# 1. Create virtualenv and install dependencies
$python3 -m venv .venv
$pip install requirements.txt
# 2. Configuration
## 2.1 Set any desired variables
$export OPENCOST_PARQUET_FILE_KEY_PREFIX="/tmp/cluster=YOUR_CLUSTER_NAME"
## 2.2 Make sure the script has access to opencost api.
### if running from your laptop for testing, create a port forward to opencost
$kubectl -n opencost port-forward service/opencost 9003:9003

# 3. Run the script to export data
$python3 opencost_parquet_exporter.py
# 4. Check the exported parquet files
$ls /tmp/cluster=YOUR_CLUSTER_NAME
```

## Backfill old data.

You can only backfill data that is still available in the opencost API.

To backfill old data you need to set both the OPENCOST_PARQUET_WINDOW_START AND OPENCOST_PARQUET_WINDOW_END for each day you want to backfill, then run the script one time for each day.

Another option is to use something like the code bellow, to backfill multiple days at once.:

```
import opencost_parquet_exporter
for days in range(1,18):
     config = opencost_parquet_exporter.get_config(window_start=f"2024-01-{str(days).zfill(2)}T00:00:00Z", window_end=f"2024-01-{str(days).zfill(2)}T23:59:59Z")
     print(config)
     result = opencost_parquet_exporter.request_data(config)
     processed_data = opencost_parquet_exporter.process_result(result,config)
     opencost_parquet_exporter.save_result(processed_data, config)
```

# Recommended setup:
Run this script as a k8s cron job once per day.

If you run on multiple clusters, set the OPENCOST_PARQUET_FILE_KEY_PREFIX with a unique indentifier per cluster.

# TODO

The following tasks need to be completed:
* Add testing to the code
* Create a docker image
* Configure the automated build for the docker image
* Create example k8s cron job resource configuration
* Create Helm chart
* Improve script to set data_types using a k8s configmap
* Improve script to set the rename_columns_config using a k8s config map

We are looking forward to have your contributions.
