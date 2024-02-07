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

### Use docker image to export yesterday data
1. Pull the docker image from github package
2. Create a directory where the export is going to be saved.
``` $mkdir exported_data ```
3. Create the proxy connection with kubectl
``` kubectl --context CONTEXT --as admin --as-group "system:masters" -n opencost port-forward service/opencost 9003:9003 --address=IP_ADDRESS ```
Where IP_ADDREQSS is your local  machine IP.
4. Run the docker image that you pulled on step one, here it is called opencost_parquet_exporter:latest, for example
```docker run -e OPENCOST_PARQUET_SVC_HOSTNAME=IP_ADDRESS --mount src=`pwd`/exported_data,target=/tmp,type=bind opencost_parquet_exporter:latest ```

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

# Athena Table setup.

Create the following athena table, after you upload the resulting files to an S3 bucket.

Change the CHANGE-ME-S3-BUCKET-NAME to your bucket name.

```
CREATE EXTERNAL TABLE `k8s_opencost`(
  `name` string COMMENT 'name from opencost cost model, includes namespace, pod and container name',
  `running_start_time` string COMMENT 'Date for when container started running during window',
  `running_end_time` string COMMENT 'Date when container stopped during the window',
  `running_minutes` double COMMENT 'Minutes container run during window',
  `cpucores` double COMMENT 'CPU cores',
  `cpucorerequestaverage` double COMMENT 'CPU Cores requested during the window',
  `cpucoreusageaverage` double COMMENT 'CPU Cores used during the window',
  `cpucorehours` double COMMENT 'CPU cores max(requests,used) every 30 seconds averaged using the running_minutes',
  `cpucost` double COMMENT 'CPU cost calculated by opencost',
  `cpucostadjustment` double COMMENT 'Internal opencost information',
  `cpuefficiency` double COMMENT 'Efficiency if Usage is available',
  `gpucount` double COMMENT 'GPU Count used',
  `gpuhours` double COMMENT 'GPU hours used',
  `gpucost` double COMMENT 'GPU Cost',
  `gpucostadjustment` double COMMENT 'Internal opencost information',
  `networktransferbytes` double COMMENT 'Bytes transfered , only work on kubecost',
  `networkreceivebytes` double COMMENT 'Bytes received, only work in kubecost',
  `networkcost` double COMMENT 'Network Cost, only work in kubecost',
  `networkcrosszonecost` double COMMENT 'Cross AZ, cost, only works in kubecost',
  `networkcrossregioncost` double COMMENT 'Cross region Cost, only works in kubecost',
  `networkinternetcost` double COMMENT 'Internet cost, only works in kubecost',
  `networkcostadjustment` double COMMENT 'Internal opencost field, only works in kubecost',
  `loadbalancercost` double COMMENT 'Loadbalancer cost',
  `loadbalancercostadjustment` double COMMENT 'Internal opencost field',
  `pvbytes` double COMMENT 'PV bytes used',
  `pvbytehours` double COMMENT 'PV Bytes used averaged during the running time',
  `pvcost` double COMMENT 'PV Cost',
  `pvcostadjustment` double COMMENT 'Internal opencost field',
  `rambytes` double COMMENT 'Rambytes used',
  `rambyterequestaverage` double COMMENT 'Rambytes requested',
  `rambyteusageaverage` double COMMENT 'Rambytes used',
  `rambytehours` double COMMENT 'Rambytes max(request,used) every 30 seconds averaged during the running minutes',
  `ramcost` double COMMENT 'Cost of ram',
  `ramcostadjustment` double COMMENT 'internal opencost field',
  `ramefficiency` double COMMENT 'Efficiency of ram',
  `externalcost` double COMMENT 'External cost, supported in kubecost',
  `sharedcost` double COMMENT 'Shared cost, Supported in kubecost',
  `totalcost` double COMMENT 'Total cost, ram+cpu+lb+pv',
  `totalefficiency` double COMMENT 'Total cost efficiency',
  `properties.cluster` string COMMENT 'Cluster name',
  `properties.node` string COMMENT 'Node name',
  `properties.container` string COMMENT 'Container name',
  `properties.controller` string COMMENT 'Controller name',
  `properties.controllerkind` string COMMENT 'Controler Kind',
  `properties.namespace` string COMMENT 'K8s namespace',
  `properties.pod` string COMMENT 'K8s pod name',
  `properties.services` array<string> COMMENT 'array of services',
  `properties.providerid` string COMMENT 'Cloud provider instance id',
  `label.node_type` string COMMENT 'Label node_type',
  `label.product` string COMMENT 'Label Product',
  `label.project` string COMMENT 'Label Project',
  `label.role` string COMMENT 'Label Role',
  `label.team` string COMMENT 'Label team',
  `namespacelabels.product` string COMMENT 'Namespace Label product',
  `namespacelabels.project` string COMMENT 'Namespace Label project',
  `namespacelabels.role` string COMMENT 'Namespace Label role',
  `namespacelabels.team` string COMMENT 'Namespace Label team',
  `window.start` string COMMENT 'When the window started. Use this to match cloud provider cost system, ex: aws cur dt',
  `window.end` string COMMENT 'When the window ended',
  `__index_level_0__` bigint COMMENT '')
PARTITIONED BY (
  `cluster` string COMMENT '',
  `year` string COMMENT '',
  `month` string COMMENT '',
  `day` string COMMENT '')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://CHANGE-ME-S3-BUCKET-NAME/'
TBLPROPERTIES (
)
```
# TODO

The following tasks need to be completed:
* Add testing to the code [DONE]
* Create a docker image [DONE]
* Configure the automated build for the docker image [DONE]
* Create example k8s cron job resource configuration
* Create Helm chart
* Improve script to set data_types using a k8s configmap
* Improve script to set the rename_columns_config using a k8s config map

We are looking forward to have your contributions.
