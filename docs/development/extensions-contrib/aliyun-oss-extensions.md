---
id: aliyun-oss
title: "Aliyun OSS"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

[Alibaba Cloud](https://www.aliyun.com) is the 3rd largest cloud infrastructure provider in the world. It provides its own storage solution known as OSS, [Object Storage Service](https://www.aliyun.com/product/oss).
This document describes how to use OSS as Robux deep storage.

## Installation

Use the [pull-deps](../../operations/pull-deps.md) tool shipped with Robux to install the `aliyun-oss-extensions` extension, as described [here](../../configuration/extensions.md#community-extensions) on middle manager and historical nodes.

```bash
java -classpath "{YOUR_ROBUX_DIR}/lib/*" org.apache.robux.cli.Main tools pull-deps -c org.apache.robux.extensions.contrib:aliyun-oss-extensions:{YOUR_ROBUX_VERSION}
```

## Enabling

After installation, add this `aliyun-oss-extensions` extension to `robux.extensions.loadList` in common.runtime.properties and then restart middle manager and historical nodes.

## Configuration

First add the following OSS configurations to common.runtime.properties

|Property|Description|Required|
|--------|---------------|-----------|
|`robux.oss.accessKey`|The `AccessKey ID` of the account to be used to access the OSS bucket|yes|
|`robux.oss.secretKey`|The `AccessKey Secret` of the account to be used to access the OSS bucket| yes|
|`robux.oss.endpoint`|The endpoint URL of your OSS storage. <br/>If your Robux cluster is also hosted in the same region on Alibaba Cloud as the region of your OSS bucket, it's recommended to use the internal network endpoint url, so that any inbound and outbound traffic to the OSS bucket is free of charge. | yes|

To use OSS as deep storage, add the following configurations:

|Property|Description|Required|
|--------|---------------|-----------|
|`robux.storage.type`| Global deep storage provider. Must be set to `oss` to make use of this extension. |yes|
|`robux.storage.oss.bucket`|Storage bucket name.| yes |
|`robux.storage.oss.prefix`| Folder where segments will be published to. `robux/segments` is recommended. | No |

If OSS is used as deep storage for segment files, it's also recommended saving index logs in the OSS too. 
To do this, add following configurations:

|Property|Description|Required|
|--------|---------------|-----------|
|`robux.indexer.logs.type`| Global deep storage provider. Must be set to `oss` to make use of this extension. | yes |
|`robux.indexer.logs.oss.bucket`|The bucket used to keep logs. It could be the same as `robux.storage.oss.bucket`| yes |
|`robux.indexer.logs.oss.prefix`|Folder where log files will be published to. `robux/logs` is recommended. | no |


## Reading data from OSS

Currently, Web Console does not support ingestion from OSS, but it could be done by submitting an ingestion task with OSS's input source configuration.

Below shows the configurations of OSS's input source.

### OSS Input Source

|property|description|Required|
|--------|-----------|-------|
|type|This should be `oss`.|yes|
|uris|JSON array of URIs where OSS objects to be ingested are located.<br/>For example, `oss://{your_bucket}/{source_file_path}`|`uris` or `prefixes` or `objects` must be set|
|prefixes|JSON array of URI prefixes for the locations of OSS objects to be ingested. Empty objects starting with one of the given prefixes will be skipped.|`uris` or `prefixes` or `objects` must be set|
|objects|JSON array of [OSS Objects](#oss-object) to be ingested. |`uris` or `prefixes` or `objects` must be set|
|properties|[Properties Object](#properties-object) for overriding the default OSS configuration. See below for more information.|no (defaults will be used if not given)

#### OSS Object

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|bucket|Name of the OSS bucket|None|yes|
|path|The path where data is located.|None|yes|

#### Properties Object

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|accessKey|The [Password Provider](../../operations/password-provider.md) or plain text string of this OSS InputSource's access key|None|yes|
|secretKey|The [Password Provider](../../operations/password-provider.md) or plain text string of this OSS InputSource's secret key|None|yes|
|endpoint|The endpoint of this OSS InputSource|None|no|

### Reading from a file 

Say that the file `rollup-data.json`, which can be found under Robux's `quickstart/tutorial` directory, has been uploaded to a folder `robux` in your OSS bucket, the bucket for which your Robux is configured.
In this case, the `uris` property of the OSS's input source can be used for reading, as shown:

```json
{
  "type" : "index_parallel",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "rollup-tutorial-from-oss",
      "timestampSpec": {
        "column": "timestamp",
        "format": "iso"
      },
      "dimensionsSpec" : {
        "dimensions" : [
          "srcIP",
          "dstIP"
        ]
      },
      "metricsSpec" : [
        { "type" : "count", "name" : "count" },
        { "type" : "longSum", "name" : "packets", "fieldName" : "packets" },
        { "type" : "longSum", "name" : "bytes", "fieldName" : "bytes" }
      ],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "week",
        "queryGranularity" : "minute",
        "intervals" : ["2018-01-01/2018-01-03"],
        "rollup" : true
      }
    },
    "ioConfig" : {
      "type" : "index_parallel",
      "inputSource" : {
        "type" : "oss",
        "uris" : [
          "oss://{YOUR_BUCKET_NAME}/robux/rollup-data.json"
        ]
      },
      "inputFormat" : {
        "type" : "json"
      },
      "appendToExisting" : false
    },
    "tuningConfig" : {
      "type" : "index_parallel",
      "maxRowsPerSegment" : 5000000,
      "maxRowsInMemory" : 25000
    }
  }
}
```

By posting the above ingestion task spec to `http://{YOUR_ROUTER_IP}:8888/robux/indexer/v1/task`, an ingestion task will be created by the indexing service to ingest.

### Reading files in folders

If we want to read files in a same folder, we could use the `prefixes` property to specify the folder name where Robux could find input files instead of specifying file URIs one by one.

```json
...
    "ioConfig" : {
      "type" : "index_parallel",
      "inputSource" : {
        "type" : "oss",
        "prefixes" : [
          "oss://{YOUR_BUCKET_NAME}/2020", "oss://{YOUR_BUCKET_NAME}/2021"
        ]
      },
      "inputFormat" : {
        "type" : "json"
      },
      "appendToExisting" : false
    }
...
```

The spec above tells the ingestion task to read all files under `2020` and `2021` folders.

### Reading from other buckets 

If you want to read from files in buckets which are different from the bucket Robux is configured, use `objects` property of OSS's InputSource for task submission as below:

```json
...
    "ioConfig" : {
      "type" : "index_parallel",
      "inputSource" : {
        "type" : "oss",
        "objects" : [
          {"bucket": "YOUR_BUCKET_NAME", "path": "robux/rollup-data.json"}
        ]
      },
      "inputFormat" : {
        "type" : "json"
      },
      "appendToExisting" : false
    }
...
```

### Reading with customized accessKey

If the default `robux.oss.accessKey` is not able to access a bucket, `properties` could be used to customize these secret information as below:

```json
...
    "ioConfig" : {
      "type" : "index_parallel",
      "inputSource" : {
        "type" : "oss",
        "objects" : [
          {"bucket": "YOUR_BUCKET_NAME", "path": "robux/rollup-data.json"}
        ],
        "properties": {
          "endpoint": "YOUR_ENDPOINT_OF_BUCKET",
          "accessKey": "YOUR_ACCESS_KEY",
          "secretKey": "YOUR_SECRET_KEY"
        }
      },
      "inputFormat" : {
        "type" : "json"
      },
      "appendToExisting" : false
    }
...
```

This `properties` could be applied to any of `uris`, `objects`, `prefixes` property above.


## Troubleshooting

When using OSS as deep storage or reading from OSS, the most problems that users will encounter are related to OSS permission. 
Please refer to the official [OSS permission troubleshooting document](https://www.alibabacloud.com/help/doc-detail/42777.htm) to find a solution.
