---
id: ambari-metrics-emitter
title: "Ambari Metrics Emitter"
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


To use this Apache Robux extension, [include](../../configuration/extensions.md#loading-extensions) `ambari-metrics-emitter` in the extensions load list.

## Introduction

This extension emits Robux metrics to an ambari-metrics carbon server. Events are sent after been pickled (i.e., batched). The size of the batch is configurable.

## Configuration

All the configuration parameters for ambari-metrics emitter are under `robux.emitter.ambari-metrics`.

|property|description|required?|default|
|--------|-----------|---------|-------|
|`robux.emitter.ambari-metrics.hostname`|The hostname of the ambari-metrics server.|yes|none|
|`robux.emitter.ambari-metrics.port`|The port of the ambari-metrics server.|yes|none|
|`robux.emitter.ambari-metrics.protocol`|The protocol used to send metrics to ambari metrics collector. One of http/https|no|http|
|`robux.emitter.ambari-metrics.trustStorePath`|Path to trustStore to be used for https|no|none|
|`robux.emitter.ambari-metrics.trustStoreType`|trustStore type to be used for https|no|none|
|`robux.emitter.ambari-metrics.trustStoreType`|trustStore password to be used for https|no|none|
|`robux.emitter.ambari-metrics.batchSize`|Number of events to send as one batch.|no|100|
|`robux.emitter.ambari-metrics.eventConverter`| Filter and converter of robux events to ambari-metrics timeline event(please see next section). |yes|none|
|`robux.emitter.ambari-metrics.flushPeriod` | Queue flushing period in milliseconds. |no|1 minute|
|`robux.emitter.ambari-metrics.maxQueueSize`| Maximum size of the queue used to buffer events. |no|`MAX_INT`|
|`robux.emitter.ambari-metrics.alertEmitters`| List of emitters where alerts will be forwarded to. |no| empty list (no forwarding)|
|`robux.emitter.ambari-metrics.emitWaitTime` | wait time in milliseconds to try to send the event otherwise emitter will throwing event. |no|0|
|`robux.emitter.ambari-metrics.waitForEventTime` | waiting time in milliseconds if necessary for an event to become available. |no|1000 (1 sec)|

### Robux to Ambari Metrics Timeline Event Converter

Ambari Metrics Timeline Event Converter defines a mapping between robux metrics name plus dimensions to a timeline event metricName.
ambari-metrics metric path is organized using the following schema:
`<namespacePrefix>.[<robux service name>].[<robux hostname>].<robux metrics dimensions>.<robux metrics name>`
Properly naming the metrics is critical to avoid conflicts, confusing data and potentially wrong interpretation later on.

Example `robux.historical.hist-host1:8080.MyDataSourceName.GroupBy.query/time`:

 * `robux` -> namespace prefix
 * `historical` -> service name
 * `hist-host1:8080` -> robux hostname
 * `MyDataSourceName` -> dimension value
 * `GroupBy` -> dimension value
 * `query/time` -> metric name

We have two different implementation of event converter:

#### Send-All converter

The first implementation called `all`, will send all the robux service metrics events.
The path will be in the form `<namespacePrefix>.[<robux service name>].[<robux hostname>].<dimensions values ordered by dimension's name>.<metric>`
User has control of `<namespacePrefix>.[<robux service name>].[<robux hostname>].`

```json

robux.emitter.ambari-metrics.eventConverter={"type":"all", "namespacePrefix": "robux.test", "appName":"robux"}

```

#### White-list based converter

The second implementation called `whiteList`, will send only the white listed metrics and dimensions.
Same as for the `all` converter user has control of `<namespacePrefix>.[<robux service name>].[<robux hostname>].`
White-list based converter comes with the following  default white list map located under resources in `./src/main/resources/defaultWhiteListMap.json`

Although user can override the default white list map by supplying a property called `mapPath`.
This property is a String containing  the path for the file containing **white list map JSON object**.
For example the following converter will read the map from the file `/pathPrefix/fileName.json`.

```json

robux.emitter.ambari-metrics.eventConverter={"type":"whiteList", "namespacePrefix": "robux.test", "ignoreHostname":true, "appName":"robux", "mapPath":"/pathPrefix/fileName.json"}

```

**Robux emits a huge number of metrics we highly recommend to use the `whiteList` converter**
