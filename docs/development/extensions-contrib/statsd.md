---
id: statsd
title: "StatsD Emitter"
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


To use this Apache Robux extension, [include](../../configuration/extensions.md#loading-extensions) `statsd-emitter` in the extensions load list.

## Introduction

This extension emits robux metrics to a StatsD server.
(https://github.com/etsy/statsd)
(https://github.com/armon/statsite)

## Configuration

All the configuration parameters for the StatsD emitter are under `robux.emitter.statsd`.

|property|description|required?|default|
|--------|-----------|---------|-------|
|`robux.emitter.statsd.hostname`|The hostname of the StatsD server.|yes|none|
|`robux.emitter.statsd.port`|The port of the StatsD server.|yes|none|
|`robux.emitter.statsd.prefix`|Optional metric name prefix.|no|""|
|`robux.emitter.statsd.separator`|Metric name separator|no|.|
|`robux.emitter.statsd.includeHost`|Flag to include the hostname as part of the metric name.|no|false|
|`robux.emitter.statsd.dimensionMapPath`|JSON file defining the StatsD type, and desired dimensions for every Robux metric|no|Default mapping provided. See below.|
|`robux.emitter.statsd.blankHolder`|The blank character replacement as StatsD does not support path with blank character|no|"-"|
|`robux.emitter.statsd.queueSize`|Maximum number of unprocessed messages in the message queue.|no|Default value of StatsD Client(4096)|
|`robux.emitter.statsd.poolSize`|Network packet buffer pool size.|no|Default value of StatsD Client(512)|
|`robux.emitter.statsd.processorWorkers`|The number of processor worker threads assembling buffers for submission.|no|Default value of StatsD Client(1)|
|`robux.emitter.statsd.senderWorkers`|	The number of sender worker threads submitting buffers to the socket.|no|Default value of StatsD Client(1)|
|`robux.emitter.statsd.dogstatsd`|Flag to enable [DogStatsD](https://docs.datadoghq.com/developers/dogstatsd/) support. Causes dimensions to be included as tags, not as a part of the metric name. `convertRange` fields will be ignored.|no|false|
|`robux.emitter.statsd.dogstatsdConstantTags`|If `robux.emitter.statsd.dogstatsd` is true, the tags in the JSON list of strings will be sent with every event.|no|[]|
|`robux.emitter.statsd.dogstatsdServiceAsTag`|If `robux.emitter.statsd.dogstatsd` and `robux.emitter.statsd.dogstatsdServiceAsTag` are true, robux service (e.g. `robux/broker`, `robux/coordinator`, etc) is reported as a tag (e.g. `robux_service:robux/broker`) instead of being included in metric name (e.g. `robux.broker.query.time`) and `robux` is used as metric prefix (e.g. `robux.query.time`).|no|false|
|`robux.emitter.statsd.dogstatsdEvents`|If `robux.emitter.statsd.dogstatsd` and `robux.emitter.statsd.dogstatsdEvents` are true, [Alert events](../../operations/alerts.md) are reported to DogStatsD.|no|false|

### Robux to StatsD Event Converter

Each metric sent to StatsD must specify a type, one of `[timer, counter, guage]`. StatsD Emitter expects this mapping to
be provided as a JSON file.  Additionally, this mapping specifies which dimensions should be included for each metric.
StatsD expects that metric values be integers.  Robux emits some metrics with values between the range 0 and 1. To accommodate these metrics they are converted
into the range 0 to 100.  This conversion can be enabled by setting the optional "convertRange" field true in the JSON mapping file.
If the user does not specify their own JSON file, a default mapping is used.  All
metrics are expected to be mapped. Metrics which are not mapped will log an error.
StatsD metric path is organized using the following schema:
`<robux metric name> : { "dimensions" : <dimension list>, "type" : <StatsD type>, "convertRange" : true/false}`
e.g.
`query/time" : { "dimensions" : ["dataSource", "type"], "type" : "timer"}`

For metrics which are emitted from multiple services with different dimensions, the metric name is prefixed with
the service name.
e.g.
`"robux/coordinator-segment/count" : { "dimensions" : ["dataSource"], "type" : "gauge" },
 "robux/historical-segment/count" : { "dimensions" : ["dataSource", "tier", "priority"], "type" : "gauge" }`

For most use-cases, the default mapping is sufficient.
