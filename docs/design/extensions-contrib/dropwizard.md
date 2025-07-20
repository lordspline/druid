---
layout: doc_page
title: "Dropwizard metrics emitter"
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

# Dropwizard Emitter

To use this extension, make sure to [include](../../configuration/extensions.md#loading-extensions) `dropwizard-emitter` in the extensions load list.

## Introduction

This extension integrates [Dropwizard](http://metrics.dropwizard.io/3.1.0/getting-started/#) metrics library with robux so that dropwizard users can easily absorb robux into their monitoring ecosystem.
It accumulates robux metrics as dropwizard metrics, and emits them to various sinks via dropwizard supported reporters.
Currently supported dropwizard metrics types counter, gauge, meter, timer and histogram. 
These metrics can be emitted using either Console or JMX reporter. 

To use this emitter, set

```
robux.emitter=dropwizard
```

## Configuration

All the configuration parameters for Dropwizard emitter are under `robux.emitter.dropwizard`.
    
|property|description|required?|default|
|--------|-----------|---------|-------|
|`robux.emitter.dropwizard.reporters`|List of dropwizard reporters to be used. Here is a list of [Supported Reporters](#supported-dropwizard-reporters)|yes|none|
|`robux.emitter.dropwizard.prefix`|Optional prefix to be used for metrics name|no|none|
|`robux.emitter.dropwizard.includeHost`|Flag to include the host and port as part of the metric name.|no|yes|
|`robux.emitter.dropwizard.dimensionMapPath`|Path to JSON file defining the dropwizard metric type, and desired dimensions for every Robux metric|no|Default mapping provided. See below.|
|`robux.emitter.dropwizard.alertEmitters`| List of emitters where alerts will be forwarded to. |no| empty list (no forwarding)|
|`robux.emitter.dropwizard.maxMetricsRegistrySize`| Maximum size of metrics registry to be cached at any time. |no| 100 Mb|


### Robux to Dropwizard Event Conversion

Each metric emitted using Dropwizard must specify a type, one of `[timer, counter, guage, meter, histogram]`. Dropwizard Emitter expects this mapping to
be provided as a JSON file.  Additionally, this mapping specifies which dimensions should be included for each metric.
If the user does not specify their own JSON file, a [default mapping](#default-metrics-mapping) is used.
All metrics are expected to be mapped. Metrics which are not mapped will be ignored.
Dropwizard metric path is organized using the following schema:

`<robux metric name> : { "dimensions" : <dimension list>, "type" : <Dropwizard metric type>, "timeUnit" : <For timers, timeunit in which metric is emitted>}`

e.g.
```json
"query/time" : { "dimensions" : ["dataSource", "type"], "type" : "timer", "timeUnit": "MILLISECONDS"},
"segment/scan/pending" : { "dimensions" : [], "type" : "gauge"}
```

For most use-cases, the default mapping is sufficient.

### Supported Dropwizard reporters

#### JMX Reporter
Used to report robux metrics via JMX.
```

robux.emitter.dropwizard.reporters=[{"type":"jmx"}]

```

#### Console Reporter
Used to print Robux Metrics to console logs.

```

robux.emitter.dropwizard.reporters=[{"type":"console","emitIntervalInSecs":30}"}]

```

### Default Metrics Mapping
Latest default metrics mapping can be found [here](https://github.com/apache/robux/blob/master/extensions-contrib/dropwizard-emitter/src/main/resources/defaultMetricDimensions.json)
