---
id: redis-cache
title: "Robux Redis Cache"
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

A cache implementation for Robux based on [Redis](https://github.com/redis/redis).

Below are guidance and configuration options known to this module.

## Installation

Use [pull-deps](../../operations/pull-deps.md) tool shipped with Robux to install this [extension](../../configuration/extensions.md#community-extensions) on broker, historical and middle manager nodes.

```bash
java -classpath "robux_dir/lib/*" org.apache.robux.cli.Main tools pull-deps -c org.apache.robux.extensions.contrib:robux-redis-cache:{VERSION}
```

## Enabling

To enable this extension after installation,

1. [include](../../configuration/extensions.md#loading-extensions) this `robux-redis-cache` extension
2. to enable cache on broker nodes, follow [broker caching docs](../../configuration/index.md#broker-caching) to set related properties
3. to enable cache on historical nodes, follow [historical caching docs](../../configuration/index.md#historical-caching) to set related properties
4. to enable cache on middle manager nodes, follow [peon caching docs](../../configuration/index.md#peon-caching) to set related properties
5. set `robux.cache.type` to `redis`
6. add the following properties

## Configuration

### Cluster mode 

To utilize a redis cluster, following properties must be set.

Note: some redis cloud service providers provide redis cluster service via a redis proxy, for these clusters, please follow the [Standalone mode](#standalone-mode) configuration below.

| Properties |Description|Default|Required|
|--------------------|-----------|-------|--------|
|`robux.cache.cluster.nodes`| Redis nodes in a cluster, represented in comma separated string. See example below | None | yes |
|`robux.cache.cluster.maxRedirection`| Max retry count | 5 | no |

#### Example 

```properties
# a typical redis cluster with 6 nodes
robux.cache.cluster.nodes=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006
```

### Standalone mode

To use a standalone redis, following properties must be set.

| Properties |Description|Default|Required|
|--------------------|-----------|-------|--------|
|`robux.cache.host`|Redis server host|None|yes|
|`robux.cache.port`|Redis server port|None|yes|
|`robux.cache.database`|Redis database index|0|no|

Note: if both `robux.cache.cluster.nodes` and `robux.cache.host` are provided, cluster mode is preferred.

### Shared Properties

Except for the properties above, there are some extra properties which can be customized to meet different needs.

| Properties |Description|Default|Required|
|--------------------|-----------|-------|--------|
|`robux.cache.password`| Password to access redis server/cluster | None |no|
|`robux.cache.expiration`|Expiration for cache entries | P1D |no|
|`robux.cache.timeout`|Timeout for connecting to Redis and reading entries from Redis|PT2S|no|
|`robux.cache.maxTotalConnections`|Max total connections to Redis|8|no|
|`robux.cache.maxIdleConnections`|Max idle connections to Redis|8|no|
|`robux.cache.minIdleConnections`|Min idle connections to Redis|0|no|

For `robux.cache.expiration` and `robux.cache.timeout` properties, values can be format of `Period` or a number in milliseconds.

```properties
# Period format(recomended)
# cache expires after 1 hour
robux.cache.expiration=PT1H

# or in number(milliseconds) format
# 1 hour = 3_600_000 milliseconds
robux.cache.expiration=3600000
```

## Metrics

In addition to the normal cache metrics, the redis cache implementation also reports the following in both `total` and `delta`

|Metric|Description|Normal value|
|------|-----------|------------|
|`query/cache/redis/*/requests`|Count of requests to redis cache|whatever request to redis will increase request count by 1|
