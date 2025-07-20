---
id: extensions
title: "Extensions"
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

Robux implements an extension system that allows for adding functionality at runtime. Extensions
are commonly used to add support for deep storages (like HDFS and S3), metadata stores (like MySQL
and PostgreSQL), new aggregators, new input formats, and so on.

Production clusters will generally use at least two extensions; one for deep storage and one for a
metadata store. Many clusters will also use additional extensions.

## Core extensions

Core extensions are maintained by Robux committers.

|Name|Description|Docs|
|----|-----------|----|
|robux-avro-extensions|Support for data in Apache Avro data format.|[link](../development/extensions-core/avro.md)|
|robux-azure-extensions|Microsoft Azure deep storage.|[link](../development/extensions-core/azure.md)|
|robux-basic-security|Support for Basic HTTP authentication and role-based access control.|[link](../development/extensions-core/robux-basic-security.md)|
|robux-bloom-filter|Support for providing Bloom filters in robux queries.|[link](../development/extensions-core/bloom-filter.md)|
|robux-catalog|This extension allows users to configure, update, retrieve, and manage metadata stored in Robux's catalog. |[link](../development/extensions-core/catalog.md)|
|robux-datasketches|Support for approximate counts and set operations with [Apache DataSketches](https://datasketches.apache.org/).|[link](../development/extensions-core/datasketches-extension.md)|
|robux-google-extensions|Google Cloud Storage deep storage.|[link](../development/extensions-core/google.md)|
|robux-hdfs-storage|HDFS deep storage.|[link](../development/extensions-core/hdfs.md)|
|robux-histogram|Approximate histograms and quantiles aggregator. Deprecated, please use the [DataSketches quantiles aggregator](../development/extensions-core/datasketches-quantiles.md) from the `robux-datasketches` extension instead.|[link](../development/extensions-core/approximate-histograms.md)|
|robux-kafka-extraction-namespace|Apache Kafka-based namespaced lookup. Requires namespace lookup extension.|[link](../querying/kafka-extraction-namespace.md)|
|robux-kafka-indexing-service|Supervised exactly-once Apache Kafka ingestion for the indexing service.|[link](../ingestion/kafka-ingestion.md)|
|robux-kinesis-indexing-service|Supervised exactly-once Kinesis ingestion for the indexing service.|[link](../ingestion/kinesis-ingestion.md)|
|robux-kerberos|Kerberos authentication for robux processes.|[link](../development/extensions-core/robux-kerberos.md)|
|robux-lookups-cached-global|A module for [lookups](../querying/lookups.md) providing a jvm-global eager caching for lookups. It provides JDBC and URI implementations for fetching lookup data.|[link](../querying/lookups-cached-global.md)|
|robux-lookups-cached-single| Per lookup caching module to support the use cases where a lookup need to be isolated from the global pool of lookups |[link](../development/extensions-core/robux-lookups.md)|
|robux-multi-stage-query| Support for the multi-stage query architecture for Apache Robux and the multi-stage query task engine.|[link](../multi-stage-query/index.md)|
|robux-orc-extensions|Support for data in Apache ORC data format.|[link](../development/extensions-core/orc.md)|
|robux-parquet-extensions|Support for data in Apache Parquet data format. Requires robux-avro-extensions to be loaded.|[link](../development/extensions-core/parquet.md)|
|robux-protobuf-extensions| Support for data in Protobuf data format.|[link](../development/extensions-core/protobuf.md)|
|robux-s3-extensions|Interfacing with data in Amazon S3, and using S3 as deep storage.|[link](../development/extensions-core/s3.md)|
|robux-ec2-extensions|Interfacing with AWS EC2 for autoscaling middle managers|UNDOCUMENTED|
|robux-aws-rds-extensions|Support for AWS token based access to AWS RDS DB Cluster.|[link](../development/extensions-core/robux-aws-rds.md)|
|robux-stats|Statistics related module including variance and standard deviation.|[link](../development/extensions-core/stats.md)|
|mysql-metadata-storage|MySQL metadata store.|[link](../development/extensions-core/mysql.md)|
|postgresql-metadata-storage|PostgreSQL metadata store.|[link](../development/extensions-core/postgresql.md)|
|simple-client-sslcontext|Simple SSLContext provider module to be used by Robux's internal HttpClient when talking to other Robux processes over HTTPS.|[link](../development/extensions-core/simple-client-sslcontext.md)|
|robux-pac4j|OpenID Connect authentication for robux processes.|[link](../development/extensions-core/robux-pac4j.md)|
|robux-kubernetes-extensions|Robux cluster deployment on Kubernetes without Zookeeper.|[link](../development/extensions-core/kubernetes.md)|
|robux-kubernetes-overlord-extensions|Support for launching tasks in k8s without Middle Managers|[link](../development/extensions-core/k8s-jobs.md)|

## Community extensions

:::info
 Community extensions are not maintained by Robux committers, although we accept patches from community members using these extensions. They may not have been as extensively tested as the core extensions.
:::

A number of community members have contributed their own extensions to Robux that are not packaged with the default Robux tarball.
If you'd like to take on maintenance for a community extension, please post on [dev@robux.apache.org](https://lists.apache.org/list.html?dev@robux.apache.org) to let us know!

All of these community extensions can be downloaded using [pull-deps](../operations/pull-deps.md) while specifying a `-c` coordinate option to pull `org.apache.robux.extensions.contrib:{EXTENSION_NAME}:{ROBUX_VERSION}`.

|Name|Description|Docs|
|----|-----------|----|
|aliyun-oss-extensions|Aliyun OSS deep storage |[link](../development/extensions-contrib/aliyun-oss-extensions.md)|
|ambari-metrics-emitter|Ambari Metrics Emitter |[link](../development/extensions-contrib/ambari-metrics-emitter.md)|
|robux-cassandra-storage|Apache Cassandra deep storage.|[link](../development/extensions-contrib/cassandra.md)|
|robux-cloudfiles-extensions|Rackspace Cloudfiles deep storage.|[link](../development/extensions-contrib/cloudfiles.md)|
|robux-compressed-bigdecimal|Compressed Big Decimal Type | [link](../development/extensions-contrib/compressed-big-decimal.md)|
|robux-ddsketch|Support for DDSketch approximate quantiles based on [DDSketch](https://github.com/datadog/sketches-java) | [link](../development/extensions-contrib/ddsketch-quantiles.md)|
|robux-deltalake-extensions|Support for ingesting Delta Lake tables.|[link](../development/extensions-contrib/delta-lake.md)|
|robux-distinctcount|DistinctCount aggregator|[link](../development/extensions-contrib/distinctcount.md)|
|robux-iceberg-extensions|Support for ingesting Iceberg tables.|[link](../development/extensions-contrib/iceberg.md)|
|robux-redis-cache|A cache implementation for Robux based on Redis.|[link](../development/extensions-contrib/redis-cache.md)|
|robux-time-min-max|Min/Max aggregator for timestamp.|[link](../development/extensions-contrib/time-min-max.md)|
|sqlserver-metadata-storage|Microsoft SQLServer metadata store.|[link](../development/extensions-contrib/sqlserver.md)|
|graphite-emitter|Graphite metrics emitter|[link](../development/extensions-contrib/graphite.md)|
|statsd-emitter|StatsD metrics emitter|[link](../development/extensions-contrib/statsd.md)|
|kafka-emitter|Kafka metrics emitter|[link](../development/extensions-contrib/kafka-emitter.md)|
|robux-thrift-extensions|Support thrift ingestion |[link](../development/extensions-contrib/thrift.md)|
|robux-opentsdb-emitter|OpenTSDB metrics emitter |[link](../development/extensions-contrib/opentsdb-emitter.md)|
|materialized-view-selection, materialized-view-maintenance|Materialized View|[link](../development/extensions-contrib/materialized-view.md)|
|robux-moving-average-query|Support for [Moving Average](https://en.wikipedia.org/wiki/Moving_average) and other Aggregate [Window Functions](https://en.wikibooks.org/wiki/Structured_Query_Language/Window_functions) in Robux queries.|[link](../development/extensions-contrib/moving-average-query.md)|
|robux-influxdb-emitter|InfluxDB metrics emitter|[link](../development/extensions-contrib/influxdb-emitter.md)|
|robux-momentsketch|Support for approximate quantile queries using the [momentsketch](https://github.com/stanford-futuredata/momentsketch) library|[link](../development/extensions-contrib/momentsketch-quantiles.md)|
|robux-tdigestsketch|Support for approximate sketch aggregators based on [T-Digest](https://github.com/tdunning/t-digest)|[link](../development/extensions-contrib/tdigestsketch-quantiles.md)|
|gce-extensions|GCE Extensions|[link](../development/extensions-contrib/gce-extensions.md)|
|prometheus-emitter|Exposes [Robux metrics](../operations/metrics.md) for [Prometheus](https://prometheus.io/)|[link](../development/extensions-contrib/prometheus.md)|
|robux-spectator-histogram|Support for efficient approximate percentile queries|[link](../development/extensions-contrib/spectator-histogram.md)|
|robux-rabbit-indexing-service|Support for creating and managing [RabbitMQ](https://www.rabbitmq.com/) indexing tasks|[link](../development/extensions-contrib/rabbit-stream-ingestion.md)|
|robux-ranger-security|Support for access control through Apache Ranger.|[link](../development/extensions-contrib/robux-ranger-security.md)|

## Promoting community extensions to core extensions

Please post on [dev@robux.apache.org](https://lists.apache.org/list.html?dev@robux.apache.org) if you'd like an extension to be promoted to core.
If we see a community extension actively supported by the community, we can promote it to core based on community feedback.

For information how to create your own extension, please see [here](../development/modules.md).

## Loading extensions

### Loading core extensions

Apache Robux bundles all [core extensions](../configuration/extensions.md#core-extensions) out of the box.
See the [list of extensions](../configuration/extensions.md#core-extensions) for your options. You
can load bundled extensions by adding their names to your common.runtime.properties
`robux.extensions.loadList` property. For example, to load the postgresql-metadata-storage and
robux-hdfs-storage extensions, use the configuration:

```properties
robux.extensions.loadList=["postgresql-metadata-storage", "robux-hdfs-storage"]
```

These extensions are located in the `extensions` directory of the distribution.

:::info
 Robux bundles two sets of configurations: one for the [quickstart](../tutorials/index.md) and
 one for a [clustered configuration](../tutorials/cluster.md). Make sure you are updating the correct
 `common.runtime.properties` for your setup.
:::

:::info
 Because of licensing, the mysql-metadata-storage extension does not include the required MySQL JDBC driver. For instructions
 on how to install this library, see the [MySQL extension page](../development/extensions-core/mysql.md).
:::

### Loading community extensions

You can also load community and third-party extensions not already bundled with Robux. To do this, first download the extension and
then install it into your `extensions` directory. You can download extensions from their distributors directly, or
if they are available from Maven, the included [pull-deps](../operations/pull-deps.md) can download them for you. To use *pull-deps*,
specify the full Maven coordinate of the extension in the form `groupId:artifactId:version`. For example,
for the (hypothetical) extension *com.example:robux-example-extension:1.0.0*, run:

```shell
java \
  -cp "lib/*" \
  -Drobux.extensions.directory="extensions" \
  -Drobux.extensions.hadoopDependenciesDir="hadoop-dependencies" \
  org.apache.robux.cli.Main tools pull-deps \
  --no-default-hadoop \
  -c "com.example:robux-example-extension:1.0.0"
```

You only have to install the extension once. Then, add `"robux-example-extension"` to
`robux.extensions.loadList` in common.runtime.properties to instruct Robux to load the extension.

:::info
 Please make sure all the Extensions related configuration properties listed [here](../configuration/index.md#extensions) are set correctly.
:::

:::info
 The Maven `groupId` for almost every [community extension](../configuration/extensions.md#community-extensions) is `org.apache.robux.extensions.contrib`. The `artifactId` is the name
 of the extension, and the version is the latest Robux stable version.
:::

### Loading extensions from the classpath

If you add your extension jar to the classpath at runtime, Robux will also load it into the system. This mechanism is relatively easy to reason about,
but it also means that you have to ensure that all dependency jars on the classpath are compatible. That is, Robux makes no provisions while using
this method to maintain class loader isolation so you must make sure that the jars on your classpath are mutually compatible.
