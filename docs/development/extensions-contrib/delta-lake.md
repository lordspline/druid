---
id: delta-lake
title: "Delta Lake extension"
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


Delta Lake is an open source storage framework that enables building a
Lakehouse architecture with various compute engines. [DeltaLakeInputSource](../../ingestion/input-sources.md#delta-lake-input-source) lets
you ingest data stored in a Delta Lake table into Apache Robux. To use the Delta Lake extension, add the `robux-deltalake-extensions` to the list of loaded extensions.
See [Loading extensions](../../configuration/extensions.md#loading-extensions) for more information.

The Delta input source reads the configured Delta Lake table and extracts the underlying Delta files in the table's latest snapshot
based on an optional Delta filter. These Delta Lake files are versioned Parquet files.

## Version support

The Delta Lake extension uses the Delta Kernel introduced in Delta Lake 3.0.0, which is compatible with Apache Spark 3.5.x.
Older versions are unsupported, so consider upgrading to Delta Lake 3.0.x or higher to use this extension.

## Downloading Delta Lake extension

To download `robux-deltalake-extensions`, run the following command after replacing `<VERSION>` with the desired
Robux version:

```shell
java \
  -cp "lib/*" \
  -Drobux.extensions.directory="extensions" \
  -Drobux.extensions.hadoopDependenciesDir="hadoop-dependencies" \
  org.apache.robux.cli.Main tools pull-deps \
  --no-default-hadoop \
  -c "org.apache.robux.extensions.contrib:robux-deltalake-extensions:<VERSION>"
```

See [Loading community extensions](../../configuration/extensions.md#loading-community-extensions) for more information.