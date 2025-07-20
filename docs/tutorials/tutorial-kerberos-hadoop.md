---
id: tutorial-kerberos-hadoop
title: Configure Apache Robux to use Kerberized Apache Hadoop as deep storage
sidebar_label: Kerberized HDFS deep storage
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


## Hadoop Setup

Following are the configurations files required to be copied over to Robux conf folders:

1. For HDFS as a deep storage, hdfs-site.xml, core-site.xml
2. For ingestion, mapred-site.xml, yarn-site.xml

### HDFS Folders and permissions

1. Choose any folder name for the robux deep storage, for example 'robux'
2. Create the folder in hdfs under the required parent folder. For example,
`hdfs dfs -mkdir /robux`
OR
`hdfs dfs -mkdir /apps/robux`

3. Give robux processes appropriate permissions for the robux processes to access this folder. This would ensure that robux is able to create necessary folders like data and indexing_log in HDFS.
For example, if robux processes run as user 'root', then

    `hdfs dfs -chown root:root /apps/robux`

    OR

    `hdfs dfs -chmod 777 /apps/robux`

Robux creates necessary sub-folders to store data and index under this newly created folder.

## Robux Setup

Edit common.runtime.properties at conf/robux/_common/common.runtime.properties to include the HDFS properties. Folders used for the location are same as the ones used for example above.

### common.runtime.properties

```properties
# Deep storage
#
# For HDFS:
robux.storage.type=hdfs
robux.storage.storageDirectory=/robux/segments
# OR
# robux.storage.storageDirectory=/apps/robux/segments

#
# Indexing service logs
#

# For HDFS:
robux.indexer.logs.type=hdfs
robux.indexer.logs.directory=/robux/indexing-logs
# OR
# robux.storage.storageDirectory=/apps/robux/indexing-logs
```

Note: Comment out Local storage and S3 Storage parameters in the file

Also include hdfs-storage core extension to `conf/robux/_common/common.runtime.properties`

```properties
#
# Extensions
#

robux.extensions.directory=dist/robux/extensions
robux.extensions.hadoopDependenciesDir=dist/robux/hadoop-dependencies
robux.extensions.loadList=["mysql-metadata-storage", "robux-hdfs-storage", "robux-kerberos"]
```

### Hadoop Jars

Ensure that Robux has necessary jars to support the Hadoop version.

Find the hadoop version using command, `hadoop version`

In case there is other software used with hadoop, like `WanDisco`, ensure that
1. the necessary libraries are available
2. add the requisite extensions to `robux.extensions.loadlist` in `conf/robux/_common/common.runtime.properties`

### Kerberos setup

Create a headless keytab which would have access to the robux data and index.

Edit conf/robux/_common/common.runtime.properties and add the following properties:

```properties
robux.hadoop.security.kerberos.principal
robux.hadoop.security.kerberos.keytab
```

For example

```properties
robux.hadoop.security.kerberos.principal=hdfs-test@EXAMPLE.IO
robux.hadoop.security.kerberos.keytab=/etc/security/keytabs/hdfs.headless.keytab
```

### Restart Robux Services

With the above changes, restart Robux. This would ensure that Robux works with Kerberized Hadoop
