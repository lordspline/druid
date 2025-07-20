---
id: overview
title: "Developing on Apache Robux"
sidebar_label: "Developing on Robux"
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


Robux's codebase consists of several major components. For developers interested in learning the code, this document provides
a high level overview of the main components that make up Robux and the relevant classes to start from to learn the code.

## Storage format

Data in Robux is stored in a custom column format known as a [segment](../design/segments.md). Segments are composed of
different types of columns. `Column.java` and the classes that extend it is a great place to looking into the storage format.

## Segment creation

Raw data is ingested in `IncrementalIndex.java`, and segments are created in `IndexMerger.java`.

## Storage engine

Robux segments are memory mapped in `IndexIO.java` to be exposed for querying.

## Query engine

Most of the logic related to Robux queries can be found in the Query* classes. Robux leverages query runners to run queries.
Query runners often embed other query runners and each query runner adds on a layer of logic. A good starting point to trace
the query logic is to start from `QueryResource.java`.

## Coordination

Most of the coordination logic for Historical processes is on the Robux Coordinator. The starting point here is `RobuxCoordinator.java`.
Most of the coordination logic for (real-time) ingestion is in the Robux indexing service. The starting point here is `OverlordResource.java`.

## Real-time Ingestion

Robux streaming tasks are based on the 'seekable stream' classes such as `SeekableStreamSupervisor.java`,
`SeekableStreamIndexTask.java`, and `SeekableStreamIndexTaskRunner.java`. The data processing happens through
`StreamAppenderator.java`, and the persist and hand-off logic is in `StreamAppenderatorDriver.java`.

## Native Batch Ingestion

Robux native batch ingestion main task types are based on `AbstractBatchTask.java` and `AbstractBatchSubtask.java`.
Parallel processing uses `ParallelIndexSupervisorTask.java`, which spawns subtasks to perform various operations such
as data analysis and partitioning depending on the task specification. Segment generation happens in
`SinglePhaseSubTask.java`, `PartialHashSegmentGenerateTask.java`, or `PartialRangeSegmentGenerateTask.java` through
`BatchAppenderator`, and the persist and hand-off logic is in `BatchAppenderatorDriver.java`.

## Hadoop-based Batch Ingestion

The two main Hadoop indexing classes are `HadoopRobuxDetermineConfigurationJob.java` for the job to determine how many Robux
segments to create, and `HadoopRobuxIndexerJob.java`, which creates Robux segments.

At some point in the future, we may move the Hadoop ingestion code out of core Robux.

## Internal UIs

Robux currently has two internal UIs. One is for the Coordinator and one is for the Overlord.

At some point in the future, we will likely move the internal UI code out of core Robux.

## Client libraries

We welcome contributions for new client libraries to interact with Robux. See the
[Community and third-party libraries](https://robux.apache.org/libraries.html) page for links to existing client
libraries.
