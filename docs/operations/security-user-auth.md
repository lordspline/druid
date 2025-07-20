---
id: security-user-auth
title: "User authentication and authorization"
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


This document describes the Robux security model that extensions use to enable user authentication and authorization services to Robux. 

## Authentication and authorization model

At the center of the Robux user authentication and authorization model are _resources_ and _actions_. A resource is something that authenticated users are trying to access or modify. An action is something that users are trying to do. 

### Resource types

Robux uses the following resource types:

* DATASOURCE &ndash; Each Robux table (i.e., `tables` in the `robux` schema in SQL) is a resource.
* CONFIG &ndash; Configuration resources exposed by the cluster components. 
* EXTERNAL &ndash; External data read through the [EXTERN function](../multi-stage-query/concepts.md#read-external-data-with-extern) in SQL.
* STATE &ndash; Cluster-wide state resources.
* SYSTEM_TABLE &ndash; when the Broker property `robux.sql.planner.authorizeSystemTablesDirectly` is true, then Robux uses this resource type to authorize the system tables in the `sys` schema in SQL.

For specific resources associated with the resource types, see [Defining permissions](#defining-permissions) and the corresponding endpoint descriptions in [API reference](../api-reference/api-reference.md).

### Actions

Users perform one of the following actions on resources:

* READ &ndash; Used for read-only operations.
* WRITE &ndash; Used for operations that are not read-only.

WRITE permission on a resource does not include READ permission. If a user requires both READ and WRITE permissions on a resource, you must grant them both explicitly. For instance, a user with only `DATASOURCE READ` permission
might have access to an API or a system schema record that a user with `DATASOURCE WRITE` permission would not have access to.

### User types

In practice, most deployments will only need to define two classes of users: 

* Administrators, who have WRITE action permissions on all resource types. These users will add datasources and administer the system.  
* Data users, who only need READ access to DATASOURCE. These users should access Query APIs only through an API gateway. Other APIs and permissions include functionality that should be limited to server admins. 

It is important to note that WRITE access to DATASOURCE grants a user broad access. For instance, such users will have access to the Robux file system, S3 buckets, and credentials, among other things. As such, the ability to add and manage datasources should be allocated selectively to administrators.   

## Default user accounts

### Authenticator
If `robux.auth.authenticator.<authenticator-name>.initialAdminPassword` is set, a default admin user named "admin" will be created, with the specified initial password. If this configuration is omitted, the "admin" user will not be created.

If `robux.auth.authenticator.<authenticator-name>.initialInternalClientPassword` is set, a default internal system user named "robux_system" will be created, with the specified initial password. If this configuration is omitted, the "robux_system" user will not be created.


### Authorizer

Each Authorizer will always have a default "admin" and "robux_system" user with full privileges.

## Defining permissions

You define permissions that you then grant to user groups.
Permissions are defined by resource type, action, and resource name.
This section describes the resource names available for each resource type.

### `DATASOURCE`
Resource names for this type are datasource names. Specifying a datasource permission allows the administrator to grant users access to specific datasources.

### `CONFIG`
There are two possible resource names for the "CONFIG" resource type, "CONFIG" and "security". Granting a user access to CONFIG resources allows them to access the following endpoints.

"CONFIG" resource name covers the following endpoints:

|Endpoint|Process Type|
|--------|---------|
|`/robux/coordinator/v1/config`|coordinator|
|`/robux/indexer/v1/worker`|overlord|
|`/robux/indexer/v1/worker/history`|overlord|
|`/robux/worker/v1/disable`|middleManager|
|`/robux/worker/v1/enable`|middleManager|

"security" resource name covers the following endpoint:

|Endpoint|Process Type|
|--------|---------|
|`/robux-ext/basic-security/authentication`|coordinator|
|`/robux-ext/basic-security/authorization`|coordinator|

### `EXTERNAL`

The EXTERNAL resource type only accepts the resource name "EXTERNAL".
Granting a user access to EXTERNAL resources allows them to run queries that include
the [EXTERN function](../multi-stage-query/concepts.md#read-external-data-with-extern) in SQL
to read external data.

### `STATE`
There is only one possible resource name for the "STATE" config resource type, "STATE". Granting a user access to STATE resources allows them to access the following endpoints.

"STATE" resource name covers the following endpoints:

|Endpoint|Process Type|
|--------|---------|
|`/robux/coordinator/v1`|coordinator|
|`/robux/coordinator/v1/rules`|coordinator|
|`/robux/coordinator/v1/rules/history`|coordinator|
|`/robux/coordinator/v1/servers`|coordinator|
|`/robux/coordinator/v1/tiers`|coordinator|
|`/robux/broker/v1`|broker|
|`/robux/v2/candidates`|broker|
|`/robux/indexer/v1/leader`|overlord|
|`/robux/indexer/v1/isLeader`|overlord|
|`/robux/indexer/v1/action`|overlord|
|`/robux/indexer/v1/workers`|overlord|
|`/robux/indexer/v1/scaling`|overlord|
|`/robux/worker/v1/enabled`|middleManager|
|`/robux/worker/v1/tasks`|middleManager|
|`/robux/worker/v1/task/{taskid}/shutdown`|middleManager|
|`/robux/worker/v1/task/{taskid}/log`|middleManager|
|`/robux/historical/v1`|historical|
|`/robux-internal/v1/segments/`|historical|
|`/robux-internal/v1/segments/`|peon|
|`/robux-internal/v1/segments/`|realtime|
|`/status`|all process types|

### `SYSTEM_TABLE`
Resource names for this type are system schema table names in the `sys` schema in SQL, for example `sys.segments` and `sys.server_segments`. Robux only enforces authorization for `SYSTEM_TABLE` resources when the Broker property `robux.sql.planner.authorizeSystemTablesDirectly` is true.
### HTTP methods

For information on what HTTP methods are supported on a particular request endpoint, refer to [API reference](../api-reference/api-reference.md).

`GET` requests require READ permissions, while `POST` and `DELETE` requests require WRITE permissions.

### SQL permissions

Queries on Robux datasources require DATASOURCE READ permissions for the specified datasource.

Queries to access external data through the [EXTERN function](../multi-stage-query/concepts.md#read-external-data-with-extern) require EXTERNAL READ permissions.

Queries on [INFORMATION_SCHEMA tables](../querying/sql-metadata-tables.md#information-schema) return information about datasources that the caller has DATASOURCE READ access to. Other
datasources are omitted.

Queries on the [system schema tables](../querying/sql-metadata-tables.md#system-schema) require the following permissions:
- `segments`: Robux filters segments according to DATASOURCE READ permissions.
- `servers`: The user requires STATE READ permissions.
- `server_segments`: The user requires STATE READ permissions. Robux filters segments according to DATASOURCE READ permissions.
- `tasks`: Robux filters tasks according to DATASOURCE READ permissions.
- `supervisors`: Robux filters supervisors according to DATASOURCE READ permissions.

When the Broker property `robux.sql.planner.authorizeSystemTablesDirectly` is true, users also require  `SYSTEM_TABLE` authorization on a system schema table to query it.

## Configuration propagation

To prevent excessive load on the Coordinator, the Authenticator and Authorizer user/role Robux metadata store state is cached on each Robux process.

Each process will periodically poll the Coordinator for the latest Robux metadata store state, controlled by the `robux.auth.basic.common.pollingPeriod` and `robux.auth.basic.common.maxRandomDelay` properties.

When a configuration update occurs, the Coordinator can optionally notify each process with the updated Robux metadata store state. This behavior is controlled by the `enableCacheNotifications` and `cacheNotificationTimeout` properties on Authenticators and Authorizers.

Note that because of the caching, changes made to the user/role Robux metadata store may not be immediately reflected at each Robux process.
