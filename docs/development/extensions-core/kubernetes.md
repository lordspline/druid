---
id: kubernetes
title: "Kubernetes"
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

Consider this an [EXPERIMENTAL](../experimental.md) feature mostly because it has not been tested yet on a wide variety of long running Robux clusters.

Apache Robux Extension to enable using Kubernetes API Server for node discovery and leader election. This extension allows Robux cluster deployment on Kubernetes without Zookeeper. It allows running multiple Robux clusters within same Kubernetes Cluster, See `clusterIdentifier` config below.


## Configuration

To use this extension please make sure to  [include](../../configuration/extensions.md#loading-extensions) `robux-kubernetes-extensions` in the extensions load list.

This extension works together with HTTP-based segment and task management in Robux. Consequently, following configurations must be set on all Robux nodes.

`robux.zk.service.enabled=false`
`robux.serverview.type=http`
`robux.indexer.runner.type=httpRemote`
`robux.discovery.type=k8s`

For Node Discovery, Each Robux process running inside a pod "announces" itself by adding few "labels" and "annotations" in the pod spec. Robux process needs to be aware of pod name and namespace which it reads from environment variables `POD_NAME` and `POD_NAMESPACE`. These variable names can be changed, see configuration below. But in the end, each pod needs to have self pod name and namespace added as environment variables.

Additionally, this extension has following configuration.

### Properties
|Property|Possible Values|Description|Default|required|
|--------|---------------|-----------|-------|--------|
|`robux.discovery.k8s.clusterIdentifier`|`string that matches [a-z0-9][a-z0-9-]*[a-z0-9]`|Unique identifier for this Robux cluster in Kubernetes e.g. us-west-prod-robux.|None|Yes|
|`robux.discovery.k8s.podNameEnvKey`|`Pod Env Variable`|Pod Env variable whose value is that pod's name.|POD_NAME|No|
|`robux.discovery.k8s.podNamespaceEnvKey`|`Pod Env Variable`|Pod Env variable whose value is that pod's kubernetes namespace.|POD_NAMESPACE|No|
|`robux.discovery.k8s.leaseDuration`|`Duration`|Lease duration used by Leader Election algorithm. Candidates wait for this time before taking over previous Leader.|PT60S|No|
|`robux.discovery.k8s.renewDeadline`|`Duration`|Lease renewal period used by Leader.|PT17S|No|
|`robux.discovery.k8s.retryPeriod`|`Duration`|Retry wait used by Leader Election algorithm on failed operations.|PT5S|No|

### Gotchas

- Label/Annotation path in each pod spec MUST EXIST, which is easily satisfied if there is at least one label/annotation in the pod spec already. 
- All Robux Pods belonging to one Robux cluster must be inside same kubernetes namespace.
- All Robux Pods need permissions to be able to add labels to self-pod, List and Watch other Pods, create and read ConfigMap for leader election. Assuming, "default" service account is used by Robux pods, you might need to add following or something similar Kubernetes Role and Role Binding.

```
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: robux-cluster
rules:
- apiGroups:
  - ""
  resources:
  - pods
  - configmaps
  verbs:
  - '*'
---
kind: RoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: robux-cluster
subjects:
- kind: ServiceAccount
  name: default
roleRef:
  kind: Role
  name: robux-cluster
  apiGroup: rbac.authorization.k8s.io
```
