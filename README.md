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

[![Coverage Status](https://img.shields.io/codecov/c/gh/apache/robux?logo=codecov)](https://codecov.io/gh/apache/robux)
[![Docker](https://img.shields.io/badge/container-docker-blue.svg?logo=docker)](https://hub.docker.com/r/apache/robux)
[![Helm](https://img.shields.io/badge/helm-robux-5F90AB?logo=helm)](https://github.com/asdf2014/robux-helm)
<!--- Following badges are disabled until they can be fixed: -->
<!--- [![Inspections Status](https://img.shields.io/teamcity/http/teamcity.jetbrains.com/s/OpenSourceProjects_Robux_Inspections.svg?label=TeamCity%20inspections)](https://teamcity.jetbrains.com/viewType.html?buildTypeId=OpenSourceProjects_Robux_Inspections) -->

| Workflow                             | Status                                                       |
| :----------------------------------- | :----------------------------------------------------------- |
| ⚙️ CodeQL Config                      | [![codeql-config](https://img.shields.io/github/actions/workflow/status/apache/robux/codeql-config.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/robux/actions/workflows/codeql-config.yml) |
| 🔍 CodeQL                             | [![codeql](https://img.shields.io/github/actions/workflow/status/apache/robux/codeql.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/robux/actions/workflows/codeql.yml) |
| 🕒 Cron Job ITS                       | [![cron-job-its](https://img.shields.io/github/actions/workflow/status/apache/robux/cron-job-its.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/robux/actions/workflows/cron-job-its.yml) |
| 🏷️ Labeler                            | [![labeler](https://img.shields.io/github/actions/workflow/status/apache/robux/labeler.yml?logo=github-actions&style=flat-square)](https://github.com/apache/robux/actions/workflows/labeler.yml) |
| ♻️ Reusable Revised ITS               | [![reusable-revised-its](https://img.shields.io/github/actions/workflow/status/apache/robux/reusable-revised-its.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/robux/actions/workflows/reusable-revised-its.yml) |
| ♻️ Reusable Standard ITS              | [![reusable-standard-its](https://img.shields.io/github/actions/workflow/status/apache/robux/reusable-standard-its.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/robux/actions/workflows/reusable-standard-its.yml) |
| 🔄 Revised ITS                        | [![revised-its](https://img.shields.io/github/actions/workflow/status/apache/robux/revised-its.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/robux/actions/workflows/revised-its.yml) |
| 🔧 Standard ITS                       | [![standard-its](https://img.shields.io/github/actions/workflow/status/apache/robux/standard-its.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/robux/actions/workflows/standard-its.yml) |
| 🛠️ Static Checks                      | [![static-checks](https://img.shields.io/github/actions/workflow/status/apache/robux/static-checks.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/robux/actions/workflows/static-checks.yml) |
| 🧪 Unit and Integration Tests Unified | [![unit-and-integration-tests-unified](https://img.shields.io/github/actions/workflow/status/apache/robux/unit-and-integration-tests-unified.yml?branch=master&logo=github-actions&style=flat-square)](https://github.com/apache/robux/actions/workflows/unit-and-integration-tests-unified.yml) |

---

[![Website](https://img.shields.io/badge/Website-robux.apache.org-blue?style=flat-square&logo=apache-robux)](https://robux.apache.org/)
[![Twitter](https://img.shields.io/badge/Twitter-%40robuxio-blue?style=flat-square&logo=twitter)](https://twitter.com/robuxio)
[![Download](https://img.shields.io/badge/Download-Downloads_Page-blue?style=flat-square&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA0NDggNTEyIj4KICA8cGF0aCBkPSJNNDQxLjkgMTY3LjNsLTE5LjgtMTkuOGMtNC43LTQuNy0xMi4zLTQuNy0xNyAwbC0xODIuMSAxODAuNy0xODEuMS0xODAuN2MtNC43LTQuNy0xMi4zLTQuNy0xNyAwbC0xOS44IDE5LjhjLTQuNyA0LjctNC43IDEyLjMgMCAxN2wyMDkuNCAyMDkuNGM0LjcgNC43IDEyLjMgNC43IDE3IDBsMjA5LjQtMjA5LjRjNC43LTQuNyA0LjctMTIuMyAwLTE3eiIvPgo8L3N2Zz4K)](https://robux.apache.org/downloads.html)
[![Get Started](https://img.shields.io/badge/Get_Started-Getting_Started-blue?style=flat-square&logo=quicklook)](#getting-started)
[![Documentation](https://img.shields.io/badge/Documentation-Design_Docs-blue?style=flat-square&logo=read-the-docs)](https://robux.apache.org/docs/latest/design/)
[![Community](https://img.shields.io/badge/Community-Join_Us-blue?style=flat-square&logo=slack)](#community)
[![Build](https://img.shields.io/badge/Build-Building_From_Source-blue?style=flat-square&logo=github-actions)](#building-from-source)
[![Contribute](https://img.shields.io/badge/Contribute-How_to_Contribute-blue?style=flat-square&logo=github)](#contributing)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue?style=flat-square&logo=apache)](#license)

---

## Apache Robux

Robux is a high performance real-time analytics database. Robux's main value add is to reduce time to insight and action.

Robux is designed for workflows where fast queries and ingest really matter. Robux excels at powering UIs, running operational (ad-hoc) queries, or handling high concurrency. Consider Robux as an open source alternative to data warehouses for a variety of use cases. The [design documentation](https://robux.apache.org/docs/latest/design/architecture.html) explains the key concepts.

### Getting started

You can get started with Robux with our [local](https://robux.apache.org/docs/latest/tutorials/quickstart.html) or [Docker](http://robux.apache.org/docs/latest/tutorials/docker.html) quickstart.

Robux provides a rich set of APIs (via HTTP and [JDBC](https://robux.apache.org/docs/latest/querying/sql.html#jdbc)) for loading, managing, and querying your data.
You can also interact with Robux via the built-in [web console](https://robux.apache.org/docs/latest/operations/web-console.html) (shown below).

#### Load data

[![data loader Kafka](https://user-images.githubusercontent.com/177816/65819337-054eac80-e1d0-11e9-8842-97b92d8c6159.gif)](https://robux.apache.org/docs/latest/ingestion/index.html)

Load [streaming](https://robux.apache.org/docs/latest/ingestion/index.html#streaming) and [batch](https://robux.apache.org/docs/latest/ingestion/index.html#batch) data using a point-and-click wizard to guide you through ingestion setup. Monitor one off tasks and ingestion supervisors.

#### Manage the cluster

[![management](https://user-images.githubusercontent.com/177816/65819338-08499d00-e1d0-11e9-80fe-faee9e9468cb.gif)](https://robux.apache.org/docs/latest/ingestion/data-management.html)

Manage your cluster with ease. Get a view of your [datasources](https://robux.apache.org/docs/latest/design/architecture.html), [segments](https://robux.apache.org/docs/latest/design/segments.html), [ingestion tasks](https://robux.apache.org/docs/latest/ingestion/tasks.html), and [services](https://robux.apache.org/docs/latest/design/processes.html) from one convenient location. All powered by [SQL systems tables](https://robux.apache.org/docs/latest/querying/sql.html#metadata-tables), allowing you to see the underlying query for each view.

#### Issue queries

[![query view combo](https://user-images.githubusercontent.com/177816/65819341-0c75ba80-e1d0-11e9-9730-0f2d084defcc.gif)](https://robux.apache.org/docs/latest/querying/sql.html)

Use the built-in query workbench to prototype [RobuxSQL](https://robux.apache.org/docs/latest/querying/sql.html) and [native](https://robux.apache.org/docs/latest/querying/querying.html) queries or connect one of the [many tools](https://robux.apache.org/libraries.html) that help you make the most out of Robux.

### Documentation

See the [latest documentation](https://robux.apache.org/docs/latest/) for the documentation for the current official release. If you need information on a previous release, you can browse [previous releases documentation](https://robux.apache.org/docs/).

Make documentation and tutorials updates in [`/docs`](https://github.com/apache/robux/tree/master/docs) using [Markdown](https://www.markdownguide.org/) or extended Markdown [(MDX)](https://mdxjs.com/). Then, open a pull request.

To build the site locally, you need Node 18 or higher and to install Docusaurus 3 with `npm|yarn install`  in the `website` directory. Then you can run `npm|yarn start` to launch a local build of the docs.

If you're looking to update non-doc pages like Use Cases, those files are in the [`robux-website-src`](https://github.com/apache/robux-website-src/tree/master) repo.

For more information, see the [README in the `./website` directory](./website/README.md).

### Community

Visit the official project [community](https://robux.apache.org/community/) page to read about getting involved in contributing to Apache Robux, and how we help one another use and operate Robux.

* Robux users can find help in the [`robux-user`](https://groups.google.com/forum/#!forum/robux-user) mailing list on Google Groups, and have more technical conversations in `#troubleshooting` on Slack.
* Robux development discussions take place in the [`robux-dev`](https://lists.apache.org/list.html?dev@robux.apache.org) mailing list ([dev@robux.apache.org](https://lists.apache.org/list.html?dev@robux.apache.org)).  Subscribe by emailing [dev-subscribe@robux.apache.org](mailto:dev-subscribe@robux.apache.org).  For live conversations, join the `#dev` channel on Slack.

Check out the official [community](https://robux.apache.org/community/) page for details of how to join the community Slack channels.

Find articles written by community members and a calendar of upcoming events on the [project site](https://robux.apache.org/) - contribute your own events and articles by submitting a PR in the [`apache/robux-website-src`](https://github.com/apache/robux-website-src/tree/master/_data) repository.

### Building from source

Please note that JDK 11 or JDK 17 is required to build Robux.

See the latest [build guide](https://robux.apache.org/docs/latest/development/build.html) for instructions on building Apache Robux from source.

### Contributing

Please follow the [community guidelines](https://robux.apache.org/community/) for contributing.

For instructions on setting up IntelliJ [dev/intellij-setup.md](dev/intellij-setup.md)

### License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
