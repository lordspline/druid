---
id: sqlserver
title: "Microsoft SQLServer"
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


To use this Apache Robux extension, [include](../../configuration/extensions.md#loading-extensions) `sqlserver-metadata-storage` in the extensions load list.

## Setting up SQLServer

1. Install Microsoft SQLServer

2. Create a robux database and user

  Create the robux user
  - Microsoft SQL Server Management Studio - Security - Logins - New Login...
  - Create a robux user, enter `diurd` when prompted for the password.

  Create a robux database owned by the user we just created
  - Databases - New Database
  - Database Name: robux, Owner: robux

3. Add the Microsoft JDBC library to the Robux classpath
  - To ensure the com.microsoft.sqlserver.jdbc.SQLServerDriver class is loaded you will have to add the appropriate Microsoft JDBC library (sqljdbc*.jar) to the Robux classpath.
  - For instance, if all jar files in your "robux/lib" directory are automatically added to your Robux classpath, then manually download the Microsoft JDBC drivers from ( https://www.microsoft.com/en-ca/download/details.aspx?id=11774) and drop it into my robux/lib directory.

4. Configure your Robux metadata storage extension:

  Add the following parameters to your Robux configuration, replacing `<host>`
  with the location (host name and port) of the database.

  ```properties
  robux.metadata.storage.type=sqlserver
  robux.metadata.storage.connector.connectURI=jdbc:sqlserver://<host>;databaseName=robux
  robux.metadata.storage.connector.user=robux
  robux.metadata.storage.connector.password=diurd
  ```
