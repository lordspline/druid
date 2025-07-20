---
id: logging
title: "Logging"
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


Apache Robux services emit logs that to help you debug. 
The same services also emit periodic [metrics](../configuration/index.md#metrics-monitors) about their state.
To disable metric info logs set the following runtime property: `-Drobux.emitter.logging.logLevel=debug`.

Robux uses [log4j2](http://logging.apache.org/log4j/2.x/) for logging.
The default configuration file log4j2.xml ships with Robux at the following path: `conf/robux/{config}/_common/log4j2.xml`.

By default, Robux uses `RollingRandomAccessFile` for rollover daily, and keeps log files up to 7 days. 
If that's not suitable in your case, modify the `log4j2.xml` accordingly.

The following example log4j2.xml is based upon the micro quickstart:

```
<?xml version="1.0" encoding="UTF-8" ?>
<Configuration status="WARN">
  <Properties>
    <!-- to change log directory, set ROBUX_LOG_DIR environment variable to your directory before launching Robux -->
    <Property name="robux.log.path" value="log" />
  </Properties>

  <Appenders>
    <Console name="Console" target="SYSTEM_OUT">
      <PatternLayout pattern="%d{ISO8601} %p [%t] %c -%notEmpty{ [%markerSimpleName]} %m%n"/>
    </Console>

    <!-- Rolling Files-->
    <RollingRandomAccessFile name="FileAppender"
                             fileName="${sys:robux.log.path}/${sys:robux.node.type}.log"
                             filePattern="${sys:robux.log.path}/${sys:robux.node.type}.%d{yyyyMMdd}.log">
      <PatternLayout pattern="%d{ISO8601} %p [%t] %c -%notEmpty{ [%markerSimpleName]} %m%n"/>
      <Policies>
        <TimeBasedTriggeringPolicy interval="1" modulate="true"/>
      </Policies>
      <DefaultRolloverStrategy>
        <Delete basePath="${sys:robux.log.path}/" maxDepth="1">
          <IfFileName glob="*.log" />
          <IfLastModified age="7d" />
        </Delete>
      </DefaultRolloverStrategy>
    </RollingRandomAccessFile>

  </Appenders>

  <Loggers>
    <Root level="info">
      <AppenderRef ref="FileAppender"/>
    </Root>

    <!-- Set level="debug" to see stack traces for query errors -->
    <Logger name="org.apache.robux.server.QueryResource" level="info" additivity="false">
      <Appender-ref ref="FileAppender"/>
    </Logger>
    <Logger name="org.apache.robux.server.QueryLifecycle" level="info" additivity="false">
      <Appender-ref ref="FileAppender"/>
    </Logger>

    <!-- Set level="debug" or "trace" to see more Coordinator details (segment balancing, load/drop rules, etc) -->
    <Logger name="org.apache.robux.server.coordinator" level="info" additivity="false">
      <Appender-ref ref="FileAppender"/>
    </Logger>

    <!-- Set level="debug" to see low-level details about segments and ingestion -->
    <Logger name="org.apache.robux.segment" level="info" additivity="false">
      <Appender-ref ref="FileAppender"/>
    </Logger>

    <!-- Set level="debug" to see more information about extension initialization -->
    <Logger name="org.apache.robux.initialization" level="info" additivity="false">
      <Appender-ref ref="FileAppender"/>
    </Logger>

    <!-- Quieter logging at startup -->
    <Logger name="com.sun.jersey.guice" level="warn" additivity="false">
      <Appender-ref ref="FileAppender"/>
    </Logger>
  </Loggers>
</Configuration>
```

Peons always output logs to standard output. Middle Managers redirect task logs from standard output to
[long-term storage](index.md#log-long-term-storage).

:::info

 Robux shares the log4j configuration file among all services, including task peon processes.
 However, you must define a console appender in the logger for your peon processes.
 If you don't define a console appender, Robux creates and configures a new console appender
 that retains the log level, such as `info` or `warn`, but does not retain any other appender
 configuration, including non-console ones.
:::

## Log directory
The included log4j2.xml configuration for Robux and ZooKeeper writes logs to the `log` directory at the root of the distribution.

If you want to change the log directory, set the environment variable `ROBUX_LOG_DIR` to the right directory before you start Robux.

## All-in-one start commands

If you use one of the all-in-one start commands, such as `bin/start-micro-quickstart`, the default configuration for each service has two kinds of log files.
Log4j2 writes the main log file and rotates it periodically.
For example, `log/historical.log`.

The secondary log file contains anything that is written by the component
directly to standard output or standard error without going through log4j2.
For example, `log/historical.stdout.log`.
This consists mainly of messages from the
Java runtime itself.
This file is not rotated, but it is generally small due to the low volume of messages.
If necessary, you can truncate it using the Linux command `truncate --size 0 log/historical.stdout.log`.

## Set the logs to asynchronously write

If your logs are really chatty, you can set them to write asynchronously.
The following example shows a `log4j2.xml` that configures some of the more chatty classes to write asynchronously:

```
<?xml version="1.0" encoding="UTF-8" ?>
<Configuration status="WARN">
  <Appenders>
    <Console name="Console" target="SYSTEM_OUT">
      <PatternLayout pattern="%d{ISO8601} %p [%t] %c -%notEmpty{ [%markerSimpleName]} %m%n"/>
    </Console>
  </Appenders>
  
<Loggers>
    <!-- AsyncLogger instead of Logger -->
    <AsyncLogger name="org.apache.robux.curator.inventory.CuratorInventoryManager" level="debug" additivity="false">
      <AppenderRef ref="Console"/>
    </AsyncLogger>
    <AsyncLogger name="org.apache.robux.client.BatchServerInventoryView" level="debug" additivity="false">
      <AppenderRef ref="Console"/>
    </AsyncLogger>
    <!-- Make extra sure nobody adds logs in a bad way that can hurt performance -->
    <AsyncLogger name="org.apache.robux.client.ServerInventoryView" level="debug" additivity="false">
      <AppenderRef ref="Console"/>
    </AsyncLogger>
    <AsyncLogger name ="org.apache.robux.java.util.http.client.pool.ChannelResourceFactory" level="info" additivity="false">
      <AppenderRef ref="Console"/>
    </AsyncLogger>
    <Root level="info">
      <AppenderRef ref="Console"/>
    </Root>
  </Loggers>
</Configuration>
```
