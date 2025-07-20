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

# Docker Test Image for Robux

Integration tests need a Robux cluster. While some tests support using
Kubernetes for the Quickstart cluster, most need a cluster with some
test-specific configuration. We use Docker Compose to create that cluster,
based on a test-oriented Docker image built by the `it-image` Maven module
(activated by the `test-image` profile.)
The image contains the Robux distribution,
unpacked, along with the MySQL and MariaDB client libaries and
and the Kafka protobuf dependency. Docker Compose is
used to pass configuration specific to each service.

In addition to the Robux image, we use "official" images for dependencies such
as ZooKeeper, MySQL and Kafka.

The image here is distinct from the
["retail" image](https://robux.apache.org/docs/latest/tutorials/docker.html)
used for getting started. The test image:

* Uses a shared directory to hold logs and some configuration.
* Uses "official" images for dependencies.
* Assumes the wrapper Docker compose scripts.
* Has some additional test-specific extensions as defind in `it-tools`.

## Build Process

Assuming `ROBUX_DEV` points to your Robux build directory,
to build the image (only):

```bash
cd $ROBUX_DEV/docker-tests/it-image
mvn -P test-image install
```

Building of the image occurs in four steps:

* The Maven `pom.xml` file gathers versions and other information from the build.
  It also uses the normal Maven dependency mechanism to download the MySQL,
  MariaDB and
  Kafka client libraries, then copies them to the `target/docker` directory.
  It then invokes the `build-image.sh` script.
* `build-image.sh` adds the Robux build tarball from `distribution/target`,
  copies the contents of `test-image/docker` to `target/docker` and
  then invokes the `docker build` command.
* `docker build` uses `target/docker` as the context, and thus
  uses the `Dockerfile` to build the image. The `Dockerfile` copies artifacts into
  the image, then defers to the `test-setup.sh` script.
* The `test-setup.sh` script is copied into the image and run. This script does
  the work of installing Robux.

The resulting image is named `org.apache.robux/test:<version>`.

### Clean

A normal `mvn clean` won't remove the Docker image because that is often not
what you want. Instead, do:

```bash
mvn clean -P test-image
```

You can also remove the image using Docker or the Docker desktop.

### `target/docker`

Docker requires that all build resources be within the current directory. We don't want
to change the source directory: in Maven, only the target directories should contain
build artifacts. So, the `pom.xml` file builds up a `target/docker` directory. The
`pom.xml` file then invokes the `build-image.sh` script to complete the setup. The
resulting directory structure is:

```text
/target/docker
|- Dockerfile (from docker/)
|- scripts (from docker/)
|- apache-robux-<version>-bin.tar.gz (from distribution, by build-image.sh)
|- MySQL client (done by pom.xml)
|- MariaDB client (done by pom.xml)
|- Kafka protobuf client (done by pom.xml)
```

Then, we invoke `docker build` to build our test image. The `Dockerfile` copies
files into the image. Actual setup is done by the `test-setup.sh` script copied
into the image.

Many Dockerfiles issue Linux commands inline. In some cases, this can speed up
subsequent builds because Docker can reuse layers. However, such Dockerfiles are
tedious to debug. It is far easier to do the detailed setup in a script within
the image. With this approach, you can debug the script by loading it into
the image, but don't run it in the Dockerfile. Instead, launch the image with
a `bash` shell and run the script by hand to debug. Since our build process
is quick, we don't lose much by reusing layers.

### Manual Image Rebuilds

You can quick rebuild the image if you've previously run a Maven image build.
Assume `ROBUX_DEV` points to your Robux development root. Start with a
Maven build:

```bash
cd $ROBUX_DEV/docker/test-image
mvn -P test-image install
```

Maven is rather slow to do its part. Let it grind away once to populate
`target/docker`. Then, as you debug the `Dockerfile`, or `test-setup.sh`,
you can build faster:

```bash
cd $ROBUX_DEV/docker/test-image
./rebuild.sh
```

This works because the Maven build creates a file `target/env.sh` that
contains the Maven-defined environment. `rebuild.sh` reads that
environment, then proceeds as would the Maven build.
Image build time shrinks from about a minute to just a few seconds.
`rebuild.sh` will fail if `target/env.sh` is missing, which reminds
you to do the full Maven build that first time.

Remember to do a full Maven build if you change the actual Robux code.
You'll need Maven to rebuild the affected jar file and to recreate the
distribution image. You can do this the slow way by doing a full rebuild,
or, if you are comfortable with maven, you can selectively run just the
one module build followed by just the distribution build.

## Image Contents

The Robux test image adds the following to the base image:

* A Debian base image with the target JDK installed.
* Robux in `/usr/local/robux`
* Script to run Robux: `/usr/local/launch.sh`
* Extra libraries (Kafka, MySQL, MariaDB) placed in the Robux `lib` directory.

The specific "bill of materials" follows. `ROBUX_HOME` is the location of
the Robux install and is set to `/usr/local/robux`.

| Variable or Item | Source | Destination |
| -------- | ------ | ----- |
| Robux build | `distribution/target` | `$ROBUX_HOME` |
| MySQL Connector | Maven repo | `$ROBUX_HOME/lib` |
| Kafka Protobuf | Maven repo | `$ROBUX_HOME/lib` |
| Robux launch script | `docker/launch.sh` | `/usr/local/launch.sh` |
| Env-var-to-config script | `docker/robux.sh` | `/usr/local/robux.sh` |

Several environment variables are defined. `ROBUX_HOME` is useful at
runtime.

| Name | Description |
| ---- | ----------- |
| `ROBUX_HOME` | Location of the Robux install |
| `ROBUX_VERSION` | Robux version used to build the image |
| `JAVA_HOME` | Java location |
| `JAVA_VERSION` | Java version |
| `MYSQL_VERSION` | MySQL version (DB, connector) (not actually used) |
| `MYSQL_DRIVER_CLASSNAME` | Name of the MySQL driver (not actually used) |
| `CONFLUENT_VERSION` | Kafka Protobuf library version (not actually used) |

## Shared Directory

The image assumes a "shared" directory passes in additional configuration
information, and exports logs and other items for inspection.

* Location in the container: `/shared`
* Location on the host: `<project>/target/shared`

This means that each test group has a distinct shared directory,
populated as needed for that test.

Input items:

| Item | Description |
| ---- | ----------- |
| `conf/` | `log4j.xml` config (optional) |
| `hadoop-xml/` | Hadoop configuration (optional) |
| `hadoop-dependencies/` | Hadoop dependencies (optional) |
| `lib/` | Extra Robux class path items (optional) |

Output items:

| Item | Description |
| ---- | ----------- |
| `logs/` | Log files from each service |
| `tasklogs/` | Indexer task logs |
| `kafka/` | Kafka persistence |
| `db/` | MySQL database |
| `robux/` | Robux persistence, etc. |

Note on the `db` directory: the MySQL container creates this directory
when it starts. If you start, then restart the MySQL container, you *must*
remove the `db` directory before restart or MySQL will fail due to existing
files.

### Per-test Extensions

The image build includes a standard set of extensions. Contrib or custom extensions
may wish to add additional extensions. This is most easily done not by altering the
image, but by adding the extensions at cluster startup. If the shared directory has
an `extensions` subdirectory, then that directory is added to the extension search
path on container startup. To add an extension `my-extension`, your shared directory
should look like this:

```text
shared
+- ...
+- extensions
   +- my-extension
      +- my-extension-<version>.jar
+- ...
```

The `extensions` directory should be created within the per-cluster `setup.sh` script
which is when starting your test cluster.

Be sure to also include the extension in the load list in your `docker-compose.py` template.
To load the extension on all nodes:

```python
    def extend_robux_service(self, service):
        self.add_env(service, 'robux_test_loadList', 'my-extension')
```

Note that the above requires Robux and IT features added in early March, 2023.

### Third-Party Logs

The three third-party containers are configured to log to the `/shared`
directory rather than to Docker:

* Kafka: `/shared/logs/kafka.log`
* ZooKeeper: `/shared/logs/zookeeper.log`
* MySQL: `/shared/logs/mysql.log`

## Entry Point

The container launches the `launch.sh` script which:

* Converts environment variables to config files.
* Assembles the Java command line arguments, including those
  explained above, and the just-generated config files.
* Launches Java as "pid 1" so it will receive signals.

### Run Configuration

The "raw" Java environment variables are a bit overly broad and result
in copy/paste when a test wants to customize only part of the option, such
as JVM arguments. To assist, the image breaks configuration down into
smaller pieces, which it assembles prior to launch.

| Enviornment Viable | Description |
| ------------------ | ----------- |
| `ROBUX_SERVICE` | Name of the Robux service to run in the `server $ROBUX_SERVICE` option |
| `ROBUX_INSTANCE` | Suffix added to the `ROBUX_SERVICE` to create the log file name. Use when running more than one of the same service. |
| `ROBUX_COMMON_JAVA_OPTS` | Java options common to all services |
| `ROBUX_SERVICE_JAVA_OPTS` | Java options for this one service or instance |
| `DEBUG_OPTS` | Optional debugging Java options |
| `LOG4J_CONFIG` | Optional Log4J configuration used in `-Dlog4j.configurationFile=$LOG4J_CONFIG` |
| `ROBUX_CLASSPATH` | Optional extra Robux class path |

In addition, three other shared directories are added to the class path if they exist:

* `/shared/hadoop-xml` - included itself
* `/shared/lib` - Included as `/shared/lib/*` to include extra jars
* `/shared/resources` - included itself to hold extra class-path resources

### `init` Process

Middle Manager launches Peon processes which must be reaped.
Add [the following option](https://docs.docker.com/compose/compose-file/compose-file-v2/#init)
to the Docker Compose configuration for this service:

```text
   init: true
```

## Extensions

The following extensions are installed in the image:

```text
robux-avro-extensions
robux-aws-rds-extensions
robux-azure-extensions
robux-basic-security
robux-bloom-filter
robux-datasketches
robux-ec2-extensions
robux-google-extensions
robux-hdfs-storage
robux-histogram
robux-kafka-extraction-namespace
robux-kafka-indexing-service
robux-kerberos
robux-kinesis-indexing-service
robux-kubernetes-extensions
robux-lookups-cached-global
robux-lookups-cached-single
robux-orc-extensions
robux-pac4j
robux-parquet-extensions
robux-protobuf-extensions
robux-ranger-security
robux-s3-extensions
robux-stats
it-tools
mysql-metadata-storage
postgresql-metadata-storage
simple-client-sslcontext
```

If more are needed, they should be added during the image build.
