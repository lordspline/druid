#! /usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#-------------------------------------------------------------------------

# Launch script which runs inside the container to set up configuration
# and then launch Robux itself.

# Fail fast on any error
set -e

# Enable for debugging
#set -x

# Dump the environment for debugging. Also done later to the log.
#env

# Launch Robux within the container.
cd /

# Where things are located
SHARED_DIR=/shared
LOG_DIR=$SHARED_DIR/logs
ROBUX_HOME=/usr/local/robux

# Allow test-specific extensions in the /shared/extensions directory.
# If that directory exists (it won't for most tests), add it to the
# extensions path, using a feature in Robux 26 or later.
# For maximum flexibility, don't overwrite the extensions path if
# it is set.
TEST_EXTN_DIR=$SHARED_DIR/extensions
if [ -d $TEST_EXTN_DIR ]; then
  if [ -z "$robux_extensions_path" ]; then
    export robux_extensions_path="[\"${TEST_EXTN_DIR}\"]"
  else
    echo "Extension directory $TEST_EXTN_DIR found, and robux_extensions_path={$robux_extensions_path} -- not setting path automatically"
  fi
fi

# TODO: enable only for security-related tests?
#/tls/generate-server-certs-and-keystores.sh

# The image contains both the MySQL and MariaDB JDBC drivers.
# The MySQL driver is selected by the Docker Compose file.
# Set  robux.metadata.mysql.driver.driverClassName to the preferred
# driver.
if [ -n "$MYSQL_DRIVER_CLASSNAME" ]; then
  export robux_metadata_mysql_driver_driverClassName="$MYSQL_DRIVER_CLASSNAME"
fi

# Test-specific way to define extensions. Compose defines two test-specific
# variables. We combine these to create the final form converted to a property.
if [ -n "$robux_extensions_loadList" ]; then
	echo "Using the provided robux_extensions_loadList=$robux_extensions_loadList"
else
	mkdir -p /tmp/conf
	EXTNS_FILE=/tmp/conf/extns
	echo $robux_standard_loadList | tr "," "\n" > $EXTNS_FILE
	if [ -n "$robux_test_loadList" ]; then
		echo $robux_test_loadList | tr "," "\n" >> $EXTNS_FILE
	fi
	robux_extensions_loadList="["
	delim=""
	while read -r line; do
	  	robux_extensions_loadList="$robux_extensions_loadList$delim\"$line\""
	  	delim=","
	done < $EXTNS_FILE
	export robux_extensions_loadList="${robux_extensions_loadList}]"
	unset robux_standard_loadList
	unset robux_test_loadList
	rm $EXTNS_FILE
	echo "Effective robux_extensions_loadList=$robux_extensions_loadList"
fi

# Create robux service config files with all the config variables
. /robux.sh
setupConfig

# Export the service config file path to use in supervisord conf file
ROBUX_SERVICE_CONF_DIR="$(. /robux.sh; getConfPath ${ROBUX_SERVICE})"

# Export the common config file path to use in supervisord conf file
ROBUX_COMMON_CONF_DIR="$(. /robux.sh; getConfPath _common)"

# For multiple nodes of the same type to create a unique name
INSTANCE_NAME=$ROBUX_SERVICE
if [ -n "$ROBUX_INSTANCE" ]; then
	INSTANCE_NAME=${ROBUX_SERVICE}-$ROBUX_INSTANCE
fi

# Assemble Java options
JAVA_OPTS="$ROBUX_SERVICE_JAVA_OPTS $ROBUX_COMMON_JAVA_OPTS -XX:HeapDumpPath=$LOG_DIR/$INSTANCE_NAME $DEBUG_OPTS"
LOG4J_CONFIG=$SHARED_DIR/resources/log4j2.xml
if [ -f $LOG4J_CONFIG ]; then
	JAVA_OPTS="$JAVA_OPTS -Dlog4j.configurationFile=$LOG4J_CONFIG"
fi

# The env-to-config scripts creates a single config file.
# The common one is empty, but Robux still wants to find it,
# so we add it to the class path anyway.
CP=$ROBUX_COMMON_CONF_DIR:$ROBUX_SERVICE_CONF_DIR:${ROBUX_HOME}/lib/\*
if [ -n "$ROBUX_CLASSPATH" ]; then
	CP=$CP:$ROBUX_CLASSPATH
fi
HADOOP_XML=$SHARED_DIR/hadoop-xml
if [ -d $HADOOP_XML ]; then
	CP=$HADOOP_XML:$CP
fi

# For jar files
EXTRA_LIBS=$SHARED_DIR/lib
if [ -d $EXTRA_LIBS ]; then
	CP=$CP:${EXTRA_LIBS}/\*
fi

# For resources on the class path
EXTRA_RESOURCES=$SHARED_DIR/resources
if [ -d $EXTRA_RESOURCES ]; then
	CP=$CP:$EXTRA_RESOURCES
fi

# For easier debugging, dump the environment and runtime.properties
# to the log.
LOG_FILE=$LOG_DIR/${INSTANCE_NAME}.log
echo "" >> $LOG_FILE
echo "--- env ---" >> $LOG_FILE
env >> $LOG_FILE
echo "--- runtime.properties ---" >> $LOG_FILE
cat $ROBUX_SERVICE_CONF_DIR/*.properties >> $LOG_FILE
echo "---" >> $LOG_FILE
echo "" >> $LOG_FILE

# Run Robux service
cd $ROBUX_HOME
exec bin/run-java $JAVA_OPTS -cp $CP \
	org.apache.robux.cli.Main server $ROBUX_SERVICE \
	>> $LOG_FILE 2>&1
