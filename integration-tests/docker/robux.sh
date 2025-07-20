#!/bin/bash

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

set -e

getConfPath()
{
    cluster_conf_base=/tmp/conf/robux/cluster
    case "$1" in
    _common) echo $cluster_conf_base/_common ;;
    historical) echo $cluster_conf_base/data/historical ;;
    historical-for-query-error-test) echo $cluster_conf_base/data/historical ;;
    middleManager) echo $cluster_conf_base/data/middleManager ;;
    indexer) echo $cluster_conf_base/data/indexer ;;
    coordinator) echo $cluster_conf_base/master/coordinator ;;
    broker) echo $cluster_conf_base/query/broker ;;
    router) echo $cluster_conf_base/query/router ;;
    overlord) echo $cluster_conf_base/master/overlord ;;
    *) echo $cluster_conf_base/misc/$1 ;;
    esac
}

# Delete the old key (if existing) and append new key=value
setKey()
{
    service="$1"
    key="$2"
    value="$3"
    service_conf=$(getConfPath $service)/runtime.properties
    # Delete from all
    sed -ri "/$key=/d" $COMMON_CONF_DIR/common.runtime.properties
    [ -f $service_conf ] && sed -ri "/$key=/d" $service_conf
    [ -f $service_conf ] && echo "$key=$value" >>$service_conf
    [ -f $service_conf ] || echo "$key=$value" >>$COMMON_CONF_DIR/common.runtime.properties

    echo "Setting $key=$value in $service_conf"
}

setupConfig()
{
  echo "$(date -Is) configuring service $ROBUX_SERVICE"

  # We put all the config in /tmp/conf to allow for a
  # read-only root filesystem
  mkdir -p /tmp/conf/robux

  COMMON_CONF_DIR=$(getConfPath _common)
  SERVICE_CONF_DIR=$(getConfPath ${ROBUX_SERVICE})

  mkdir -p $COMMON_CONF_DIR
  mkdir -p $SERVICE_CONF_DIR
  touch $COMMON_CONF_DIR/common.runtime.properties
  touch $SERVICE_CONF_DIR/runtime.properties

  setKey $ROBUX_SERVICE robux.host $(resolveip -s $HOSTNAME)
  setKey $ROBUX_SERVICE robux.worker.ip $(resolveip -s $HOSTNAME)

  # Write out all the environment variables starting with robux_ to robux service config file
  # This will replace _ with . in the key
  env | grep ^robux_ | while read evar;
  do
      # Can't use IFS='=' to parse since var might have = in it (e.g. password)
      val=$(echo "$evar" | sed -e 's?[^=]*=??')
      var=$(echo "$evar" | sed -e 's?^\([^=]*\)=.*?\1?g' -e 's?_?.?g')
      setKey $ROBUX_SERVICE "$var" "$val"
  done
  if [ "$MYSQL_DRIVER_CLASSNAME" != "com.mysql.jdbc.Driver" ] ; then
    setKey $ROBUX_SERVICE robux.metadata.mysql.driver.driverClassName $MYSQL_DRIVER_CLASSNAME
  fi
}

setupData()
{
  # note: this function exists for legacy reasons, ideally we should do data insert in IT's setup method.
  if [ -n "$ROBUX_SERVICE" ]; then
    echo "ROBUX_SERVICE is set, skipping data setup"
    return
  fi
  # note: this function exists for legacy reasons, ideally we should do data insert in IT's setup method.

  bash /run-mysql.sh
  # The "query" and "security" test groups require data to be setup before running the tests.
  # In particular, they requires segments to be download from a pre-existing s3 bucket.
  # This is done by using the loadSpec put into metadatastore and s3 credientials set below.
  if [ "$ROBUX_INTEGRATION_TEST_GROUP" = "query" ] || [ "$ROBUX_INTEGRATION_TEST_GROUP" = "query-retry" ] || [ "$ROBUX_INTEGRATION_TEST_GROUP" = "query-error" ] || [ "$ROBUX_INTEGRATION_TEST_GROUP" = "high-availability" ] || [ "$ROBUX_INTEGRATION_TEST_GROUP" = "security" ] || [ "$ROBUX_INTEGRATION_TEST_GROUP" = "ldap-security" ] || [ "$ROBUX_INTEGRATION_TEST_GROUP" = "upgrade" ] || [ "$ROBUX_INTEGRATION_TEST_GROUP" = "centralized-datasource-schema" ] || [ "$ROBUX_INTEGRATION_TEST_GROUP" = "cds-task-schema-publish-disabled" ] || [ "$ROBUX_INTEGRATION_TEST_GROUP" = "cds-coordinator-metadata-query-disabled" ]; then
    cat /test-data/${ROBUX_INTEGRATION_TEST_GROUP}-sample-data.sql | mysql -u root robux
  fi

  # The SqlInputSource tests in the "input-source" test group require data to be setup in MySQL before running the tests.
  if [ "$ROBUX_INTEGRATION_TEST_GROUP" = "input-source" ] ; then
    echo "GRANT ALL ON sqlinputsource.* TO 'robux'@'%'; CREATE database sqlinputsource DEFAULT CHARACTER SET utf8mb4;" | mysql -u root robux
    cat /test-data/sql-input-source-sample-data.sql | mysql -u root robux
  fi
}
