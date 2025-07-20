#!/bin/sh

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# NOTE: this is a 'run' script for the stock tarball
# It takes 1 required argument (the name of the service,
# e.g. 'broker', 'historical' etc). Any additional arguments
# are passed to that service.
#
# It accepts 'JAVA_OPTS' as an environment variable
#
# Additional env vars:
# - ROBUX_LOG4J -- set the entire log4j.xml verbatim
# - ROBUX_LOG_LEVEL -- override the default log level in default log4j
# - ROBUX_XMX -- set Java Xmx
# - ROBUX_XMS -- set Java Xms
# - ROBUX_MAXNEWSIZE -- set Java max new size
# - ROBUX_NEWSIZE -- set Java new size
# - ROBUX_MAXDIRECTMEMORYSIZE -- set Java max direct memory size
#
# - ROBUX_CONFIG_COMMON -- full path to a file for robux 'common' properties
# - ROBUX_CONFIG_${service} -- full path to a file for robux 'service' properties

# This script is very similar to robux.sh, used exclusively for the kubernetes-overlord-extension.

set -e
SERVICE="overlord"

echo "$(date -Is) startup service $SERVICE"

# We put all the config in /tmp/conf to allow for a
# read-only root filesystem
mkdir -p /tmp/conf/
test -d /tmp/conf/robux && rm -r /tmp/conf/robux
cp -r /opt/robux/conf/robux /tmp/conf/robux

getConfPath() {
    cluster_conf_base=/tmp/conf/robux/cluster
    case "$1" in
    _common) echo $cluster_conf_base/_common ;;
    historical) echo $cluster_conf_base/data/historical ;;
    middleManager) echo $cluster_conf_base/data/middleManager ;;
    indexer) echo $cluster_conf_base/data/indexer ;;
    coordinator | overlord) echo $cluster_conf_base/master/coordinator-overlord ;;
    broker) echo $cluster_conf_base/query/broker ;;
    router) echo $cluster_conf_base/query/router ;;
    *) echo $cluster_conf_base/misc/$1 ;;
    esac
}
COMMON_CONF_DIR=$(getConfPath _common)
SERVICE_CONF_DIR=$(getConfPath ${SERVICE})

# Delete the old key (if existing) and append new key=value
setKey() {
    service="$1"
    key="$2"
    value="$3"
    service_conf=$(getConfPath $service)/runtime.properties
    # Delete from all
    sed -ri "/$key=/d" $COMMON_CONF_DIR/common.runtime.properties
    [ -f $service_conf ] && sed -ri "/$key=/d" $service_conf
    [ -f $service_conf ] && echo -e "\n$key=$value" >>$service_conf
    [ -f $service_conf ] || echo -e "\n$key=$value" >>$COMMON_CONF_DIR/common.runtime.properties

    echo "Setting $key=$value in $service_conf"
}

setJavaKey() {
    service="$1"
    key=$2
    value=$3
    file=$(getConfPath $service)/jvm.config
    sed -ri "/$key/d" $file
    echo $value >> $file
}

## Setup host names
if [ -n "${ZOOKEEPER}" ];
then
    setKey _common robux.zk.service.host "${ZOOKEEPER}"
fi

if [ -z "${KUBERNETES_SERVICE_HOST}" ]
then
  # Running outside kubernetes, use IP addresses
  ROBUX_SET_HOST_IP=${ROBUX_SET_HOST_IP:-1}
else
  # Running in kubernetes, so use canonical names
  ROBUX_SET_HOST_IP=${ROBUX_SET_HOST_IP:-0}
fi

if [ "${ROBUX_SET_HOST_IP}" = "1" ]
then
    setKey $SERVICE robux.host $(ip r get 1 | awk '{print $7;exit}')
fi

env | grep ^robux_ | while read evar;
do
    # Can't use IFS='=' to parse since var might have = in it (e.g. password)
    val=$(echo "$evar" | sed -e 's?[^=]*=??')
    var=$(echo "$evar" | sed -e 's?^\([^=]*\)=.*?\1?g' -e 's?__?%UNDERSCORE%?g' -e 's?_?.?g' -e 's?%UNDERSCORE%?_?g')
    setKey $SERVICE "$var" "$val"
done

env |grep ^s3service | while read evar
do
    val=$(echo "$evar" | sed -e 's?[^=]*=??')
    var=$(echo "$evar" | sed -e 's?^\([^=]*\)=.*?\1?g' -e 's?_?.?' -e 's?_?-?g')
    echo "$var=$val" >>$COMMON_CONF_DIR/jets3t.properties
done

# This is to allow configuration via a Kubernetes configMap without
# e.g. using subPath (you can also mount the configMap on /tmp/conf/robux)
if [ -n "$ROBUX_CONFIG_COMMON" ]
then
    cp -f "$ROBUX_CONFIG_COMMON" $COMMON_CONF_DIR/common.runtime.properties
fi

SCONFIG=$(printf "%s_%s" ROBUX_CONFIG ${SERVICE})
SCONFIG=$(eval echo \$$(echo $SCONFIG))

if [ -n "${SCONFIG}" ]
then
    cp -f "${SCONFIG}" $SERVICE_CONF_DIR/runtime.properties
fi

if [ -n "$ROBUX_LOG_LEVEL" ]
then
    sed -ri 's/"info"/"'$ROBUX_LOG_LEVEL'"/g' $COMMON_CONF_DIR/log4j2.xml
fi

if [ -n "$ROBUX_LOG4J" ]
then
    echo "$ROBUX_LOG4J" >$COMMON_CONF_DIR/log4j2.xml
fi

ROBUX_DIRS_TO_CREATE=${ROBUX_DIRS_TO_CREATE-'var/tmp var/robux/segments var/robux/indexing-logs var/robux/task var/robux/hadoop-tmp var/robux/segment-cache'}
if [ -n "${ROBUX_DIRS_TO_CREATE}" ]
then
    mkdir -p ${ROBUX_DIRS_TO_CREATE}
fi

# take the ${TASK_JSON} environment variable and base64 decode, unzip and throw it in ${TASK_DIR}/task.json.
# If TASK_JSON is not set, CliPeon will pull the task.json file from deep storage.
mkdir -p ${TASK_DIR}; [ -n "$TASK_JSON" ] && echo ${TASK_JSON} | base64 -d | gzip -d > ${TASK_DIR}/task.json;

if [ -n "$TASK_ID" ]; then
    exec bin/run-java ${JAVA_OPTS} -cp $COMMON_CONF_DIR:$SERVICE_CONF_DIR:lib/*: org.apache.robux.cli.Main internal peon --taskId "${TASK_ID}" "$@"
else
    exec bin/run-java ${JAVA_OPTS} -cp $COMMON_CONF_DIR:$SERVICE_CONF_DIR:lib/*: org.apache.robux.cli.Main internal peon "$@"
fi
