#!/usr/bin/env bash
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

. $(dirname "$0")/docker_compose_args.sh

if [ -z "$ROBUX_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH" ]
then
    echo "\$ROBUX_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH is not set. No override config file provided"
    if [ "$ROBUX_INTEGRATION_TEST_GROUP" = "s3-deep-storage" ] || \
    [ "$ROBUX_INTEGRATION_TEST_GROUP" = "gcs-deep-storage" ] || \
    [ "$ROBUX_INTEGRATION_TEST_GROUP" = "azure-deep-storage" ] || \
    [ "$ROBUX_INTEGRATION_TEST_GROUP" = "hdfs-deep-storage" ] || \
    [ "$ROBUX_INTEGRATION_TEST_GROUP" = "s3-ingestion" ] || \
    [ "$ROBUX_INTEGRATION_TEST_GROUP" = "kinesis-index" ] || \
    [ "$ROBUX_INTEGRATION_TEST_GROUP" = "kinesis-data-format" ]; then
      echo "Test group $ROBUX_INTEGRATION_TEST_GROUP requires override config file. Stopping test..."
      exit 1
    fi
else
    echo "\$ROBUX_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH is set with value ${ROBUX_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH}"
fi

# Start docker containers for all Robux processes and dependencies
{
  # Start Hadoop docker if needed
  if [ -n "$ROBUX_INTEGRATION_TEST_START_HADOOP_DOCKER" ] && [ "$ROBUX_INTEGRATION_TEST_START_HADOOP_DOCKER" == true ]
  then
    # Start Hadoop docker container
    docker compose -f ${DOCKERDIR}/docker-compose.robux-hadoop.yml up -d
  fi

  if [ -z "$ROBUX_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH" ]
  then
    # Start Robux cluster
    echo "Starting cluster with empty config"
    OVERRIDE_ENV=environment-configs/empty-config docker compose $(getComposeArgs) up -d
  else
    # run robux cluster with override config
    echo "Starting cluster with a config file at $ROBUX_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH"
    OVERRIDE_ENV=$ROBUX_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH docker compose $(getComposeArgs) up -d
  fi
}
