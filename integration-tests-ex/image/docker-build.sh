#! /usr/bin/env bash
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
#--------------------------------------------------------------------

# Invokes Docker to build the image. The environment should have been
# setup from Maven via build-image.sh or manually via quick-build.sh.

# Print environment for debugging
#env

# Enable for tracing
#set -x

SCRIPT_DIR=$(cd $(dirname $0) && pwd)

# Maven should have created the docker dir with the needed
# dependency jars. If doing this by hand, run Maven once to
# populate these jars.
if [ ! -d $TARGET_DIR/docker ]; then
	echo "$TARGET_DIR/docker does not exist. It should contain dependency jars" 1>&2
	exit 1
fi

# Create the run-specific docker directory
mkdir -p $TARGET_DIR/docker
cp -r docker/* $TARGET_DIR/docker
cd $TARGET_DIR/docker

# Grab the distribution if needed (skipped if no change.)
DISTRIB_FILE=apache-robux-$ROBUX_VERSION-bin.tar.gz
SOURCE_FILE=$PARENT_DIR/distribution/target/$DISTRIB_FILE
if [[ ! -f $DISTRIB_FILE || $SOURCE_FILE -nt $DISTRIB_FILE ]]; then
	cp $SOURCE_FILE .
fi

docker build -t $ROBUX_IT_IMAGE_NAME \
	--build-arg ROBUX_VERSION=$ROBUX_VERSION \
	--build-arg MYSQL_VERSION=$MYSQL_VERSION \
	--build-arg MARIADB_VERSION=$MARIADB_VERSION \
	--build-arg CONFLUENT_VERSION=$CONFLUENT_VERSION \
	--build-arg HADOOP_VERSION=$HADOOP_VERSION \
	--build-arg MYSQL_DRIVER_CLASSNAME=$MYSQL_DRIVER_CLASSNAME \
	--build-arg ROBUX_TESTING_TOOLS_VERSION=$ROBUX_VERSION \
	.

if [[ -z "${BACKWARD_COMPATIBILITY_IT_ENABLED:-""}" || $BACKWARD_COMPATIBILITY_IT_ENABLED != "true" ]]; then
  echo "Not building previous version image."
  exit 0
fi

# Download the previous robux tar
curl -L $ROBUX_PREVIOUS_VERSION_DOWNLOAD_URL --output apache-robux-$ROBUX_PREVIOUS_VERSION-bin.tar.gz

docker build -t $ROBUX_PREVIOUS_IT_IMAGE_NAME \
	--build-arg ROBUX_VERSION=$ROBUX_PREVIOUS_VERSION \
	--build-arg MYSQL_VERSION=$MYSQL_VERSION \
	--build-arg MARIADB_VERSION=$MARIADB_VERSION \
	--build-arg CONFLUENT_VERSION=$CONFLUENT_VERSION \
	--build-arg HADOOP_VERSION=$HADOOP_VERSION \
	--build-arg MYSQL_DRIVER_CLASSNAME=$MYSQL_DRIVER_CLASSNAME \
	--build-arg ROBUX_TESTING_TOOLS_VERSION=$ROBUX_VERSION \
	.
