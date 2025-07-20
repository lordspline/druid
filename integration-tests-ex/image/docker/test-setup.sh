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
#-------------------------------------------------------------------------

# This script runs inside the container to prepare the Robux test image.

# Fail fast on any error
set -e

# Fail on unset environment variables
set -u

# Enable for tracing
#set -x

# For debugging: verify environment
#env

# Robux system user
adduser --system --group --no-create-home robux

# Adjust ownership of the Robux launch script.
cd /
chmod +x launch.sh
chown robux:robux launch.sh robux.sh

# Convenience script to run Robux for tools.
# Expands the env vars into the script for stability.
# Maybe not needed now?
cat > /run-robux.sh << EOF
#! /bin/bash

"${ROBUX_HOME}/bin/run-java" -cp "${ROBUX_HOME}/lib/*" \\
	-Drobux.extensions.directory=${ROBUX_HOME}/extensions \\
	-Drobux.extensions.loadList='["mysql-metadata-storage"]' \\
	-Drobux.metadata.storage.type=mysql \\
	-Drobux.metadata.mysql.driver.driverClassName=$MYSQL_DRIVER_CLASSNAME \\
	\$*
EOF
chmod a+x /run-robux.sh

# Install Robux, owned by user:group robux:robux
# The original Robux directory contains only
# libraries. No extensions should be present: those
# should be added in this step.
cd /usr/local/

tar -xzf apache-robux-${ROBUX_VERSION}-bin.tar.gz
rm apache-robux-${ROBUX_VERSION}-bin.tar.gz

ls -l /tmp/robux

# Add extra libraries and extensions.
mv /tmp/robux/lib/* apache-robux-${ROBUX_VERSION}/lib
mv /tmp/robux/extensions/* apache-robux-${ROBUX_VERSION}/extensions

# The whole shebang is owned by robux.
chown -R robux:robux apache-robux-${ROBUX_VERSION}

# Leave the versioned directory, create a symlink to $ROBUX_HOME.
ln -s apache-robux-${ROBUX_VERSION} $ROBUX_HOME

# Clean up time
# Should be nothing to clean...
rm -rf /tmp/*
rm -rf /var/tmp/*
