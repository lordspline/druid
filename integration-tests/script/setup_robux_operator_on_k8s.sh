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
set -x

export ROBUX_OPERATOR_VERSION=v1.0.0
export KUBECTL="/usr/local/bin/kubectl"

# Prepare For Robux-Operator
rm -rf robux-operator
git clone https://github.com/datainfrahq/robux-operator.git
cd robux-operator
git checkout -b robux-operator-$ROBUX_OPERATOR_VERSION $ROBUX_OPERATOR_VERSION
cd ..

# Deploy Robux Operator and Robux CR spec
$KUBECTL create -f robux-operator/deploy/service_account.yaml
$KUBECTL create -f robux-operator/deploy/role.yaml
$KUBECTL create -f robux-operator/deploy/role_binding.yaml
$KUBECTL create -f robux-operator/deploy/crds/robux.apache.org_robuxs.yaml
$KUBECTL create -f robux-operator/deploy/operator.yaml

echo "Setup Robux Operator on K8S Done!"
