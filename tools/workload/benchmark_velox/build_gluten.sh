#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

BASEDIR=$(dirname $0)
echo "Script called with: $0"
echo "BASEDIR resolved to: $BASEDIR"

GLUTEN_HOME=$(realpath $BASEDIR/../../..)
echo "Located Gluten in: ${GLUTEN_HOME}"

sudo rm -rf ${GLUTEN_HOME}/ep/build-velox/build/velox_ep/ || true

spark_version=$(head  -n1 $SPARK_HOME/RELEASE | awk '{print $2}')
short_version=${spark_version%.*}

sed -i "s/3.2 3.3 3.4 3.5/$short_version/" $GLUTEN_HOME/dev/buildbundle-veloxbe.sh

# Update local docker image to make more cache hit for vcpkg lib binary.
sudo docker pull apache/gluten:vcpkg-centos-7

sudo docker run --rm \
        -v ${GLUTEN_HOME}:/root/gluten \
        -v ${HOME}/.cache/vcpkg:/root/.cache/vcpkg \
        -v ${HOME}/.m2:/root/.m2 \
        -v ${HOME}/.ccache:/root/.ccache \
        -e http_proxy \
        -e https_proxy \
        --workdir /root/gluten \
        apache/gluten:vcpkg-centos-7 \
        ./dev/package-vcpkg.sh

