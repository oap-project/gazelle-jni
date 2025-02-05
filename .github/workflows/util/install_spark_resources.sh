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

# Download Spark resources, required by some Spark UTs. The resource path should be set
# for spark.test.home in mvn test.

set -e

INSTALL_DIR=/opt/
case "$1" in
3.2)
    # Spark-3.2
    cd ${INSTALL_DIR} && \
    wget -nv https://archive.apache.org/dist/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz && \
    tar --strip-components=1 -xf spark-3.2.2-bin-hadoop3.2.tgz spark-3.2.2-bin-hadoop3.2/jars/ && \
    rm -rf spark-3.2.2-bin-hadoop3.2.tgz && \
    mkdir -p ${INSTALL_DIR}/shims/spark32/spark_home/assembly/target/scala-2.12 && \
    mv jars ${INSTALL_DIR}/shims/spark32/spark_home/assembly/target/scala-2.12 && \
    wget -nv https://github.com/apache/spark/archive/refs/tags/v3.2.2.tar.gz && \
    tar --strip-components=1 -xf v3.2.2.tar.gz spark-3.2.2/sql/core/src/test/resources/  && \
    mkdir -p shims/spark32/spark_home/ && \
    mv sql shims/spark32/spark_home/
    ;;
3.3)
    # Spark-3.3
    cd ${INSTALL_DIR} && \
    wget -nv https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz && \
    tar --strip-components=1 -xf spark-3.3.1-bin-hadoop3.tgz spark-3.3.1-bin-hadoop3/jars/ && \
    rm -rf spark-3.3.1-bin-hadoop3.tgz && \
    mkdir -p ${INSTALL_DIR}/shims/spark33/spark_home/assembly/target/scala-2.12 && \
    mv jars ${INSTALL_DIR}/shims/spark33/spark_home/assembly/target/scala-2.12 && \
    wget -nv https://github.com/apache/spark/archive/refs/tags/v3.3.1.tar.gz && \
    tar --strip-components=1 -xf v3.3.1.tar.gz spark-3.3.1/sql/core/src/test/resources/  && \
    mkdir -p shims/spark33/spark_home/ && \
    mv sql shims/spark33/spark_home/
    ;;
3.4)
    # Spark-3.4
    cd ${INSTALL_DIR} && \
    wget -nv https://archive.apache.org/dist/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz && \
    tar --strip-components=1 -xf spark-3.4.4-bin-hadoop3.tgz spark-3.4.4-bin-hadoop3/jars/ && \
    rm -rf spark-3.4.4-bin-hadoop3.tgz && \
    mkdir -p ${INSTALL_DIR}/shims/spark34/spark_home/assembly/target/scala-2.12 && \
    mv jars ${INSTALL_DIR}/shims/spark34/spark_home/assembly/target/scala-2.12 && \
    wget -nv https://github.com/apache/spark/archive/refs/tags/v3.4.4.tar.gz && \
    tar --strip-components=1 -xf v3.4.4.tar.gz spark-3.4.4/sql/core/src/test/resources/  && \
    mkdir -p shims/spark34/spark_home/ && \
    mv sql shims/spark34/spark_home/
    ;;
3.5)
    # Spark-3.5
    cd ${INSTALL_DIR} && \
    wget -nv https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
    tar --strip-components=1 -xf spark-3.5.4-bin-hadoop3.tgz spark-3.5.4-bin-hadoop3/jars/ && \
    rm -rf spark-3.5.4-bin-hadoop3.tgz && \
    mkdir -p ${INSTALL_DIR}/shims/spark35/spark_home/assembly/target/scala-2.12 && \
    mv jars ${INSTALL_DIR}/shims/spark35/spark_home/assembly/target/scala-2.12 && \
    wget -nv https://github.com/apache/spark/archive/refs/tags/v3.5.4.tar.gz && \
    tar --strip-components=1 -xf v3.5.4.tar.gz spark-3.5.4/sql/core/src/test/resources/  && \
    mkdir -p shims/spark35/spark_home/ && \
    mv sql shims/spark35/spark_home/
    ;;
3.5-scala2.13)
    # Spark-3.5, scala 2.13
    cd ${INSTALL_DIR} && \
    wget -nv https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
    tar --strip-components=1 -xf spark-3.5.4-bin-hadoop3.tgz spark-3.5.4-bin-hadoop3/jars/ && \
    rm -rf spark-3.5.4-bin-hadoop3.tgz && \
    mkdir -p ${INSTALL_DIR}/shims/spark35-scala2.13/spark_home/assembly/target/scala-2.13 && \
    mv jars ${INSTALL_DIR}/shims/spark35-scala2.13/spark_home/assembly/target/scala-2.13 && \
    wget -nv https://github.com/apache/spark/archive/refs/tags/v3.5.4.tar.gz && \
    tar --strip-components=1 -xf v3.5.4.tar.gz spark-3.5.4/sql/core/src/test/resources/  && \
    mkdir -p shims/spark35-scala2.13/spark_home/ && \
    mv sql shims/spark35-scala2.13/spark_home/
    ;;
*)
    echo "Spark version is expected to be specified."
    exit 1
    ;;
esac
