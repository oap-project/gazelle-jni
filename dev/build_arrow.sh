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

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
source ${CURRENT_DIR}/build_helper_functions.sh
VELOX_ARROW_BUILD_VERSION=15.0.0
ARROW_PREFIX=$CURRENT_DIR/arrow_ep
# Always uses BUNDLED in case of that thrift is not installed.
THRIFT_SOURCE="BUNDLED"
BUILD_TYPE=Release

sudo rm -rf arrow_ep/
wget_and_untar https://archive.apache.org/dist/arrow/arrow-${VELOX_ARROW_BUILD_VERSION}/apache-arrow-${VELOX_ARROW_BUILD_VERSION}.tar.gz arrow_ep
cd arrow_ep/
patch -p1 < $CURRENT_DIR/../ep/build-velox/src/modify_arrow.patch
patch -p1 < $CURRENT_DIR/../ep/build-velox/src/modify_arrow_dataset_scan_option.patch

function build_arrow_cpp() {
 if [ -n "$1" ]; then
   BUILD_TYPE=$1
 fi
 pushd $ARROW_PREFIX/cpp

 cmake_install \
       -DARROW_PARQUET=ON \
       -DARROW_FILESYSTEM=ON \
       -DARROW_PROTOBUF_USE_SHARED=OFF \
       -DARROW_WITH_THRIFT=ON \
       -DARROW_WITH_LZ4=ON \
       -DARROW_WITH_SNAPPY=ON \
       -DARROW_WITH_ZLIB=ON \
       -DARROW_WITH_ZSTD=ON \
       -DARROW_JEMALLOC=OFF \
       -DARROW_SIMD_LEVEL=NONE \
       -DARROW_RUNTIME_SIMD_LEVEL=NONE \
       -DARROW_WITH_UTF8PROC=OFF \
       -DARROW_TESTING=ON \
       -DCMAKE_INSTALL_PREFIX=/usr/local \
       -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
       -DARROW_BUILD_STATIC=ON \
       -DThrift_SOURCE=${THRIFT_SOURCE}
 popd
}

function build_arrow_java() {
    ARROW_INSTALL_DIR="${ARROW_PREFIX}/install"

    pushd $ARROW_PREFIX/java
    # Because arrow-bom module need the -DprocessAllModules
    mvn versions:set -DnewVersion=15.0.0-gluten -DprocessAllModules

    mvn clean install -pl bom,maven/module-info-compiler-maven-plugin,vector -am \
          -DskipTests -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -Dassembly.skipAssembly

    # Arrow C Data Interface CPP libraries
    mvn generate-resources -P generate-libs-cdata-all-os -Darrow.c.jni.dist.dir=$ARROW_INSTALL_DIR \
      -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -N

    # Arrow JNI Date Interface CPP libraries
    export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}
    mvn generate-resources -Pgenerate-libs-jni-macos-linux -N -Darrow.dataset.jni.dist.dir=$ARROW_INSTALL_DIR \
      -DARROW_GANDIVA=OFF -DARROW_JAVA_JNI_ENABLE_GANDIVA=OFF -DARROW_ORC=OFF -DARROW_JAVA_JNI_ENABLE_ORC=OFF \
	    -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -N

    # Arrow Java libraries
    mvn install  -Parrow-jni -P arrow-c-data -pl c,dataset -am \
      -Darrow.c.jni.dist.dir=$ARROW_INSTALL_DIR/lib -Darrow.dataset.jni.dist.dir=$ARROW_INSTALL_DIR/lib -Darrow.cpp.build.dir=$ARROW_INSTALL_DIR/lib \
      -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -Dassembly.skipAssembly
    popd
}
