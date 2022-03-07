#!/usr/bin/env bash

set -eu
set -x

BUILD_CPP=${1:-ON}
BUILD_TESTS=${2:-OFF}
BUILD_ARROW=${3:-ON}
STATIC_ARROW=${4:-OFF}
BUILD_PROTOBUF=${5:-ON}
ARROW_ROOT=${6:-/usr/local}
ARROW_BFS_INSTALL_DIR=${7}
BUILD_JEMALLOC=${8:-ON}
BUILD_GAZELLE_CPP=${9:-OFF}

if [ "$BUILD_CPP" == "ON" ]; then
  NPROC=$(nproc --ignore=2)

  CURRENT_DIR=$(
    cd "$(dirname "$BASH_SOURCE")"
    pwd
  )
  cd "${CURRENT_DIR}"

  if [ -d build ]; then
    rm -r build
  fi
  mkdir build
  cd build
  cmake .. \
    -DBUILD_TESTS=${BUILD_TESTS} \
    -DBUILD_ARROW=${BUILD_ARROW} \
    -DSTATIC_ARROW=${STATIC_ARROW} \
    -DBUILD_PROTOBUF=${BUILD_PROTOBUF} \
    -DARROW_ROOT=${ARROW_ROOT} \
    -DARROW_BFS_INSTALL_DIR=${ARROW_BFS_INSTALL_DIR} \
    -DBUILD_JEMALLOC=${BUILD_JEMALLOC} \
    -DBUILD_GAZELLE_CPP=${BUILD_GAZELLE_CPP}
  make -j$NPROC
fi
