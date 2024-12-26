#!/bin/bash
####################################################################################################
#  The main function of this script is to allow developers to build the environment with one click #
#  Recommended commands for first-time installation:                                               #
#  ./dev/buildbundle-veloxbe.sh                                                            #
####################################################################################################
set -exu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."
BUILD_TYPE=Release
BUILD_TESTS=OFF
BUILD_EXAMPLES=OFF
BUILD_BENCHMARKS=OFF
ENABLE_JEMALLOC_STATS=OFF
BUILD_VELOX_TESTS=OFF
BUILD_VELOX_BENCHMARKS=OFF
ENABLE_QAT=OFF
ENABLE_IAA=OFF
ENABLE_HBM=OFF
ENABLE_GCS=OFF
ENABLE_S3=OFF
ENABLE_HDFS=OFF
ENABLE_ABFS=OFF
ENABLE_VCPKG=OFF
RUN_SETUP_SCRIPT=ON
VELOX_REPO=""
VELOX_BRANCH=""
VELOX_HOME=""
VELOX_PARAMETER=""
BUILD_ARROW=ON
SPARK_VERSION=ALL

# set default number of threads as cpu cores minus 2
if [[ "$(uname)" == "Darwin" ]]; then
    physical_cpu_cores=$(sysctl -n hw.physicalcpu)
    ignore_cores=2
    if [ "$physical_cpu_cores" -gt "$ignore_cores" ]; then
        NUM_THREADS=${NUM_THREADS:-$(($physical_cpu_cores - $ignore_cores))}
    else
        NUM_THREADS=${NUM_THREADS:-$physical_cpu_cores}
    fi
else
    NUM_THREADS=${NUM_THREADS:-$(nproc --ignore=2)}
fi

for arg in "$@"
do
    case $arg in
        --build_type=*)
        BUILD_TYPE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_tests=*)
        BUILD_TESTS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_examples=*)
        BUILD_EXAMPLES=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_benchmarks=*)
        BUILD_BENCHMARKS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_jemalloc_stats=*)
        ENABLE_JEMALLOC_STATS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_qat=*)
        ENABLE_QAT=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_iaa=*)
        ENABLE_IAA=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_hbm=*)
        ENABLE_HBM=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_gcs=*)
        ENABLE_GCS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_s3=*)
        ENABLE_S3=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_hdfs=*)
        ENABLE_HDFS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_abfs=*)
        ENABLE_ABFS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_vcpkg=*)
        ENABLE_VCPKG=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --run_setup_script=*)
        RUN_SETUP_SCRIPT=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --velox_repo=*)
        VELOX_REPO=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --velox_branch=*)
        VELOX_BRANCH=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --velox_home=*)
        VELOX_HOME=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_velox_tests=*)
        BUILD_VELOX_TESTS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_velox_benchmarks=*)
        BUILD_VELOX_BENCHMARKS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_arrow=*)
        BUILD_ARROW=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --num_threads=*)
        NUM_THREADS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --spark_version=*)
        SPARK_VERSION=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
	      *)
        OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

function concat_velox_param {
    # check velox repo
    if [[ -n $VELOX_REPO ]]; then
        VELOX_PARAMETER+="--velox_repo=$VELOX_REPO "
    fi

    # check velox branch
    if [[ -n $VELOX_BRANCH ]]; then
        VELOX_PARAMETER+="--velox_branch=$VELOX_BRANCH "
    fi

    # check velox home
    if [[ -n $VELOX_HOME ]]; then
        VELOX_PARAMETER+="--velox_home=$VELOX_HOME "
    fi
}

if [ "$ENABLE_VCPKG" = "ON" ]; then
    # vcpkg will install static depends and init build environment
    BUILD_OPTIONS="--build_tests=$BUILD_TESTS --enable_s3=$ENABLE_S3 --enable_gcs=$ENABLE_GCS \
                   --enable_hdfs=$ENABLE_HDFS --enable_abfs=$ENABLE_ABFS"
    source ./dev/vcpkg/env.sh ${BUILD_OPTIONS}
fi

if [ "$SPARK_VERSION" = "3.2" ] || [ "$SPARK_VERSION" = "3.3" ] \
  || [ "$SPARK_VERSION" = "3.4" ] || [ "$SPARK_VERSION" = "3.5" ] \
  || [ "$SPARK_VERSION" = "ALL" ]; then
  echo "Building for Spark $SPARK_VERSION"
else
  echo "Invalid Spark version: $SPARK_VERSION"
  exit 1
fi

concat_velox_param

function build_arrow {
  cd $GLUTEN_DIR/dev
  ./build_arrow.sh
}

function build_velox {
  echo "Start to build Velox"
  cd $GLUTEN_DIR/ep/build-velox/src
  # When BUILD_TESTS is on for gluten cpp, we need turn on VELOX_BUILD_TEST_UTILS via build_test_utils.
  ./build_velox.sh --enable_s3=$ENABLE_S3 --enable_gcs=$ENABLE_GCS --build_type=$BUILD_TYPE --enable_hdfs=$ENABLE_HDFS \
                   --enable_abfs=$ENABLE_ABFS --build_test_utils=$BUILD_TESTS \
                   --build_tests=$BUILD_VELOX_TESTS --build_benchmarks=$BUILD_VELOX_BENCHMARKS --num_threads=$NUM_THREADS \
                   --velox_home=$VELOX_HOME
}

function build_gluten_cpp {
  echo "Start to build Gluten CPP"
  cd $GLUTEN_DIR/cpp
  rm -rf build
  mkdir build
  cd build
  cmake -DBUILD_VELOX_BACKEND=ON -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
        -DVELOX_HOME=${VELOX_HOME} \
        -DBUILD_TESTS=$BUILD_TESTS -DBUILD_EXAMPLES=$BUILD_EXAMPLES -DBUILD_BENCHMARKS=$BUILD_BENCHMARKS -DENABLE_JEMALLOC_STATS=$ENABLE_JEMALLOC_STATS \
        -DENABLE_HBM=$ENABLE_HBM -DENABLE_QAT=$ENABLE_QAT -DENABLE_IAA=$ENABLE_IAA -DENABLE_GCS=$ENABLE_GCS \
        -DENABLE_S3=$ENABLE_S3 -DENABLE_HDFS=$ENABLE_HDFS -DENABLE_ABFS=$ENABLE_ABFS ..
  make -j $NUM_THREADS
}

function build_velox_backend {
  if [ $BUILD_ARROW == "ON" ]; then
    build_arrow
  fi
  build_velox
  build_gluten_cpp
}

(
  cd $GLUTEN_DIR/ep/build-velox/src
  ./get_velox.sh $VELOX_PARAMETER
)

if [ "$VELOX_HOME" == "" ]; then
  VELOX_HOME="$GLUTEN_DIR/ep/build-velox/build/velox_ep"
fi

OS=`uname -s`
ARCH=`uname -m`
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$CURRENT_DIR/../ep/_ep}
mkdir -p ${DEPENDENCY_DIR}

source $GLUTEN_DIR/dev/build_helper_functions.sh
if [ -z "${GLUTEN_VCPKG_ENABLED:-}" ] && [ $RUN_SETUP_SCRIPT == "ON" ]; then
  echo "Start to install dependencies"
  pushd $VELOX_HOME
  if [ $OS == 'Linux' ]; then
    setup_linux
  elif [ $OS == 'Darwin' ]; then
    setup_macos
  else
    echo "Unsupported kernel: $OS"
    exit 1
  fi
  if [ $ENABLE_S3 == "ON" ]; then
    if [ $OS == 'Darwin' ]; then
      echo "S3 is not supported on MacOS."
      exit 1
    fi
    ${VELOX_HOME}/scripts/setup-adapters.sh aws
  fi
  if [ $ENABLE_HDFS == "ON" ]; then
    if [ $OS == 'Darwin' ]; then
      echo "HDFS is not supported on MacOS."
      exit 1
    fi
    pushd $VELOX_HOME
    install_libhdfs3
    popd
  fi
  if [ $ENABLE_GCS == "ON" ]; then
    ${VELOX_HOME}/scripts/setup-adapters.sh gcs
  fi
  if [ $ENABLE_ABFS == "ON" ]; then
    export AZURE_SDK_DISABLE_AUTO_VCPKG=ON
    ${VELOX_HOME}/scripts/setup-adapters.sh abfs
  fi
  popd
fi

commands_to_run=${OTHER_ARGUMENTS:-}
(
  if [[ "x$commands_to_run" == "x" ]]; then
    build_velox_backend
  else
    echo "Commands to run: $commands_to_run"
    for cmd in "$commands_to_run"; do
       "${cmd}"
    done
  fi
)
