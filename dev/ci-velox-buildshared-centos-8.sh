#!/bin/bash

set -e

source /opt/rh/gcc-toolset-11/enable
./dev/builddeps-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF --build_tests=ON \
    --build_examples=ON --build_benchmarks=ON
