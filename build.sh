#!/bin/bash
set -e

git submodule init
git submodule update --recursive --progress

cd external
./build_aws.sh || (echo "*** build_aws build failed with $?" ; exit 1)
cd ..
./build_server.sh || (echo "*** dikeCS build failed with $?" ; exit 1)

