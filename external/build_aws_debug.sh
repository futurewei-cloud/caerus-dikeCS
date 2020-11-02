#!/bin/bash

export INSTALL_PATH=$(pwd)/build-aws-debug

cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=${INSTALL_PATH} -DCMAKE_INSTALL_PREFIX=${INSTALL_PATH} -S aws-c-common -B aws-c-common/build
cmake --build aws-c-common/build --target install
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=${INSTALL_PATH} -DCMAKE_INSTALL_PREFIX=${INSTALL_PATH} -S aws-checksums -B aws-checksums/build
cmake --build aws-checksums/build --target install
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=${INSTALL_PATH} -DCMAKE_INSTALL_PREFIX=${INSTALL_PATH} -S aws-c-event-stream -B aws-c-event-stream/build
cmake --build aws-c-event-stream/build --target install

