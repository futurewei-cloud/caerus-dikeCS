#!/bin/bash

export INSTALL_PATH=$(pwd)/build-aws

if [ $(cmake -P CMakeCheckVersion.txt) == "OK" ]; then
cmake -D CMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=${INSTALL_PATH} -DCMAKE_INSTALL_PREFIX=${INSTALL_PATH} -S aws-c-common -B aws-c-common/build
cmake --build aws-c-common/build --target install
cmake -D CMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=${INSTALL_PATH} -DCMAKE_INSTALL_PREFIX=${INSTALL_PATH} -S aws-checksums -B aws-checksums/build
cmake --build aws-checksums/build --target install
cmake -D CMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=${INSTALL_PATH} -DCMAKE_INSTALL_PREFIX=${INSTALL_PATH} -S aws-c-event-stream -B aws-c-event-stream/build
cmake --build aws-c-event-stream/build --target install

else
mkdir -p aws-c-common/build
cd aws-c-common/build
cmake -D CMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=${INSTALL_PATH} -DCMAKE_INSTALL_PREFIX=${INSTALL_PATH} ..
make && make install
cd ../../

mkdir -p aws-checksums/build
cd aws-checksums/build
cmake -D CMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=${INSTALL_PATH} -DCMAKE_INSTALL_PREFIX=${INSTALL_PATH} ..
make && make install
cd ../../

mkdir -p aws-c-event-stream/build
cd aws-c-event-stream/build
cmake -D CMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=${INSTALL_PATH} -DCMAKE_INSTALL_PREFIX=${INSTALL_PATH} ..
make && make install
fi

