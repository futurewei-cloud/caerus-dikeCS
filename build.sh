#!/bin/bash

BUILD_TYPE=Release

if [ $(cmake -P CMakeCheckVersion.txt) == "OK" ]; then
cmake -D CMAKE_BUILD_TYPE=${BUILD_TYPE} -S . -B build/${BUILD_TYPE}
cmake --build ./build/${BUILD_TYPE}
else
mkdir -p build/${BUILD_TYPE}
cd build/${BUILD_TYPE}
cmake -D CMAKE_BUILD_TYPE=${BUILD_TYPE} ../..
cd ../../
fi

