#!/bin/bash

BUILD_TYPE=Debug

cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -S . -B build/${BUILD_TYPE}
cmake --build ./build/${BUILD_TYPE}

