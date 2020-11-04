#!/bin/bash

docker run -it --rm  \
-p 9000:9000 \
-v "$(pwd)/build/Release/"://usr/local/bin \
-v "$(pwd)/../minio/data":/data \
--network dike-net \
--name dikeCS \
ubuntu:20.04 dikeCS
#ubuntu:20.04 "$@"
