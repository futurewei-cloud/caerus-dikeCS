#! /bin/bash

#./stop.sh
docker run -d --rm  \
-p 9000:9000 \
-v "$(pwd)/build/Release/"://usr/local/bin \
-v "$(pwd)/../data":/data \
--network dike-net \
--name minioserver \
ubuntu:20.04 dikeCS

