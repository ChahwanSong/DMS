#!/bin/bash

docker build -t dms:latest -f /Users/mason/DMS/docker/Dockerfile /Users/mason/DMS/docker --no-cache

docker run --rm -dit --name dms --privileged --network host --ipc host -v /Users/mason/DMS:/workspace/DMS dms:latest bash