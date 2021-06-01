#!/bin/bash

TEST_NAME=${1:-test-helloworld.sh}
TEST_ARGS=${@:2}

NETWORK=${DOCKER_TEST_NETWORK:-testing_net}

# Build gateway.
docker build -t "gateway:latest" .

# Launch gateway container into the network and execute the intended test.
docker run\
    --rm\
    --network=$NETWORK\
    --name=gateway\
    --entrypoint="/tmp/tests/$TEST_NAME"\
    "gateway:latest"\
    ${TEST_ARGS[@]}