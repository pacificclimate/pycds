#!/bin/bash

# Usage: run-db.sh <port> <password>

${BASH_SOURCE%/*}/init_test_db/common/create_scripts.sh
docker run -d \
    -e POSTGRES_PASSWORD=$2 \
    -p $1:5432 \
    -v $(pwd)/docker/test-db/init_test_db/actions:/docker-entrypoint-initdb.d \
    --name pycds-test-db \
    pcic/pycds-test-db -c 'listen_addresses=0.0.0.0'