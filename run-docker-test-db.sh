#!/bin/bash

# Usage: run-docker-test-db.sh <password> <port>

docker run -d -e POSTGRES_PASSWORD=$2 -p $1:5432 -v $(pwd)/init_test_db:/docker-entrypoint-initdb.d --name pycds-test-db pycds-test-db -c 'listen_addresses=0.0.0.0'