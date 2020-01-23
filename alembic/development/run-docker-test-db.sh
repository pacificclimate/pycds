#!/bin/bash

# Usage: run-docker-test-db.sh <password> <port>

${BASH_SOURCE%/*}/init_test_db/common/create_scripts.sh
docker run -d -e POSTGRES_PASSWORD=$2 -p $1:5432 -v $(pwd)/alembic/development/init_test_db/actions:/docker-entrypoint-initdb.d --name pycds-test-db pycds-test-db -c 'listen_addresses=0.0.0.0'