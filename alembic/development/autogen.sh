#!/bin/bash
find ./alembic/versions/ -maxdepth 1 ! -name "522eed334c85_create_initial_database.py" -type f -delete
PYCDS_SCHEMA_NAME=crmp alembic -x db=dev revision --autogenerate -m "test" &>autogenerate.out
