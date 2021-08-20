# Test migrations with a test database

We have defined a Docker image `pcic/pycds-test-db` 
that establishes a test PyCDS database.
It is mainly used for testing the Alembic customization 
(`alembic/env.py`) and database migrations.

The image defines a Docker image with PostgreSQL 9.3,
PostGIS 2.4, and PL/Python installed. 
This configuration matches our test environment,
and approximately matches our production environment, which currently runs 
PostgreSQL 9.1 and some compatible version of PostGIS.
(It seems that it is not possible to replicate the production environment 
environment exactly in test environments.)

## Build image

```
$ make test-db-image
```

## Run image (container)

Run it from the project root directory:

```shell script
./docker/test-db/run-db.sh <port> <password>
```

This script starts a container mapped to port `<port>` on `localhost`.

The container creates:
- a PostgreSQL 9.3 server
- with users:
  - user `postgres`, password `<password>`,
  - user `tester`, password `tester`,
- with database `pycds_test`, owned by `tester`,
- with extensions PL/Postgres, PL/Python, and PostGIS 2.4 installed,
- with empty schemas `crmp` and `other`, both owned by `tester`.

## Use running test db (container)

To connect to the test database container on the command line:

```shell script
psql -h localhost -p <port> -U {postgres,tester} -d {crmp,other}
```

The DSN for this database is:

```
postgresql://tester@localhost:<port>/pycds_test
```

## Stop and remove test database container

To stop the local Docker test database container:

```shell script
./docker/test-db/down-db.sh
```

It's a pretty trivial convenience, but it's a _convenient_ convenience.

## Note: Unit test data from production

Some data used in the unit tests was sourced from a production database. 
The steps to produce this were:

1. As database superuser, run
   `CREATE SCHEMA subset AUTHORIZATION <username>;`
2. As that user, run `psql -h <db_host> -f create_crmp_subset.sql crmp`.
   This insert a selection of data into the `subset` schema.
3. Then, `pg_dump -h <db_host> -d crmp --schema=subset --data-only --no-owner --no-privileges --no-tablespaces --column-inserts -f pycds/data/crmp_subset_data.sql`
4. Edit this file to remove the `SET search_path...` line,
5. Re-order the data inserts to respect foreign key constraints.
    Default sort is alphabetical and the only changes that should need to be 
    made are ordering `meta_network`,
    `meta_station`, and `meta_history` first and leaving the remaining inserts ordered as is.
