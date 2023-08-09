# Creating the initial migration

Since this only needs be done once and is preserved in the migration `alembic/versions/522eed334c85_create_initial_database.py`, this information is largely for archival purposes.

Our modified Alembic environment generally respects the schema name (see above), but fails in one case: when the specified schema does not (yet) contain an `alembic_version` table. To remedy this, issue the command

```shell script
[PYCDS_SCHEMA_NAME=<schema name>] alembic -x db=<db-label> stamp head
```

which creates the required table.

Following this, issue the command

```shell script
[PYCDS_SCHEMA_NAME=<schema name>] alembic -x db=<db-label> revision --autogenerate -m "create initial database"
```

Notes:
- `PYCDS_SU_ROLE_NAME` is not required for either operation.
