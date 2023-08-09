# Applying a migration: Downgrade

Migrations can also be undone, by downgrading the database schema to a
migration preceding its current one:

```shell script
[PYCDS_SCHEMA_NAME=<schema name>] [PYCDS_SU_ROLE_NAME=<role name>] alembic -x db=<db-label> downgrade <rev-id>
```

For information on revision identifiers,
see [Partial Revision Identifiers](https://alembic.sqlalchemy.org/en/latest/tutorial.html#partial-revision-identifiers).

