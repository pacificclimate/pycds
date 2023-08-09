# Applying a migration: Upgrade

Once a PyCDS database has been initialized, subsequently created migrations are
simple to apply.

To apply all migrations later than the database's current migration:

```shell script
[PYCDS_SCHEMA_NAME=<schema name>] [PYCDS_SU_ROLE_NAME=<role name>] alembic -x db=<db-label> upgrade head
```

To apply migrations up to a specific migration with revision identifier `<rev-id>`:

```shell script
[PYCDS_SCHEMA_NAME=<schema name>] [PYCDS_SU_ROLE_NAME=<role name>] alembic -x db=<db-label> upgrade <rev-id>
```

For information on revision identifiers, see [Partial Revision Identifiers](https://alembic.sqlalchemy.org/en/latest/tutorial.html#partial-revision-identifiers).

