# Alembic database migrations

## Alembic configuration

Modified generic single-database configuration:
- User names one or more databases in `alembic.ini`, specifies one with `alembic` CLI parameter
  `-x db=<db-label>`

## Respecting schema name in autogenerated migrations (IMPORTANT)

PyCDS is agnostic to schema name. The schema name is specified by the environment variable
`PYCDS_SCHEMA_NAME`, and defaults to `'crmp'` when the environment variable is not set.

An autogenerated migration script (`alembic -x db=<db> revision --autogenerate -m "message""`)
sets the schema name to the schema in the database on which the autogenerate was based.
This can appear in the autogenerated code in several ways, most commonly:

- as the keyword parameter `schema=<schema>` in operations such as `op.add_column()`
- in the names of tables named in strings (e.g., in `ForeignKeyConstraint` expressions)
 
You must replace the fixed schema name with the schema name returned by `pycds.get_schema_name()`. 
See migration 'create initial database' for example code. 
