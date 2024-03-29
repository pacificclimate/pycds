# Database maintenance

Database maintenance includes creation of new databases and controlled
migration (schema changes) to existing databases. This document also
covers how to add new migrations to the existing set.

## Table of contents

- [Introduction](#introduction)
- [Specifying the database to operate on](#specifying-the-database-to-operate-on)
- [Environment variables affecting Alembic operations](#environment-variables-affecting-alembic-operations)
- [Specifying the schema within the database](#specifying-the-schema-within-the-database)
- [Creating a new database](#creating-a-new-database)
- [Upgrading an existing PyCDS database (schema)](#upgrading-an-existing-pycds-database-schema)
- [Downgrading an existing PyCDS database (schema)](#downgrading-an-existing-pycds-database-schema)
- [Creating a new migration](#creating-a-new-migration)
- [Creating the initial migration](#creating-the-initial-migration)

## Introduction

Modifications to the PyCDS schema definition are managed using
[Alembic](https://alembic.sqlalchemy.org/), a database migration management 
tool based on SQLAlchemy.

In short, Alembic supports and disciplines two processes of database schema change:

1. Creation of database migration scripts (Python programs) that modify the 
   schema of a database.

2. Application of migrations to specific database instances.

  - In particular, Alembic can be used to *create* a new instance of a ``modelmeta`` database by migrating an
    empty database to the current state. This is described in detail below.

For more information, see the [Alembic tutorial](https://alembic.sqlalchemy.org/en/latest/tutorial.html).

## Specifying the database to operate on

We have customized the Alembic environment manager (`alembic/env.py`) so that it is
possible to operate on any of an arbitrary number of databases defined in `alembic.ini`,
according to an `alembic` command line argument. This argument takes the form

```shell script
alembic -x db=<db-label> ...
```

and it must **always** be included in any `alembic` command.

The database to operate upon is specified by `<db-label>` (e.g., `dev`), and the meaning of that name is itself specified
in `alembic.ini`. For any db-name you use, a corresponding entry in `alembic.ini` must exist, of the following
form:

```ini
[<db-label>]
sqlalchemy.url = <DSN>
```

For example:

```ini
[dev]
sqlalchemy.url = postgresql://tester@localhost:30599/pycds_test
```

The file `alembic.ini` already contains several such db-names.
We expect to expand this list as more CRMP-type databases are added to the PCIC stable.

## Environment variables affecting Alembic operations

The following environment variables affect Alembic operations:

- `PYCDS_SCHEMA_NAME`: Name of the schema within the database to be targeted
   by Alembic operations. 
   - Default if not specified: `crmp`.
- `PYCDS_SU_ROLE_NAME_`: Name of role to use for high-privilege operations
   (e.g., creating functions using untrusted languages such as `plypythonu`).
   - Default if not specified: `pcicdba`. 
   - Such a role is typically only enabled for the usual lower-privileged user 
   for a limited period while revisions requiring the higher privilege are 
   executed, and not otherwise.
   - Most revisions do not require higher privilege, so this is moot for
   migrations that do not include them.

All Alembic commands therefore have the generic form

```shell script
[PYCDS_SCHEMA_NAME=<schema name>] [PYCDS_SU_ROLE_NAME=<role name>] alembic -x db=<db-label> ...
```

But note that default values and infrequent need for high privilege mean
that both options may be omitted.

## Specifying the schema within the database

PyCDS enables the user to specify the schema (i.e., named collection of tables, etc.) to be operated upon
using the environment variable `PYCDS_SCHEMA_NAME`.

This has been accomplished in two ways:

- By creating a modified Alembic environment that uses the specified schema name
  (see `env.py#run_migrations_online()` and `env.py#run_migrations_offline()`;
  specifically, `context.configure(version_table_schema=target_metadata.schema)`).
- By using the specified schema name in all migrations. See note below.

**IMPORTANT:** Autogenerated migrations **must** be edited to replace the specific schema name (e.g., `schema='crmp'`)
with the specified schema name (`schema=pycds.get_schema_name()`) wherever it occurs.

## Creating a new database

1. On the server or in the container of your choice:
   1. Create a new database with the desired database name, `<db-name>`
   1. Install extensions PL/Postgres, PL/Python, and PostGIS 2.4
   1. Create an empty schema with the desired name, `<schema-name`
   1. Grant the desired user (name `<user-name>`) write permission to the schema.
      (You may need to created the desired user first.)
   1. For an example of such an arrangement, run the shell script
      `alembic/development/init_test_db/common/create_scripts.sh` and examine
      the SQL scripts it writes to `alembic/development/init_test_db/common`.

1. Add a new DSN for the database, including the appropriate user name,
   to `alembic.ini`. Choose a name by which to refer to it in the Alembic CLI,
   `<db-label>`.

   ```ini
   [<db-label>]
   sqlalchemy.url = postgresql://<user-name>@<server-name>/<db-name>
   ```

1. Initialize the new schema by upgrading it to `head`:

   ```shell script
   [PYCDS_SCHEMA_NAME=<schema name>] [PYCDS_SU_ROLE_NAME=<role name>] alembic -x db=<db-label> upgrade head
   ```

## Upgrading an existing PyCDS database (schema)

Once a PyCDS database has been initialized, subsequently created migrations are
simple to apply

To apply all migrations subsequent to the databases's current migration:

```shell script
[PYCDS_SCHEMA_NAME=<schema name>] [PYCDS_SU_ROLE_NAME=<role name>] alembic -x db=<db-label> upgrade head
```

To apply migrations up to a specific migration with revision identifier `<rev-id>`:

```shell script
[PYCDS_SCHEMA_NAME=<schema name>] [PYCDS_SU_ROLE_NAME=<role name>] alembic -x db=<db-label> upgrade <rev-id>
```

For information on revision identifiers,
see [Partial Revision Identifiers](https://alembic.sqlalchemy.org/en/latest/tutorial.html#partial-revision-identifiers).

## Downgrading an existing PyCDS database (schema)

Migrations can also be undone, by downgrading the database schema to a
migration preceding its current one:

```shell script
[PYCDS_SCHEMA_NAME=<schema name>] [PYCDS_SU_ROLE_NAME=<role name>] alembic -x db=<db-label> downgrade <rev-id>
```

For information on revision identifiers,
see [Partial Revision Identifiers](https://alembic.sqlalchemy.org/en/latest/tutorial.html#partial-revision-identifiers).

## Creating a new migration

After the PyCDS ORM has been modified, you will almost certainly want to create an Alembic
migration script to enable existing databases to be upgraded to the new model.

The easiest and likely most reliable way to create a migration script is to autogenerate it using Alembic.
For more details, see the [Alembic documentation](https://alembic.sqlalchemy.org/en/latest/autogenerate.html).

To autogenerate a migration script, you must have a reference database schema for Alembic to compare
to the modified PyCDS ORM.
After Alembic autogenerates the script, you must edit it to ensure completeness, correctness, and
that it respects the specified schema name (see instructions below).

Instructions:

1. Choose or create a reference database schema that is at the latest migration.
   (Or a database schema at an otherwise desired "base" migration, which is unusual but not necessarily wrong.)
   To serve as a reference, a schema need not contain any data.

1. If the reference database is not named in `alembic.ini`, create an entry there for it of the form

   ```ini
   [<db-label>]
   sqlalchemy.url = postgresql://<user-name>@<server-name>/<db-name>
   ```

1. Autogenerate the migration script:

    ```shell script
    [PYCDS_SCHEMA_NAME=<schema name>] alembic -x db=<db-label> revision --autogenerate -m "<message>"
    ```
   
    Notes:
    - `PYCDS_SU_ROLE_NAME` is not required.
    - The `<message>` should be a succinct description of what the migration 
    accomplishes; for example "Add name column to Users". This message becomes 
    part of the name and content of the script.

1. Alembic writes a new script to the directory `alembic/versions`. Its name includes
   a unique revision identifier (a SHA) and a version of `<message>`.

1. Review and edit the script to ensure correctness, completeness,
   and that it respects the specified schema name. Specifically:

   1. There are (still) significant differences between the CRMP schema and this ORM. Expect to remove or alter many of the autogenerated operations. 
      
   2. Review to ensure that it picks up all changes to tables and implements them appropriately.
      In particular, Alembic does not pick up on changes of table or column name, which must be manually converted
      from "drop old name, add new name" to "rename". Some other schema changes are also not detected.
      
   3. For more information, see
      [What does Autogenerate Detect (and what does it not detect?)](https://alembic.sqlalchemy.org/en/latest/autogenerate.html#what-does-autogenerate-detect-and-what-does-it-not-detect).

   4. Manually add creation or dropping of the following things not handled by Alembic autogenerate:
      1. Functions
      1. Views
      1. Materialized views

   5. **Ensure that all changes respect the specified schema name** that the user will supply when this
      migration is applied. In particular:
      1. Replace any autogenerated schema name usage (e.g., `schema='crmp'`)
         with the specified schema name (`schema=pycds.get_schema_name()`).
      1. Ensure the specified schema name is used for functions, views and materialized views.
         This should come without special effort due to the structure of how these items are declared,
         but it is worth verifying.

   6. Do this for both upgrade and downgrade functions in the script!

1. Write some tests for the migration. Examples can be found in the existing code.

1. Commit the new migration script and its tests to the repo.

## Creating the initial migration

Since this only needed to be done once and is preserved in the migration
`alembic/versions/522eed334c85_create_initial_database.py`,
this information is largely for archival purposes.

Our modified Alembic environment generally respects the schema name (see above),
but fails in one case: when the specified schema does not (yet) contain 
an `alembic_version` table.
To remedy this, issue the command

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

