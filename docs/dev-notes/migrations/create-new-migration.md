# Creating a new migration

After the PyCDS ORM has been modified, you will usually want to create an Alembic migration script to enable existing databases to be upgraded to the new model.

Alembic can generate a migration script for you. It can generate an empty script, or autogenerate a script containing changes inferred by comparison between the ORM and an existing database. The command for this is `alembic revision ...`.

In the context of PCIC's databases (CRMP, Metnorth), autogeneration is less useful than one might hope. PCIC's databases have not been managed solely through PyCDS, and so diverge from the ORM definitions. Much of this divergence has been cleaned up, but there are still differences in some table definitions, indexes and constraints. Furthermore, the autogenerate function is only aware of tables and table-related objects. It does not handle functions, views, and matviews, and these are critical to our databases' operation. 

In this situation, autogenerate generates draft migrations containing a great deal of spurious and undesirable content, and also omits all content related to non-table objects. Fixing this actually adds unnecessary work.

Therefore, developers usually create a new migration _without_ using the `--autogenerate` flag. The result is a skeleton migration script with empty `upgrade` and `downgrade` functions. The benefits are that it generates a unique revision identifier, creates an appropriately named file in the correct directory, and inserts the migration into the migration sequence automatically. See [Create a Migration Script](https://alembic.sqlalchemy.org/en/latest/tutorial.html#create-a-migration-script) for an example.

## Generating a new migration script (without autogeneration)

Instructions:

1. Generate the migration script:

    ```shell script
    alembic revision -m "<message>"
    ```

   Notes:
   - `PYCDS_SU_ROLE_NAME` is not required for this operation.
   - The `<message>` should be a succinct description of what the migration accomplishes; for example "Add name column to Users". This message becomes part of the name and content of the script.

2. Alembic writes a new script to the directory `pycds/alembic/versions`. Its name includes a unique revision identifier (a SHA) and a version of `<message>`.

3. Add code to create, modify, and/or drop commands for all items to be managed in this migration. (These commands are on the object `alembic.op`, which is imported in the skeleton script.)

   1. **Ensure that all changes respect the specified schema name** that the user will supply when this migration is applied. In particular:
      1. It's useful to obtain the specified schema name with `schema=pycds.get_schema_name()`.
      2. Ensure the specified schema name is used for all objects, including functions, views and materialized views. This should come without special effort due to the structure of how these items are declared, but it is worth verifying.

   2. Do this for both upgrade and downgrade functions in the script!

4. Write some tests for the migration. Examples can be found in the existing code.

5. Commit the new migration script and its tests to the repo.

For more information, see [Create a Migration Script](https://alembic.sqlalchemy.org/en/latest/tutorial.html#create-a-migration-script).

Modify the script to perform the necessary actions for upgrading and downgrading a database to/from this revision. For useful examples, see other scripts in directory `pycds/alembic/versions`. 

## Autogenerating a new migration script

As noted above, **_autogeneration is not usually helpful_**. However, here are some instructions if you wish to do so. For more details, see the [Alembic documentation](https://alembic.sqlalchemy.org/en/latest/autogenerate.html).

To autogenerate a migration script, you must have a reference database schema for Alembic to compare to the modified PyCDS ORM. After Alembic autogenerates the script, you must edit it to ensure completeness, correctness, and that it respects the specified schema name (see instructions below).

Instructions:

1. Choose or create a reference database schema that is at the latest migration. (Or a database schema at an otherwise desired "base" migration, which is unusual but not necessarily wrong.)
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
   - `PYCDS_SU_ROLE_NAME` is not required for this operation.
   - The `<message>` should be a succinct description of what the migration accomplishes; for example "Add name column to Users". This message becomes part of the name and content of the script.

1. Alembic writes a new script to the directory `alembic/versions`. Its name includes a unique revision identifier (a SHA) and a version of `<message>`.

1. Review and edit the script to ensure correctness, completeness, and that it respects the specified schema name. Specifically:

   1. There are (still) significant differences between the CRMP schema and this ORM. Expect to remove or alter many of the autogenerated operations.

   2. Review to ensure that it picks up all changes to tables and implements them appropriately.
      In particular, Alembic does not pick up on changes of table or column name, which must be manually converted from "drop old name, add new name" to "rename". Some other schema changes are also not detected.

   3. For more information, see [What does Autogenerate Detect (and what does it not detect?)](https://alembic.sqlalchemy.org/en/latest/autogenerate.html#what-does-autogenerate-detect-and-what-does-it-not-detect).

   4. Manually add creation or dropping of the following things not handled by Alembic autogenerate:
      1. Functions
      1. Views
      1. Materialized views

   5. **Ensure that all changes respect the specified schema name** that the user will supply when this migration is applied. In particular:
      1. Replace any autogenerated schema name usage (e.g., `schema='crmp'`) with the specified schema name (`schema=pycds.get_schema_name()`).
      1. Ensure the specified schema name is used for functions, views and materialized views. This should come without special effort due to the structure of how these items are declared, but it is worth verifying.

   6. Do this for both upgrade and downgrade functions in the script!

1. Write some tests for the migration. Examples can be found in the existing code.

1. Commit the new migration script and its tests to the repo.

