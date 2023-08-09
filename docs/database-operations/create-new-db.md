# Creating a new PyCDS database

To create a new PyCDS database at the latest revision:

1. On the server or in the container of your choice:
   1. Create a new database with the desired database name, `<db-name>`
   1. Install extensions PL/Postgres, PL/Python, and PostGIS
   1. Create an empty schema with the desired name, `<schema-name>`
   1. Grant the desired user (name `<user-name>`) write permission to the schema.
      (You may need to create the desired user first.)
   1. For an example of such an arrangement, run the shell script
      `alembic/development/init_test_db/common/create_scripts.sh` and examine
      the SQL scripts it writes to `alembic/development/init_test_db/common`.

1. Add a new DSN for the database, including the appropriate username,
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

If you wish to create the database at a different revision, replace `head` above  with the desired revision id.
