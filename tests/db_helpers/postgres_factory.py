import logging
import testing.postgresql
import pycds

from sqlalchemy import text
from sqlalchemy.schema import CreateSchema
from sqlalchemy import create_engine
from pycds.context import get_standard_table_privileges


def init_db(postgresql):
    """
    Because alembic was applied to a pre-existing database, we need to fill in a few gaps that aren't currently covered
    by the migration scripts. This includes creating the schema and roles.

    These are operations that must executed prior to the creation of any (other) content in the database in order for the
    tests to work.
    """
    engine = create_engine(postgresql.url())
    db_setup(engine)


def db_setup(engine):
    schema_name = pycds.get_schema_name()
    test_user = "testuser"
    with engine.begin() as conn:
        for role, _ in get_standard_table_privileges():
            conn.execute(text(f"CREATE ROLE {role};"))
        conn.execute(
            text(f"CREATE ROLE {pycds.get_su_role_name()} WITH SUPERUSER NOINHERIT;")
        )
        conn.execute(text(f"CREATE USER {test_user} WITH SUPERUSER NOINHERIT;;"))

        logging.basicConfig()
        logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)

        # print(f"### initial user {conn.execute('SELECT current_user').scalar()}")

        conn.execute(text("CREATE EXTENSION postgis;"))
        conn.execute(text("CREATE EXTENSION plpython3u;"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS citext;"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS hstore;"))

        # We need this function available, and it does not come pre-installed.
        conn.execute(
            text(
                "CREATE OR REPLACE FUNCTION public.moddatetime() "
                "RETURNS trigger "
                "LANGUAGE 'c' "
                "COST 1 "
                "VOLATILE NOT LEAKPROOF "
                "AS '$libdir/moddatetime', 'moddatetime';"
            )
        )

        conn.execute(CreateSchema(schema_name))
        # schemas = conn.execute("select schema_name from information_schema.schemata").fetchall()
        # print(f"### schemas: {[x[0] for x in schemas]}")

        conn.execute(
            text(f"GRANT ALL PRIVILEGES ON SCHEMA {schema_name} TO {test_user};")
        )

        privs = [
            f"GRANT ALL PRIVILEGES ON ALL {objects} IN SCHEMA {schema_name} TO {test_user};"
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema_name} GRANT ALL PRIVILEGES ON TABLES TO {test_user};"
            for objects in ("TABLES", "SEQUENCES", "FUNCTIONS")
        ]
        conn.execute(text("".join(privs)))

        # One of the following *should* set the current user to `test_user`.
        # But it's hard to tell if it does, because `SELECT current_user`
        # *always* returns `postgres`, except when it is executed in the same
        # `conn.execute` operation as the `SET ROLE/AUTH` statement.
        # Subsequent `SELECT current_user` queries then return `postgres` again,
        # so it's very hard to tell what is actually happening.

        # conn.execute(f"SET ROLE '{test_user}';")
        conn.execute(text(f"SET SESSION AUTHORIZATION '{test_user}';"))

        # result = conn.execute(f"SELECT current_user").scalar()
        #   --> "postgres"
        # result = conn.execute(f"SET SESSION AUTHORIZATION '{test_user}'; SELECT current_user").scalar()
        #   --> "testuser"
        # print(f'### final user {result}')


def short_init_db(postgresql):
    """
    Initialize the database with a minimal setup for tests that do not require pre-initialization.
    """
    engine = create_engine(postgresql.url())
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS plpython3u;"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS citext;"))
        conn.execute(CreateSchema(pycds.get_schema_name()))
        # No roles or privileges are set up in this minimal initialization.


# Two factories, one for our use of the database in strict pycds tests which we pre-init
# The other is an empty database for use in tests that do not require any pre-initialization.
Pycds_postgres = testing.postgresql.PostgresqlFactory(
    cache_initialized_db=True, on_initialized=init_db
)
Base_postgres = testing.postgresql.PostgresqlFactory(
    cache_initialized_db=True, on_initialized=short_init_db
)
