import logging
import os
import pytest

from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateSchema
from sqlalchemydiff.util import get_temporary_uri


@pytest.fixture
def alembic_root():
    return os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "pycds", "alembic")
    )


@pytest.fixture
def uri_left(base_database_uri):
    yield get_temporary_uri(base_database_uri)


@pytest.fixture
def uri_right(base_database_uri):
    yield get_temporary_uri(base_database_uri)


@pytest.fixture
def db_setup(schema_name):
    """
    Database setup operations. These are operations that must executed prior
    to the creation of any (other) content in the database in order for the
    tests to work.

    The function returned by this fixture is passed to
    `alembicverify_util.prepare_schema_from_migrations`, which invokes it.
    """

    def f(engine):
        test_user = "testuser"

        # print(f"### initial user {engine.execute('SELECT current_user').scalar()}")

        engine.execute(text("CREATE EXTENSION postgis"))
        engine.execute(text("CREATE EXTENSION plpython3u"))
        engine.execute(text("CREATE EXTENSION IF NOT EXISTS citext"))
        engine.execute(text("CREATE EXTENSION IF NOT EXISTS hstore"))

        engine.execute(CreateSchema(schema_name))
        # schemas = engine.execute("select schema_name from information_schema.schemata").fetchall()
        # print(f"### schemas: {[x[0] for x in schemas]}")

        engine.execute(
            text(f"GRANT ALL PRIVILEGES ON SCHEMA {schema_name} TO {test_user};")
        )

        privs = [
            f"GRANT ALL PRIVILEGES ON ALL {objects} IN SCHEMA {schema_name} TO {test_user};"
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema_name} GRANT ALL PRIVILEGES ON TABLES TO {test_user};"
            for objects in ("TABLES", "SEQUENCES", "FUNCTIONS")
        ]
        engine.execute(text("".join(privs)))

        # One of the following *should* set the current user to `test_user`.
        # But it's hard to tell if it does, because `SELECT current_user`
        # *always* returns `postgres`, except when it is executed in the same
        # `engine.execute` operation as the `SET ROLE/AUTH` statement.
        # Subsequent `SELECT current_user` queries then return `postgres` again,
        # so it's very hard to tell what is actually happening.

        # engine.execute(f"SET ROLE '{test_user}';")
        auth = f"SET SESSION AUTHORIZATION '{test_user}';"
        engine.execute(text(auth))

        # result = engine.execute(f"SELECT current_user").scalar()
        #   --> "postgres"
        # result = engine.execute(f"SET SESSION AUTHORIZATION '{test_user}'; SELECT current_user").scalar()
        #   --> "testuser"
        # print(f'### final user {result}')

    return f

@pytest.fixture
def prepared_schema_from_migrations_left(
    alembic_engine, alembic_runner, target_revision=None
):
    """
    Generic prepared_schema_from_migrations_left fixture. It prepares the
    schema to a revision specified by the value of the fixture
    `target_revision` (see above). If the target revision for a specific
     test needs to be different, it can be overridden by specifying it as
     an indirect parameter, as follows:

        @pytest.mark.parametrize(
            "prepared_schema_from_migrations_left",
            (<revision sha>,),
            indirect=True,
        )

    """
    # We don't really need to spam the logs with all the migration details, especially
    # as this runs for each test that this fixture is used in
    logging.getLogger("alembic").setLevel(logging.CRITICAL)
    migration_target = "head" if target_revision is None else target_revision
    alembic_runner.migrate_up_to(migration_target)

    yield alembic_engine

@pytest.fixture(scope="session")
def prepared_schema_from_migrations_left_s(
    alembic_engine, alembic_runner, target_revision=None
):
    """
    Generic prepared_schema_from_migrations_left fixture. It prepares the
    schema to a revision specified by the value of the fixture
    `target_revision` (see above). If the target revision for a specific
     test needs to be different, it can be overridden by specifying it as
     an indirect parameter, as follows:

        @pytest.mark.parametrize(
            "prepared_schema_from_migrations_left",
            (<revision sha>,),
            indirect=True,
        )

    """
    # We don't really need to spam the logs with all the migration details, especially
    # as this runs for each test that this fixture is used in
    logging.getLogger("alembic").setLevel(logging.CRITICAL)
    migration_target = "head" if target_revision is None else target_revision
    alembic_runner.migrate_up_to(migration_target)

    yield alembic_engine


@pytest.fixture
def sesh_in_prepared_schema_left(prepared_schema_from_migrations_left):
    engine = prepared_schema_from_migrations_left
    sesh = Session(engine)

    yield sesh

    sesh.close()

@pytest.fixture(scope="session")
def sesh_in_prepared_schema_left_s(prepared_schema_from_migrations_left):
    engine = prepared_schema_from_migrations_left
    sesh = Session(engine)

    yield sesh

    sesh.close()