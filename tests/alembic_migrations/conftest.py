# Fixtures required by
# [`alembic-verify`](https://alembic-verify.readthedocs.io/en/latest/)

import os
import pytest

from sqlalchemy.schema import CreateSchema
from sqlalchemydiff.util import get_temporary_uri

from ..alembicverify_util import prepare_schema_from_migrations


# TODO: Repeated. Hoist.
@pytest.fixture
def alembic_root():
    return os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "pycds", "alembic")
    )


@pytest.fixture(scope="module")
def uri_left(base_database_uri):
    yield get_temporary_uri(base_database_uri)


@pytest.fixture(scope="module")
def uri_right(base_database_uri):
    yield get_temporary_uri(base_database_uri)


# Fixtures specific to our tests


@pytest.fixture(scope="module")
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

        engine.execute("CREATE EXTENSION postgis")
        engine.execute("CREATE EXTENSION plpythonu")
        engine.execute("CREATE EXTENSION IF NOT EXISTS citext")

        engine.execute(CreateSchema(schema_name))
        # schemas = engine.execute("select schema_name from information_schema.schemata").fetchall()
        # print(f"### schemas: {[x[0] for x in schemas]}")

        engine.execute(f"GRANT ALL PRIVILEGES ON SCHEMA {schema_name} TO {test_user};")

        privs = [
            f"GRANT ALL PRIVILEGES ON ALL {objects} IN SCHEMA {schema_name} TO {test_user};"
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema_name} GRANT ALL PRIVILEGES ON TABLES TO {test_user};"
            for objects in ("TABLES", "SEQUENCES", "FUNCTIONS")
        ]
        engine.execute("".join(privs))

        # One of the following *should* set the current user to `test_user`.
        # But it's hard to tell if it does, because `SELECT current_user`
        # *always* returns `postgres`, except when it is executed in the same
        # `engine.execute` operation as the `SET ROLE/AUTH` statement.
        # Subsequent `SELECT current_user` queries then return `postgres` again,
        # so it's very hard to tell what is actually happening.

        # engine.execute(f"SET ROLE '{test_user}';")
        engine.execute(f"SET SESSION AUTHORIZATION '{test_user}';")

        # result = engine.execute(f"SELECT current_user").scalar()
        #   --> "postgres"
        # result = engine.execute(f"SET SESSION AUTHORIZATION '{test_user}'; SELECT current_user").scalar()
        #   --> "testuser"
        # print(f'### final user {result}')

    return f


@pytest.fixture(scope="module")
def env_config(schema_name):
    """
    Additional Alembic migration environment configuration values. These
    values are required for the tests to operate properly; in particular for
    them to respect the schema the migrations are being applied to.

    These values are passed to `alembicverify_util.get_current_revision`,
    `.get_head_revision` as kw args of the same name.
    """
    return {
        "version_table": "alembic_version",
        "version_table_schema": schema_name,
    }


@pytest.fixture(scope="module")
def target_revision():
    """
    Placeholder for fixture that must be defined individually for each
    migration test module.
    """
    raise NotImplementedError("`target_revision` not defined for this migration.")


@pytest.fixture(scope="function")
def prepared_schema_from_migrations_left(
    uri_left, alembic_config_left, db_setup, target_revision, request
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
    engine, script = prepare_schema_from_migrations(
        uri_left,
        alembic_config_left,
        db_setup=db_setup,
        revision=getattr(request, "param", target_revision),
    )

    yield engine, script

    engine.dispose()
