import logging, logging.config
import sys
import os
import pycds
import pycds.alembic.info
import pytest
import logging
import testing.postgresql
import alembic.config


from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateSchema

from pytest import fixture

from pycds.context import get_standard_table_privileges
from pycds.context import get_standard_table_privileges
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateSchema
from sqlalchemydiff.util import get_temporary_uri

from tests.helpers import insert_crmp_data


def db_setup(engine, schema_name, user="testuser"):
    """
    Perform once-per-cluster database setup operations.

    Because alembic was applied to a pre-existing database, we need to fill in a few gaps that aren't currently covered
    by the migration scripts. This includes creating the schema and roles.

    These are operations that must executed prior
    to the creation of any (other) content in the database in order for the
    tests to work.
    """

    with engine.begin() as conn:
        for role, _ in get_standard_table_privileges():
            conn.execute(text(f"CREATE ROLE {role}"))
        conn.execute(
            text(f"CREATE ROLE {pycds.get_su_role_name()} WITH SUPERUSER NOINHERIT;")
        )
        conn.execute(text(f"CREATE USER {user} WITH SUPERUSER NOINHERIT;;"))
        test_user = "testuser"

        logging.basicConfig()
        logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)

        # print(f"### initial user {conn.execute('SELECT current_user').scalar()}")

        conn.execute(text("CREATE EXTENSION postgis"))
        conn.execute(text("CREATE EXTENSION plpython3u"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS citext"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS hstore"))

        # We need this function available, and it does not come pre-installed.
        conn.execute(
            text(
                "CREATE OR REPLACE FUNCTION public.moddatetime() "
                "RETURNS trigger "
                "LANGUAGE 'c' "
                "COST 1 "
                "VOLATILE NOT LEAKPROOF "
                "AS '$libdir/moddatetime', 'moddatetime' "
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

def set_search_path(engine):
    with engine.begin() as conn:
        conn.execute(text(f"SET search_path TO public"))



@fixture
def base_database_uri():
    """Test-session scoped base database."""
    with testing.postgresql.Postgresql() as pg:
        uri = pg.url()
        yield uri


# TODO: Separate out add_functions
@fixture
def base_engine(base_database_uri, schema_name):
    """
    Test-session scoped base database engine.
    "Base" engine indicates that it has no ORM content created in it.
    """
    engine = create_engine(base_database_uri)
    set_search_path(engine)
    db_setup(engine, schema_name)
    yield engine
    

def pytest_runtest_setup():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    logging.getLogger("tests").setLevel(logging.DEBUG)
    logging.getLogger("alembic").setLevel(logging.DEBUG)
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    # logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)


@fixture
def schema_name():
    return pycds.get_schema_name()


@fixture
def schema_func(schema_name):
    return getattr(func, schema_name)

# Fixtures required by
# [`alembic-verify`](https://alembic-verify.readthedocs.io/en/latest/)


@pytest.fixture
def alembic_root():
    return os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "pycds", "alembic")
    )


@pytest.fixture(scope="function")
def uri_left(base_database_uri):
    yield get_temporary_uri(base_database_uri)


@pytest.fixture(scope="function")
def uri_right(base_database_uri):
    yield get_temporary_uri(base_database_uri)




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


@pytest.fixture
def alembic_engine(schema_name):
    """
    While testing we want to use a "fresh" database based around postgres. We leverage
    the `testing.postgresql` package to create a temporary database cluster for each test.
    """
    with testing.postgresql.Postgresql() as pg:
        uri = pg.url()
        engine = create_engine(uri)
        db_setup(engine, schema_name)
        yield engine
        engine.dispose()


@pytest.fixture
def alembic_config():
    """
    In a test config, none of the existing environments are appropriate, we want to
    use a blank local database, so we generate a new config here.
    """
    alembic_config = alembic.config.Config()
    alembic_config.set_main_option("script_location", "pycds/alembic")
    return alembic_config


@fixture(scope="module")
def target_revision():
    """
    Define the target revision for tests. Typically, the target revision is the head
    revision in the migration sequence, and this is the default set here. This fixture
    can be overridden for tests not at the head revision (e.g., tests of each individual
    migration in tests/alembic_migrations). See the overrides there.
    """
    return pycds.alembic.info.get_current_head()

@fixture(scope="function")
def pycds_engine(base_engine):
    """Test-session scoped database engine, with pycds ORM created in it."""
    pycds.Base.metadata.create_all(bind=base_engine)
    yield base_engine

@fixture(scope="function")
def pycds_sesh(pycds_engine):
    """Test-function scoped database session.
    All session actions are rolled back on teardown.
    """
    sesh = Session(pycds_engine)
    set_search_path(pycds_engine)
    yield sesh
    sesh.rollback()
    sesh.close()


@fixture(scope="function")
def db_with_large_data(alembic_engine, alembic_runner, target_revision, schema_name):
    """ Sets up a datatabase with a large data set for testing.
    """
    alembic_runner.migrate_up_to(target_revision if target_revision else "head")
    with alembic_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema_name}, public"))
        logger = logging.getLogger("sqlalchemy.engine")
        save_level = logger.level
        logger.setLevel(logging.CRITICAL)
        insert_crmp_data(conn)
        logger.setLevel(save_level)
    
    return alembic_engine



@pytest.fixture(scope="function")
def sesh_with_large_data(db_with_large_data):
    """Extends the db_with_large_data fixture to provide a session
    with the large data set already inserted.
    """
    sesh = Session(db_with_large_data)
    set_search_path(db_with_large_data)
    yield sesh
    sesh.rollback()
    sesh.close()