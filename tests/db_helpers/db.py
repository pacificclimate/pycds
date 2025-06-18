import pycds.alembic.info

from sqlalchemy.orm import Session
from pytest import fixture
from sqlalchemy import create_engine, text, func
from tests.db_helpers.postgres_factory import Pycds_postgres, Base_postgres

def set_search_path(engine):
    with engine.begin() as conn:
        conn.execute(text(f"SET search_path TO public"))



@fixture(scope="session")
def schema_name():
    return pycds.get_schema_name()


@fixture
def schema_func(schema_name):
    return getattr(func, schema_name)



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



@fixture(scope="session")
def base_database_uri():
    """Test-session scoped base database."""
    with Base_postgres() as pg:
        uri = pg.url()
        yield uri


# TODO: Separate out add_functions
@fixture(scope="session")
def base_engine(base_database_uri, schema_name):
    """
    Test-session scoped base database engine.
    "Base" engine indicates that it has no ORM content created in it.
    """
    engine = create_engine(base_database_uri)
    set_search_path(engine)
    yield engine

@fixture(scope="session")
def target_revision():
    """
    Define the target revision for tests. Typically, the target revision is the head
    revision in the migration sequence, and this is the default set here. This fixture
    can be overridden for tests not at the head revision (e.g., tests of each individual
    migration in tests/alembic_migrations). See the overrides there.
    """
    return pycds.alembic.info.get_current_head()