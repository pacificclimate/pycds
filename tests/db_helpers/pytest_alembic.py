

from contextlib import contextmanager
import alembic.config
from pytest_alembic.config import Config

from pytest import fixture
import pytest_alembic
from sqlalchemy import create_engine
from .postgres_factory import Pycds_postgres

def alembic_config_impl():
    """
    In a test config, none of the existing environments are appropriate, we want to
    use a blank local database, so we generate a new config here.
    """
    alembic_config = alembic.config.Config()
    alembic_config.set_main_option("script_location", "pycds/alembic")
    return alembic_config

def alembic_engine_impl():
    """
    While testing we want to use a "fresh" database based around postgres. We leverage
    the `testing.postgresql` package to create a temporary database cluster for each test.
    """
    with Pycds_postgres() as pg:
        uri = pg.url()
        engine = create_engine(uri)
        yield engine
        engine.dispose()


@fixture
def alembic_config():
    return alembic_config_impl()

@fixture(scope="session")
def alembic_config_s():
    return alembic_config_impl()

@fixture
def alembic_engine():
    yield from alembic_engine_impl()

@fixture(scope="session")
def alembic_engine_s():
    yield from alembic_engine_impl()
    
@fixture(scope="session")
def alembic_runner_s(alembic_config_s, alembic_engine_s):
    """
    Taken from the pytest-alembic documentation, this fixture provides a session-scoped
    version of the alembic runner, which is used to apply migrations
    """
    config = Config.from_raw_config(alembic_config_s)
    with pytest_alembic.runner(config=config, engine=alembic_engine_s) as runner:
        yield runner