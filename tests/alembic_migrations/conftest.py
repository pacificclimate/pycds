# Fixtures required by
# [`alembic-verify`](https://alembic-verify.readthedocs.io/en/latest/)

import os
import pytest

from sqlalchemy.schema import CreateSchema

from sqlalchemydiff.util import get_temporary_uri

from pycds import get_su_role_name


@pytest.fixture
def alembic_root():
    return os.path.normpath(
        os.path.join(
            os.path.dirname(__file__), '..', '..', 'pycds', 'alembic'
        )
    )


@pytest.fixture(scope='module')
def uri_left(base_database_uri):
    yield get_temporary_uri(base_database_uri)


@pytest.fixture(scope='module')
def uri_right(base_database_uri):
    yield get_temporary_uri(base_database_uri)


# Fixtures specific to our tests

@pytest.fixture(scope='module')
def db_setup(schema_name):
    def f(engine):
        engine.execute('CREATE EXTENSION postgis')
        engine.execute('CREATE EXTENSION plpythonu')
        engine.execute(CreateSchema(schema_name))
    return f


@pytest.fixture(scope='module')
def env_config(schema_name):
    return {
        'version_table': "alembic_version",
        'version_table_schema': schema_name
    }
