import pytest
from sqlalchemy.orm import sessionmaker
from ...alembicverify_util import prepare_schema_from_migrations


@pytest.fixture(scope="module")
def target_revision():
    return "4a2f1879293a"


@pytest.fixture(scope="function")
def sesh_in_prepared_schema_left(prepared_schema_from_migrations_left):
    engine, script = prepared_schema_from_migrations_left
    sesh = sessionmaker(bind=engine)()

    yield sesh

    sesh.close()
