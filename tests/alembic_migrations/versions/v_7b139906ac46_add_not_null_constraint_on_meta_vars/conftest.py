import pytest
from sqlalchemy.orm import sessionmaker
from ....helpers import insert_crmp_data


@pytest.fixture(scope="module")
def target_revision():
    return "7b139906ac46"


@pytest.fixture(scope="function")
def sesh_with_data(prepared_schema_from_migrations_left):
    engine, script = prepared_schema_from_migrations_left
    sesh = sessionmaker(bind=engine)()

    yield sesh

    sesh.close()
