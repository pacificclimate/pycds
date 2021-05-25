import pytest
from sqlalchemy.orm import sessionmaker
from ....helpers import insert_crmp_data


@pytest.fixture(scope="module")
def target_revision():
    return "84b7fc2596d5"



@pytest.fixture(scope='function')
def sesh_with_large_data(prepared_schema_from_migrations_left):
    engine, script = prepared_schema_from_migrations_left
    sesh = sessionmaker(bind=engine)()
    insert_crmp_data(sesh)

    yield sesh

    sesh.close()
