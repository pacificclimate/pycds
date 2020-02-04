import pytest
from sqlalchemy.orm import sessionmaker
from ...alembicverify_util import prepare_schema_from_migrations
from ....helpers import insert_crmp_data


@pytest.fixture(scope='function')
def prepared_schema_from_migrations_left(
        uri_left, alembic_config_left, db_setup
):
    yield prepare_schema_from_migrations(
        uri_left,
        alembic_config_left,
        db_setup=db_setup,
        revision='84b7fc2596d5'
    )


@pytest.fixture(scope='function')
def sesh_with_large_data(prepared_schema_from_migrations_left):
    engine, script = prepared_schema_from_migrations_left
    sesh = sessionmaker(bind=engine)()
    insert_crmp_data(sesh)
    yield sesh


