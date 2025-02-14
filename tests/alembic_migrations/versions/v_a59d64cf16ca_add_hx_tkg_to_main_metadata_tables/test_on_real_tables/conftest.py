import logging

import pytest
from sqlalchemy.orm import sessionmaker
from .....alembicverify_util import prepare_schema_from_migrations
from .....helpers import insert_crmp_data


@pytest.fixture(scope="module")
def target_revision():
    # Migrate initially to here
    return "7ab87f8fbcf4"


@pytest.fixture(scope="function")
def prepared_schema_from_migrations_left(uri_left, alembic_config_left, db_setup):
    engine, script = prepare_schema_from_migrations(
        uri_left,
        alembic_config_left,
        db_setup=db_setup,
        revision="7ab87f8fbcf4",
    )

    yield engine, script

    engine.dispose()


@pytest.fixture(scope="function")
def sesh_with_large_data(prepared_schema_from_migrations_left, schema_name):
    engine, script = prepared_schema_from_migrations_left
    sesh = sessionmaker(bind=engine)()
    sesh.execute(f"SET search_path TO {schema_name}, public")
    logger = logging.getLogger("sqlalchemy.engine")
    save_level = logger.level
    logger.setLevel(logging.CRITICAL)
    insert_crmp_data(sesh)
    logger.setLevel(save_level)
    sesh.commit()

    yield sesh

    sesh.close()
