import logging

import pytest
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from .....alembicverify_util import prepare_schema_from_migrations
from .....helpers import insert_crmp_data


@pytest.fixture(scope="module")
def target_revision():
    # Migrate initially to here
    return "7ab87f8fbcf4"




@pytest.fixture(scope="function")
def sesh_with_large_data(alembic_engine, alembic_runner, target_revision, schema_name):
    alembic_runner.migrate_up_to(target_revision)
    with alembic_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema_name}, public"))
        logger = logging.getLogger("sqlalchemy.engine")
        save_level = logger.level
        logger.setLevel(logging.CRITICAL)
        insert_crmp_data(conn)
        logger.setLevel(save_level)
