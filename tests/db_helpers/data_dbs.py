import logging
from pytest import fixture
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import Session
from tests.helpers import insert_crmp_data
from .db import set_search_path


@fixture
def db_with_large_data(alembic_engine, alembic_runner, target_revision, schema_name):
    """Sets up a datatabase with a large data set for testing."""
    alembic_runner.migrate_up_to(target_revision if target_revision else "head")
    with alembic_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema_name}, public"))
        logger = logging.getLogger("sqlalchemy.engine")
        save_level = logger.level
        logger.setLevel(logging.CRITICAL)
        insert_crmp_data(conn)
        logger.setLevel(save_level)

    return alembic_engine

@fixture(scope="session")
def db_with_large_data_s(alembic_engine_s, alembic_runner_s, target_revision, schema_name):
    """Sets up a datatabase with a large data set for testing."""
    alembic_runner_s.migrate_up_to(target_revision if target_revision else "head")
    with alembic_engine_s.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema_name}, public"))
        logger = logging.getLogger("sqlalchemy.engine")
        save_level = logger.level
        logger.setLevel(logging.CRITICAL)
        insert_crmp_data(conn)
        logger.setLevel(save_level)

    return alembic_engine_s

@fixture
def sesh_with_large_data_rw(db_with_large_data):
    """Extends the db_with_large_data fixture to provide a session
    with the large data set already inserted.
    """
    sesh = Session(db_with_large_data)
    set_search_path(db_with_large_data)
    yield sesh
    sesh.rollback()
    sesh.close()


def set_read_only(sesh):
    """
    Set the session to read-only mode. This is useful for tests that should not
    modify the database, ensuring that no accidental writes occur.
    """
    sesh.execute(text("SET TRANSACTION READ ONLY"))
    sesh.execute(text("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"))

@fixture(scope="session")
def sesh_with_large_data(db_with_large_data_s):
    """Extends the db_with_large_data_s fixture to provide a session
    with the large data set already inserted.
    """
    sesh = Session(db_with_large_data_s)
    set_search_path(db_with_large_data_s)
    set_read_only(sesh)
    yield sesh
    sesh.rollback()
    sesh.close()

    