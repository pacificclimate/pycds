import logging
import sys
import logging

from pytest import fixture

# import fixtures from subdirectories
from .db_helpers.pytest_alembic import (
    alembic_config,
    alembic_config_s,
    alembic_engine,
    alembic_engine_s,
    alembic_runner_s,
)
from .db_helpers.data_dbs import (
    db_with_large_data,
    db_with_large_data_s,
    sesh_with_large_data_rw,
    sesh_with_large_data,
)
from .db_helpers.alembic_verify import alembic_root, uri_left, uri_right
from .db_helpers.db import (
    base_database_uri,
    base_engine,
    pycds_engine,
    pycds_sesh,
    schema_name,
    schema_func,
    target_revision,
)


def pytest_runtest_setup():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    logging.getLogger("tests").setLevel(logging.DEBUG)
    logging.getLogger("alembic").setLevel(logging.DEBUG)
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    # logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)
