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
