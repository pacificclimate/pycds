import os
import pytest
from sqlalchemydiff import compare
from sqlalchemydiff.util import (
    get_temporary_uri,
)


# Fixtures required by
# [`alembic-verify`](https://alembic-verify.readthedocs.io/en/latest/)

@pytest.fixture
def alembic_root():
    return os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "pycds", "alembic")
    )


@pytest.fixture(scope="function")
def uri_left(base_database_uri):
    yield get_temporary_uri(base_database_uri)


@pytest.fixture(scope="function")
def uri_right(base_database_uri):
    yield get_temporary_uri(base_database_uri)
