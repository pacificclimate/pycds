import pytest
from pycds.alembic.helpers import (
    get_postgresql_version,
    db_supports_statement,
    db_supports_matviews,
)


def test_get_postgres_version(base_engine):
    version = get_postgresql_version(base_engine)
    print(version)
    assert type(version) == tuple
    assert all(type(n) == int for n in version)
    assert version >= (9, 0, 0)  # Just for laughs


@pytest.mark.parametrize(
    "statement, expected",
    [
        ("CREATE GRONK test (foo char(5))", False),
        ("CREATE TABLE test (foo char(5))", True),
        ("CREATE MATERIALIZED VIEW test AS SELECT 1", True),
    ],
)
def test_db_supports_statement(base_engine, statement, expected):
    assert db_supports_statement(base_engine, statement) is expected


def test_db_supports_matviews(base_engine):
    assert db_supports_matviews(base_engine) is True
