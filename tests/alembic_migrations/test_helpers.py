import pytest
from pycds.database import (
    get_postgresql_version,
    db_supports_statement,
    db_supports_matviews,
)


@pytest.mark.update20
def test_get_postgres_version(alembic_engine):
    version = get_postgresql_version(alembic_engine)
    assert type(version) == tuple
    assert all(type(n) == int for n in version)
    assert version >= (10, 0)  # Just for laughs


@pytest.mark.update20
@pytest.mark.parametrize(
    "statement, expected",
    [
        ("CREATE GRONK test (foo char(5))", False),
        ("CREATE TABLE test (foo char(5))", True),
        ("CREATE MATERIALIZED VIEW test AS SELECT 1", True),
    ],
)
def test_db_supports_statement(alembic_engine, statement, expected):
    assert db_supports_statement(alembic_engine, statement) is expected


@pytest.mark.update20
def test_db_supports_matviews(alembic_engine):
    assert db_supports_matviews(alembic_engine) is True
