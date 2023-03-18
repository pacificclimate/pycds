import pytest
from sqlalchemy.orm import sessionmaker
from ....alembicverify_util import prepare_schema_from_migrations
from ....helpers import insert_crmp_data


@pytest.fixture(scope="function")
def prepared_schema_from_migrations_left(
    uri_left, alembic_config_left, db_setup, mocker, request
):
    """
    This fixture has an optional indirect parameter that determines
    whether the helper function `db_supports_matviews` is mocked, and if so,
    what its value is. Without an indirect parameter, the function is
    unmocked and performs as usual. This allows us to test upgrade and downgrade
    migrations in each case of matview support. Mocking must be done here
    because fixtures are instantiated *before* the test is run, so it is
    too late by the time we are inside the test.
    """
    if hasattr(request, "param"):
        mocker.patch("pycds.database.db_supports_matviews", return_value=request.param)

    engine, script = prepare_schema_from_migrations(
        uri_left,
        alembic_config_left,
        db_setup=db_setup,
        revision="7a3b247c577b",
    )

    yield engine, script

    engine.dispose()


@pytest.fixture(scope="function")
def sesh_with_large_data(prepared_schema_from_migrations_left):
    engine, script = prepared_schema_from_migrations_left
    sesh = sessionmaker(bind=engine)()
    insert_crmp_data(sesh)

    yield sesh

    sesh.close()
