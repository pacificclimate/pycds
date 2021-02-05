import pytest
from sqlalchemy.orm import sessionmaker
from ...alembicverify_util import prepare_schema_from_migrations


@pytest.fixture(scope='function')
def prepared_schema_from_migrations_left(
        uri_left, alembic_config_left, db_setup
):
    engine, script = prepare_schema_from_migrations(
        uri_left,
        alembic_config_left,
        db_setup=db_setup,
        revision='4a2f1879293a'
    )
    schemas = engine.execute("select schema_name from information_schema.schemata").fetchall()
    print(f"### 4af2 schemas: {[x[0] for x in schemas]}")
    result = engine.execute(f"SELECT current_user").scalar()
    print(f'### 4af2 user {result}')

    yield engine, script

    engine.dispose()


@pytest.fixture(scope='function')
def sesh_in_prepared_schema_left(prepared_schema_from_migrations_left):
    engine, script = prepared_schema_from_migrations_left
    sesh = sessionmaker(bind=engine)()

    yield sesh

    sesh.close()
