"""Upgrade and downgrade tests for the station-query covering index."""

from sqlalchemy import text


REVISION = "c7f1a4d92e6b"
INDEX_NAME = "obs_raw_history_time_variable_covering_idx"


def _index_definition(conn, schema_name):
    return conn.scalar(
        text(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = :schema_name
              AND tablename = 'obs_raw'
              AND indexname = :index_name
            """
        ),
        {"schema_name": schema_name, "index_name": INDEX_NAME},
    )


def test_covering_index_upgrade(alembic_engine, alembic_runner, schema_name):
    alembic_runner.migrate_up_before(REVISION)

    with alembic_engine.begin() as conn:
        assert _index_definition(conn, schema_name) is None

    alembic_runner.migrate_up_one()

    with alembic_engine.begin() as conn:
        definition = _index_definition(conn, schema_name)

    assert "(history_id, obs_time, vars_id) INCLUDE (datum)" in definition


def test_covering_index_upgrade_when_index_already_exists(
    alembic_engine,
    alembic_runner,
    schema_name,
):
    """Upgrade tolerates an equivalent index created outside Alembic."""
    alembic_runner.migrate_up_before(REVISION)

    with alembic_engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE INDEX {INDEX_NAME}
                ON {schema_name}.obs_raw (history_id, obs_time, vars_id)
                INCLUDE (datum)
                """
            )
        )

    alembic_runner.migrate_up_one()

    with alembic_engine.begin() as conn:
        definition = _index_definition(conn, schema_name)

    assert "(history_id, obs_time, vars_id) INCLUDE (datum)" in definition


def test_covering_index_downgrade(alembic_engine, alembic_runner, schema_name):
    alembic_runner.migrate_up_to(REVISION)

    with alembic_engine.begin() as conn:
        assert _index_definition(conn, schema_name) is not None

    alembic_runner.migrate_down_one()

    with alembic_engine.begin() as conn:
        assert _index_definition(conn, schema_name) is None
