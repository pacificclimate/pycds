from datetime import datetime
from importlib.resources import files

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from pycds import VarsPerHistory, get_schema_name, schema_func
from pycds.orm.station_queries import do_query_one_station, query_one_station


TARGET_STATION_ID = 2313
TARGET_HISTORY_ID = 2716
PEER_HISTORY_ID = 2717
EMPTY_STATION_ID = 2315
EMPTY_HISTORY_ID = 2718

getstationvariabletable = schema_func.getstationvariabletable


@pytest.fixture
def sesh_with_query_one_station_data(
    alembic_engine,
    alembic_runner,
    target_revision,
    schema_name,
):
    """Provide a migrated session with focused query_one_station fixture data."""
    alembic_runner.migrate_up_to(target_revision if target_revision else "head")

    fixture_data = (
        files("pycds").joinpath("data/query_one_station_fixture_data.sql").read_text()
    )

    with alembic_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema_name}, public"))
        conn.execute(text(fixture_data))

    sesh = Session(alembic_engine)
    sesh.execute(text(f"SET search_path TO {schema_name}, public"))

    yield sesh

    sesh.rollback()
    sesh.close()


@pytest.mark.usefixtures("new_db_left")
def test_query_one_station_filters_station_variables_and_climatology(
    sesh_with_query_one_station_data,
):
    """
    Return only variables observed at the target station and requested kind.

    The legacy function discovers variables at the network level, so it also
    returns peer-station precipitation and wind-speed columns for this target.
    """
    sesh = sesh_with_query_one_station_data
    sesh.execute(VarsPerHistory.refresh())

    target_variable_ids = sesh.scalars(
        select(VarsPerHistory.vars_id)
        .where(VarsPerHistory.history_id == TARGET_HISTORY_ID)
        .order_by(VarsPerHistory.vars_id)
    ).all()
    peer_variable_ids = sesh.scalars(
        select(VarsPerHistory.vars_id)
        .where(VarsPerHistory.history_id == PEER_HISTORY_ID)
        .order_by(VarsPerHistory.vars_id)
    ).all()

    assert target_variable_ids == [497, 498]
    assert peer_variable_ids == [496, 499]

    legacy_query = sesh.query(
        getstationvariabletable(TARGET_STATION_ID, False)
    ).scalar()
    legacy_column_names = list(sesh.execute(text(legacy_query)).keys())

    # Characterize the defect being fixed: these variables are defined for the
    # network and observed by the peer station, but not by the target station.
    assert legacy_column_names == [
        "obs_time",
        "precipitation",
        "temperature",
        "wind_speed",
    ]

    observation_result = do_query_one_station(
        sesh,
        TARGET_STATION_ID,
        climo=False,
    )
    assert list(observation_result.keys()) == ["obs_time", "temperature"]

    observation_rows = observation_result.mappings().all()
    assert [row["obs_time"] for row in observation_rows] == [
        datetime(2006, 2, 10, 15),
        datetime(2006, 2, 10, 18),
        datetime(2006, 2, 11, 6),
        datetime(2006, 2, 12, 12),
    ]
    assert [row["temperature"] for row in observation_rows] == pytest.approx(
        [9.7, 4.8, 4.6, 9.2]
    )

    climo_result = do_query_one_station(
        sesh,
        TARGET_STATION_ID,
        climo=True,
    )
    assert list(climo_result.keys()) == [
        "obs_time",
        "temperature_climatology",
    ]

    climo_rows = climo_result.mappings().all()
    assert [row["obs_time"] for row in climo_rows] == [
        datetime(2006, 2, 10, 15),
    ]
    assert [row["temperature_climatology"] for row in climo_rows] == (
        pytest.approx([7.0])
    )


@pytest.mark.parametrize("climo", [False, True])
def test_query_one_station_returns_empty_obs_time_table_when_no_variables(
    sesh_with_query_one_station_data,
    climo,
):
    """
    An existing station with no observations yields a valid obs_time-only
    result. The legacy function incorrectly exposes network-level variables.
    """
    sesh = sesh_with_query_one_station_data
    sesh.execute(VarsPerHistory.refresh())

    assert (
        sesh.scalars(
            select(VarsPerHistory.vars_id).where(
                VarsPerHistory.history_id == EMPTY_HISTORY_ID
            )
        ).all()
        == []
    )

    legacy_query = sesh.query(getstationvariabletable(EMPTY_STATION_ID, climo)).scalar()
    legacy_result = sesh.execute(text(legacy_query))

    legacy_column_names = list(legacy_result.keys())

    # Peer stations have matching raw and climatological variables, so the
    # legacy network-scoped discovery returns a misleading non-empty set of
    # variable columns in both modes.
    assert legacy_column_names[0] == "obs_time"
    assert len(legacy_column_names) > 1
    assert legacy_result.all() == []

    orm_result = sesh.execute(
        query_one_station(
            sesh,
            EMPTY_STATION_ID,
            climo=climo,
        )
    )

    assert list(orm_result.keys()) == ["obs_time"]
    assert orm_result.all() == []
