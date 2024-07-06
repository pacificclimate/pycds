import datetime
import pytest
from sqlalchemy.orm import sessionmaker
from pycds import (
    Network,
    Station,
    History,
    Variable,
    NativeFlag,
    PCICFlag,
)


@pytest.fixture(scope="function")
def prepared_sesh_left(prepared_schema_from_migrations_left):
    engine, script = prepared_schema_from_migrations_left
    sesh = sessionmaker(bind=engine)()

    yield sesh

    sesh.close()


@pytest.fixture
def network1():
    return Network(name="Network 1")


@pytest.fixture
def network2():
    return Network(name="Network 2")


@pytest.fixture
def station1(network1):
    return Station(network=network1)


@pytest.fixture
def station2(network2):
    return Station(network=network2)


history_transition_date = datetime.datetime(2010, 1, 1)


@pytest.fixture
def history_stn1_hourly(station1):
    return History(
        station=station1,
        station_name="Station 1",
        sdate=datetime.datetime.min,
        edate=history_transition_date,
        freq="1-hourly",
    )


@pytest.fixture
def history_stn1_12_hourly(station1):
    return History(
        station=station1,
        station_name="Station 1",
        sdate=datetime.datetime.min,
        edate=history_transition_date,
        freq="12-hourly",
    )


@pytest.fixture
def history_stn1_daily(station1):
    return History(
        station=station1,
        station_name="Station 1",
        sdate=history_transition_date,
        edate=None,
        freq="daily",
    )


@pytest.fixture
def history_stn2_hourly(station2):
    return History(
        station=station2,
        station_name="Station 2",
        sdate=datetime.datetime.min,
        edate=history_transition_date,
        freq="1-hourly",
    )


@pytest.fixture
def var_temp_point(network1):
    return Variable(
        network=network1,
        standard_name="air_temperature",
        cell_method="time: point",
        display_name="foo bar",
    )


@pytest.fixture
def var_temp_point2(network2):
    return Variable(
        network=network2,
        standard_name="air_temperature",
        cell_method="time: point",
        display_name="foo bar",
    )


@pytest.fixture
def var_temp_max(network1):
    return Variable(
        network=network1,
        standard_name="air_temperature",
        cell_method="time: maximum",
        display_name="foo bar",
    )


@pytest.fixture
def var_temp_min(network1):
    return Variable(
        network=network1,
        standard_name="air_temperature",
        cell_method="time: minimum",
        display_name="foo bar",
    )


@pytest.fixture
def var_temp_mean(network1):
    return Variable(
        network=network1,
        standard_name="air_temperature",
        cell_method="time: mean",
        display_name="foo bar",
    )


@pytest.fixture
def var_foo(network1):
    return Variable(
        network=network1,
        standard_name="foo",
        cell_method="time: point",
        display_name="foo bar",
    )


@pytest.fixture
def var_precip_net1_1(network1):
    return Variable(
        network=network1,
        standard_name="thickness_of_rainfall_amount",
        cell_method="time: sum",
        display_name="foo bar",
    )


@pytest.fixture
def var_precip_net1_2(network1):
    return Variable(
        network=network1,
        standard_name="thickness_of_rainfall_amount",
        cell_method="time: sum",
        display_name="foo bar",
    )


@pytest.fixture
def var_precip_net2_1(network2):
    return Variable(
        network=network2,
        standard_name="thickness_of_rainfall_amount",
        cell_method="time: sum",
        display_name="foo bar",
    )


@pytest.fixture
def native_flag_discard():
    return NativeFlag(discard=True)


@pytest.fixture
def native_flag_non_discard():
    return NativeFlag(discard=False)


@pytest.fixture
def pcic_flag_discard():
    return PCICFlag(discard=True)


@pytest.fixture
def pcic_flag_non_discard():
    return PCICFlag(discard=False)
