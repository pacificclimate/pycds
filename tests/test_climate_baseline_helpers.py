import datetime
from calendar import monthrange
import struct

from pytest import fixture, mark, raises

import pycds
from pycds.util import generic_sesh
from pycds import Network, Station, History, Variable, DerivedValue
from pycds.climate_baseline_helpers import \
    pcic_climate_variable_network_name, \
    get_or_create_pcic_climate_variables_network, \
    create_pcic_climate_baseline_variables, \
    load_pcic_climate_baseline_values, \
    field_format


@fixture
def empty_database_session(mod_blank_postgis_session):
    sesh = mod_blank_postgis_session
    engine = sesh.get_bind()
    pycds.Base.metadata.create_all(bind=engine)
    yield sesh


@fixture
def sesh_with_climate_baseline_variables(empty_database_session):
    sesh = empty_database_session
    create_pcic_climate_baseline_variables(sesh)
    yield sesh


@fixture
def stations():
    return [Station(id=1, native_id='100'), Station(id=2, native_id='200'), ]


@fixture
def histories(stations):
    return [History(
        station_id=station.id,
        station_name='Station {0}'.format(station.native_id),
        sdate=datetime.datetime(year, 1, 1),
        edate=datetime.datetime(year+1, 1, 1),
    ) for station in stations for year in [2000, 2001]]


def describe_get__or__create__pcic__climate__variables__network():

    def test_creates_the_expected_new_network_record(empty_database_session):
        sesh = empty_database_session
        network = get_or_create_pcic_climate_variables_network(sesh)
        results = sesh.query(Network).filter(Network.name == pcic_climate_variable_network_name)
        assert results.count() == 1
        result = results.first()
        assert network.id == result.id
        assert result.publish == True
        assert 'PCIC' in result.long_name

    def test_creates_no_more_than_one_of_them(empty_database_session):
        sesh = empty_database_session
        get_or_create_pcic_climate_variables_network(sesh)
        get_or_create_pcic_climate_variables_network(sesh)
        results = sesh.query(Network).filter(Network.name == pcic_climate_variable_network_name)
        assert results.count() == 1

def describe_create__pcic__climate__baseline__variables():

    def test_causes_network_to_be_created(sesh_with_climate_baseline_variables):
        sesh = sesh_with_climate_baseline_variables
        results = sesh.query(Network).filter(Network.name == pcic_climate_variable_network_name)
        assert results.count() == 1

    @mark.parametrize('name, keyword, kwd', [
        ('Tx_Climatology', 'maximum', 'Max.'),
        ('Tn_Climatology', 'minimum', 'Min.'),
    ])
    def test_creates_temperature_variables(sesh_with_climate_baseline_variables, name, keyword, kwd):
        sesh = sesh_with_climate_baseline_variables
        network = get_or_create_pcic_climate_variables_network(sesh)
        result = sesh.query(Variable).filter(Variable.name == name).first()
        assert result
        assert (result.unit, result.standard_name, result.network_id) == \
               (u'celsius', u'air_temperature', network.id)
        assert result.short_name == u'air_temperature {}'.format(result.cell_method)
        assert result.cell_method == u't: {} within days t: mean within months t: mean over years'.format(keyword)
        assert result.description == u'Climatological mean of monthly mean of {} daily temperature'.format(keyword)
        assert result.display_name == u'Temperature Climatology ({})'.format(kwd)

    def test_creates_precip_variable(sesh_with_climate_baseline_variables):
        sesh = sesh_with_climate_baseline_variables
        network = get_or_create_pcic_climate_variables_network(sesh)
        result = sesh.query(Variable).filter(Variable.name == 'Precip_Climatology').first()
        assert (result.unit, result.standard_name, result.network_id) == \
               (u'mm', u'lwe_thickness_of_precipitation_amount', network.id)
        assert result.short_name == u'lwe_thickness_of_precipitation_amount {}'.format(result.cell_method)
        assert result.cell_method == u't: sum within months t: mean over years'
        assert result.description == u'Climatological mean of monthly total precipitation'
        assert result.display_name == u'Precipitation Climatology'

    def test_creates_no_more_than_one_of_each(empty_database_session):
        sesh = empty_database_session
        create_pcic_climate_baseline_variables(sesh)
        create_pcic_climate_baseline_variables(sesh)
        results = sesh.query(Variable).filter(Variable.name.like('%_Climatology'))
        assert results.count() == 3


def describe_load__pcic__climate__baseline__values():
    def describe_with_station_and_history_records():

        @fixture
        def sesh_with_station_and_history_records(sesh_with_climate_baseline_variables, stations, histories):
            for sesh in generic_sesh(sesh_with_climate_baseline_variables, stations + histories):
                yield sesh

        def describe_with_an_invalid_climate_variable_name():
            var_name = 'foo'

            def test_throws_an_exception(sesh_with_station_and_history_records):
                sesh = sesh_with_station_and_history_records
                with raises(ValueError):
                    load_pcic_climate_baseline_values(sesh, var_name, [])


        def describe_with_a_valid_climate_variable_name():
            var_name = 'Tx_Climatology'  # Any valid one will do

            def describe_with_an_invalid_network_name():
                network_name = 'foo'

                def test_throws_an_exception(sesh_with_station_and_history_records):
                    sesh = sesh_with_station_and_history_records
                    with raises(ValueError):
                        load_pcic_climate_baseline_values(sesh, var_name, [], network_name=network_name)

            def describe_with_a_valid_network_name():
                # use default value for param network_name in load_pcic_climate_baseline_values

                def describe_with_a_fake_source():

                    @fixture
                    def source(stations):
                        lines = []
                        for station in stations:
                            temps = [str(100*station.id + 2*month + 0.5).encode('ascii') for month in range(1, 13)]
                            temps.append(b'99')
                            line = struct.pack(
                                field_format,
                                bytes(station.native_id.encode('ascii')), b' ', b'Station Name', b'elev', b' ', b'long', b'lat',
                                *temps
                            ).decode('ascii').replace('\0', ' ')
                            lines.append(line + '\n')
                        return lines

                    def test_loads_the_values_into_the_database(sesh_with_station_and_history_records, stations, source):
                        sesh = sesh_with_station_and_history_records
                        load_pcic_climate_baseline_values(sesh, var_name, source)
                        derived_values = sesh.query(DerivedValue)

                        assert derived_values.count() == 12 * len(stations)

                        expected_variable = sesh.query(Variable).filter_by(name=var_name).first()
                        for station in stations:
                            station_values = derived_values.join(History).join(Station) \
                                .filter(Station.id == station.id) \
                                .order_by(DerivedValue.time)
                            latest_history = sesh.query(History)\
                                .filter(History.station.has(id=station.id))\
                                .order_by(History.sdate.desc())\
                                .first()
                            for i, value in enumerate(station_values):
                                month = i + 1
                                last_day = monthrange(2000, month)[1]
                                assert value.time == datetime.datetime(2000, month, last_day, 23)
                                assert value.datum == 100*station.id + 2*month + 0.5
                                assert value.history == latest_history
                                assert value.variable == expected_variable

