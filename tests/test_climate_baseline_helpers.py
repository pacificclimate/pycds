import datetime
from calendar import monthrange
import struct

from pytest import fixture, mark, raises, fail

from pycds.util import generic_sesh
from pycds import Network, Station, History, Variable, DerivedValue
from pycds.climate_baseline_helpers import \
    pcic_climate_variable_network_name, \
    get_or_create_pcic_climate_variables_network, \
    get_or_create_pcic_climate_baseline_variables, \
    load_pcic_climate_baseline_values, \
    field_format, \
    verify_baseline_network_and_variables, \
    verify_baseline_values


@fixture
def sesh_with_climate_baseline_network(session):
    get_or_create_pcic_climate_variables_network(session)
    yield session


@fixture
def sesh_with_climate_baseline_variables(session):
    get_or_create_pcic_climate_baseline_variables(session)
    yield session


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


@fixture
def sesh_with_station_and_history_records(sesh_with_climate_baseline_variables, stations, histories):
    for sesh in generic_sesh(sesh_with_climate_baseline_variables, stations + histories):
        yield sesh


climatology_var_names = ['Tx_Climatology', 'Tn_Climatology', 'Precip_Climatology']


def describe_get__or__create__pcic__climate__variables__network():

    def test_creates_the_expected_new_network_record(session):
        sesh = session
        network = get_or_create_pcic_climate_variables_network(sesh)
        results = sesh.query(Network).filter(Network.name == pcic_climate_variable_network_name)
        assert results.count() == 1
        result = results.first()
        assert network.id == result.id
        assert result.publish == True
        assert 'PCIC' in result.long_name

    def test_creates_no_more_than_one_of_them(session):
        sesh = session
        get_or_create_pcic_climate_variables_network(sesh)
        get_or_create_pcic_climate_variables_network(sesh)
        results = sesh.query(Network).filter(Network.name == pcic_climate_variable_network_name)
        assert results.count() == 1

def describe_create__pcic__climate__baseline__variables():

    def test_returns_the_expected_variables(session):
        sesh = session
        variables = get_or_create_pcic_climate_baseline_variables(sesh)
        assert len(variables) == 3
        assert set([v.name for v in variables]) == set(climatology_var_names)
        # More aggressive testing of each variable below

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

    def test_creates_no_more_than_one_of_each(session):
        sesh = session
        get_or_create_pcic_climate_baseline_variables(sesh)
        get_or_create_pcic_climate_baseline_variables(sesh)
        results = sesh.query(Variable).filter(Variable.name.like('%_Climatology'))
        assert results.count() == 3


def describe_load__pcic__climate__baseline__values():
    def describe_with_station_and_history_records():

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

                        n_loaded, n_skipped = load_pcic_climate_baseline_values(sesh, var_name, source)
                        assert n_loaded == 2
                        assert n_skipped == 0

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


def describe_verify__baseline__network__and__variables():
    def describe_without_baseline_network():
        def it_raises_an_exception(session):
            with raises(AssertionError) as excinfo:
                verify_baseline_network_and_variables(session)
            assert pcic_climate_variable_network_name in str(excinfo.value)
            assert 'not found in database' in str(excinfo.value)

    def describe_with_baseline_network_missing_a_variable():

        @fixture
        def sesh_with_1_var_missing(request, sesh_with_climate_baseline_variables):
            sesh = sesh_with_climate_baseline_variables
            variable = next(var for var in get_or_create_pcic_climate_baseline_variables(sesh)
                            if var.name == request.param)
            sesh.delete(variable)
            yield sesh
            # get_or_create_pcic_climate_baseline_variables(sesh)


        @mark.parametrize('missing_var, sesh_with_1_var_missing', [
            ('Tx_Climatology', 'Tx_Climatology'),
            ('Tn_Climatology', 'Tn_Climatology'),
            ('Precip_Climatology', 'Precip_Climatology')
        ], indirect=['sesh_with_1_var_missing'])
        def it_raises_an_exception(missing_var, sesh_with_1_var_missing):
            with raises(AssertionError) as excinfo:
                verify_baseline_network_and_variables(sesh_with_1_var_missing)
            assert missing_var in str(excinfo.value)
            assert 'not found in database' in str(excinfo.value)

    def describe_with_baseline_network_and_variables():
        def describe_with_all_as_expected():
            def it_doesnt_raise_an_exception(sesh_with_climate_baseline_variables):
                try:
                    finished = verify_baseline_network_and_variables(sesh_with_climate_baseline_variables)
                except AssertionError:
                    fail('Unexpected AssertionError from verify')
                assert finished

        def describe_with_incorrect_attributes_in_variables():

            @mark.parametrize('var_name', climatology_var_names)
            @mark.parametrize('attr_name, attr_value', [
                ('unit', u'foo'),
                ('standard_name', u'foo'),
                ('short_name', u'foo'),
                ('cell_method', u'foo'),
                ('description', u'foo'),
                ('display_name', u'foo'),
            ])
            def it_raises_an_exception(sesh_with_climate_baseline_variables, var_name, attr_name, attr_value):
                sesh = sesh_with_climate_baseline_variables
                variable = next(var for var in get_or_create_pcic_climate_baseline_variables(sesh)
                                if var.name == var_name)
                setattr(variable, attr_name, attr_value)
                sesh.flush()
                with raises(AssertionError) as excinfo:
                    verify_baseline_network_and_variables(sesh)
                assert var_name in str(excinfo.value)
                assert attr_name in str(excinfo.value)
                assert 'expected' in str(excinfo.value)

def describe_verify__baseline__values():
    def describe_with_baseline_network_and_variables():
        def describe_with_no_values():

            @mark.parametrize('var_name', climatology_var_names)
            def it_raises_an_exception(sesh_with_climate_baseline_variables, var_name):
                sesh = sesh_with_climate_baseline_variables
                with raises(AssertionError) as excinfo:
                    verify_baseline_values(sesh, 1, var_name, None)
                assert var_name in str(excinfo.value)
                assert 'values count' in str(excinfo.value)
                assert 'expected "12"' in str(excinfo.value)
                assert 'got "0"' in str(excinfo.value)

        def describe_with_valid_climate_baseline_values_in_database():

            @fixture
            def sesh_with_climate_baseline_values(sesh_with_station_and_history_records):
                sesh = sesh_with_station_and_history_records
                variables = sesh.query(Variable)\
                    .filter(Variable.network.has(name=pcic_climate_variable_network_name)) \
                    .all()
                stations = sesh.query(Station).all()
                histories = [sesh.query(History)
                                 .filter(History.station == station)
                                 .order_by(History.sdate.desc())
                                 .first()
                             for station in stations]
                derived_values = [
                    DerivedValue(
                        time=datetime.datetime(2000, month, monthrange(2000, month)[1], 23),
                        datum=float(month),
                        variable=variable,
                        history=history
                    )
                    for variable in variables
                    for history in histories
                    for month in range(1,13)
                ]
                for s in generic_sesh(sesh, derived_values):
                    yield s

            def describe_with_compatible_value_expectations():

                @mark.parametrize('var_name', climatology_var_names)
                @mark.parametrize('expected_stations_and_values', [
                    [],
                    [
                        {'station_native_id': '100', 'values': range(1, 13)},
                    ],
                    [
                        {'station_native_id': '100', 'values': range(1, 13)},
                        {'station_native_id': '200', 'values': range(1, 13)},
                    ],
                ])
                def it_succeeds(sesh_with_climate_baseline_values, var_name, expected_stations_and_values):
                    sesh = sesh_with_climate_baseline_values
                    try:
                        finished = verify_baseline_values(sesh, 2, var_name, expected_stations_and_values)
                    except AssertionError:
                        fail('Unexpected AssertionError from verify')
                    assert finished

            def describe_with_incompatible_value_expectations():

                @mark.parametrize('var_name', climatology_var_names)
                @mark.parametrize('station_native_id, values, expected_keyword', [
                    ('foo', range(1, 13), 'value count'),
                    ('100', list(range(1,5)) + [99] + list(range(6,13)), 'datum'),
                ])
                def it_raises_an_exception(sesh_with_climate_baseline_values, var_name,
                                           station_native_id, values, expected_keyword):
                    sesh = sesh_with_climate_baseline_values
                    expected_stations_and_values = [
                        {'station_native_id': station_native_id, 'values': values},
                    ]
                    with raises(AssertionError) as excinfo:
                        verify_baseline_values(sesh, 2, var_name, expected_stations_and_values)
                    assert var_name in str(excinfo.value)
                    assert station_native_id in str(excinfo.value)
                    assert expected_keyword in str(excinfo.value)
