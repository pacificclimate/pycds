import datetime
from calendar import monthrange
import struct

from pytest import fixture, mark, raises, fail

from .common import climatology_var_names

from ..helpers import add_then_delete_objs
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
def sesh_with_other_network_and_climatology_variables(pycds_sesh, other_network, other_climatology_variables):
    for sesh in add_then_delete_objs(pycds_sesh, [other_network] + other_climatology_variables):
        yield sesh


@fixture
def sesh_with_climate_baseline_variables(sesh_with_other_network_and_climatology_variables):
    get_or_create_pcic_climate_baseline_variables(sesh_with_other_network_and_climatology_variables)
    yield sesh_with_other_network_and_climatology_variables


@fixture
def sesh_with_station_and_history_records(sesh_with_climate_baseline_variables, stations, histories):
    for sesh in add_then_delete_objs(sesh_with_climate_baseline_variables, stations + histories):
        yield sesh


def describe_get__or__create__pcic__climate__variables__network():

    def it_creates_the_expected_new_network_record(pycds_sesh):
        network = get_or_create_pcic_climate_variables_network(pycds_sesh)
        results = pycds_sesh.query(Network).filter(Network.name == pcic_climate_variable_network_name)
        assert results.count() == 1
        result = results.first()
        assert network.id == result.id
        assert result.publish is True
        assert 'PCIC' in result.long_name

    def it_creates_no_more_than_one_of_them(pycds_sesh):
        get_or_create_pcic_climate_variables_network(pycds_sesh)
        get_or_create_pcic_climate_variables_network(pycds_sesh)
        results = pycds_sesh.query(Network).filter(Network.name == pcic_climate_variable_network_name)
        assert results.count() == 1


def describe_create__pcic__climate__baseline__variables():

    def it_returns_the_expected_variables(sesh_with_other_network_and_climatology_variables):
        variables = get_or_create_pcic_climate_baseline_variables(
            sesh_with_other_network_and_climatology_variables)
        assert len(variables) == 3
        assert set([v.name for v in variables]) == set(climatology_var_names)
        # More aggressive testing of each variable below

    def it_causes_network_to_be_created(sesh_with_climate_baseline_variables):
        results = sesh_with_climate_baseline_variables.query(Network).filter(
            Network.name == pcic_climate_variable_network_name)
        assert results.count() == 1

    @mark.parametrize('name, keyword, kwd', [
        ('Tx_Climatology', 'maximum', 'Max.'),
        ('Tn_Climatology', 'minimum', 'Min.'),
    ])
    def it_creates_temperature_variables(sesh_with_climate_baseline_variables, name, keyword, kwd):
        sesh = sesh_with_climate_baseline_variables
        network = get_or_create_pcic_climate_variables_network(sesh)
        result = sesh.query(Variable)\
            .filter(Variable.name == name)\
            .filter(Variable.network.has(name=pcic_climate_variable_network_name))\
            .first()
        assert result
        assert (result.unit, result.standard_name, result.network_id) == \
               ('celsius', 'air_temperature', network.id)
        assert result.short_name == 'air_temperature {}'.format(result.cell_method)
        assert result.cell_method == 't: {} within days t: mean within months t: mean over years'.format(keyword)
        assert result.description == 'Climatological mean of monthly mean of {} daily temperature'.format(keyword)
        assert result.display_name == 'Temperature Climatology ({})'.format(kwd)

    def it_creates_precip_variable(sesh_with_climate_baseline_variables):
        sesh = sesh_with_climate_baseline_variables
        network = get_or_create_pcic_climate_variables_network(sesh)
        result = sesh.query(Variable)\
            .filter(Variable.name == 'Precip_Climatology') \
            .filter(Variable.network.has(name=pcic_climate_variable_network_name)) \
            .first()
        assert (result.unit, result.standard_name, result.network_id) == \
               ('mm', 'lwe_thickness_of_precipitation_amount', network.id)
        assert result.short_name == 'lwe_thickness_of_precipitation_amount {}'.format(result.cell_method)
        assert result.cell_method == 't: sum within months t: mean over years'
        assert result.description == 'Climatological mean of monthly total precipitation'
        assert result.display_name == 'Precipitation Climatology'

    def it_creates_no_more_than_one_of_each(pycds_sesh):
        sesh = pycds_sesh
        get_or_create_pcic_climate_baseline_variables(sesh)
        get_or_create_pcic_climate_baseline_variables(sesh)
        results = sesh.query(Variable).filter(Variable.name.like('%_Climatology'))
        assert results.count() == 3


def describe_load__pcic__climate__baseline__values():
    def describe_with_station_and_history_records():

        @fixture
        def sesh_with_station_and_history_records(sesh_with_climate_baseline_variables, stations, histories):
            for sesh in add_then_delete_objs(sesh_with_climate_baseline_variables, stations + histories):
                yield sesh

        def describe_with_an_invalid_climate_network_or_variable_name():

            @mark.parametrize('network_name, var_name', [
                ('foo', 'Tx_Climatology'),
                (pcic_climate_variable_network_name, 'foo'),
            ])
            def it_throws_an_exception(sesh_with_station_and_history_records, network_name, var_name):
                sesh = sesh_with_station_and_history_records
                with raises(ValueError):
                    load_pcic_climate_baseline_values(sesh, var_name, [], network_name=network_name)

        def describe_with_valid_network_and_variable_names():
            # use default value for param network_name in load_pcic_climate_baseline_values

            def describe_with_no_missing_data():

                @fixture
                def source(request, stations):
                    '''Returns an interable with input lines for testing loading.
                    First lines are created by packing data into the expected format.
                    Last line is blank, to test blank line handling.
                    '''
                    lines = []
                    for station in stations:
                        if request.param in ['Tx_Climatology', 'Tn_Climatology']:
                            values = [str(int(10*(100*station.id + 2*month + 0.5))).encode('ascii')
                                      for month in range(1, 13)]
                        else:
                            values = [str(100*station.id + 2*month).encode('ascii')
                                      for month in range(1, 13)]
                        values.append(b'99')
                        line = struct.pack(
                            field_format,
                            bytes(station.native_id.encode('ascii')),
                            b' ', b'Station Name', b'elev', b' ', b'long', b'lat',
                            *values
                        ).decode('ascii').replace('\0', ' ')
                        lines.append(line + '\n')
                    lines.append('\n')  # add a blank line
                    return lines

                @mark.parametrize('var_name, source', [
                    ('Tx_Climatology', 'Tx_Climatology'), 
                    ('Tn_Climatology', 'Tn_Climatology'), 
                    ('Precip_Climatology', 'Precip_Climatology'), 
                ], indirect=['source'])
                @mark.parametrize('exclude, n_exclude_matching', [
                    ([], 0),
                    (['100'], 1),
                    (['foo'], 0),
                    (['100', 'foo', '200'], 2)
                ])
                def it_correctly_converts_and_loads_values_into_the_database(
                        sesh_with_station_and_history_records, stations, var_name, source, exclude, n_exclude_matching):
                    sesh = sesh_with_station_and_history_records

                    n_lines_added, n_values_added, n_lines_errored, n_lines_excluded, n_lines_skipped = \
                        load_pcic_climate_baseline_values(sesh, var_name, source, exclude)
                    assert n_lines_added == len(stations) - n_exclude_matching
                    assert n_values_added == n_lines_added * 12
                    assert n_lines_errored == 1
                    assert n_lines_excluded == n_exclude_matching
                    assert n_lines_skipped == 0

                    derived_values = sesh.query(DerivedValue)\
                        .join(DerivedValue.variable)\
                        .filter(Variable.name == var_name)

                    assert derived_values.count() == 12 * n_lines_added

                    expected_variable = sesh.query(Variable)\
                        .filter_by(name=var_name) \
                        .filter(Variable.network.has(name=pcic_climate_variable_network_name)) \
                        .first()
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
                            if var_name in ['Tx_Climatology', 'Tn_Climatology']:
                                assert value.datum == 100*station.id + 2*month + 0.5
                            else:
                                assert value.datum == 100*station.id + 2*month
                            assert value.history == latest_history
                            assert value.variable == expected_variable

            def describe_with_a_station_with_missing_data_for_some_months():
                
                @fixture
                def source(stations):
                    values = [b'-9999', b'2', b'3', b'4', b'5', b'-9999',
                              b'7', b'8', b'-9999', b'-9999', b'11', b'12', b'50']
                    line = struct.pack(
                        field_format,
                        bytes(stations[0].native_id.encode('ascii')),
                        b' ', b'Station Name', b'elev', b' ', b'long', b'lat',
                        *values
                    ).decode('ascii').replace('\0', ' ')
                    return [line + '\n']
                
                @mark.parametrize('var_name', ['Tx_Climatology', 'Tn_Climatology', 'Precip_Climatology'])
                def it_loads_only_non_absent_values(
                        sesh_with_station_and_history_records, stations, var_name, source):
                    sesh = sesh_with_station_and_history_records

                    n_lines_added, n_values_added, n_lines_errored, n_lines_excluded, n_lines_skipped = \
                        load_pcic_climate_baseline_values(sesh, var_name, source)
                    assert n_lines_added == 1
                    assert n_values_added == 8
                    assert n_lines_errored == 0
                    assert n_lines_excluded == 0
                    assert n_lines_skipped == 0

                    derived_values = sesh.query(DerivedValue) \
                        .join(DerivedValue.variable) \
                        .filter(Variable.name == var_name)
                    station_values = derived_values.join(History).join(Station) \
                        .filter(Station.id == stations[0].id)
                    assert set([sv.time.month for sv in station_values]) == {2, 3, 4, 5, 7, 8, 11, 12}

                    
def describe_verify__baseline__network__and__variables():
    def describe_without_baseline_network():
        def it_raises_an_exception(pycds_sesh):
            with raises(AssertionError) as excinfo:
                verify_baseline_network_and_variables(pycds_sesh)
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
                ('unit', 'foo'),
                ('standard_name', 'foo'),
                ('short_name', 'foo'),
                ('cell_method', 'foo'),
                ('description', 'foo'),
                ('display_name', 'foo'),
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
                    verify_baseline_values(sesh, var_name, 1, None)
                assert var_name in str(excinfo.value)
                assert 'station count' in str(excinfo.value)
                assert 'expected "1"' in str(excinfo.value)
                assert 'got "0"' in str(excinfo.value)

        def describe_with_valid_climate_baseline_values_in_database():

            @fixture
            def sesh_with_climate_baseline_values(sesh_with_station_and_history_records, histories):
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
                        history=histories[h]
                    )
                    for variable in variables
                    for h in range(2)
                    for month in range(1, 13, h+1)  # a bit tricky: for history[1], leave out every other month
                ]
                for s in add_then_delete_objs(sesh, derived_values):
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
                        {'station_native_id': '200', 'values': [(m if m % 2 else None) for m in range(1, 13)]},
                    ],
                ])
                def it_succeeds(sesh_with_climate_baseline_values, var_name, expected_stations_and_values):
                    sesh = sesh_with_climate_baseline_values
                    try:
                        finished = verify_baseline_values(sesh, var_name, 2, expected_stations_and_values)
                    except AssertionError:
                        fail('Unexpected AssertionError from verify')
                    assert finished

            def describe_with_incompatible_value_expectations():

                @mark.parametrize('var_name', climatology_var_names)
                @mark.parametrize('station_native_id, values, expected_keyword', [
                    ('foo', range(1, 13), 'value count'),
                    ('200', range(1, 13), 'value count'),
                    ('100', list(range(1, 5)) + [99] + list(range(6, 13)), 'datum'),
                    ('200', [(m if m % 2 == 0 else None) for m in range(1, 13)], 'datum'),
                ])
                def it_raises_an_exception(sesh_with_climate_baseline_values, var_name,
                                           station_native_id, values, expected_keyword):
                    sesh = sesh_with_climate_baseline_values
                    expected_stations_and_values = [
                        {'station_native_id': station_native_id, 'values': values},
                    ]
                    with raises(AssertionError) as excinfo:
                        verify_baseline_values(sesh, var_name, 2, expected_stations_and_values)
                    assert var_name in str(excinfo.value)
                    assert station_native_id in str(excinfo.value)
                    assert expected_keyword in str(excinfo.value)
