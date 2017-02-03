"""Tools for loading climate baseline data into database from flat files.
"""

import logging
import struct
import datetime
from calendar import monthrange
from pycds import Network, Station, History, Variable, DerivedValue

pcic_climate_variable_network_name = 'PCIC Climate Variables'

logger = logging.getLogger(__name__)


def get_or_create_pcic_climate_variables_network(session, network_name=pcic_climate_variable_network_name):
    """Get or, if it does not exist, create the synthetic network for derived variables

    Args:
        session (...): SQLAlchemy session for accessing the database

        network_name (str): name of the network to create

    Returns:
        Network object for synthetic network for derived variables
    """

    network = session.query(Network).filter(Network.name == network_name).first()
    if not network:
        network = Network(
            name=network_name,
            long_name='Synthetic network for climate variables computed by PCIC',
            publish=True,
            # color = '#??????', # TODO: Does this need to be defined?
        )
        session.add(network)
        session.flush()
    return network


def get_or_create_pcic_climate_baseline_variables(session):
    """Get or, if they do not exist, create the derived variables for climate baseline values.
    Create the necessary synthetic network for them if it does not already exist.

    Args:
        session (...): SQLAlchemy session for accessing the database

    Returns:
        List of climate baseline Variables created (or existing) in database
    """

    network = get_or_create_pcic_climate_variables_network(session)

    temp_unit = 'celsius'
    temp_standard_name = 'air_temperature'
    precip_unit = 'mm'
    precip_standard_name = 'lwe_thickness_of_precipitation_amount'

    variable_specs = [
        {
            'name': 'Tx_Climatology',
            'unit': temp_unit,
            'standard_name': temp_standard_name,
            'cell_method': 't: maximum within days t: mean within months t: mean over years',
            'description': 'Climatological mean of monthly mean of maximum daily temperature',
            'display_name': 'Temperature Climatology (Max.)'
        },
        {
            'name': 'Tn_Climatology',
            'unit': temp_unit,
            'standard_name': temp_standard_name,
            'cell_method': 't: minimum within days t: mean within months t: mean over years',
            'description': 'Climatological mean of monthly mean of minimum daily temperature',
            'display_name': 'Temperature Climatology (Min.)'
        },
        {
            'name': 'Precip_Climatology',
            'unit': precip_unit,
            'standard_name': precip_standard_name,
            'cell_method': 't: sum within months t: mean over years',
            'description': 'Climatological mean of monthly total precipitation',
            'display_name': 'Precipitation Climatology'
        },
    ]

    variables = []
    for vs in variable_specs:
        variable = session.query(Variable)\
            .filter(Variable.name == vs['name']) \
            .filter(Variable.network.has(name=pcic_climate_variable_network_name)) \
            .first()
        if not variable:
            vs.update(short_name='{0} {1}'.format(vs['standard_name'], vs['cell_method']),
                      network_id=network.id)
            variable = Variable(**vs)
            session.add(variable)
        variables.append(variable)
    session.flush()
    return variables


# Names and widths of fields in flat source files. Copied from Faron's R code.
# native_id is the native_id recorded in the database for the station
# no history record is given but can use latest for the identified station
field_names = ['native_id', 'assay_flag', 'station_name', 'elev', 'elevation_flag', 'long', 'lat',
               '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'annual']
field_widths = [8, 1, 12, 5, 1, 12, 12, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6]

# Format string for module `struct`
field_format = ' '.join(['{}s'.format(fw) for fw in field_widths])


def load_pcic_climate_baseline_values(session, var_name, lines,
                                      exclude=[],
                                      network_name=pcic_climate_variable_network_name):
    """Load baseline values into the database.
    Create the necessary variables and synthetic network if they do not already exist.

    Args:
        session (...): SQLAlchemy session for accessing the database

        var_name (str): name of climate baseline variable for which the values are to be loaded

        lines (iterable): an interable that returns a sequence of fixed-width formatted ASCII lines
            (strings) containing the data to be loaded; typically result of `file.readlines()`.
            Each line represents a single station.

        exclude (list): a list of station native id's that should be excluded from the stations
            loaded from `lines`.

        network_name (str): name of the network to which the climate variable (identified by `var_name`)
            must be associated

    Returns:
        n_lines_added,      # count of lines loaded (stations processed) into database
        n_values_added,     # count of climatology values added to database
        n_lines_errored,    # count of lines that got an error during parsing (unpacking)
        n_lines_excluded,   # count of lines excluded via exclusion list
        n_lines_skipped,    # count of lines skipped (normally because no matching station exists in database)

    Read the input lines one by one and interpret each under a fixed-width format (provided externally; defined above
    in variables field_names, field_widths, field_format.

    The data fields are further formatted as follows:

    - Temperature values are given in 10ths of a degree C.
    - Precipitation values are given in mm.
    - A raw value of -9999 indicates no data for that particular station and month. These values are not
      stored in the database.
    """

    # Time (attribute) for each climate value should be the last hour of the last day of the month, year 2000.
    baseline_year = 2000

    def baseline_day(month):
        """Return last day of month in baseline_year"""
        return monthrange(baseline_year, month)[1]
    baseline_hour = 23

    def parse_line(line):
        field_values = struct.unpack(field_format, bytes(line.rstrip('\n').encode('ascii')))
        # struct.unpack creates null-terminated strings
        field_values = [fv.decode('ascii').rstrip('\0 ') for fv in field_values]
        return dict(zip(field_names, field_values))

    get_or_create_pcic_climate_baseline_variables(session)
    variable = session.query(Variable)\
        .filter(Variable.name == var_name)\
        .filter(Variable.network.has(name=network_name))\
        .first()
    if not variable:
        raise ValueError("Climate variable named '{}' associated with network {} was not found in the database"
                         .format(var_name, network_name))

    if var_name in ['Tx_Climatology', 'Tn_Climatology']:
        convert = lambda temp_in_10thsC: float(temp_in_10thsC) / 10
    else:
        convert = lambda precip_in_mm: float(precip_in_mm)

    logger.info('Loading...')
    n_lines_total = 0
    n_lines_added = 0
    n_values_added = 0
    n_lines_errored = 0
    n_lines_excluded = 0
    n_lines_skipped = 0

    for line in lines:
        n_lines_total += 1
        try:
            data = parse_line(line)
        except struct.error as e:
            logger.info('Error processing input line:')
            logger.info(line)
            logger.info('Error: {}'.format(repr(e)))
            n_lines_errored += 1
            continue
        station_native_id = data['native_id'].strip()
        if station_native_id not in exclude:
            latest_history = session.query(History)\
                .filter(History.station.has(native_id=station_native_id))\
                .order_by(History.sdate.desc())\
                .first()
            if latest_history:
                logger.info('Adding station "{}"'.format(station_native_id))
                for month in range(1, 13):
                    datum = data[str(month)]
                    if datum != '-9999':
                        session.add(
                            DerivedValue(
                                time=datetime.datetime(baseline_year, month, baseline_day(month), baseline_hour),
                                datum=convert(datum),
                                variable=variable,
                                history=latest_history
                            )
                        )
                        n_values_added += 1
                n_lines_added += 1
            else:
                logger.info('Skipping input line:')
                logger.info(line)
                logger.info('Reason: No history record(s) found for station with native_id = "{}"'
                             .format(station_native_id))
                n_lines_skipped += 1
        else:
            logger.info('Excluding station with native id = "{}": found in exclude list'.format(station_native_id))
            n_lines_excluded += 1

    session.flush()

    assert n_lines_total == n_lines_errored + n_lines_added + n_lines_excluded + n_lines_skipped, \
        'Total number of lines processed {} is not total of errored {} +added {} + excluded {} + skipped {}'\
            .format(n_lines_total, n_lines_errored, n_lines_added, n_lines_excluded, n_lines_skipped)

    logger.info('Loading complete')
    logger.info('{} input lines processed'.format(n_lines_total))
    logger.info('{} stations (input lines) processed into to database'.format(n_lines_added))
    logger.info('{} climatology values added to database'.format(n_values_added))
    logger.info('{} input lines errored'.format(n_lines_errored))
    logger.info('{} stations excluded'.format(n_lines_excluded))
    logger.info('{} stations skipped'.format(n_lines_skipped))

    return n_lines_added, n_values_added, n_lines_errored, n_lines_excluded, n_lines_skipped


def expect_value(what, value, expected):
    assert value == expected, \
        '{}: expected "{}", got "{}"'.format(what, expected, value)


def verify_baseline_network_and_variables(session):
    """Verify that the expected baseline network and variable records are present in database.
    These reproduce most of the unit tests for the corresponding upload functions.

    :param session:
    :return: boolean (always True)
    :raise AssertionError when any error is detected
    """

    def expect_variable(name):
        variable = session.query(Variable) \
            .join(Variable.network) \
            .filter((Network.name == pcic_climate_variable_network_name) & (Variable.name == name)) \
            .first()
        assert variable, 'Climate baseline variable named "{}" not found in database'.format(name)
        return variable

    def expect_variable_attr(variable, attr, expected):
        value = getattr(variable, attr)
        expect_value('Variable "{}" {}'.format(variable.name, attr), value, expected)

    # Network
    networks = session.query(Network).filter(Network.name == pcic_climate_variable_network_name)
    assert networks.count() == 1, 'Network "{}" not found in database'.format(pcic_climate_variable_network_name)
    network = networks.first()

    # Temperature variables
    for (name, keyword, kwd) in [
        ('Tx_Climatology', 'maximum', 'Max.'),
        ('Tn_Climatology', 'minimum', 'Min.'),
    ]:
        temp_variable = expect_variable(name)
        expected_attrs = {
            'unit': 'celsius',
            'standard_name': 'air_temperature',
            'network_id': network.id,
            'short_name': 'air_temperature t: {} within days t: mean within months t: mean over years'.format(keyword),
            'cell_method': 't: {} within days t: mean within months t: mean over years'.format(keyword),
            'description': 'Climatological mean of monthly mean of {} daily temperature'.format(keyword),
            'display_name': 'Temperature Climatology ({})'.format(kwd),
        }
        for attr, value in expected_attrs.items():
            expect_variable_attr(temp_variable, attr, value)
            
    # Precipitation variable
    precip_variable = expect_variable('Precip_Climatology')
    expected_attrs = {
        'unit': 'mm',
        'standard_name': 'lwe_thickness_of_precipitation_amount',
        'network_id': network.id,
        'short_name': 'lwe_thickness_of_precipitation_amount t: sum within months t: mean over years',
        'cell_method': 't: sum within months t: mean over years',
        'description': 'Climatological mean of monthly total precipitation',
        'display_name': 'Precipitation Climatology',
    }
    for attr, value in expected_attrs.items():
        expect_variable_attr(precip_variable, attr, value)

    return True


def verify_baseline_values(session, var_name, station_count, expected_stations_and_values):
    """Verify that the database contains the expected content for baseline values. Specifically:

    - the expected number (count) of climate baseline values, given the number of stations with baseline values
    - specific example values, taken from visual intepretation of an arbitrary selection of stations
      in the input file.

    :param session:
    :param station_count: number of stations for which baseline climate values should exist
        (we cannot rely on just counting the number of stations in the database, I think)
    :param var_name: name of variable to be checked
    :param expected_stations_and_values: list of expected stations and associated climate baseline values
        for the specified variable to be checked. Of the form
        [
            {
                'station_native_id': str, # identifies station
                'values': list(numeric), # 12 expected values, in ascending month order;
                                         # absent value indicated by value None
            },
            ...

        ]
    :return boolean: always True
    :raise AssertionError when any error is detected
    """

    stations_with_dvs = \
        session.query(Station.native_id)\
        .select_from(DerivedValue) \
        .join(DerivedValue.history) \
        .join(History.station) \
        .join(Variable.network) \
        .filter(Network.name == pcic_climate_variable_network_name) \
        .filter(Variable.name == var_name) \
        .group_by(Station.native_id)

    expect_value('{} station count'.format(var_name), stations_with_dvs.count(), station_count)

    derived_values = \
        session.query(DerivedValue) \
        .join(DerivedValue.history) \
        .join(DerivedValue.variable) \
        .join(History.station) \
        .join(Variable.network) \
        .filter(Network.name == pcic_climate_variable_network_name) \
        .filter(Variable.name == var_name)

    for expected_stn_and_values in expected_stations_and_values:
        station_native_id = expected_stn_and_values['station_native_id']
        stn_dvs = derived_values \
            .filter(Station.native_id == station_native_id) \
            .order_by(DerivedValue.time) \
            .all()
        expected_values = expected_stn_and_values['values']
        expect_value('Variable "{}", Station "{}" value count'.format(var_name, station_native_id),
                     len(stn_dvs), 12 - expected_values.count(None))
        stn_idx = 0
        month = 1
        for expected_value in expected_values:
            if expected_value is not None:
                expect_value('Variable {}, Station {}, Month {} datum'.format(var_name, station_native_id, month),
                             stn_dvs[stn_idx].datum, expected_value)
                stn_idx += 1
            month += 1

    return True
