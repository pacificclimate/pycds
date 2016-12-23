"""Tools for loading climate baseline data into database from flat files.
"""

import struct
import datetime
from calendar import monthrange
from pycds import Network, History, Variable, DerivedValue

pcic_climate_variable_network_name = 'PCIC Climate Variables'


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


def create_pcic_climate_baseline_variables(session):
    """Create, if they do not exist, the derived variables for climate baseline values.
    Create the necessary synthetic network for them if it does not already exist.

    Args:
        session (...): SQLAlchemy session for accessing the database

    Returns:
        None
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

    for vs in variable_specs:
        variable = session.query(Variable).filter(Variable.name == vs['name']).first()
        if not variable:
            vs.update(short_name='{0} {1}'.format(vs['standard_name'], vs['cell_method']),
                      network_id=network.id)
            session.add(Variable(**vs))
    session.flush()


# Names and widths of fields in flat source files. Copied from Faron's R code.
# native_id is the native_id recorded in the database for the station
# no history record is given but can use latest for the identified station
field_names = ['native_id', 'assay_flag', 'station_name', 'elev', 'elevation_flag', 'long', 'lat',
               '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'annual']
field_widths = [8, 1, 12, 5, 1, 12, 12, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6]

# Format string for module `struct`
field_format = ' '.join(['{}s'.format(fw) for fw in field_widths])


def load_pcic_climate_baseline_values(session, var_name, lines, network_name=pcic_climate_variable_network_name):
    """Load baseline values into the database.
    Create the necessary variables and synthetic network if they do not already exist.

    Args:
        session (...): SQLAlchemy session for accessing the database

        var_name (str): name of climate baseline variable for which the values are to be loaded

        lines (iterable): an interable that returns a sequence of fixed-width formatted ASCII lines
            (strings) containing the data to be loaded; typically result of `file.readlines()`

        network_name (str): name of the network to which the climate variable (identified by var_name)
            must be associated

    Returns:
        None
    """

    # Time (attribute) for each climate value should be the last hour of the last day of the month, year 2000.
    baseline_year = 2000
    def baseline_day(month):
        """Return last day of month in baseline_year"""
        return monthrange(baseline_year, month)[1]
    baseline_hour = 23

    def parse_line(line):
        field_values = struct.unpack(field_format, bytes(line.rstrip('\n').encode('ascii')))
        field_values = [fv.decode('ascii').rstrip('\0 ') for fv in field_values]  # struct.unpack creates null-terminated strings
        return dict(zip(field_names, field_values))

    create_pcic_climate_baseline_variables(session)
    variable = session.query(Variable)\
        .filter_by(name=var_name)\
        .filter(Variable.network.has(name=network_name))\
        .first()
    if not variable:
        raise ValueError("Climate variable named '{}' associate with network {} was not found in the database"
                         .format(var_name, network_name))

    print('Loading...')
    n_added = 0
    n_skipped = 0

    for line in lines:
        data = parse_line(line)
        latest_history = session.query(History)\
            .filter(History.station.has(native_id=data['native_id']))\
            .order_by(History.sdate.desc())\
            .first()
        if latest_history:
            session.add_all(
                [DerivedValue(
                    time=datetime.datetime(baseline_year, month, baseline_day(month), baseline_hour),
                    datum=float(data[str(month)]),
                    vars_id=variable.id,
                    history_id=latest_history.id
                ) for month in range(1, 13)]
            )
            n_added += 1
        else:
            print('\nSkipping input line:')
            print(line)
            print('Reason: No history record(s) found for station with native_id = "{}"'.format(data['native_id']))
            n_skipped += 1

    session.flush()

    print('Loading complete')
    print('{} input lines successfully processed'.format(n_added))
    print('{} input lines skipped'.format(n_skipped))
