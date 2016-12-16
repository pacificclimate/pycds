import struct
import datetime
from pycds import Network, History, Variable, DerivedValue

pcic_climate_variable_name = 'PCIC Climate Variables'


def get_or_create_pcic_climate_variables_network(session):
    """Get or, if it does not exist, create the synthetic network for derived variables

    Args:
        session (...): SQLAlchemy session for accessing the database

    Returns:
        Network object for synthetic network for derived variables
    """

    network = session.query(Network).filter(Network.name == pcic_climate_variable_name).first()
    if not network:
        network = Network(
            name=pcic_climate_variable_name,
            long_name='Synthetic network for derived variables computed by PCIC',
            # virtual='???',   # TODO: What does this mean? No existing networks define it
            publish=False,  # TODO: What does this mean?
            # color = '#??????', # TODO: Does this need to be defined?
        )
        session.add(network)
        session.flush()
    return network


def create_pcic_climate_baseline_variables(session):
    """Create the derived variables for climate baseline values.
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
            'name': 'T_mean_Climatology',
            'unit': temp_unit,
            'standard_name': temp_standard_name,
            'cell_method': 't: mean within days t: mean within months t: mean over years',
            'description': 'Climatological mean of monthly mean of mean daily temperature',
            'display_name': 'Temperature Climatology (Mean)'
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


def load_pcic_climate_baseline_values(session, var_name, source):
    """Load baseline values into the database.
    Create the necessary variables and synthetic network if they do not already exist.

    Args:
        session (...): SQLAlchemy session for accessing the database

        var_name (str): name of climate baseline variable for which the values are to be loaded

        source (iterable): an interable that returns a sequence of fixed-width formatted ASCII lines
            (strings) containing the data to be loaded; typically a `file.readlines()`

    Returns:
        None
    """

    baseline_year = 9999  # TODO: find out what this should be
    baseline_day = 1  # TODO: confirm

    def parse_line(line):
        field_values = struct.unpack(field_format, line.rstrip('\n'))
        field_values = [fv.rstrip('\0 ') for fv in field_values]  # struct.unpack creates null-terminated strings
        return dict(zip(field_names, field_values))

    create_pcic_climate_baseline_variables(session)
    variable = session.query(Variable).filter(Variable.name == var_name).first()

    for line in source:
        data = parse_line(line)
        latest_history = session.query(History)\
            .filter(History.station.has(native_id=data['native_id']))\
            .order_by(History.sdate.desc())\
            .first()
        assert latest_history
        session.add_all(
            [DerivedValue(
                time=datetime.datetime(baseline_year, month, baseline_day),
                datum=float(data[str(month)]),
                vars_id=variable.id,
                history_id=latest_history.id
            ) for month in range(1, 13)]
        )

    session.flush()
