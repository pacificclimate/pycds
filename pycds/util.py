from collections import namedtuple
from datetime import datetime
from pkg_resources import resource_filename

from pycds import *
from pycds import Base


# http://stackoverflow.com/questions/5631078/sqlalchemy-print-the-actual-query
def compile_query(statement, bind=None):
    """
    print a query, with values filled in
    for debugging purposes *only*
    for security, you should always separate queries from their values
    please also note that this function is quite slow
    """
    import sqlalchemy.orm
    if isinstance(statement, sqlalchemy.orm.Query):
        if bind is None:
            bind = statement.session.get_bind(
                statement._mapper_zero_or_none()
            )
            statement = statement.statement
        elif bind is None:
            bind = statement.bind

        dialect = bind.dialect
        compiler = statement._compiler(dialect)

        class LiteralCompiler(compiler.__class__):

            def visit_bindparam(
                    self, bindparam, within_columns_clause=False,
                    literal_binds=False, **kwargs
            ):
                return super(LiteralCompiler, self).render_literal_bindparam(
                    bindparam, within_columns_clause=within_columns_clause,
                    literal_binds=literal_binds, **kwargs
                )

    compiler = LiteralCompiler(dialect, statement)
    return compiler.process(statement)



TestContact = namedtuple('TestContact', 'name title organization email phone')
TestNetwork = namedtuple('TestNetwork', 'name long_name color')
TestStation = namedtuple('TestStation', 'native_id network histories')
TestHistory = namedtuple(
    'TestHistory', 'station_name elevation sdate edate province country freq')
TestVariable = namedtuple(
    'TestVariable',
    'name unit standard_name cell_method precision description display_name '
    'short_name network')


def create_test_database(engine):
    Base.metadata.create_all(bind=engine)


def create_test_data(sesh):
    moti = Network(**TestNetwork(
        'MOTI', 'Ministry of Transportation and Infrastructure', '000000'
    )._asdict())
    moe = Network(**TestNetwork(
        'MOTI', 'Ministry of Transportation and Infrastructure', '000000'
    )._asdict())
    sesh.add_all([moti, moe])

    simon = Contact(**TestContact(
        'Simon', 'Avalanche Guy', 'MOTI', 'simn@moti.bc.gov.ca', '250-555-1212'
    )._asdict())
    simon.networks = [moti]
    ted = Contact(**TestContact(
        'Ted', 'Air Quailty Guy', 'MOE', 'ted@moti.bc.gov.ca', '250-555-2121'
    )._asdict())
    ted.networks = [moe]
    sesh.add_all([simon, ted])

    histories = [
        TestHistory('Brandywine', 496, datetime(2001, 1, 22, 13),
                    datetime(2011, 4, 6, 11), 'BC', 'Canada', '1-hourly'),
        TestHistory('Stewart', 15, datetime(2004, 1, 22, 13),
                    datetime(2011, 4, 6, 11), 'BC', 'Canada', '1-hourly'),
        TestHistory('Cayoosh Summit', 1350, datetime(1997, 1, 22, 13),
                    datetime(2011, 4, 6, 11), 'BC', 'Canada', '1-hourly'),
        TestHistory('Boston Bar RCMP Station', 180, datetime(1999, 1, 22, 13),
                    datetime(2002, 4, 6, 11), 'BC', 'Canada', '1-hourly'),
        TestHistory('Prince Rupert', 35, datetime(1990, 1, 22, 13),
                    datetime(1996, 4, 6, 11), 'BC', 'Canada', '1-hourly'),
        TestHistory('Prince Rupert', 36, datetime(1997, 1, 22, 13), None, 'BC',
                    'Canada', '1-hourly'),
    ]
    histories = [History(**hist._asdict()) for hist in histories]
    sesh.add_all(histories)
    
    stations = [
        TestStation('11091', moti, [histories[0]]),
        TestStation('51129', moti, [histories[1]]),
        TestStation('26224', moti, [histories[2]]),
        TestStation('E238240', moe, [histories[3]]),
        TestStation('M106037', moe, histories[4:6])
    ]

    for station in stations:
        sesh.add(Station(**station._asdict()))

    variables = [
        TestVariable('air-temperature', 'degC', 'air_temperature',
                     'time: point', None, 'Instantaneous air temperature',
                     'Temperature (Point)', '', moti),
        TestVariable(
            'average-direction', 'km/h', 'wind_from_direction', 'time: mean',
            None, 'Hourly average wind direction', 'Wind Direction (Mean)', '',
            moti
        ),
        TestVariable(
            'dew-point', 'degC', 'dew_point_temperature', 'time: point', None,
            '', 'Dew Point Temperature (Mean)', '', moti
        ),
        TestVariable(
            'BAR_PRESS_HOUR', 'millibar', 'air_pressure', 'time:point', None,
            'Instantaneous air pressure', 'Air Pressure (Point)', '', moe
        ),
    ]

    for variable in variables:
        sesh.add(Variable(**variable._asdict()))


def insert_crmp_data(sesh):
    fname = resource_filename('pycds', 'data/crmp_subset_data.sql')
    with open(fname, 'r') as f:
        data = f.read()

    sesh.execute(data)


def generic_sesh(sesh, sa_objects):
    '''All session fixtures follow a common pattern, abstracted in this generator function.

    Args:
        sesh (sqlalchemy.orm.session.Session): database session

        sa_objects: list of SQLAlchemy ORM objects to be added to database for setup and removed on teardown
            Order within list is respected for setup and teardown, so that dependencies can be respected.

    Returns:
        yields sesh after setup

    To use this generator correctly, i.e., so that the teardown after the yield is also performed,
    a fixture must first yield the result of next(g), then call next(g) again. This can be done two ways:

      gs = generic_sesh(...)
      yield next(gs)
      next(gs)

    or, slightly shorter:

      for sesh in generic_sesh(...):
          yield sesh

    The shorter method is used throughout.
    '''
    for sao in sa_objects:
        sesh.add(sao)
        sesh.flush()
    yield sesh
    for sao in reversed(sa_objects):
        sesh.delete(sao)
        sesh.flush()
