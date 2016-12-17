from collections import namedtuple
from datetime import datetime
from pkg_resources import resource_filename

from sqlalchemy import not_, and_, or_, Integer, Column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import MetaData

from pycds import *
from pycds import Base, DeferredBase


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


def orm_station_table(sesh, stn_id, raw=True):
    '''Construct a 'station table' i.e. a table such that each row
       corresponds to a single timestep and each column corresponds to
       a separate variable or flag

       :param sesh: sqlalchemy session
       :param stn_id: id corresponding to meta_station.station_id or Station.id
       :type stn_id: int
       :param raw: Should this query be for raw observations? Setting this to False will fetch climatologies.
       :type raw: bool
       :rtype: :py:class:`sqlalchemy.orm.query.Query`

    '''
    if raw:
        raw_filter = not_(and_(ObsWithFlags.cell_method.like(
            '%within%'), ObsWithFlags.cell_method.like('%over%')))
    else:
        raw_filter = or_(ObsWithFlags.cell_method.like(
            '%within%'), ObsWithFlags.cell_method.like('%over%'))

    # Get all of the variables for which observations exist
    # and iterate over them
    vars_ = sesh.query(ObsWithFlags.vars_id, ObsWithFlags.net_var_name)\
        .filter(ObsWithFlags.station_id == stn_id).filter(raw_filter)\
        .distinct().order_by(ObsWithFlags.vars_id)

    # Start with all of the times for which observations exist
    # and then use this as a basis for a left join
    # (sqlite doesn't support full outer joins
    times = sesh.query(ObsWithFlags.obs_time.label('obs_time'))\
        .filter(ObsWithFlags.station_id == stn_id)\
        .order_by(ObsWithFlags.obs_time).distinct()
    stmt = times.subquery()

    for vars_id, var_name in vars_.all():

        # Construct a query for all values of this variable
        right = sesh.query(
            ObsWithFlags.obs_time.label('obs_time'),
            ObsWithFlags.datum.label(var_name),
            ObsWithFlags.flag_name.label(var_name + '_flag')
        ).filter(ObsWithFlags.vars_id == vars_id)\
            .filter(ObsWithFlags.station_id == stn_id).subquery()

        # Then join it to the query we're already building
        join_query = sesh.query(stmt, right).outerjoin(
            right, stmt.c.obs_time == right.c.obs_time)

        stmt = join_query.subquery()

    return sesh.query(stmt)


def sql_station_table(sesh, stn_id):
    return compile_query(orm_station_table(sesh, stn_id))

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
    DeferredBase.metadata.create_all(bind=engine)

# This is fragile, fragile code
# Kind of assumes a postgres read_engine and an sqlite write_engine


def create_reflected_test_database(read_engine, write_engine):
    meta = MetaData(bind=write_engine)
    meta.reflect(bind=read_engine)

    for tablename in ('matviews', 'stats_station_var'):
        meta.remove(meta.tables[tablename])

    logger.info("Overriding PG types that are unknown to sqlite")
    meta.tables['meta_history'].columns['tz_offset'].type = Integer()
    meta.tables['obs_raw'].columns['mod_time'].server_default = None
    meta.tables['meta_history'].columns['the_geom'].type = Integer()
    # These are all BIGINT in postgres
    meta.tables['obs_raw'].columns['obs_raw_id'].type = Integer()
    meta.tables['obs_raw_native_flags'].columns['obs_raw_id'].type = Integer()
    meta.tables['obs_raw_pcic_flags'].columns['obs_raw_id'].type = Integer()

    logger.info("Unsetting all of the sequence defaults")
    for tablename, table in meta.tables.iteritems():
        if hasattr(table, 'primary_key'):
            for column in table.primary_key.columns.values():
                if column.server_default:
                    column.server_default = None

    logger.info("Creating a subset of the tables")
    to_search = [
        'obs_raw', 'meta_history', 'meta_station', 'meta_network', 'meta_vars',
        'meta_contact'
    ]
    to_create = [
        table for tablename, table in meta.tables.iteritems()
        if tablename in to_search
    ]
    # Don't have contact in the postgres database yet 2013.12.04
    meta.tables['meta_network'].append_column(Column('contact_id', Integer))
    meta.create_all(tables=to_create)


def create_test_data(write_engine):
    sesh = sessionmaker(bind=write_engine)()

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

    sesh.commit()


def insert_crmp_data(sesh):
    fname = resource_filename('pycds', 'data/crmp_subset_data.sql')
    with open(fname, 'r') as f:
        data = f.read()

    sesh.execute(data)


def create_test_data_from_reflection(read_engine, write_engine):
    rSession = sessionmaker(bind=read_engine)()
    wSession = sessionmaker(bind=write_engine)()

    q = rSession.query(Variable)
    for var in q.all():
        merged_object = wSession.merge(var)
        wSession.add(merged_object)

    logger.info("Querying the networks")
    q = rSession.query(Network.name, Network.long_name, Network.color)
    for name, long_name, color in q.all():
        new_object = Network(name=name, long_name=long_name, color=color)
        wSession.add(new_object)

    q = rSession.query(Station)
    for station in q.all():
        merged_object = wSession.merge(station)
        wSession.add(merged_object)
        for hist in station.histories:
            new_hist = wSession.merge(hist)
            wSession.add(new_hist)

    wSession.commit()


def generic_sesh(sesh, sa_objects):
    '''All session fixtures follow a common pattern, abstracted in this generator function.
    To use the generator correctly, i.e., so that the teardown after the yield is also performed,
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
        sesh.commit()
    sesh.flush()
    yield sesh
    for sao in reversed(sa_objects):
        sesh.delete(sao)
        sesh.commit()
    sesh.flush()
