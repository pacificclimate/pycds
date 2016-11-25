import datetime

__all__ = [
    'Network', 'Contact', 'Variable', 'Station', 'History', 'Obs',
    'CrmpNetworkGeoserver', 'ObsCountPerMonthHistory', 'VarsPerHistory',
    'ObsWithFlags', 'ObsRawNativeFlags', 'NativeFlag', 'MetaSensor'
]

from sqlalchemy import Table, Column, Integer, BigInteger, Float, String, Date
from sqlalchemy import DateTime, Boolean, ForeignKey, Numeric, Interval
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql import text, column
from geoalchemy2 import Geometry

from pycds.view_helpers import create_view

Base = declarative_base()
metadata = Base.metadata


class Network(Base):
    '''This class maps to the table which represents various `networks` of
    data for the Climate Related Monitoring Program. There is one
    network row for each data provider, typically a BC Ministry, crown
    corporation or private company.
    '''
    __tablename__ = 'meta_network'
    id = Column('network_id', Integer, primary_key=True)
    name = Column('network_name', String)
    long_name = Column('description', String)
    virtual = Column(String(255))
    publish = Column(Boolean)
    color = Column('col_hex', String)
    contact_id = Column(Integer, ForeignKey('meta_contact.contact_id'))

    stations = relationship(
        "Station", backref=backref('meta_network', order_by=id))
    variables = relationship(
        "Variable", backref=backref('meta_network', order_by=id))

    def __str__(self):
        return '<CRMP Network %s>' % self.name


class MetaNetworkGeoserver(Network):
    __tablename__ = 'meta_network_geoserver'

    network_id = Column(ForeignKey(
        'meta_network.network_id'), primary_key=True)
    network_name = Column(String(255))
    col_hex = Column(String(7))


class Contact(Base):
    '''This class maps to the table which represents contact people and
    representatives for the networks of the Climate Related Monitoring
    Program.
    '''
    __tablename__ = 'meta_contact'
    id = Column('contact_id', Integer, primary_key=True)
    name = Column('name', String)
    title = Column('title', String)
    organization = Column('organization', String)
    email = Column('email', String)
    phone = Column('phone', String)

    networks = relationship(
        "Network", backref=backref('meta_contact', order_by=id))


class Station(Base):
    '''This class maps to the table which represents a single weather
    station. One weather station can potentially have multiple
    physical locations (though, few do in practice) and periods of
    operation
    '''
    __tablename__ = 'meta_station'
    id = Column('station_id', Integer, primary_key=True)
    native_id = Column(String)
    network_id = Column(Integer, ForeignKey('meta_network.network_id'))
    min_obs_time = Column(DateTime)
    max_obs_time = Column(DateTime)

    network = relationship(
        "Network", backref=backref('meta_station', order_by=id))
    histories = relationship(
        "History", backref=backref('meta_station', order_by=id))

    def __str__(self):
        return '<CRMP Station %s:%s>' % (self.network.name, self.native_id)


class History(Base):
    '''This class maps to the table which represents a history record for
    a weather station. Since a station can potentially (and do) move
    small distances (e.g. from one end of the airport runway to
    another) or change the frequency of its observations, this table
    records the details of those changes.
    '''
    __tablename__ = 'meta_history'
    id = Column('history_id', Integer, primary_key=True)
    station_id = Column('station_id', Integer,
                        ForeignKey('meta_station.station_id'))
    station_name = Column(String)
    lon = Column(Numeric)
    lat = Column(Numeric)
    elevation = Column('elev', Float)
    sdate = Column(Date)
    edate = Column(Date)
    tz_offset = Column(Interval)
    province = Column(String)
    country = Column(String)
    comments = Column(String(255))
    freq = Column(String)
    sensor_id = Column(ForeignKey(u'meta_sensor.sensor_id'))
    the_geom = Column(Geometry('GEOMETRY', 4326))

    sensor = relationship("MetaSensor")
    station = relationship(
        "Station", backref=backref('meta_history', order_by=id))
    observations = relationship(
        "Obs", backref=backref('meta_history', order_by=id))

# Association table for Obs *--* NativeFLag
obs_raw_native_flags_t = Table(
    'obs_raw_native_flags', Base.metadata,
    Column('obs_raw_id', BigInteger,
           ForeignKey('obs_raw.obs_raw_id')),
    Column('native_flag_id', Integer, ForeignKey(
        'meta_native_flag.native_flag_id')),
    UniqueConstraint(
        'obs_raw_id', 'native_flag_id', name='obs_raw_native_flag_unique')
)
ObsRawNativeFlags = obs_raw_native_flags_t

# Association table for Obs *--* PCICFLag
obs_raw_pcic_flags_t = Table(
    'obs_raw_pcic_flags', Base.metadata,
    Column('obs_raw_id', BigInteger,
           ForeignKey('obs_raw.obs_raw_id')),
    Column('pcic_flag_id', Integer, ForeignKey(
        'meta_pcic_flag.pcic_flag_id')),
    UniqueConstraint(
        'obs_raw_id', 'pcic_flag_id', name='obs_raw_pcic_flag_unique')
)
ObsRawPCICFlags = obs_raw_pcic_flags_t


class MetaSensor(Base):
    __tablename__ = 'meta_sensor'

    id = Column('sensor_id', Integer, primary_key=True)
    name = Column(String(255))


class Obs(Base):
    '''This class maps to the table which records the details of weather
    observations. Each row is one single data point for one single
    quantity.
    '''
    __tablename__ = 'obs_raw'
    id = Column('obs_raw_id', BigInteger, primary_key=True)
    time = Column('obs_time', DateTime)
    mod_time = Column(DateTime, nullable=False,
                      default=datetime.datetime.utcnow)
    datum = Column(Float)
    vars_id = Column(Integer, ForeignKey('meta_vars.vars_id'))
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))

    # Relationships
    history = relationship("History", backref=backref('obs_raw', order_by=id))
    variable = relationship(
        "Variable", backref=backref('obs_raw', order_by=id))
    flags = relationship("NativeFlag", secondary=obs_raw_native_flags_t, backref="flagged_obs")
    # better named alias for 'flags'; don't repeat backref
    native_flags = relationship("NativeFlag", secondary=obs_raw_native_flags_t)
    pcic_flags = relationship("PCICFlag", secondary=obs_raw_pcic_flags_t, backref="flagged_obs")

    # Constraints
    __table_args__ = (
        UniqueConstraint('obs_time', 'history_id', 'vars_id',
                         name='time_place_variable_unique'),
    )


class Variable(Base):
    '''This class maps to the table which records the details of the
    physical quantities which are recorded by the weather stations.
    '''
    __tablename__ = 'meta_vars'
    id = Column('vars_id', Integer, primary_key=True)
    name = Column('net_var_name', String)
    unit = Column(String)
    standard_name = Column(String)
    cell_method = Column(String)
    precision = Column(Float)
    description = Column('long_description', String)
    display_name = Column(String)
    short_name = Column(String)
    network_id = Column(Integer, ForeignKey('meta_network.network_id'))

    network = relationship(
        "Network", backref=backref('meta_vars', order_by=id))
    obs = relationship("Obs", backref=backref('meta_vars', order_by=id))


class NativeFlag(Base):
    '''This class maps to the table which records all observations which
    have been `flagged` by the data provider (i.e. the network) for
    some reason. This table records the details of the flags.
    '''
    __tablename__ = 'meta_native_flag'
    id = Column('native_flag_id', Integer, primary_key=True)
    name = Column('flag_name', String)
    description = Column(String)
    network_id = Column(Integer, ForeignKey('meta_network.network_id'))
    value = Column(String)
    discard = Column(Boolean)

    network = relationship("Network", backref=backref('meta_native_flag', order_by=id))

    # Constraints
    __table_args__ = (
        UniqueConstraint('network_id', 'value',
                         name='meta_native_flag_unique'),
    )

class PCICFlag(Base):
    __tablename__ = 'meta_pcic_flag'
    id = Column('pcic_flag_id', Integer, primary_key=True)
    name = Column('flag_name', String)
    discard = Column(Boolean)

    # TODO: Constraints?

# "Proper" views - defined using view functionality within SQLAlchemy using tools in pycds.view_helpers

# View: Daily maximum temperature
# View: Daily minimum temperature
#   - These views support views that deliver monthly average of daily max/min temperature.
#   - Observations flagged with meta_native_flag.discard or meta_pcic_flag.discard are not included in the view.
#   - data_coverage is the fraction of observations actually available in a day relative to those potentially available
#      in a day. The computation is correct for a given day if and only if the observation frequency does not change
#      during that day. If such a change does occur, the data_coverage fraction for the day will be > 1, which is not
#      fatal to distinguishing adequate coverage.
#   - These views are defined with plain-text SQL queries instead of with SQLAlchemy select expressions.
#       The SQL SELECT statements were already written, and the work required to translate them to SQLAlchemy seemed
#       excessive and unnecessary. See https://docs.sqlalchemy.org/en/latest/core/tutorial.html#using-textual-sql

class DailyMaxTemperature(Base):
    __table__ = create_view(
        'daily_max_temperature_v',
        metadata,
        text('''
            SELECT
                hx.history_id AS history_id,
                obs.vars_id AS vars_id,
                date_trunc('day', obs.obs_time) AS obs_day,
                max(obs.datum) AS statistic,
                sum(
                    CASE hx.freq
                    WHEN 'daily' THEN 1.0::float
                    WHEN '1-hourly' THEN (1.0 / 24.0)::float
                    END
                ) AS data_coverage
            FROM
                obs_raw AS obs
                INNER JOIN meta_vars AS vars USING (vars_id)
                INNER JOIN meta_history AS hx USING (history_id)
            WHERE
                obs.obs_raw_id IN (
                    -- Return id of each observation without any associated discard flags;
                    -- in other words, exclude observations marked discard, and don't be fooled by
                    -- additional flags that don't discard (hence the aggregate BOOL_OR's).
                    SELECT obs.obs_raw_id
                    FROM
                        obs_raw AS obs
                        LEFT JOIN obs_raw_native_flags  AS ornf ON (obs.obs_raw_id = ornf.obs_raw_id)
                        LEFT JOIN meta_native_flag      AS mnf  ON (ornf.native_flag_id = mnf.native_flag_id)
                        LEFT JOIN obs_raw_pcic_flags    AS orpf ON (obs.obs_raw_id = orpf.obs_raw_id)
                        LEFT JOIN meta_pcic_flag        AS mpf  ON (orpf.pcic_flag_id = mpf.pcic_flag_id)
                    GROUP BY obs.obs_raw_id
                    HAVING
                        BOOL_OR(COALESCE(mnf.discard, FALSE)) = FALSE
                        AND BOOL_OR(COALESCE(mpf.discard, FALSE)) = FALSE
                )
                AND vars.standard_name = 'air_temperature'
                AND vars.cell_method IN ('time: maximum', 'time: point') -- possibly include time: mean
                AND vars.cell_method IN ('time: minimum', 'time: point', 'time: mean')
                AND hx.freq IN ('1-hourly', 'daily')
            GROUP BY
                hx.history_id, vars_id, obs_day
        ''').columns(
                column('history_id'),
                column('vars_id'),
                column('obs_day'),
                column('statistic'),
                column('data_coverage')
        )
    )
    __mapper_args__ = {
        'primary_key': [__table__.c.history_id, __table__.c.vars_id, __table__.c.obs_day]
    }

# class DailyMinTemperature(Base):
#     __table__ = create_view(
#             'daily_min_temperature_v',
#             metadata,
#             text('''
#                 SELECT
#                     hx.station_id AS station_id,
#                     obs.vars_id AS vars_id,
#                     date_trunc('day', obs_time) AS obs_day,
#                     min(datum) AS statistic,
#                     sum(
#                         CASE hx.freq
#                         WHEN 'daily' THEN 1.0
#                         WHEN '1-hourly' THEN 1.0 / 24.0
#                         END
#                     ) AS data_coverage
#                 FROM
#                     obs_raw AS obs
#                     INNER JOIN meta_vars AS vars USING (vars_id)
#                     INNER JOIN meta_history AS hx USING (history_id)
#                     LEFT JOIN obs_raw_native_flags USING (obs_raw_id)
#                     LEFT JOIN meta_native_flag AS mnf USING (native_flag_id)
#                     LEFT JOIN obs_raw_pcic_flags USING (obs_raw_id)
#                     LEFT JOIN meta_pcic_flag AS mpf USING (pcic_flag_id)
#                 WHERE
#                     mnf.discard IS NOT TRUE
#                     AND mpf.discard IS NOT TRUE
#                     AND vars.standard_name = 'air_temperature'
#                     AND vars.cell_method IN ('time: minimum', 'time: point') -- possibly include time: mean
#                     AND hx.freq IN ('1-hourly', 'daily')
#                 GROUP BY
#                     obs_day, station_id, vars_id
#             ''')
#     )

# # Materialized View: Monthly average of daily maximum temperature
# # Materialized View: Monthly average of daily minimum temperature
# #   - data_coverage is the fraction of of observations actually available in a month relative to those potentially available
# #       in a month, and is robust to varying reporting frequencies on different days in the month (but see caveat for
# #       daily data coverage above).
# #   - These views are defined with plain-text SQL queries instead of with SQLAlchemy select expressions.
# #       The SQL SELECT statements were already written, and the work required to translate them to SQLAlchemy seemed
# #       excessive and unnecessary. See https://docs.sqlalchemy.org/en/latest/core/tutorial.html#using-textual-sql
#
# class MonthlyAverageOfDailyMaxTemperature(Base):
#     __table__ = create_materialized_view(
#             'monthly_average_of_daily_max_temperature_mv',
#             metadata,
#             text('''
#                 SELECT
#                     station_id,
#                     vars_id,
#                     date_trunc('month', obs_day) AS obs_month,
#                     avg(statistic) as statistic,
#                     sum(data_coverage) / DaysInMonth(obs_month)::real AS data_coverage
#                 FROM
#                     daily_max_temperature_v
#                 GROUP BY
#                     obs_month, station_id, vars_id
#             ''')
#     )
#
# class MonthlyAverageOfDailyMinTemperature(Base):
#     __table__ = create_materialized_view(
#             'monthly_average_of_daily_max_temperature_mv',
#             metadata,
#             text('''
#                 SELECT
#                     station_id,
#                     vars_id,
#                     date_trunc('month', obs_day) AS obs_month,
#                     avg(statistic) as statistic,
#                     sum(data_coverage) / DaysInMonth(obs_month)::real AS data_coverage
#                 FROM
#                     daily_min_temperature_v
#                 GROUP BY
#                     obs_month, station_id, vars_id
#             ''')
#     )
#
# # Materialized View: Monthly total precipitation
# #   - Observations flagged with meta_native_flag.discard or meta_pcic_flag.discard are not included in the view.
# #   - data_coverage is the fraction of observations actually available in a month relative to those potentially
# #      available in a month. This computation is correct if and only if the observation frequency does not change
# #      during any one day in the month. It remains approximately correct if such days are rare, and remains valid
# #      for the purpose of distinguishing adequate coverage.
# #   - This view is defined with plain-text SQL queries instead of with SQLAlchemy select expressions.
# #       The SQL SELECT statements were already written, and the work required to translate them to SQLAlchemy seemed
# #       excessive and unnecessary. See https://docs.sqlalchemy.org/en/latest/core/tutorial.html#using-textual-sql
# class MonthlyTotalPrecipitation(Base):
#     __table__ = create_materialized_view(
#             'monthly_total_precipitation_mv',
#             metadata,
#             text('''
#                 SELECT
#                     obs.station_id,
#                     obs.vars_id,
#                     date_trunc('month', obs_time) AS obs_month,
#                     sum(datum) AS statistic,
#                     sum(
#                         CASE hx.freq
#                         WHEN 'daily' THEN 1.0 / DaysInMonth(obs_time)
#                         WHEN '1-hourly' THEN 1.0 / (DaysInMonth(obs_time) * 24.0)
#                         END
#                     ) AS data_coverage
#                 FROM
#                     obs_raw AS obs,
#                     INNER JOIN meta_vars AS vars USING (vars_id),
#                     INNER JOIN meta_history AS hx USING (history_id),
#                     LEFT JOIN meta_native_flag AS mnf USING (native_flag_id)
#                     LEFT JOIN meta_pcic_flag AS mpf USING (pcic_flag_id)
#                 WHERE
#                     mnf.discard IS NOT TRUE
#                     AND mpf.discard IS NOT TRUE
#                     AND vars.standard_name IN (
#                         'lwe_thickness_of_precipitation_amount',
#                         'thickness_of_rainfall_amount',
#                         'thickness_of_snowfall_amount'  -- verify that this is rainfall equiv!
#                     )
#                     AND vars.cell_method = 'time:sum'
#                     AND hx.freq IN ('1-hourly', 'daily')
#                 GROUP BY
#                     obs_month, station_id, vars_id
#             '''),
#             True
#     )


# "Improper" views - defined in crmp repo, and accessed in ORM by referring to them as tables.
# DeferredBase is currently used for these views.
# When testing, not using proper views may create issues
# TODO: Implement as proper views as above

DeferredBase = declarative_base(metadata=metadata, cls=DeferredReflection)
deferred_metadata = DeferredBase.metadata


class CrmpNetworkGeoserver(DeferredBase):
    '''This table maps to a convenience view that is used by geoserver for mapping.
    '''
    __tablename__ = 'crmp_network_geoserver'
    network_name = Column(String)
    native_id = Column(String)
    station_name = Column(String)
    lon = Column(Numeric)
    lat = Column(Numeric)
    elevation = Column('elev', Float)
    min_obs_time = Column(DateTime)
    max_obs_time = Column(DateTime)
    freq = Column(String)
    tz_offset = Column(Interval)
    province = Column(String)
    station_id = Column(Integer, ForeignKey('meta_station.station_id'))
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))
    country = Column(String)
    comments = Column(String)
    sensor_id = Column(Integer)
    description = Column(String(255))
    network_id = Column(Integer)
    col_hex = Column(String(7))
    vars = Column(String)
    display_names = Column(String)
    the_geom = Column(Geometry('GEOMETRY', 4326))


class ObsCountPerMonthHistory(DeferredBase):
    '''This class maps to a materialized view that is required for web app
    performance. It is used for approximating the number of
    observations which will be returned by station selection criteria.
    '''
    __tablename__ = 'obs_count_per_month_history_mv'
    count = Column(Integer)
    date_trunc = Column(DateTime)
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))
    history = relationship("History")


class ClimoObsCount(DeferredBase):
    '''This class maps to a materialized view that is required for web app
    performance. It is used for approximating the number of
    climatologies which will be returned by station selection
    criteria.
    '''
    __tablename__ = 'climo_obs_count_mv'
    count = Column(BigInteger)
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))


class VarsPerHistory(Base):
    '''This class maps to a materialized view that is required for web app
    performance. It is used to link recorded quantities (variables) to
    the station/history level, rather than just the network level
    (just because one station in the network records a quantity,
    doesn't mean that all stations in the network do). To some extent,
    this view is an add on to compensate for poor database
    normalization, but it's close enough to get by.
    '''
    __tablename__ = 'vars_per_history_mv'
    history_id = Column(Integer, ForeignKey(
        'meta_history.history_id'), primary_key=True)
    vars_id = Column(Integer, ForeignKey(
        'meta_vars.vars_id'), primary_key=True)


class ObsWithFlags(Base):
    '''This class maps to a convenience view that is used to construct a
    table of flagged observations; i.e. one row per observation with
    additional columns for each attached flag.
    '''
    __tablename__ = 'obs_with_flags'
    vars_id = Column(Integer, ForeignKey('meta_vars.vars_id'))
    network_id = Column(Integer, ForeignKey('meta_network.network_id'))
    unit = Column(String)
    standard_name = Column(String)
    cell_method = Column(String)
    net_var_name = Column(String)
    obs_raw_id = Column(Integer, ForeignKey(
        'obs_raw.obs_raw_id'), primary_key=True)
    station_id = Column(Integer, ForeignKey('meta_station.station_id'))
    obs_time = Column(DateTime)
    mod_time = Column(DateTime)
    datum = Column(Float)
    native_flag_id = Column(Integer, ForeignKey(
        'meta_native_flag.native_flag_id'))
    flag_name = Column(String)
    description = Column(String)
    flag_value = Column(String)
