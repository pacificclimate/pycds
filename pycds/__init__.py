import datetime

__all__ = [
    'Network', 'Contact', 'Variable', 'Station', 'History', 'Obs',
    'CrmpNetworkGeoserver', 'ObsCountPerMonthHistory', 'VarsPerHistory',
    'ObsWithFlags', 'ObsRawNativeFlags', 'NativeFlag', 'ObsRawPCICFlags', 'PCICFlag',
    'MetaSensor'
]

import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, BigInteger, Float, String, Date, Index
from sqlalchemy import DateTime, Boolean, ForeignKey, Numeric, Interval
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import DDL, UniqueConstraint
from geoalchemy2 import Geometry


Base = declarative_base(metadata=MetaData(schema='crmp'))
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
ObsRawNativeFlags = Table(
    'obs_raw_native_flags', Base.metadata,
    Column('obs_raw_id', BigInteger,
           ForeignKey('obs_raw.obs_raw_id')),
    Column('native_flag_id', Integer, ForeignKey(
        'meta_native_flag.native_flag_id')),
    UniqueConstraint(
        'obs_raw_id', 'native_flag_id', name='obs_raw_native_flag_unique')
)


# Association table for Obs *--* PCICFLag
ObsRawPCICFlags = Table(
    'obs_raw_pcic_flags', Base.metadata,
    Column('obs_raw_id', BigInteger,
           ForeignKey('obs_raw.obs_raw_id')),
    Column('pcic_flag_id', Integer, ForeignKey(
        'meta_pcic_flag.pcic_flag_id')),
    UniqueConstraint(
        'obs_raw_id', 'pcic_flag_id', name='obs_raw_pcic_flag_unique')
)


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
    flags = relationship("NativeFlag", secondary=ObsRawNativeFlags, backref="flagged_obs")
    # better named alias for 'flags'; don't repeat backref
    native_flags = relationship("NativeFlag", secondary=ObsRawNativeFlags)
    pcic_flags = relationship("PCICFlag", secondary=ObsRawPCICFlags, backref="flagged_obs")

    # Constraints
    __table_args__ = (
        UniqueConstraint('obs_time', 'history_id', 'vars_id',
                         name='time_place_variable_unique'),
    )

    # Indexes
    mod_time_idx = Index('mod_time_idx', 'mod_time')
    obs_raw_comp_idx = Index('obs_raw_comp_idx', 'obs_time', 'vars_id',
                             'history_id')
    obs_raw_history_id_idx = Index('obs_raw_history_id_idx', 'history_id')
    obs_raw_id_idx = Index('obs_raw_id_idx', 'obs_raw_id')


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
    '''This class maps to the table which records all 'flags' for observations which have been `flagged` by the
    data provider (i.e. the network) for some reason. This table records the details of the flags.
    Actual flagging is recorded in the class/table ObsRawNativeFlags.
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
    '''This class maps to the table which records all 'flags' for observations which have been flagged by PCIC
    for some reason. This table records the details of the flags.
    Actual flagging is recorded in the class/table ObsRawNativeFlags.
    '''
    __tablename__ = 'meta_pcic_flag'
    id = Column('pcic_flag_id', Integer, primary_key=True)
    name = Column('flag_name', String)
    description = Column(String)
    discard = Column(Boolean)


class DerivedValue(Base):
    __tablename__ = 'obs_derived_values'
    id = Column('obs_derived_value_id', Integer, primary_key=True)
    time = Column('value_time', DateTime)
    mod_time = Column(DateTime, nullable=False,
                      default=datetime.datetime.utcnow)
    datum = Column(Float)
    vars_id = Column(Integer, ForeignKey('meta_vars.vars_id'))
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))

    # Relationships
    history = relationship('History', backref=backref('obs_derived_values', order_by=id))
    variable = relationship('Variable', backref=backref('obs_derived_values', order_by=id))

    # Constraints
    __table_args__ = (
        UniqueConstraint('value_time', 'history_id', 'vars_id',
                         name='obs_derived_value_time_place_variable_unique'),
    )


# "Improper" views - defined in crmp repo, and accessed in ORM by referring to
# them as tables.
# DeferredBase is currently used for these views.
# When testing, not using proper views may create issues
# TODO: Implement as proper views using pycds.view_helpers

DeferredBase = declarative_base(metadata=metadata, cls=DeferredReflection)
deferred_metadata = DeferredBase.metadata


class CrmpNetworkGeoserver(DeferredBase):
    """This table maps to a convenience view that is used by geoserver for
    mapping.
    """
    __tablename__ = 'crmp_network_geoserver'
    __table_args__ = {'info': {'is_view': True}}
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


class ObsWithFlags(DeferredBase):
    '''This class maps to a convenience view that is used to construct a
    table of flagged observations; i.e. one row per observation with
    additional columns for each attached flag.
    '''
    __tablename__ = 'obs_with_flags'
    __table_args__ = {'info': {'is_view': True}}
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


# "External manual materialized views": CRMP contains a set of auxiliary tables,
# views, functions, and triggers that implement simulated or manually
# maintained materialized views (as opposed to natively supported matviews).
# For some details, see
# https://github.com/pacificclimate/crmp/tree/master/database/manual-matviews.
# The following classes map onto the manual matviews defined in CRMP.
# NOTE: There are *no* mappings in PyCDS for the infrastructure for
# implementing manual matviews.

class ObsCountPerMonthHistory(DeferredBase):
    """This class maps to a manual materialized view that is required for web
    app performance. It is used for approximating the number of
    observations which will be returned by station selection criteria.
    """
    __tablename__ = 'obs_count_per_month_history_mv'
    count = Column(Integer)
    date_trunc = Column(DateTime)
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))
    history = relationship("History")


class ClimoObsCount(DeferredBase):
    """This class maps to a manual materialized view that is required for
    web app performance. It is used for approximating the number of
    climatologies which will be returned by station selection
    criteria.
    """
    __tablename__ = 'climo_obs_count_mv'
    count = Column(BigInteger)
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))


class VarsPerHistory(Base):
    """This class maps to a manual materialized view that is required for
    web app performance. It is used to link recorded quantities (variables) to
    the station/history level, rather than just the network level
    (just because one station in the network records a quantity,
    doesn't mean that all stations in the network do). To some extent,
    this view is an add on to compensate for poor database
    normalization, but it's close enough to get by.
    """
    __tablename__ = 'vars_per_history_mv'
    # TODO: These columns are not primary keys in the CRMP database.
    #  Which is right?
    history_id = Column(Integer, ForeignKey(
        'meta_history.history_id'), primary_key=True)
    vars_id = Column(Integer, ForeignKey(
        'meta_vars.vars_id'), primary_key=True)


class CollapsedVariables(DeferredBase):
    """This class maps to a manual materialized view that supports the
    view CrmpNetworkGeoserver."""
    __tablename__ = 'collapsed_vars_mv'
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))
    vars = Column(String)
    display_names = Column(String)


class StationObservationStats(DeferredBase):
    __tablename__ = 'station_obs_stats_mv'
    station_id = Column(Integer, ForeignKey('meta_station.station_id'))
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))
    min_obs_time = Column(DateTime)
    max_obs_time = Column(DateTime)
    obs_count = BigInteger
