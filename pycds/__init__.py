import re
import os.path
from pkg_resources import resource_filename

__all__ = ['Network', 'Contact', 'Variable', 'Station', 'History', 'Obs', 'CrmpNetworkGeoserver', 'ObsCountPerMonthHistory', 'VarsPerHistory', 'ObsWithFlags', 'NativeFlag', 'test_dsn', 'test_session']

from sqlalchemy.types import DateTime
from sqlalchemy.dialects.sqlite import DATETIME, VARCHAR, INTEGER
from sqlalchemy import Table, Column, Integer, BigInteger, Float, String, Date, Boolean, ForeignKey, MetaData
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint
from geoalchemy import GeometryColumn, Point
from geoalchemy import Geometry

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

MyBigInteger = BigInteger().with_variant(INTEGER(), 'sqlite')
MyGeometry = Geometry('POINT').with_variant(VARCHAR(), 'sqlite')
MyDateTime = DateTime(timezone=True).with_variant(
    DATETIME(storage_format="%(year)04d-%(month)02d-%(day)02dT%(hour)02d:%(minute)02d:%(second)02d",
             regexp=r"(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)",
             ), "sqlite")

Base = declarative_base()

class Network(Base):
    '''This class maps to the table which represents various `networks` of data for the Climate Related Monitoring Program. There is one network row for each data provider, typically a BC Ministry, crown corporation or private company.
    '''
    __tablename__ = 'meta_network'
    id = Column('network_id', Integer, primary_key=True)
    name = Column('network_name', String)
    long_name = Column('description', String)
    color = Column('col_hex', String)
    contact_id = Column(Integer, ForeignKey('meta_contact.contact_id'))

    stations = relationship("Station", backref=backref('meta_network', order_by=id))
    variables = relationship("Variable", backref=backref('meta_network', order_by=id))

    def __str__(self):
        return '<CRMP Network %s>' % self.name

class Contact(Base):
    '''This class maps to the table which represents contact people and representatives for the networks of the Climate Related Monitoring Program.
    '''
    __tablename__ = 'meta_contact'
    id = Column('contact_id', Integer, primary_key=True)
    name = Column('name', String)
    title = Column('title', String)
    organization = Column('organization', String)
    email = Column('email', String)
    phone = Column('phone', String)

    networks = relationship("Network", backref=backref('meta_contact', order_by=id))
    
class Station(Base):
    '''This class maps to the table which represents a single weather station. One weather station can potentially have multiple physical locations (though, few do in practice) and periods of operation
    '''
    __tablename__ = 'meta_station'
    id = Column('station_id', Integer, primary_key=True)
    native_id = Column(String)
    network_id = Column(Integer, ForeignKey('meta_network.network_id'))

    network = relationship("Network", backref=backref('meta_station', order_by=id))
    histories = relationship("History", backref=backref('meta_station', order_by=id))

    def __str__(self):
        return '<CRMP Station %s:%s>' % (self.network.name, self.native_id)

class History(Base):
    '''This class maps to the table which represents a history record for a weather station. Since a station can potentially (and do) move small distances (e.g. from one end of the airport runway to another) or change the frequency of its observations, this table records the details of those changes.
    '''
    __tablename__ = 'meta_history'
    id = Column('history_id', Integer, primary_key=True)
    station_id = Column('station_id', Integer, ForeignKey('meta_station.station_id'))
    station_name = Column(String)
    elevation = Column('elev', Float)
    sdate = Column(Date)
    edate = Column(Date)
    province = Column(String)
    country = Column(String)
    freq = Column(String)
    the_geom = Column(MyGeometry)

    station = relationship("Station", backref=backref('meta_history', order_by=id))
    observations = relationship("Obs", backref=backref('meta_history', order_by=id))

association_table = Table('obs_raw_native_flags', Base.metadata,
                          Column('obs_raw_id', BigInteger, ForeignKey('obs_raw.obs_raw_id')),
                          Column('native_flag_id', Integer, ForeignKey('meta_native_flag.native_flag_id')),
                          UniqueConstraint('obs_raw_id', 'native_flag_id', name='obs_raw_native_flag_unique')
)

class Obs(Base):
    '''This class maps to the table which records the details of weather observations. Each row is one single data point for one single quantity.
    '''
    __tablename__ = 'obs_raw'
    id = Column('obs_raw_id', MyBigInteger, primary_key=True)
    time = Column('obs_time', MyDateTime)
    datum = Column(Float)
    vars_id = Column(Integer, ForeignKey('meta_vars.vars_id'))
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))

    # Relationships
    history = relationship("History", backref=backref('obs_raw', order_by=id))
    variable = relationship("Variable", backref=backref('obs_raw', order_by=id))
    flags = relationship("NativeFlag", secondary=association_table, backref="flagged_obs")

    # Constraints
    __table_args__ = (
        UniqueConstraint('obs_time', 'history_id', 'vars_id', name='time_place_variable_unique'),
    )
    
class Variable(Base):
    '''This class maps to the table which records the details of the physical quantities which are recorded by the weather stations.
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

    network = relationship("Network", backref=backref('meta_vars', order_by=id))
    obs = relationship("Obs", backref=backref('meta_vars', order_by=id))

class NativeFlag(Base):
    '''This class maps to the table which records all observations which have been `flagged` by the data provider (i.e. the network) for some reason. This table records the details of the flags.
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
        UniqueConstraint('network_id', 'value', name='meta_native_flag_unique'),
    )
    
#class PcicFlag(Base):
DeferredBase = declarative_base(cls=DeferredReflection)

class CrmpNetworkGeoserver(DeferredBase):
    '''This table maps to a convenience view that is used by geoserver for mapping.
    '''
    __tablename__ = 'crmp_network_geoserver'
    network_name = Column(String)
    native_id = Column(String)
    station_name = Column(String)
    elevation = Column('elev', Float)
    min_obs_time = Column(MyDateTime)
    max_obs_time = Column(MyDateTime)
    freq = Column(String)
    province = Column(String)
    station_id = Column(Integer, ForeignKey('meta_station.station_id'))
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))
    country = Column(String)
    comments = Column(String)
    network_id = Column(Integer)
    col_hex = Column(String)
    vars = Column(String)
    display_names = Column(String)
    the_geom = Column(MyGeometry)

class ObsCountPerMonthHistory(DeferredBase):
    '''This class maps to a materialized view that is required for web app performance. It is used for approximating the number of observations which will be returned by station selection criteria.
    '''
    __tablename__ = 'obs_count_per_month_history_mv'
    count = Column(Integer)
    date_trunc = Column(MyDateTime)
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))
    history = relationship("History")

class ClimoObsCount(DeferredBase):
    '''This class maps to a materialized view that is required for web app performance. It is used for approximating the number of climatologies which will be returned by station selection criteria.
    '''
    __tablename__ = 'climo_obs_count_mv'
    count = Column(BigInteger)
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))

class VarsPerHistory(Base):
    '''This class maps to a materialized view that is required for web app performance. It is used to link recorded quantities (variables) to the station/history level, rather than just the network level (just because one station in the network records a quantity, doesn't mean that all stations in the network do). To some extent, this view is an add on to compensate for poor database normalization, but it's close enough to get by.
    '''
    __tablename__ = 'vars_per_history_mv'
    history_id = Column(Integer, ForeignKey('meta_history.history_id'), primary_key=True)
    vars_id = Column(Integer, ForeignKey('meta_vars.vars_id'), primary_key=True)

class ObsWithFlags(Base):
    '''This class maps to a convenience view that is used to construct a table of flagged observations; i.e. one row per observation with additional columns for each attached flag.
    '''
    __tablename__ = 'obs_with_flags'
    vars_id = Column(Integer, ForeignKey('meta_vars.vars_id'))
    network_id = Column(Integer, ForeignKey('meta_network.network_id'))
    unit = Column(String)
    standard_name = Column(String)
    cell_method = Column(String)
    net_var_name = Column(String)
    obs_raw_id = Column(Integer, ForeignKey('obs_raw.obs_raw_id'), primary_key=True)
    station_id = Column(Integer, ForeignKey('meta_station.station_id'))
    obs_time = Column(MyDateTime)
    mod_time = Column(MyDateTime)
    datum = Column(Float)
    native_flag_id = Column(Integer, ForeignKey('obs_raw_native_flags.native_flag_id'))
    flag_name = Column(String)
    description = Column(String)
    flag_value = Column(String)
    
test_dsn = 'sqlite+pysqlite:///{0}'.format(resource_filename('pycds', 'data/crmp.sqlite'))

def test_session():
    '''This creates a testing database session that can be used as a test fixture. It uses a subset testing sqlite database and loads the spatialite extension for use of the geometry functionality.
    '''
    from pysqlite2 import dbapi2 as sqlite
    dsn = 'sqlite:///{0}'.format(resource_filename('pycds', 'data/crmp.sqlite'))
    engine = create_engine(dsn, module=sqlite, echo=True)
    engine.echo = True
    Session = sessionmaker(bind=engine)
    sesh = Session()
    sesh.connection().connection.enable_load_extension(True)
    sesh.execute("select load_extension('libspatialite.so')")
    return sesh
