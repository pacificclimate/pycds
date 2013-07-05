import os.path

__all__ = ['Network', 'Variable', 'Station', 'History', 'Obs', 'CrmpNetworkGeoserver', 'ObsCountPerMonthHistory', 'test_dsn']

from sqlalchemy import Table, Column, Integer, BigInteger, Float, String, Date, DateTime, Boolean, ForeignKey, MetaData
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.orm import relationship, backref

Base = declarative_base()

class Network(Base):
    __tablename__ = 'meta_network'
    id = Column('network_id', Integer, primary_key=True)
    name = Column('network_name', String)
    long_name = Column('description', String)
    color = Column('col_hex', String)

    stations = relationship("Station", backref=backref('meta_network', order_by=id))
    variables = relationship("Variable", backref=backref('meta_network', order_by=id))

    def __str__(self):
        return '<CRMP Network %s>' % self.name


class Station(Base):
    __tablename__ = 'meta_station'
    id = Column('station_id', Integer, primary_key=True)
    native_id = Column(String)
    network_id = Column(Integer, ForeignKey('meta_network.network_id'))

    network = relationship("Network", backref=backref('meta_station', order_by=id))
    histories = relationship("History", backref=backref('meta_station', order_by=id))

    def __str__(self):
        return '<CRMP Station %s:%s>' % (self.network.name, self.native_id)

class History(Base):
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

    station = relationship("Station", backref=backref('meta_history', order_by=id))
    observations = relationship("Obs", backref=backref('meta_history', order_by=id))

association_table = Table('obs_raw_native_flags', Base.metadata,
                          Column('obs_raw_id', BigInteger, ForeignKey('obs_raw.obs_raw_id')),
                          Column('native_flag_id', Integer, ForeignKey('meta_native_flag.native_flag_id'))
                          )

class Obs(Base):
    __tablename__ = 'obs_raw'
    id = Column('obs_raw_id', BigInteger, primary_key=True)
    time = Column('obs_time', DateTime(timezone=True))
    datum = Column(Float)
    vars_id = Column(Integer, ForeignKey('meta_vars.vars_id'))
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))

    # Relationships
    history = relationship("History", backref=backref('obs_raw', order_by=id))
    variable = relationship("Variable", backref=backref('obs_raw', order_by=id))
    flags = relationship("NativeFlag", secondary=association_table, backref="flagged_obs")

class Variable(Base):
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
    __tablename__ = 'meta_native_flag'
    id = Column('native_flag_id', Integer, primary_key=True)
    name = Column('flag_name', String)
    description = Column(String)
    #network_id
    value = Column(String)
    discard = Column(Boolean)    

#class PcicFlag(Base):
DeferredBase = declarative_base(cls=DeferredReflection)

class CrmpNetworkGeoserver(DeferredBase):
    __tablename__ = 'crmp_network_geoserver'
    network_name = Column(String)
    native_id = Column(String)
    station_name = Column(String)
    elevation = Column('elev', Float)
    min_obs_time = Column(DateTime(timezone=True))
    max_obs_time = Column(DateTime(timezone=True))
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

class ObsCountPerMonthHistory(DeferredBase):
    __tablename__ = 'obs_count_per_month_history_mv'
    count = Column(Integer)
    date_trunc = Column(DateTime(timezone=True))
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))
    history = relationship("History")

class ClimoObsCount(DeferredBase):
    __tablename__ = 'climo_obs_count_mv'
    count = Column(BigInteger)
    history_id = Column(Integer, ForeignKey('meta_history.history_id'))

class VarsPerHistory(Base):
    __tablename__ = 'vars_per_history_mv'
    history_id = Column(Integer, ForeignKey('meta_history.history_id'), primary_key=True)
    vars_id = Column(Integer, ForeignKey('meta_vars.vars_id'), primary_key=True)
    
def foo():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    engine = create_engine('postgresql://hiebert@windy.pcic.uvic.ca/crmp?sslmode=require', echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

test_dsn = os.path.join(os.path.dirname(__file__), 'data','crmp.sqlite')
