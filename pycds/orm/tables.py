"""
ORM declarations for tables.

Notes:

1. Index declarations should be made *outside* of ORM classes. If not, they are
paradoxically not included in the internal list of indexes available for an
ORM class via `ORMClass.__table__.indexes`. The latter is very convenient, so
we make sure always to declare indexes outside of classes. See code below for
many examples.
"""

import datetime

from sqlalchemy import MetaData
from sqlalchemy import (
    Table,
    Column,
    Integer,
    BigInteger,
    Float,
    String,
    Date,
    Index,
)
from sqlalchemy import DateTime, Boolean, ForeignKey, Numeric, Interval
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.orm import relationship, backref, synonym
from sqlalchemy.schema import DDL, UniqueConstraint
from geoalchemy2 import Geometry

from pycds.context import get_schema_name


Base = declarative_base(metadata=MetaData(schema=get_schema_name()))
metadata = Base.metadata


class Network(Base):
    """This class maps to the table which represents various `networks` of
    data for the Climate Related Monitoring Program. There is one
    network row for each data provider, typically a BC Ministry, crown
    corporation or private company.
    """

    __tablename__ = "meta_network"
    id = Column("network_id", Integer, primary_key=True)
    name = Column("network_name", String)
    long_name = Column("description", String)
    virtual = Column(String(255))
    publish = Column(Boolean)
    color = Column("col_hex", String)
    contact_id = Column(Integer, ForeignKey("meta_contact.contact_id"))

    stations = relationship(
        "Station", order_by="Station.id", back_populates="network"
    )
    meta_station = synonym("stations")
    variables = relationship("Variable", back_populates="network")
    meta_vars = synonym("variables")
    contact = relationship("Contact", back_populates="networks")
    meta_contact = synonym("contact")  # Retain backwards compatibility

    def __str__(self):
        return "<CRMP Network %s>" % self.name


class Contact(Base):
    """This class maps to the table which represents contact people and
    representatives for the networks of the Climate Related Monitoring
    Program.
    """

    __tablename__ = "meta_contact"
    id = Column("contact_id", Integer, primary_key=True)
    name = Column("name", String)
    title = Column("title", String)
    organization = Column("organization", String)
    email = Column("email", String)
    phone = Column("phone", String)

    networks = relationship(
        "Network", order_by="Network.id", back_populates="contact"
    )


class Station(Base):
    """This class maps to the table which represents a single weather
    station. One weather station can potentially have multiple
    physical locations (though, few do in practice) and periods of
    operation
    """

    __tablename__ = "meta_station"
    id = Column("station_id", Integer, primary_key=True)
    native_id = Column(String)
    network_id = Column(Integer, ForeignKey("meta_network.network_id"))
    min_obs_time = Column(DateTime)
    max_obs_time = Column(DateTime)

    # Relationships
    network = relationship("Network", back_populates="stations")
    meta_network = synonym("network")
    histories = relationship(
        "History", order_by="History.id", back_populates="station"
    )
    meta_history = synonym("histories")  # Retain backwards compatibility

    def __str__(self):
        return "<CRMP Station %s:%s>" % (self.network.name, self.native_id)


Index("fki_meta_station_network_id_fkey", Station.network_id)


class History(Base):
    """This class maps to the table which represents a history record for
    a weather station. Since a station can potentially (and do) move
    small distances (e.g. from one end of the airport runway to
    another) or change the frequency of its observations, this table
    records the details of those changes.

    WARNING: The GeoAlchemy2 `Geometry` column (attribute `the_geom`) forces
    all reads on that column to be wrapped with Postgis function `ST_AsEWKB`.
    This may or may not be desirable for all use cases, specifically views.
    See the GeoAlchemy2 documentation for details.
    """

    __tablename__ = "meta_history"
    id = Column("history_id", Integer, primary_key=True)
    station_id = Column(
        "station_id", Integer, ForeignKey("meta_station.station_id")
    )
    station_name = Column(String)
    lon = Column(Numeric)
    lat = Column(Numeric)
    elevation = Column("elev", Float)
    sdate = Column(Date)
    edate = Column(Date)
    tz_offset = Column(Interval)
    province = Column(String)
    country = Column(String)
    comments = Column(String(255))
    freq = Column(String)
    sensor_id = Column(ForeignKey(u"meta_sensor.sensor_id"))
    the_geom = Column(Geometry("GEOMETRY", 4326))

    # Relationships
    sensor = relationship("MetaSensor")
    station = relationship("Station", back_populates="histories")
    meta_station = synonym("station")  # Retain backwards compatibility
    observations = relationship(
        "Obs", order_by="Obs.id", back_populates="history"
    )
    obs_raw = synonym("observations")  # Retain backwards compatibility
    derived_values = relationship(
        "DerivedValue", order_by="DerivedValue.id", back_populates="history"
    )
    obs_derived_values = synonym("derived_values")  # Backwards compatibility


Index("fki_meta_history_station_id_fk", History.station_id)
Index("meta_history_freq_idx", History.freq)


# Association table for Obs *--* NativeFLag
# TODO: See https://github.com/pacificclimate/pycds/issues/95
ObsRawNativeFlags = Table(
    "obs_raw_native_flags",
    Base.metadata,
    Column("obs_raw_id", BigInteger, ForeignKey("obs_raw.obs_raw_id")),
    Column(
        "native_flag_id", Integer, ForeignKey("meta_native_flag.native_flag_id")
    ),
    UniqueConstraint(
        "obs_raw_id", "native_flag_id", name="obs_raw_native_flag_unique"
    ),
    # Indexes
    Index("flag_index", "obs_raw_id"),
)


# Association table for Obs *--* PCICFLag
# TODO: See https://github.com/pacificclimate/pycds/issues/95
ObsRawPCICFlags = Table(
    "obs_raw_pcic_flags",
    Base.metadata,
    Column("obs_raw_id", BigInteger, ForeignKey("obs_raw.obs_raw_id")),
    Column("pcic_flag_id", Integer, ForeignKey("meta_pcic_flag.pcic_flag_id")),
    UniqueConstraint(
        "obs_raw_id", "pcic_flag_id", name="obs_raw_pcic_flag_unique"
    ),
    # Indexes
    Index("pcic_flag_index", "obs_raw_id"),
)


class MetaSensor(Base):
    __tablename__ = "meta_sensor"

    id = Column("sensor_id", Integer, primary_key=True)
    name = Column(String(255))


class Obs(Base):
    """This class maps to the table which records the details of weather
    observations. Each row is one single data point for one single
    quantity.
    """

    __tablename__ = "obs_raw"
    id = Column("obs_raw_id", BigInteger, primary_key=True)
    time = Column("obs_time", DateTime)
    mod_time = Column(
        DateTime, nullable=False, default=datetime.datetime.utcnow
    )
    datum = Column(Float)
    vars_id = Column(Integer, ForeignKey("meta_vars.vars_id"))
    history_id = Column(Integer, ForeignKey("meta_history.history_id"))

    # Relationships
    history = relationship(
        "History", back_populates="observations"
    )
    meta_history = synonym("history")  # Retain backwards compatibility
    variable = relationship("Variable", back_populates="obs")
    meta_vars = synonym("variable")  # To keep backwards compatibility
    native_flags = relationship(
        "NativeFlag", secondary=ObsRawNativeFlags, back_populates="flagged_obs"
    )
    flags = synonym("native_flags")  # Retain backwards compatibility
    pcic_flags = relationship(
        "PCICFlag", secondary=ObsRawPCICFlags, backref="flagged_obs"
    )

    # Constraints
    __table_args__ = (
        UniqueConstraint(
            "obs_time",
            "history_id",
            "vars_id",
            name="time_place_variable_unique",
        ),
    )


Index("mod_time_idx", Obs.mod_time)
Index("obs_raw_comp_idx", Obs.time, Obs.vars_id, Obs.history_id)
Index("obs_raw_history_id_idx", Obs.history_id)
Index("obs_raw_id_idx", Obs.id)


class TimeBound(Base):
    """This class maps to a table which records the start and end times
    for an observation on a variable that spans a changeable time period,
    rather than a variable that is at a point in time or which spans a
    fixed, known time period and is represented by a single standardized point
    in that time period). Variable time periods are typically for climatologies
    and cumulative precipitations.
    """

    __tablename__ = "time_bounds"
    obs_raw_id = Column(
        Integer, ForeignKey("obs_raw.obs_raw_id"), primary_key=True
    )
    start = Column(DateTime)
    end = Column(DateTime)


class Variable(Base):
    """This class maps to the table which records the details of the
    physical quantities which are recorded by the weather stations.
    """

    __tablename__ = "meta_vars"
    id = Column("vars_id", Integer, primary_key=True)
    name = Column("net_var_name", String)
    unit = Column(String)
    standard_name = Column(String)
    cell_method = Column(String)
    precision = Column(Float)
    description = Column("long_description", String)
    display_name = Column(String)
    short_name = Column(String)
    network_id = Column(Integer, ForeignKey("meta_network.network_id"))

    # Relationships
    network = relationship(
        "Network", back_populates="variables"
    )
    meta_network = synonym("network")
    obs = relationship("Obs", order_by="Obs.id", back_populates="variable")
    observations = synonym("obs")  # Better name
    obs_raw = synonym("obs")  # To keep backwards compatibility
    derived_values = relationship(
        "DerivedValue", order_by="DerivedValue.id", back_populates="variable"
    )
    obs_derived_values = synonym("derived_values") # Backwards compatibility

    def __repr__(self):
        return "<{} id={id} name='{name}' standard_name='{standard_name}' cell_method='{cell_method}' network_id={network_id}>".format(
            self.__class__.__name__, **self.__dict__
        )


Index("fki_meta_vars_network_id_fkey", Variable.network_id)


class NativeFlag(Base):
    """This class maps to the table which records all 'flags' for observations which have been `flagged` by the
    data provider (i.e. the network) for some reason. This table records the details of the flags.
    Actual flagging is recorded in the class/table ObsRawNativeFlags.
    """

    __tablename__ = "meta_native_flag"
    id = Column("native_flag_id", Integer, primary_key=True)
    name = Column("flag_name", String)
    description = Column(String)
    network_id = Column(Integer, ForeignKey("meta_network.network_id"))
    value = Column(String)
    discard = Column(Boolean)

    network = relationship(
        "Network", backref=backref("meta_native_flag", order_by=id)
    )
    flagged_obs = relationship(
        "Obs", secondary=ObsRawNativeFlags, back_populates="native_flags"
    )

    # Constraints
    __table_args__ = (
        UniqueConstraint("network_id", "value", name="meta_native_flag_unique"),
    )


class PCICFlag(Base):
    """This class maps to the table which records all 'flags' for observations which have been flagged by PCIC
    for some reason. This table records the details of the flags.
    Actual flagging is recorded in the class/table ObsRawNativeFlags.
    """

    __tablename__ = "meta_pcic_flag"
    id = Column("pcic_flag_id", Integer, primary_key=True)
    name = Column("flag_name", String)
    description = Column(String)
    discard = Column(Boolean)


class DerivedValue(Base):
    __tablename__ = "obs_derived_values"
    id = Column("obs_derived_value_id", Integer, primary_key=True)
    time = Column("value_time", DateTime)
    mod_time = Column(
        DateTime, nullable=False, default=datetime.datetime.utcnow
    )
    datum = Column(Float)
    vars_id = Column(Integer, ForeignKey("meta_vars.vars_id"))
    history_id = Column(Integer, ForeignKey("meta_history.history_id"))

    # Relationships
    history = relationship("History", back_populates="derived_values")
    variable = relationship("Variable", back_populates="derived_values")

    # Constraints
    __table_args__ = (
        UniqueConstraint(
            "value_time",
            "history_id",
            "vars_id",
            name="obs_derived_value_time_place_variable_unique",
        ),
    )


class ObsCountPerMonthHistory(Base):
    """This class maps to a manual materialized view that is required for web
    app performance. It is used for approximating the number of
    observations which will be returned by station selection criteria.
    """

    __tablename__ = "obs_count_per_month_history_mv"
    count = Column(Integer)
    date_trunc = Column(DateTime, primary_key=True)
    history_id = Column(
        Integer, ForeignKey("meta_history.history_id"), primary_key=True
    )

    # Relationships
    history = relationship("History")


Index(
    "obs_count_per_month_history_idx",
    ObsCountPerMonthHistory.date_trunc,
    ObsCountPerMonthHistory.history_id,
)


class ClimoObsCount(Base):
    """This class maps to a manual materialized view that is required for
    web app performance. It is used for approximating the number of
    climatologies which will be returned by station selection
    criteria.
    """

    __tablename__ = "climo_obs_count_mv"
    count = Column(BigInteger)
    history_id = Column(
        Integer, ForeignKey("meta_history.history_id"), primary_key=True
    )


Index("climo_obs_count_idx", ClimoObsCount.history_id)


class CollapsedVariables(Base):
    """This class maps to a manual materialized view that supports the
    view CrmpNetworkGeoserver."""

    __tablename__ = "collapsed_vars_mv"
    history_id = Column(
        Integer, ForeignKey("meta_history.history_id"), primary_key=True
    )
    vars = Column(String)
    display_names = Column(String)


Index("collapsed_vars_idx", CollapsedVariables.history_id)


class StationObservationStats(Base):
    __tablename__ = "station_obs_stats_mv"
    station_id = Column(
        Integer, ForeignKey("meta_station.station_id"), primary_key=True
    )
    history_id = Column(Integer, ForeignKey("meta_history.history_id"))
    min_obs_time = Column(DateTime)
    max_obs_time = Column(DateTime)
    obs_count = Column(BigInteger)


Index(
    "station_obs_stats_mv_idx",
    StationObservationStats.min_obs_time,
    StationObservationStats.max_obs_time,
    StationObservationStats.obs_count,
    StationObservationStats.station_id,
    StationObservationStats.history_id,
)
