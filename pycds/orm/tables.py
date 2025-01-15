"""
ORM declarations for tables.

Notes:

1. Index declarations should be made *outside* of ORM classes. If not, they are
paradoxically not included in the internal list of indexes available for an
ORM class via `ORMClass.__table__.indexes`. The latter is very convenient, so
we make sure always to declare indexes outside of classes. See code below for
many examples.

TODO: This behaviour is different in 2.0 and should be reviewed during that upgrade
2. We prefer to declare relationships using `back_populates=`, *not* using
`backref=`. Using `back_populates` is slightly redundant, but the redundancy
ensures that each class explicitly names all its relationship attributes.
(Using `backref` requires one to scan the all other classes to
find all the relationship attributes that a given class may have.)
"""

import datetime

from sqlalchemy import MetaData, func, literal_column
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
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import relationship, synonym, declarative_base
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.schema import CheckConstraint
from geoalchemy2 import Geometry

from sqlalchemy.dialects.postgresql import CITEXT as CIText

from pycds.alembic.change_history_utils import hx_table_name
from pycds.context import get_schema_name


Base = declarative_base(metadata=MetaData(schema=get_schema_name()))
metadata = Base.metadata


# string templating functions for check functions applied against multiple columns
def no_newline_ck_name(column):
    return f"ck_{column}_no_newlines"


def no_newline_ck_check(column):
    return f"{column} !~ '[\r\n]'"


class Network(Base):
    """This class maps to the table which represents various `networks` of
    data for the Climate Related Monitoring Program. There is one
    network row for each data provider, typically a BC Ministry, crown
    corporation or private company.
    """

    __tablename__ = "meta_network"

    # Columns
    id = Column("network_id", Integer, primary_key=True)
    name = Column("network_name", String)
    long_name = Column("description", String)
    virtual = Column(String(255))
    publish = Column(Boolean)
    color = Column("col_hex", String)
    contact_id = Column(Integer, ForeignKey("meta_contact.contact_id"))
    mod_time = Column(DateTime, nullable=False, server_default=func.now())
    mod_user = Column(
        String(64), nullable=False, server_default=literal_column("current_user")
    )

    # Relationships
    stations = relationship(
        "Station",
        order_by="Station.id",
        back_populates="network",
        cascade_backrefs=False,
    )
    meta_station = synonym("stations")
    variables = relationship(
        "Variable", back_populates="network", cascade_backrefs=False
    )
    meta_vars = synonym("variables")
    contact = relationship("Contact", back_populates="networks", cascade_backrefs=False)
    meta_contact = synonym("contact")  # Retain backwards compatibility
    native_flags = relationship(
        "NativeFlag",
        order_by="NativeFlag.id",
        back_populates="network",
        cascade_backrefs=False,
    )
    meta_native_flag = synonym("native_flags")  # Retain backwards compatibility

    def __str__(self):
        return f"<CRMP Network {self.name}>"


class NetworkHistory(Base):
    """This class maps to the history table for table Network."""

    __tablename__ = hx_table_name(Network.__tablename__, schema=None)

    # Columns
    network_id = Column(Integer, nullable=False, index=True)
    name = Column("network_name", String)
    long_name = Column("description", String)
    virtual = Column(String(255))
    publish = Column(Boolean)
    color = Column("col_hex", String)
    contact_id = Column(Integer)
    mod_time = Column(DateTime, nullable=False, server_default=func.now())
    mod_user = Column(
        String(64), nullable=False, server_default=literal_column("current_user")
    )
    deleted = Column(Boolean, default=False)
    meta_network_hx_id = Column(Integer, primary_key=True)

    def __str__(self):
        return f"<CRMP NetworkHistory {self.name}>"


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
        "Network",
        order_by="Network.id",
        back_populates="contact",
        cascade_backrefs=False,
    )


class Station(Base):
    """This class maps to the table which represents a single weather
    station. One weather station can potentially have multiple
    physical locations (though, few do in practice) and periods of
    operation
    """

    __tablename__ = "meta_station"

    # Columns
    id = Column("station_id", Integer, primary_key=True)
    native_id = Column(String)
    network_id = Column(Integer, ForeignKey("meta_network.network_id"))
    min_obs_time = Column(DateTime)
    max_obs_time = Column(DateTime)
    publish = Column(Boolean, default=True, nullable=False)
    mod_time = Column(DateTime, nullable=False, server_default=func.now())
    mod_user = Column(
        String(64), nullable=False, server_default=literal_column("current_user")
    )

    # Relationships
    network = relationship("Network", back_populates="stations", cascade_backrefs=False)
    meta_network = synonym("network")
    histories = relationship(
        "History",
        order_by="History.id",
        back_populates="station",
        cascade_backrefs=False,
    )
    meta_history = synonym("histories")  # Retain backwards compatibility

    def __str__(self):
        return f"<CRMP Station {self.network.name}:{self.native_id}>"


Index("fki_meta_station_network_id_fkey", Station.network_id)


class StationHistory(Base):
    """
    This class maps to the table containing the change history for Station/meta_station.
    """

    __tablename__ = hx_table_name(Station.__tablename__, schema=None)

    # Columns
    station_id = Column(Integer, nullable=False, index=True)
    native_id = Column(String)
    network_id = Column(Integer)
    min_obs_time = Column(DateTime)
    max_obs_time = Column(DateTime)
    publish = Column(Boolean, default=True, nullable=False)
    mod_time = Column(DateTime, nullable=False, server_default=func.now())
    mod_user = Column(
        String(64), nullable=False, server_default=literal_column("current_user")
    )
    deleted = Column(Boolean, default=False)
    meta_station_hx_id = Column(Integer, primary_key=True)
    meta_network_hx_id = Column(
        Integer, ForeignKey("meta_network_hx.meta_network_hx_id")
    )

    def __str__(self):
        return f"<CRMP StationHistory {self.network.name}:{self.native_id}>"


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

    # Columns
    id = Column("history_id", Integer, primary_key=True)
    station_id = Column("station_id", Integer, ForeignKey("meta_station.station_id"))
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
    sensor_id = Column(ForeignKey("meta_sensor.sensor_id"))
    the_geom = Column(Geometry("GEOMETRY", 4326))
    mod_time = Column(DateTime, nullable=False, server_default=func.now())
    mod_user = Column(
        String(64), nullable=False, server_default=literal_column("current_user")
    )

    # Relationships
    sensor = relationship("MetaSensor")
    station = relationship(
        "Station", back_populates="histories", cascade_backrefs=False
    )
    meta_station = synonym("station")  # Retain backwards compatibility
    observations = relationship("Obs", back_populates="history", cascade_backrefs=False)
    obs_raw = synonym("observations")  # Retain backwards compatibility
    derived_values = relationship(
        "DerivedValue", back_populates="history", cascade_backrefs=False
    )
    obs_derived_values = synonym("derived_values")  # Backwards compatibility


Index("fki_meta_history_station_id_fk", History.station_id)
Index("meta_history_freq_idx", History.freq)


class HistoryHistory(Base):
    """This class maps to the history table for table History.
    Yes, the naming is a bit awkward."""

    __tablename__ = hx_table_name(History.__tablename__, schema=None)

    # Columns
    history_id = Column(Integer, nullable=False, index=True)
    station_id = Column("station_id", Integer)
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
    sensor_id = Column(Integer)
    the_geom = Column(Geometry("GEOMETRY", 4326))
    mod_time = Column(DateTime, nullable=False, server_default=func.now())
    mod_user = Column(
        String(64), nullable=False, server_default=literal_column("current_user")
    )
    deleted = Column(Boolean, default=False)
    meta_history_hx_id = Column(Integer, primary_key=True)
    meta_station_hx_id = Column(
        Integer, ForeignKey("meta_station_hx.meta_station_hx_id")
    )


# Association table for Obs *--* NativeFLag
# TODO: See https://github.com/pacificclimate/pycds/issues/95
ObsRawNativeFlags = Table(
    "obs_raw_native_flags",
    Base.metadata,
    Column("obs_raw_id", BigInteger, ForeignKey("obs_raw.obs_raw_id"), nullable=False),
    Column(
        "native_flag_id",
        Integer,
        ForeignKey("meta_native_flag.native_flag_id"),
        nullable=False,
    ),
    UniqueConstraint("obs_raw_id", "native_flag_id", name="obs_raw_native_flag_unique"),
    # Indexes
    Index("flag_index", "obs_raw_id"),
)


# Association table for Obs *--* PCICFLag
# TODO: See https://github.com/pacificclimate/pycds/issues/95
ObsRawPCICFlags = Table(
    "obs_raw_pcic_flags",
    Base.metadata,
    Column("obs_raw_id", BigInteger, ForeignKey("obs_raw.obs_raw_id"), nullable=False),
    Column(
        "pcic_flag_id",
        Integer,
        ForeignKey("meta_pcic_flag.pcic_flag_id"),
        nullable=False,
    ),
    UniqueConstraint("obs_raw_id", "pcic_flag_id", name="obs_raw_pcic_flag_unique"),
    # Indexes
    Index("pcic_flag_index", "obs_raw_id"),
)


class MetaSensor(Base):
    __tablename__ = "meta_sensor"

    id = Column("sensor_id", Integer, primary_key=True)
    name = Column(String(255))


class Obs(Base):
    """This class maps to the table which records (raw) weather observations.
    Each row is a single observation for a specific time (`time`), place (`history_id`),
    and variable (`vars_id`).
    """

    __tablename__ = "obs_raw"
    id = Column("obs_raw_id", BigInteger, primary_key=True)
    time = Column("obs_time", DateTime)
    mod_time = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    datum = Column(Float)
    vars_id = Column(Integer, ForeignKey("meta_vars.vars_id"))
    history_id = Column(Integer, ForeignKey("meta_history.history_id"))

    # Relationships
    history = relationship(
        "History", back_populates="observations", cascade_backrefs=False
    )
    meta_history = synonym("history")  # Retain backwards compatibility
    variable = relationship("Variable", back_populates="obs", cascade_backrefs=False)
    meta_vars = synonym("variable")  # To keep backwards compatibility
    native_flags = relationship(
        "NativeFlag",
        secondary=ObsRawNativeFlags,
        back_populates="flagged_obs",
        cascade_backrefs=False,
    )
    flags = synonym("native_flags")  # Retain backwards compatibility
    pcic_flags = relationship(
        "PCICFlag",
        secondary=ObsRawPCICFlags,
        back_populates="flagged_obs",
        cascade_backrefs=False,
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


class ObsHistory(Base):
    """This class maps to the history table for table Obs."""

    __tablename__ = hx_table_name(Obs.__tablename__, schema=None)

    obs_raw_id = Column(BigInteger)
    time = Column("obs_time", DateTime)
    mod_time = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    datum = Column(Float)
    vars_id = Column(Integer, ForeignKey("meta_vars.vars_id"))
    history_id = Column(Integer, ForeignKey("meta_history.history_id"))
    mod_user = Column(
        String(64), nullable=False, server_default=literal_column("current_user")
    )
    deleted = Column(Boolean, default=False)
    obs_raw_hx_id = Column(BigInteger, primary_key=True)
    meta_history_hx_id = Column(
        Integer, ForeignKey("meta_history_hx.meta_history_hx_id")
    )
    meta_vars_hx_id = Column(Integer, ForeignKey("meta_vars_hx.meta_vars_hx_id"))


class TimeBound(Base):
    """This class maps to a table which records the start and end times
    for an observation on a variable that spans a changeable time period,
    rather than a variable that is at a point in time or which spans a
    fixed, known time period and is represented by a single standardized point
    in that time period). Variable time periods are typically for climatologies
    and cumulative precipitations.
    """

    __tablename__ = "time_bounds"
    obs_raw_id = Column(Integer, ForeignKey("obs_raw.obs_raw_id"), primary_key=True)
    start = Column(DateTime)
    end = Column(DateTime)


class Variable(Base):
    """This class maps to the table which records the details of the
    physical quantities which are recorded by the weather stations.
    """

    __tablename__ = "meta_vars"

    # Columns
    id = Column("vars_id", Integer, primary_key=True)
    name = Column("net_var_name", CIText())
    unit = Column(String)
    standard_name = Column(String, nullable=False)
    cell_method = Column(String, nullable=False)
    precision = Column(Float)
    description = Column("long_description", String)
    display_name = Column(String, nullable=False)
    short_name = Column(String)
    network_id = Column(Integer, ForeignKey("meta_network.network_id"))
    mod_time = Column(DateTime, nullable=False, server_default=func.now())
    mod_user = Column(
        String(64), nullable=False, server_default=literal_column("current_user")
    )

    # Relationships
    network = relationship(
        "Network", back_populates="variables", cascade_backrefs=False
    )
    meta_network = synonym("network")
    obs = relationship("Obs", back_populates="variable", cascade_backrefs=False)
    observations = synonym("obs")  # Better name
    obs_raw = synonym("obs")  # To keep backwards compatibility
    derived_values = relationship(
        "DerivedValue", back_populates="variable", cascade_backrefs=False
    )
    obs_derived_values = synonym("derived_values")  # Backwards compatibility

    _newline_checked_columns_ = [
        "unit",
        "standard_name",
        "cell_method",
        "display_name",
        "short_name",
        "long_description",
    ]
    # Constraints
    __table_args__ = (
        UniqueConstraint(
            "network_id", "net_var_name", name="network_variable_name_unique"
        ),
        # Values should conform to valid postgres identifiers:
        # https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        CheckConstraint(
            "net_var_name ~ '^[a-zA-Z_][a-zA-Z0-9_$]*$'",
            name="ck_net_var_name_valid_identifier",
        ),
    ) + tuple(
        CheckConstraint(no_newline_ck_check(column), name=no_newline_ck_name(column))
        for column in _newline_checked_columns_
    )

    def __repr__(self):
        return "<{} id={id} name='{name}' standard_name='{standard_name}' cell_method='{cell_method}' network_id={network_id}>".format(
            self.__class__.__name__, **self.__dict__
        )


Index("fki_meta_vars_network_id_fkey", Variable.network_id)


class VariableHistory(Base):
    """This class maps to the history table for table Variable."""

    __tablename__ = hx_table_name(Variable.__tablename__, schema=None)

    # Columns
    vars_id = Column(Integer, nullable=False, index=True)
    name = Column("net_var_name", CIText())
    unit = Column(String)
    standard_name = Column(String, nullable=False)
    cell_method = Column(String, nullable=False)
    precision = Column(Float)
    description = Column("long_description", String)
    display_name = Column(String, nullable=False)
    short_name = Column(String)
    network_id = Column(Integer)
    mod_time = Column(DateTime, nullable=False, server_default=func.now())
    mod_user = Column(
        String(64), nullable=False, server_default=literal_column("current_user")
    )
    deleted = Column(Boolean, default=False)
    meta_vars_hx_id = Column(Integer, primary_key=True)
    meta_network_hx_id = Column(
        Integer, ForeignKey("meta_network_hx.meta_network_hx_id")
    )


class NativeFlag(Base):
    """This class maps to the table which records all 'flags' for observations
    which have been `flagged` by the data provider (i.e. the network) for some
    reason. This table records the details of the flags. Actual flagging is
    recorded in the class/table ObsRawNativeFlags.
    """

    __tablename__ = "meta_native_flag"
    id = Column("native_flag_id", Integer, primary_key=True)
    name = Column("flag_name", String)
    description = Column(String)
    network_id = Column(Integer, ForeignKey("meta_network.network_id"))
    value = Column(String)
    discard = Column(Boolean)

    network = relationship("Network", back_populates="native_flags")
    flagged_obs = relationship(
        "Obs",
        secondary=ObsRawNativeFlags,
        back_populates="native_flags",
        cascade_backrefs=False,
    )

    # Constraints
    __table_args__ = (
        UniqueConstraint("network_id", "value", name="meta_native_flag_unique"),
    )


class PCICFlag(Base):
    """This class maps to the table which records all 'flags' for observations
    which have been flagged by PCIC for some reason. This table records the
    details of the flags. Actual flagging is recorded in the class/table
    ObsRawPCICFlags.
    """

    __tablename__ = "meta_pcic_flag"
    id = Column("pcic_flag_id", Integer, primary_key=True)
    name = Column("flag_name", String)
    description = Column(String)
    discard = Column(Boolean)

    flagged_obs = relationship(
        "Obs",
        secondary=ObsRawPCICFlags,
        back_populates="pcic_flags",
        cascade_backrefs=False,
    )


class DerivedValue(Base):
    __tablename__ = "obs_derived_values"
    id = Column("obs_derived_value_id", Integer, primary_key=True)
    time = Column("value_time", DateTime)
    mod_time = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    datum = Column(Float)
    vars_id = Column(Integer, ForeignKey("meta_vars.vars_id"))
    history_id = Column(Integer, ForeignKey("meta_history.history_id"))

    # Relationships
    history = relationship(
        "History", back_populates="derived_values", cascade_backrefs=False
    )
    variable = relationship(
        "Variable", back_populates="derived_values", cascade_backrefs=False
    )

    # Constraints
    __table_args__ = (
        UniqueConstraint(
            "value_time",
            "history_id",
            "vars_id",
            name="obs_derived_value_time_place_variable_unique",
        ),
    )


class ClimatologicalStation(Base):
    # TODO: Columns in this table parallel those in meta_station and meta_history.
    # They differ in the following ways, which may be questioned:
    #
    # - String types do not provide lengths. All strings are ultimately variable
    #   length in PG and there seems no reason to limit them. (Except perhaps to
    #   prevent entry of erroneous values that just happen to be very long?)
    #
    # - None are nullable. In contrast, most in the model tables are.
    __tablename__ = "meta_climatological_station"

    climatological_station_id = Column(Integer, primary_key=True)
    station_type = Column(String, nullable=False)  # TODO: Use Enumeration type?
    basin_id = Column(Integer, nullable=False)  # TODO: Same as basin id in SCIP?
    station_name = Column(String, nullable=False)
    province = Column(String, nullable=False)
    country = Column(String, nullable=False)
    tz_offset = Column(Interval, nullable=False)
    comments = Column(String, nullable=False)
    location = Column(Geometry("GEOMETRY", 4326))
    pass


class ClimatologicalStationXHistory(Base):
    __tablename__ = "meta_climatological_station_x_meta_history"
    climatological_station_id = Column(
        Integer,
        ForeignKey("meta_climatological_station.climatological_station_id"),
        primary_key=True,
    )
    history_id = Column(
        Integer,
        ForeignKey("meta_history.history_id"),
        primary_key=True,
    )


class ClimatologicalVariable(Base):
    # TODO: Columns in this table parallel those in meta_station and meta_history.
    # They differ in the following ways, which may be questioned:
    #
    # - String types do not provide lengths. All strings are ultimately variable
    #   length in PG and there seems no reason to limit them. (Except perhaps to
    #   prevent entry of erroneous values that just happen to be very long?)
    #
    # - None are nullable. In contrast, most in the model tables are.
    __tablename__ = "meta_climatological_variable"
    
    climatological_variable_id = Column(Integer, primary_key=True)
    # TODO: Duration can be computed from climatology_bounds. Do this with a provided
    #  function or store in separate column (this one)?
    #  In either case, represent value as an enumeration type?
    duration = Column(String, nullable=False)  # Use enumeration type?
    # climatology_bounds corresponds to the attribute of the same name defined in
    # CF Metadata Standards, 7.4 Climatological Statistics
    climatology_bounds = Column(ARRAY(String, dimensions=2), nullable=False)
    num_years = Column(Integer, nullable=False)
    unit = Column(String, nullable=False)
    precision = Column(String, nullable=False)  # Type? Utility???
    standard_name = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    short_name = Column(String, nullable=False)
    cell_methods = Column(String, nullable=False)
    net_var_name = Column(CIText(), nullable=False)


class ClimatologicalValue(Base):
    # TODO: Columns in this table parallel those in obs_raw.
    # They differ in the following ways, which may be questioned:
    #
    # - None are nullable. In contrast, most in the model tables are.
    __tablename__ = "climatological_value"

    climatological_value_id = Column(Integer, primary_key=True)
    mod_time = Column(DateTime, nullable=False)
    datum_time = Column(DateTime, nullable=False)
    datum = Column(Float, nullable=False)
    num_contributing_years = Column(Integer, nullable=False)
    climatological_variable_id = Column(Integer, ForeignKey("meta_climatological_value.climatological_value_id"))
