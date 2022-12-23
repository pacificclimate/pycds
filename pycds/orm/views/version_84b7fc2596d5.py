"""
Utility views.

WARNING: The `History` class defines column `the_geom` using GeoAlchemy2
data type `Geometry`. This forces
all reads on that column to be wrapped with Postgis function `ST_AsEWKB`.
This may or may not be desirable for all use cases, specifically views.
If views are behaving oddly with respect to geometry, this is worth looking at.
"""

from sqlalchemy import (
    func,
    Column,
    Integer,
    String,
    Numeric,
    Float,
    Date,
    DateTime,
    BigInteger,
    Interval,
)
from sqlalchemy.orm import Query
from geoalchemy2 import Geometry

from pycds.orm.tables import (
    Network,
    Station,
    History,
    Variable,
    Obs,
    StationObservationStats,
    CollapsedVariables,
)
from pycds.alembic.extensions.replaceable_objects import ReplaceableView
from pycds.orm.view_base import Base


class CrmpNetworkGeoserver(Base, ReplaceableView):
    """
    This view is used by the PDP Geoserver backend for generating station
    map layers.

    Note: In this class (which maps a table to the view), a primary key must be
    declared (SQLAlchemy requirement). A view cannot have a PK, but this table
    declaration does not affect the view.
    """

    __tablename__ = "crmp_network_geoserver"

    network_name = Column(String)
    native_id = Column(String)
    station_name = Column(String)
    lon = Column(Numeric)
    lat = Column(Numeric)
    elev = Column(Float)
    min_obs_time = Column(DateTime)
    max_obs_time = Column(DateTime)
    freq = Column(String)
    tz_offset = Column(Interval)
    province = Column(String)
    station_id = Column(Integer)
    history_id = Column(Integer, primary_key=True)
    country = Column(String)
    comments = Column(String(255))
    the_geom = Column(Geometry("GEOMETRY", 4326))
    sensor_id = Column(Integer)
    description = Column(String)
    network_id = Column(Integer)
    col_hex = Column(String)
    vars = Column(String)
    display_names = Column(String)

    __selectable__ = (
        Query(
            [
                Network.name.label("network_name"),
                Station.native_id.label("native_id"),
                History.station_name.label("station_name"),
                History.lon.label("lon"),
                History.lat.label("lat"),
                History.elevation.label("elev"),
                StationObservationStats.min_obs_time.label("min_obs_time"),
                StationObservationStats.max_obs_time.label("max_obs_time"),
                History.freq.label("freq"),
                History.tz_offset.label("tz_offset"),
                History.province.label("province"),
                History.station_id.label("station_id"),
                History.id.label("history_id"),
                History.country.label("country"),
                History.comments.label("comments"),
                History.the_geom.label("the_geom"),
                History.sensor_id.label("sensor_id"),
                Network.long_name.label("description"),
                Station.network_id.label("network_id"),
                Network.color.label("col_hex"),
                CollapsedVariables.vars.label("vars"),
                CollapsedVariables.display_names.label("display_names"),
            ]
        )
        .select_from(History)
        .join(Station)
        .join(Network)
        .outerjoin(CollapsedVariables)
        .outerjoin(
            StationObservationStats, StationObservationStats.history_id == History.id,
        )
    ).selectable


class HistoryStationNetwork(Base, ReplaceableView):
    """
    This view, as its name suggests, is a convenience view that joins History, Station,
    and Network tables.

    Note: In this class (which maps a table to the view), a primary key must be
    declared (SQLAlchemy requirement). A view cannot have a PK, but this table
    declaration does not affect the view.
    """

    __tablename__ = "history_join_station_network"  # Legacy name

    network_id = Column(Integer)
    station_id = Column(Integer)
    history_id = Column(Integer, primary_key=True)
    station_name = Column(String)
    lon = Column(Numeric)
    lat = Column(Numeric)
    elev = Column(Float)
    tz_offset = Column(Interval)
    province = Column(String)
    sdate = Column(Date)
    edate = Column(Date)
    country = Column(String)
    comments = Column(String(255))
    the_geom = Column(Geometry("GEOMETRY", 4326))
    sensor_id = Column(Integer)
    native_id = Column(Integer)
    network_name = Column(String)
    virtual = Column(String(255))
    description = Column(String)

    __selectable__ = (
        Query(
            [
                Station.network_id.label("network_id"),
                History.station_id.label("station_id"),
                History.id.label("history_id"),
                History.station_name.label("station_name"),
                History.lon.label("lon"),
                History.lat.label("lat"),
                History.elevation.label("elev"),
                History.sdate.label("sdate"),
                History.edate.label("edate"),
                History.tz_offset.label("tz_offset"),
                History.province.label("province"),
                History.country.label("country"),
                History.comments.label("comments"),
                History.the_geom.label("the_geom"),
                History.sensor_id.label("sensor_id"),
                Station.native_id.label("native_id"),
                Network.name.label("network_name"),
                Network.long_name.label("description"),
                Network.virtual.label("virtual"),
            ]
        )
        .select_from(History)
        .join(Station)
        .join(Network)
    ).selectable


class ObsCountPerDayHistory(Base, ReplaceableView):
    """
    This view provides counts of observations grouped by day (date) and history_id

    Note: In this class (which maps a table to the view), a primary key must be
    declared (SQLAlchemy requirement). A view cannot have a PK, but this table
    declaration does not affect the view.
    """

    __tablename__ = "obs_count_per_day_history_v"  # Legacy name

    count = Column(BigInteger)
    date_trunc = Column(DateTime)
    history_id = Column(Integer, primary_key=True)

    __selectable__ = (
        Query(
            [
                func.count().label("count"),
                func.date_trunc("day", Obs.time).label("date_trunc"),
                Obs.history_id.label("history_id"),
            ]
        )
        .select_from(Obs)
        .group_by(func.date_trunc("day", Obs.time), Obs.history_id)
    ).selectable


class ObsWithFlags(Base, ReplaceableView):
    """
    This view joins Obs with History and Variable.

    Note: In this class (which maps a table to the view), a primary key must be
    declared (SQLAlchemy requirement). A view cannot have a PK, but this table
    declaration does not affect the view.
    """

    # TODO: Why is this called 'ObsWithFlags'? There are no flags!
    #  Better name: ObsWithMetadata
    __tablename__ = "obs_with_flags"

    vars_id = Column(Integer)
    network_id = Column(Integer)
    unit = Column(String)
    standard_name = Column(String)
    cell_method = Column(String)
    # Variable net_var_name has type CIText in table meta_vars,
    # not String
    net_var_name = Column(String)
    station_id = Column(Integer)
    obs_raw_id = Column(Integer, primary_key=True)
    obs_time = Column(DateTime)
    mod_time = Column(DateTime)
    datum = Column(Float)

    __selectable__ = (
        Query(
            [
                Variable.id.label("vars_id"),
                Variable.network_id.label("network_id"),
                Variable.unit.label("unit"),
                Variable.standard_name.label("standard_name"),
                Variable.cell_method.label("cell_method"),
                Variable.name.label("net_var_name"),
                History.station_id.label("station_id"),
                Obs.id.label("obs_raw_id"),
                Obs.time.label("obs_time"),
                Obs.mod_time.label("mod_time"),
                Obs.datum.label("datum"),
            ]
        )
        .select_from(Obs)
        .join(Variable)
        .join(History)
    ).selectable
