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
from sqlalchemy.dialects.postgresql import ARRAY, TEXT
from geoalchemy2 import Geometry

from pycds.alembic.extensions.replaceable_objects import ReplaceableView
from pycds.orm.view_base import make_declarative_base


Base = make_declarative_base()


def __selectable__():
    # TODO: import straight from pycds here
    from pycds.orm.tables import Station, History
    from pycds.orm.views import Network
    from pycds.orm.native_matviews import StationObservationStats, CollapsedVariables
    return (
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
                CollapsedVariables.vars_ids.label("vars_ids"),
                CollapsedVariables.unique_variable_tags.label("unique_variable_tags"),
                CollapsedVariables.vars.label("vars"),
                CollapsedVariables.display_names.label("display_names"),
            ]
        )
        .select_from(History)
        .join(Station, Station.id == History.station_id)
        .join(Network, Network.id == Station.network_id)
        .outerjoin(CollapsedVariables, CollapsedVariables.history_id == History.id)
        .outerjoin(
            StationObservationStats,
            StationObservationStats.history_id == History.id,
        )
    ).selectable


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
    vars_ids = Column(ARRAY(Integer))
    unique_variable_tags = Column(ARRAY(TEXT))

    __selectable__ = __selectable__
