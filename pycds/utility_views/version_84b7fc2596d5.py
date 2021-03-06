"""Views

Define views as proper SQL views in the database.

SQLAlchemy does not directly support views. A view can be represented simply
as a table in the ORM, but this does not permit its definition to be part of
the ORM, or to be maintained and migrated.

This module defines views in the ORM as SQL views in the database, using
a SQLAlchemy compiler extension provided by `./view_helpers`. See that module
for more information. In particular, see the note about using a separate
declarative base for views; here it is `Base`.

WARNING: The `History` class defines column `the_geom` using GeoAlchemy2
data type `Geometry`. This forces
all reads on that column to be wrapped with Postgis function `ST_AsEWKB`.
This may or may not be desirable for all use cases, specifically views.
If views are behaving oddly with respect to geometry, this is worth looking at.
"""

from sqlalchemy import MetaData, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query
from pycds import get_schema_name, Network, Station, History, Variable, Obs, \
    StationObservationStats, CollapsedVariables
from pycds.view_helpers import ViewMixin


Base = declarative_base(metadata=MetaData(schema=get_schema_name()))


class CrmpNetworkGeoserver(Base, ViewMixin):
    """
    This view is used by the PDP Geoserver backend for generating station
    map layers.
    """
    __viewname__ = 'crmp_network_geoserver'
    __selectable__ = (
        Query([
            Network.name.label('network_name'),
            Station.native_id.label('native_id'),
            History.station_name.label('station_name'),
            History.lon.label('lon'),
            History.lat.label('lat'),
            History.elevation.label('elev'),
            StationObservationStats.min_obs_time.label('min_obs_time'),
            StationObservationStats.max_obs_time.label('max_obs_time'),
            History.freq.label('freq'),
            History.tz_offset.label('tz_offset'),
            History.province.label('province'),
            History.station_id.label('station_id'),
            History.id.label('history_id'),
            History.country.label('country'),
            History.comments.label('comments'),
            History.the_geom.label('the_geom'),
            History.sensor_id.label('sensor_id'),
            Network.long_name.label('description'),
            Station.network_id.label('network_id'),
            Network.color.label('col_hex'),
            CollapsedVariables.vars.label('vars'),
            CollapsedVariables.display_names.label('display_names')
        ])
        .select_from(History)
            .join(Station)
            .join(Network)
            .outerjoin(CollapsedVariables)
            .outerjoin(
                StationObservationStats,
                StationObservationStats.history_id == History.id
            )
    ).selectable
    __primary_key__ = ['station_id']


class HistoryStationNetwork(Base, ViewMixin):
    """
    This view, as its name suggests, is a convenience view that joins
    History, Station, and Network tables.
    """
    __viewname__ = 'history_join_station_network'  # Legacy name
    __selectable__ = (
        Query([
            Station.network_id.label('network_id'),
            History.station_id.label('station_id'),
            History.id.label('history_id'),
            History.station_name.label('station_name'),
            History.lon.label('lon'),
            History.lat.label('lat'),
            History.elevation.label('elev'),
            History.sdate.label('sdate'),
            History.edate.label('edate'),
            History.tz_offset.label('tz_offset'),
            History.province.label('province'),
            History.country.label('country'),
            History.comments.label('comments'),
            History.the_geom.label('the_geom'),
            History.sensor_id.label('sensor_id'),
            Station.native_id.label('native_id'),
            Network.name.label('network_name'),
            Network.long_name.label('description'),
            Network.virtual.label('virtual')
        ])
        .select_from(History)
            .join(Station)
            .join(Network)
    ).selectable
    __primary_key__ = ['history_id']


class ObsCountPerDayHistory(Base, ViewMixin):
    """
    This view provides counts of observations grouped by day (date) and
    history_id
    """
    __viewname__ = 'obs_count_per_day_history_v'  # Legacy name
    __selectable__ = (
        Query([
            func.count().label('count'),
            func.date_trunc('day', Obs.time).label('date_trunc'),
            Obs.history_id.label('history_id')
        ])
        .select_from(Obs)
        .group_by(func.date_trunc('day', Obs.time), Obs.history_id)
    ).selectable
    __primary_key__ = ['history_id']


class ObsWithFlags(Base, ViewMixin):
    """
    This view joins Obs with History and Variable.
    """
    # TODO: Why is this called 'ObsWithFlags'? There are no flags!
    #  Better name: ObsWithMetadata
    __viewname__ = 'obs_with_flags'
    __selectable__ = (
        Query([
            Variable.id.label('vars_id'),
            Variable.network_id.label('network_id'),
            Variable.unit.label('unit'),
            Variable.standard_name.label('standard_name'),
            Variable.cell_method.label('cell_method'),
            Variable.name.label('net_var_name'),
            History.station_id.label('station_id'),
            Obs.id.label('obs_raw_id'),
            Obs.time.label('obs_time'),
            Obs.mod_time.label('mod_time'),
            Obs.datum.label('datum')
        ])
        .select_from(Obs)
            .join(Variable)
            .join(History)
    ).selectable
    __primary_key__ = ['obs_raw_id']
