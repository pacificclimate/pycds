"""Views

Define views as proper SQL views in the database.

SQLAlchemy does not directly support views. A view can be represented simply
as a table in the ORM, but this does not permit its definition to be part of
the ORM, or to be maintained and migrated.

This module defines views in the ORM as SQL views in the database, using
a SQLAlchemy compiler extension provided by `./view_helpers`. See that module
for more information.
"""

from sqlalchemy.sql import text, column
from pycds import Base
from pycds.view_helpers import ViewMixin


class CrmpNetworkGeoserver(Base, ViewMixin):
    __viewname__ = 'crmp_network_geoserver'
    __selectable__ = text('''
        SELECT 
            meta_network.network_name, 
            meta_station.native_id, 
            meta_history.station_name, 
            meta_history.lon, 
            meta_history.lat, 
            meta_history.elev, 
            station_obs_stats_mv.min_obs_time, 
            station_obs_stats_mv.max_obs_time, 
            meta_history.freq::text AS freq, 
            meta_history.tz_offset, 
            meta_history.province, 
            meta_history.station_id, 
            meta_history.history_id, 
            meta_history.country, 
            meta_history.comments, 
            meta_history.the_geom, 
            meta_history.sensor_id, 
            meta_network.description, 
            meta_station.network_id, 
            meta_network.col_hex, 
            collapsed_vars_mv.vars, 
            collapsed_vars_mv.display_names
        FROM meta_history
            NATURAL JOIN meta_station
            NATURAL JOIN meta_network
            LEFT JOIN collapsed_vars_mv USING (history_id)
            LEFT JOIN station_obs_stats_mv USING (history_id)
        WHERE meta_history.province::text = 'BC'::text;
    ''').columns(
        column('network_name'),
        column('native_id'),
        column('station_name'),
        column('lon'),
        column('lat'),
        column('elev'),
        column('min_obs_time'),
        column('max_obs_time'),
        column('freq'),
        column('tz_offset'),
        column('province'),
        column('station_id'),
        column('history_id'),
        column('country'),
        column('comments'),
        column('the_geom'),
        column('sensor_id'),
        column('description'),
        column('network_id'),
        column('col_hex'),
        column('vars'),
        column('display_names')
    )
    __primary_key__ = ['station_id']