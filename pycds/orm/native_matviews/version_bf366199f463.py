from sqlalchemy import (
    Index,
    Column,
    Integer,
    BigInteger,
    DateTime,
    ForeignKey,
    func,
)
from sqlalchemy.orm import Query
from sqlalchemy.sql.expression import select

from pycds.alembic.extensions.replaceable_objects import ReplaceableNativeMatview
from pycds.orm.tables import Obs, Variable, History
from pycds.orm.view_base import Base
from pycds import get_schema_name


schema_name = get_schema_name()
schema_func = getattr(func, schema_name)


# The selectable in the matview uses this subquery.
stats_by_station = (
    select(
        func.min(Obs).label("min_obs_time"),
        func.max(Obs).label("max_obs_time"),
        Obs.history_id.label("history_id"),
        func.count(Obs.obs_time).distinct().label("obs_count"),
    )
    .select_from(Obs)
    .group_by(Obs.history_id)
    .subquery()
)


class StationObservationStats(Base, ReplaceableNativeMatview):
    """
    This class defines a materialized view required for web app performance. It
    precomputes some basic statistics about observations by station and history.
    """

    __tablename__ = "station_obs_stats_mv"

    station_id = Column(Integer, ForeignKey("meta_station.station_id"))
    history_id = Column(
        Integer, ForeignKey("meta_history.history_id"), primary_key=True
    )
    min_obs_time = Column(DateTime)
    max_obs_time = Column(DateTime)
    obs_count = Column(BigInteger)

    __selectable__ = Query(
        History.station_id.label("station_id"),
        stats_by_station.station_id.label("station_id"),
        stats_by_station.max_obs_time.label("max_obs_time"),
        stats_by_station.min_obs_time.label("min_obs_time"),
        stats_by_station.obs_count.label("obs_count"),
    )


Index(
    "station_obs_stats_mv_idx",
    StationObservationStats.min_obs_time,
    StationObservationStats.max_obs_time,
    StationObservationStats.obs_count,
    StationObservationStats.station_id,
    StationObservationStats.history_id,
)
