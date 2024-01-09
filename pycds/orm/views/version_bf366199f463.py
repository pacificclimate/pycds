from sqlalchemy import (
    Index,
    Column,
    Integer,
    BigInteger,
    DateTime,
    ForeignKey,
    func,
)

from pycds.alembic.extensions.replaceable_objects import ReplaceableView
from pycds.orm.native_matviews.version_bf366199f463 import (
    StationObservationStats as StationObservationStatsMv,
)
from pycds.orm.view_base import make_declarative_base


Base = make_declarative_base()


class StationObservationStats(Base, ReplaceableView):
    """
    This view is required for the fake materialized view (later replaced with a native
    matview) `station_obs_stats_mv`. To prevent unnecessary code repetition, its
    selectable is copied from the native matview that replaces it.
    Columns cannot be copied so.
    """

    __tablename__ = "station_obs_stats_v"

    station_id = Column(Integer, ForeignKey("meta_station.station_id"))
    history_id = Column(
        Integer, ForeignKey("meta_history.history_id"), primary_key=True
    )
    min_obs_time = Column(DateTime)
    max_obs_time = Column(DateTime)
    obs_count = Column(BigInteger)

    __selectable__ = StationObservationStatsMv.__selectable__
