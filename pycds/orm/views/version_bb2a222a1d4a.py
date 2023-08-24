from sqlalchemy import (
    Index,
    Column,
    Integer,
    DateTime,
    ForeignKey,
)

from pycds.alembic.extensions.replaceable_objects import ReplaceableView
from pycds.orm.view_base import Base
from pycds.orm.native_matviews.version_bb2a222a1d4a import (
    ObsCountPerMonthHistory as ObsCountPerMonthHistoryMatview
)


class ObsCountPerMonthHistory(Base, ReplaceableView):
    """
    This view is required for the fake materialized view (later replaced with a native
    matview) `obs_count_per_month_history_mv`. To prevent unnecessary code repetition, its
    selectable is copied from the native matview that replaces it.
    Columns cannot be copied so.
    """

    __tablename__ = "obs_count_per_month_history_mv"

    count = Column(Integer)
    date_trunc = Column(DateTime, primary_key=True)
    history_id = Column(
        Integer, ForeignKey("meta_history.history_id"), primary_key=True
    )

    __selectable__ = ObsCountPerMonthHistoryMatview.__selectable__
