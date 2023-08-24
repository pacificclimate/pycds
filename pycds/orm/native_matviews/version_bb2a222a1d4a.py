from sqlalchemy import (
    Index,
    Column,
    Integer,
    DateTime,
    ForeignKey,
    func,
    select,
)
from sqlalchemy.orm import relationship

from pycds.alembic.extensions.replaceable_objects import ReplaceableNativeMatview
from pycds.orm.view_base import Base
from pycds.orm.tables import Obs


class ObsCountPerMonthHistory(Base, ReplaceableNativeMatview):
    """
    This class defines a materialized view that is required for web
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

    __selectable__ = (
        select(
            func.count().label("count"),
            func.date_trunc('month', Obs.obs_time).label("date_trunc"),
            Obs.history_id.label("history_id"),
        )
    )


Index(
    "obs_count_per_month_history_idx",
    ObsCountPerMonthHistory.date_trunc,
    ObsCountPerMonthHistory.history_id,
)
