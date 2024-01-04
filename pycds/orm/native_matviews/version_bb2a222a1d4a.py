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
from pycds.orm.tables import Obs, History
from pycds.orm.view_base import make_declarative_base


Base = make_declarative_base()


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

    # TODO: The original *table* declaration included this relationship declaration.
    #  The various attempts below produce errors, as do the post-hoc attempts following
    #  the class definition.
    # Relationships
    # history = relationship(History, primaryjoin=(History.id == ObsCountPerMonthHistory.history_id))
    # history = relationship(History, foreign_keys=[History.id])
    # history = relationship("History")

    __selectable__ = (
        select(
            func.count(Obs.id).label("count"),
            func.date_trunc("month", Obs.time).label("date_trunc"),
            Obs.history_id.label("history_id"),
        )
        .select_from(Obs)
        .group_by(func.date_trunc("month", Obs.time), Obs.history_id)
    )


# See comment above.
# ObsCountPerMonthHistory.__mapper__.add_property("history", relationship(History, primaryjoin=(History.id == ObsCountPerMonthHistory.history_id)))
# ObsCountPerMonthHistory.__mapper__.add_property("history", relationship("History", primaryjoin="History.id == ObsCountPerMonthHistory.history_id"))
# ObsCountPerMonthHistory.__mapper__.add_property("history", relationship("History"))
# ObsCountPerMonthHistory.__mapper__.add_property("history", relationship(History))


Index(
    "obs_count_per_month_history_idx",
    ObsCountPerMonthHistory.date_trunc,
    ObsCountPerMonthHistory.history_id,
)
