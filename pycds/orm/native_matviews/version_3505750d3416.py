from sqlalchemy import Index, Column, Integer, Date, func
from sqlalchemy.orm import Query
from pycds.alembic.extensions.replaceable_objects import ReplaceableNativeMatview
from pycds.orm.tables import Obs
from pycds.orm.view_base import make_declarative_base


Base = make_declarative_base()


class VarsPerHistory(Base, ReplaceableNativeMatview):
    """This materialized view speeds up the PDP and station data
    portal by linking variables to stations, rather than needing
    to query the very large obs_raw table to find what what variables
    are associated with a station over what timespan.

    Compared to the previous version of this view, this version
    adds the earliest and latest timestamps data is recorded for a
    variable.

    Definition of supporting *view*:
    SELECT DISTINCT obs_raw.history_id, obs_raw.vars_id,
       min(obs_raw.time) AS start_time, max(obs_raw.time) AS end_time
    FROM obs_raw
    GROUP BY history_id, vars_id;
    """

    __tablename__ = "vars_per_history_mv"

    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)
    start_time = Column(Date)
    end_time = Column(Date)

    __selectable__ = (
        Query(
            [
                Obs.history_id.label("history_id"),
                Obs.vars_id.label("vars_id"),
                func.min(Obs.time).label("start_time"),
                func.max(Obs.time).label("end_time"),
            ]
        ).group_by(Obs.history_id, Obs.vars_id)
    ).selectable


Index("var_hist_idx", VarsPerHistory.history_id, VarsPerHistory.vars_id)
