from sqlalchemy import MetaData, Index, Column, Integer
from sqlalchemy.orm import Query
from sqlalchemy.ext.declarative import declarative_base
from pycds.util import get_schema_name
from pycds.alembic.extensions.replaceable_objects import (
    ReplaceableNativeMatview,
)
from pycds.orm.tables import Obs

# TODO: Factor Base one level up, common to all versions
Base = declarative_base(metadata=MetaData(schema=get_schema_name()))


class VarsPerHistory(Base, ReplaceableNativeMatview):
    """This materialized view is used by the PDP backend to give feasible
    web app performance. It is used to link recorded quantities (variables)
    to the station/history level, rather than just the network level (just
    because one station in the network records a quantity, doesn't mean that
    all stations in the network do). To some extent, this view is an add-on
    to compensate for poor database normalization, but it's close enough to
    get by.

    In previous revisions of PyCDS, this was a manual materialized view,
    defined and maintained externally to Alembic management, and with a plain
    table ORM model to map to it. It has now been brought under Alembic
    management.

    Definition of supporting *view* in CRMP:
    SELECT DISTINCT obs_raw.history_id, obs_raw.vars_id
    FROM obs_raw;
    """

    __tablename__ = "vars_per_history_mv"

    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)

    __selectable__ = (
        Query(
            [Obs.history_id.label("history_id"), Obs.vars_id.label("vars_id")]
        ).distinct()
    ).selectable


Index("var_hist_idx", VarsPerHistory.history_id, VarsPerHistory.vars_id)
