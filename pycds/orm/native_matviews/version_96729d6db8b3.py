from sqlalchemy import Index, Column, Integer, BigInteger, ForeignKey, func, text
from sqlalchemy.orm import Query
from sqlalchemy.dialects.postgresql import array, ARRAY, TEXT

from pycds.alembic.extensions.replaceable_objects import ReplaceableNativeMatview
from pycds.orm.tables import Obs, Variable
from pycds import get_schema_name
from pycds.orm.view_base import make_declarative_base
from pycds.util import variable_tags


Base = make_declarative_base()
schema_name = get_schema_name()


class ClimoObsCount(Base, ReplaceableNativeMatview):
    """
    This class defines a materialized view required for web app performance. It is
    used to approximate the number of climatologies which will be returned by station
    selection criteria.
    """

    __tablename__ = "climo_obs_count_mv"
    count = Column(BigInteger)
    history_id = Column(
        Integer, ForeignKey("meta_history.history_id"), primary_key=True
    )

    __selectable__ = (
        Query(
            [
                func.count().label("count"),
                Obs.history_id.label("history_id"),
            ]
        )
        .select_from(Obs)
        .join(Variable)
        .where(variable_tags(Variable).contains(array(["climatology"])))
        .group_by(Obs.history_id)
    ).selectable


Index("climo_obs_count_idx", ClimoObsCount.history_id)
