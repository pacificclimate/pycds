from sqlalchemy import Index, Column, Integer, BigInteger, ForeignKey, func, text
from sqlalchemy.orm import Query
from sqlalchemy.dialects.postgresql import array, ARRAY, TEXT

from pycds.alembic.extensions.replaceable_objects import ReplaceableNativeMatview
from pycds.orm.tables import Obs, Variable
from pycds.orm.view_base import Base
from pycds import get_schema_name


schema_name = get_schema_name()
schema_func = getattr(func, schema_name)


class ClimoObsCount(Base, ReplaceableNativeMatview):
    """This class maps to a manual materialized view that is required for
    web app performance. It is used for approximating the number of
    climatologies which will be returned by station selection
    criteria.
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
        .where(
            schema_func.variable_tags(
                text(Variable.__tablename__), type_=ARRAY(TEXT)
            ).contains(array(["climatology"]))
        )
        .group_by(Obs.history_id)
    ).selectable


Index("climo_obs_count_idx", ClimoObsCount.history_id)
