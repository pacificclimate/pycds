from sqlalchemy import func, Column, Integer, BigInteger, ForeignKey
from sqlalchemy.orm import Query
from pycds.orm.tables import Obs, Variable
from sqlalchemy.dialects.postgresql import TEXT
from pycds.alembic.extensions.replaceable_objects import ReplaceableView

from pycds.orm.view_base import make_declarative_base


Base = make_declarative_base()


class ClimoObsCount(Base, ReplaceableView):
    """
    This class maps to a view that is required by the original view that was present
    when the database was brought under management. To prevent unnecessary code

    It's definition was pulled from https://redmine51.pcic.uvic.ca:4433/issues/1921
    to help with the downgrade done as part of the migration to native matviews (96729d6db8b3)
    """

    __tablename__ = "climo_obs_count_v"

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
        .where(Variable.cell_method.cast(TEXT).op("~")("(within|over)"))
        .group_by(Obs.history_id)
    ).selectable
