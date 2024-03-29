from sqlalchemy import Column, Integer, BigInteger, ForeignKey
from pycds.alembic.extensions.replaceable_objects import ReplaceableView
from pycds.orm.native_matviews.version_96729d6db8b3 import (
    ClimoObsCount as ClimoObsCountMv,
)
from pycds.orm.view_base import make_declarative_base


Base = make_declarative_base()


class ClimoObsCount(Base, ReplaceableView):
    """
    This class maps to a view that is required by the (formerly) externally-managed
    matview `climo_obs_count_mv`. To prevent unnecessary code repetition, its
    selectable is copied from the native matview that replaces it.
    """

    __tablename__ = "climo_obs_count_v"

    count = Column(BigInteger)
    history_id = Column(
        Integer, ForeignKey("meta_history.history_id"), primary_key=True
    )

    __selectable__ = ClimoObsCountMv.__selectable__
