from pycds.orm.view_base import Base
from pycds.alembic.extensions.replaceable_objects import ReplaceableView
from pycds.orm.native_matviews.version_96729d6db8b3 import (
    ClimoObsCount as ClimoObsCountMv,
)


class ClimoObsCount(Base, ReplaceableView):
    """
    This class maps to a view that is required by the (formerly) externally-managed
    matview `climo_obs_count_mv`. To prevent unnecessary code repetition, its
    columns and selectable are copied from the native matview that replaces it.
    """

    __tablename__ = "climo_obs_count_v"

    count = ClimoObsCountMv.count
    history_id = ClimoObsCountMv.history_id

    __selectable__ = ClimoObsCountMv.__selectable__
