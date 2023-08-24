from sqlalchemy import (
    Index,
    Column,
    Integer,
    String,
    ForeignKey,
    func,
)

from pycds.alembic.extensions.replaceable_objects import ReplaceableView
from pycds.orm.view_base import Base
from pycds.orm.native_matviews.version_22819129a609 import (
    CollapsedVariables as CollapsedVariablesMatview,
)


class CollapsedVariables(Base, ReplaceableView):
    """
    This view is required for the fake materialized view (later replaced with a native
    matview) `collapsed_vars_mv`. To prevent unnecessary code repetition, its
    selectable is copied from the native matview that replaces it.
    Columns cannot be copied so.
    """

    __tablename__ = "collapsed_vars_v"

    history_id = Column(
        Integer, ForeignKey("meta_history.history_id"), primary_key=True
    )
    vars = Column(String)
    display_names = Column(String)

    __selectable__ = CollapsedVariablesMatview.__selectable__


Index("collapsed_vars_idx", CollapsedVariables.history_id)
