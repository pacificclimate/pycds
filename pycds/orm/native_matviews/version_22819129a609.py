from sqlalchemy import (
    Index,
    Column,
    Integer,
    String,
    ForeignKey,
    func,
)
from sqlalchemy.orm import Query

from pycds.alembic.extensions.replaceable_objects import ReplaceableNativeMatview
# TODO: These ought to be able to be imported from top-level pycds, but maybe that
#  produces some kind of error.
from pycds.orm.tables import Variable
from pycds.orm.native_matviews import VarsPerHistory
from pycds.orm.view_base import Base


class CollapsedVariables(Base, ReplaceableNativeMatview):
    """
    This class defines a materialized view that supports the view CrmpNetworkGeoserver,
    to improve that view's performance.
    """

    __tablename__ = "collapsed_vars_mv"

    history_id = Column(
        Integer, ForeignKey("meta_history.history_id"), primary_key=True
    )
    vars = Column(String)
    display_names = Column(String)

    # TODO: Can this and other view, matview queries use the sqlalchemy function
    #  select() instead of Query()....selectable? If so, do so everywhere.
    __selectable__ = (
        Query(
            [
                VarsPerHistory.history_id.label("history_id"),
                func.array_to_string(
                    func.array_agg(
                        Variable.standard_name
                        + func.regexp_replace(Variable.cell_method, "time: ", "_", "g"),
                        ", "
                    )
                ).label("vars"),
                func.array_to_string(
                    func.array_agg(Variable.display_name), "|"
                ).label("display_names")
            ]
        )
        .select_from(VarsPerHistory)
        .join(Variable, Variable.id == VarsPerHistory.vars_id)
        .group_by(VarsPerHistory.history_id)
    ).selectable


Index("collapsed_vars_idx", CollapsedVariables.history_id)
