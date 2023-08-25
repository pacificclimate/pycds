from sqlalchemy import (
    Index,
    Column,
    Integer,
    String,
    Text,
    ForeignKey,
    func,
    select,
)

from pycds.alembic.extensions.replaceable_objects import ReplaceableNativeMatview

# These cannot be imported from pycds because it is not yet fully initialized.
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

    __selectable__ = (
        # This query is likely wrong.
        # See https://github.com/pacificclimate/pycds/issues/180
        select(
            VarsPerHistory.history_id.label("history_id"),
            func.array_to_string(
                func.array_agg(
                    Variable.standard_name
                    + func.regexp_replace(Variable.cell_method, "time: ", "_", "g")
                ),
                ", ",
            ).label("vars"),
            func.array_to_string(func.array_agg(Variable.display_name), "|").label(
                "display_names"
            ),
        )
        .select_from(VarsPerHistory)
        .join(Variable, Variable.id == VarsPerHistory.vars_id)
        .group_by(VarsPerHistory.history_id)
    )


Index("collapsed_vars_idx", CollapsedVariables.history_id)
