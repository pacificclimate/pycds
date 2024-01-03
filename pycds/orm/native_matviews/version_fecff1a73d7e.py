from sqlalchemy.orm import declarative_base

from sqlalchemy import (
    Index,
    Column,
    Integer,
    String,
    ForeignKey,
    func,
    select,
    text,
    MetaData,
)
from sqlalchemy.dialects.postgresql import aggregate_order_by, ARRAY, TEXT

from pycds.alembic.extensions.replaceable_objects import ReplaceableNativeMatview

# These cannot be imported from `pycds` (top-level) because it is not yet fully
# initialized at this point.
from pycds.orm.tables import Variable
from pycds.orm.native_matviews import VarsPerHistory
from pycds.context import get_schema_name

# Only one definition is permitted for each named object (e.g., table) in given metadata
# object. Therefore, we must use a separate declarative base for multiple definitions of
# an object with the same name, such as this matview.
# TODO: Do this everywhere, and remove the common base(s) for non-table objects.
# TODO: Define a factory for these?
Base = declarative_base(metadata=MetaData(schema=get_schema_name()))

# TODO: Move this out to a utility module, and make it visible to clients.
schema_name = get_schema_name()
schema_func = getattr(func, schema_name)  # Explicitly specify schema of function


def variable_tags(Table):
    return schema_func.variable_tags(text(Table.__tablename__), type_=ARRAY(TEXT))


# TODO: end utility module

# This CTE is used in the selectable for matview `CollapsedVariables`. For each history,
# it aggregates values computed from columns of `Variable`. `VarsPerHistory` provides
# the connection between variables and histories. The main reason for its existence is
# to enable the arrays of variable tags to be flattened and unique'd, which requires or
# at least is most easily done with an intermediate stage created here. Other things are
# placed here too, mainly for readability, but they could just as easily be computed
# directly within the matview selectable.
aggregated_vars = (
    select(
        VarsPerHistory.history_id.label("history_id"),
        func.array_agg(VarsPerHistory.vars_id).label("vars_ids"),
        func.array_agg(aggregate_order_by(variable_tags(Variable), Variable.id)).label(
            "all_variable_tags"
        ),
        func.array_agg(aggregate_order_by(Variable.display_name, Variable.id)).label(
            "display_names"
        ),
        # See comment below re. column `vars`.
        func.array_agg(
            Variable.standard_name
            + func.regexp_replace(Variable.cell_method, "time: ", "_", "g")
        ).label("cell_methods"),
    )
    .select_from(VarsPerHistory)
    .join(Variable, Variable.id == VarsPerHistory.vars_id)
    .group_by(VarsPerHistory.history_id)
).cte("aggregated_vars")

# This subquery is used in the selectable for the matview.
unique_variable_tags_sq = (
    select(text("*"))
    .select_from(func.unnest(aggregated_vars.c.all_variable_tags))
    .distinct()
).subquery()


class CollapsedVariables(Base, ReplaceableNativeMatview):
    """
    This class defines a materialized view that supports the view CrmpNetworkGeoserver,
    to improve that view's performance.
    """

    __tablename__ = "collapsed_vars_mv"

    history_id = Column(
        Integer, ForeignKey("meta_history.history_id"), primary_key=True
    )
    vars_ids = Column(ARRAY(Integer))
    unique_variable_tags = Column(ARRAY(TEXT))
    display_names = Column(String)
    vars = Column(String)

    __selectable__ = select(
        aggregated_vars.c.history_id.label("history_id"),
        aggregated_vars.c.vars_ids.label("vars_ids"),
        func.array(unique_variable_tags_sq).label("unique_variable_tags"),
        func.array_to_string(aggregated_vars.c.display_names, "|").label(
            "display_names"
        ),
        # Column `vars` is peculiar and much of its former use has been replaced
        # by column `unique_variable_tags`. Unfortunately it is still in use in
        # parts of pdp_util. TODO: What parts? Filtering, I think.
        func.array_to_string(aggregated_vars.c.cell_methods, "|").label("vars"),
    ).select_from(aggregated_vars)


Index("collapsed_vars_idx", CollapsedVariables.history_id)
