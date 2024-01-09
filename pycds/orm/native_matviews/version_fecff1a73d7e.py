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
from pycds.orm.view_base import make_declarative_base
from pycds.util import variable_tags

Base = make_declarative_base()
schema_name = get_schema_name()

# This CTE is used in the selectable for matview `CollapsedVariables`. For each history,
# it aggregates values computed from columns of `Variable`. `VarsPerHistory` provides
# the connection between variables and histories. The main reason for its existence is
# column `all_variable_tags`, which is an intermediate stage to computing the matview
# column `unique_variable_tags`. Other values are placed here too, mainly for
# readability; they could just as easily be computed directly within the matview
# selectable.
aggregated_vars = (
    select(
        VarsPerHistory.history_id.label("history_id"),
        func.array_agg(VarsPerHistory.vars_id).label("vars_ids"),
        # (2D) Array of all variable tags for all variables associated to history.
        # Supports computation of `unique_variable_tags`.
        func.array_agg(aggregate_order_by(variable_tags(Variable), Variable.id)).label(
            "all_variable_tags"
        ),
        func.array_agg(aggregate_order_by(Variable.display_name, Variable.id)).label(
            "display_names"
        ),
        # See comment below re. column `vars`.
        func.array_agg(
            aggregate_order_by(
                Variable.standard_name
                + func.regexp_replace(Variable.cell_method, "time: ", "_", "g"),
                Variable.id,
            )
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

# This query defines the matview.
selectable = select(
    aggregated_vars.c.history_id.label("history_id"),
    aggregated_vars.c.vars_ids.label("vars_ids"),
    func.array(unique_variable_tags_sq).label("unique_variable_tags"),
    func.array_to_string(aggregated_vars.c.display_names, "|").label("display_names"),
    # Column `vars`: See notes above.
    func.array_to_string(aggregated_vars.c.cell_methods, ", ").label("vars"),
).select_from(aggregated_vars)


class CollapsedVariables(Base, ReplaceableNativeMatview):
    """
    This class defines a materialized view that supports the view CrmpNetworkGeoserver,
    to improve that view's performance. It provides aggregated information about all
    variables associated to a given history -- which is to say all the variables for
    which there are observations associated to that history. It exists to speed up
    critical queries in the backends.

    Most columns are self-explanatory; see comments in column definition below. Further
    explanation for specific columns:

    - `unique_variable_tags`: The set (rendered as a SQLAlchemy native array) of all
      results of the `variable_tags` function applied to each variable associated to
      the history. It enables us to distinguish stations (histories) that report
      climatological variables from those which do not, which is critical for some apps.
      It is a flattened and unique version of the array of all variable tags arrays
      returned by the function.

    - `vars`: A string formed by concatenating an "identifier" derived from each value
      of `cell_method` to each variable associated to the history (separator: ', '). It
      is a legacy value, formerly used both to distinguish stations with climatological
      variables from those without, and for filtering stations based on the "identifier".
      The first usage has been replaced by column `unique_variable_tags`; the second is
      regrettably still in use. The identifier is formed by a regex replacement (see CTE
      `aggregated_vars` below) that yields a string that is no longer a programming
      language identifier and is in any case idiosyncratic. For more information, see
      issue https://github.com/pacificclimate/pycds/issues/180.
    """

    __tablename__ = "collapsed_vars_mv"

    # Id of history record.
    history_id = Column(
        Integer, ForeignKey("meta_history.history_id"), primary_key=True
    )
    # Array of id's of all variables associated with the history, ordered by id.
    vars_ids = Column(ARRAY(Integer))
    # Array of all variable tags returned for all variables associated with the history,
    # flattened and reduced to unique values (no repetitions in array). Arbitrary order.
    unique_variable_tags = Column(ARRAY(TEXT))
    # String aggregate of `display_name` of all variables associated with the history,
    # separated by '|', ordered by id.
    display_names = Column(String)
    # String aggregate of "identifiers" computed from `cell_method`  of all variables
    # associated with the history, separated by ', ', ordered by id.
    vars = Column(String)

    __selectable__ = selectable


Index("collapsed_vars_idx", CollapsedVariables.history_id)
