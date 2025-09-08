"""Functions, stored procedures, and trigger functions supporting bookmark operations.

TODO: Rename to Alembic version.

TODO: Define ReplaceableProcedure and supporting SQLA components, parallel to
  ReplaceableFunction to create stored procedures.
  See https://www.postgresql.org/docs/current/sql-createprocedure.html
  Maybe a separate branch/PR and migration preceding this one. Yak shaving.
  For now, use ReplaceableFunction ... DO NOT FORGET THIS.

TODO:
  Functions. Possibilities
    - get LU
    - validate history tuple
    - ? create bookmark label
    - create bookmark association
    - create bookmark association now
    - bracket updates
    - create rollback
  Trigger functions:
    -
"""

from pycds.alembic.extensions.replaceable_objects import ReplaceableFunction
from pycds.context import get_schema_name


schema_name = get_schema_name()


# Get LU history ids.
# This follows the pattern of other hxtk_ utility functions (receives collection name,
# etc.).
# Arguments:
#   collection name
#   where condition
#
#  Returns table of history id's for LU set from collection history satisfying condition.
hxtk_get_latest_undeleted_hx_ids =  ReplaceableFunction(

)


# hxtk_create_bookmark_label = # Necessary? This is just an insert to the table.


