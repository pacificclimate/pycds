"""
Most up-to-date manual matviews.

Manual matviews are managed by Alembic migrations. A migration may add, drop,
or change a view. The views present in one migration version of the database
can be different than those in another version.

To enable Alembic to work properly, it is necessary retain all
versions of views, not just the latest. The views defined in migration
version `<version>` affecting these views are stored in the module
`pycds.orm.manual_matviews.version_<version>`.

This file, the top level of the `pycds.orm.manual_matviews` module, exports the
most recent version of each view available, so that users of it can simply
refer to `pycds.orm.manual_matviews.<View>` without having to know the
migration version in which that view was most recently added or modified.

As migrations that modify views are added, this file should be updated to
export the latest version. When a PyCDS release is created, it will "freeze"
this set of views. Following any PyCDS release, further migrations and further
releases will "freeze" later sets of views.

At present all manual matviews have been updated to native matviews, and this
file is present only in case of future downgrades.
"""

from .version_8fd8f556c548 import Base
