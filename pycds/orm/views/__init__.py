"""
Most up-to-date views.

Views are managed by Alembic migrations. A migration may add, drop, or change
a view. The views present in one migration version of the database can be
different than those in another version.

To enable Alembic to work properly, it is necessary retain all
versions of views, not just the latest. The views defined in migration
revision `<version>` are stored in the module `version_<version>.py`
in this directory.

This file, the top level of the views module, exports the most recent version
of each view available, so that users of PyCDS can simply refer to
`pycds.views.<ViewName>` without having to know the migration version in which
that view was added or modified.

As migrations that modify views are added, this file should be updated to
export the latest version. When a PyCDS release is created, it will "freeze"
this set of views. Following any PyCDS release, further migrations and further
releases will "freeze" later sets of views.
"""

from .version_6cb393f711c3 import CrmpNetworkGeoserver
from .version_84b7fc2596d5 import HistoryStationNetwork
from .version_84b7fc2596d5 import ObsCountPerDayHistory
from .version_84b7fc2596d5 import ObsWithFlags
from .version_22819129a609 import CollapsedVariables
from .version_bb2a222a1d4a import ObsCountPerMonthHistory
