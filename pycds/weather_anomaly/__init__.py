"""Most up-to-date weather anomaly views/matviews.

Views, materialized and regular, are managed by Alembic migrations.
A migration may add, drop, or change a view. The views present in one
migration version of the database can be different than those in another version.

To enable Alembic to work properly, it is necessary retain all
versions of views, not just the latest. The views defined in migration
version `<version>` affecting these views are stored in the module
`pycds.weather_anomaly.version_<version>`.

This file, the top level of the `pycds.weather_anomaly` module, exports the
most recent version of each view available, so that users of PyCDS can simply
refer to `pycds.weather_anomaly.<ViewName>` without having to know the
migration version in which that view was most recently added or modified.

As migrations that modify views are added, this file should be updated to
export the latest version. When a PyCDS release is created, it will "freeze"
this set of views. Following any PyCDS release, further migrations and further
releases will "freeze" later sets of views.
"""

from .version_8fd8f556c548 import DailyMaxTemperature
from .version_8fd8f556c548 import DailyMinTemperature
from .version_8fd8f556c548 import MonthlyAverageOfDailyMaxTemperature
from .version_8fd8f556c548 import MonthlyAverageOfDailyMinTemperature
from .version_8fd8f556c548 import MonthlyTotalPrecipitation
