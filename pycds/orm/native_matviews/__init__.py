"""
Most up-to-date native materialized views.

Materialized views are managed by Alembic migrations. A migration may add, drop,
or change a view. The views present in one migration version of the database
can be different than those in another version.

To enable Alembic to work properly, it is necessary retain all
versions of matviews, not just the latest. The matviews defined in migration
version `<version>` are stored in the module `version_<version>.py`
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

from .version_3505750d3416 import VarsPerHistory
from .version_96729d6db8b3 import ClimoObsCount
from .version_bf366199f463 import StationObservationStats
from .version_fecff1a73d7e import CollapsedVariables
from .version_bb2a222a1d4a import ObsCountPerMonthHistory
from .version_081f17262852 import MonthlyTotalPrecipitation
from .version_081f17262852 import DailyMaxTemperature
from .version_081f17262852 import DailyMinTemperature
from .version_081f17262852 import MonthlyAverageOfDailyMaxTemperature
from .version_081f17262852 import MonthlyAverageOfDailyMinTemperature

# only used for tests
from .version_081f17262852 import daily_temperature_extremum
from .version_081f17262852 import (
    monthly_average_of_daily_temperature_extremum_with_avg_coverage,
)
from .version_081f17262852 import (
    monthly_average_of_daily_temperature_extremum_with_total_coverage,
)
from .version_081f17262852 import monthly_total_precipitation_with_avg_coverage
from .version_081f17262852 import monthly_total_precipitation_with_total_coverage
from .version_081f17262852 import good_obs
