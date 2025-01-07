"""
ORM for the CRMP database.

This ORM includes table mappings for all table-like objects: tables, views,
and materialized views.

**Note: Schema name:**

SQLAlchemy does not make it easy to set the schema name of
a declarative base to a value determined at run-time. It must be specified
when the metadata for the declarative base is created, and once set it cannot
be changed. This happens declaratively, not procedurally, so the value of
the name must be determined externally to the code.

The most convenient way to specify the schema name externally is via an
environment variable, which in this case is named `PYCDS_SCHEMA_NAME`.
If `PYCDS_SCHEMA_NAME` is not specified, the default value of `crmp` is used,
making this backward compatible with all existing code. The function
`get_schema_name()` returns this value and is used throughout as the
source of the schema name in all contexts (database function definitions,
tables, views, materialized views).

Clients of this package must take care to specify `PYCDS_SCHEMA_NAME` correctly
when performing any database operations with it. Otherwise the operations will
fail with errors of the form "could not find object X in schema Y".
"""

__all__ = [
    # Utility methods
    "get_schema_name",
    "get_su_role_name",
    "schema_func",
    "variable_tags",
    # Tables
    "Base",
    "Network",
    "NetworkHistory",
    "Contact",
    "Variable",
    "VariableHistory",
    "Station",
    "StationHistory",
    "History",
    "HistoryHistory",
    "Obs",
    "ObsHistory",
    "MetaSensor",
    "TimeBound",
    "ObsRawNativeFlags",
    "NativeFlag",
    "ObsRawPCICFlags",
    "PCICFlag",
    "DerivedValue",
    "CollapsedVariables",
    # Alembic-managed native matviews
    "VarsPerHistory",
    "ClimoObsCount",
    "StationObservationStats",
    "ObsCountPerMonthHistory",
    "CollapsedVariables",
    "MonthlyTotalPrecipitation",
    # Alembic-managed manual matviews
    "DailyMaxTemperature",
    "DailyMinTemperature",
    "MonthlyAverageOfDailyMaxTemperature",
    "MonthlyAverageOfDailyMinTemperature",
    # Alembic-managed views
    "CrmpNetworkGeoserver",
    "HistoryStationNetwork",
    "ObsCountPerDayHistory",
    "ObsWithFlags",
]

from pycds.context import get_schema_name, get_su_role_name
from pycds.util import schema_func, variable_tags

from .orm.tables import (
    Base,
    Network,
    NetworkHistory,
    Contact,
    Station,
    StationHistory,
    History,
    HistoryHistory,
    ObsRawNativeFlags,
    ObsRawPCICFlags,
    MetaSensor,
    Obs,
    ObsHistory,
    TimeBound,
    Variable,
    VariableHistory,
    NativeFlag,
    PCICFlag,
    DerivedValue,
)

from .orm.views import (
    CrmpNetworkGeoserver,
    HistoryStationNetwork,
    ObsCountPerDayHistory,
    ObsWithFlags,
)

from .orm.native_matviews import (
    VarsPerHistory,
    ClimoObsCount,
    StationObservationStats,
    CollapsedVariables,
    ObsCountPerMonthHistory,
    MonthlyTotalPrecipitation,
    DailyMaxTemperature,
    DailyMinTemperature,
    MonthlyAverageOfDailyMaxTemperature,
    MonthlyAverageOfDailyMinTemperature,
)
