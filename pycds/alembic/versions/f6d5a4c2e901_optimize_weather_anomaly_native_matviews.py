"""Optimize weather-anomaly native materialized views.

Revision ID: f6d5a4c2e901
Revises: 33179b5ae85a
Create Date: 2026-06-24

This migration:
* adds a materialized view containing discarded observation IDs;
* changes daily extrema and monthly precipitation to anti-join that helper;
* marks effective_day immutable and parallel safe;
* adds a (history_id, obs_time) index used by station_obs_stats_mv.

The normal refresh job must refresh discarded_obs_raw_mv before its dependent
weather-anomaly materialized views.
"""

from alembic import op

from pycds import get_schema_name, get_su_role_name
from pycds.alembic.util import create_matview, drop_matview
from pycds.database import get_schema_item_names
from pycds.orm.functions.version_f6d5a4c2e901 import effective_day
from pycds.orm.functions.version_4a2f1879293a import (
    effective_day as previous_effective_day,
)
from pycds.orm.native_matviews.version_f6d5a4c2e901 import (
    DiscardedObsRaw,
    DailyMaxTemperature,
    DailyMinTemperature,
    MonthlyTotalPrecipitation,
    MonthlyAverageOfDailyMaxTemperature,
    MonthlyAverageOfDailyMinTemperature,
)
from pycds.orm.native_matviews.version_081f17262852 import (
    DailyMaxTemperature as PreviousDailyMaxTemperature,
    DailyMinTemperature as PreviousDailyMinTemperature,
    MonthlyTotalPrecipitation as PreviousMonthlyTotalPrecipitation,
    MonthlyAverageOfDailyMaxTemperature as PreviousMonthlyAverageOfDailyMaxTemperature,
    MonthlyAverageOfDailyMinTemperature as PreviousMonthlyAverageOfDailyMinTemperature,
)


revision = "f6d5a4c2e901"
down_revision = "33179b5ae85a"
branch_labels = None
depends_on = None


schema_name = get_schema_name()

# Creation order expresses the new dependency graph. Drop in reverse order.
updated_matviews = (
    DiscardedObsRaw,
    DailyMaxTemperature,
    DailyMinTemperature,
    MonthlyTotalPrecipitation,
    MonthlyAverageOfDailyMaxTemperature,
    MonthlyAverageOfDailyMinTemperature,
)

previous_matviews = (
    PreviousDailyMaxTemperature,
    PreviousDailyMinTemperature,
    PreviousMonthlyTotalPrecipitation,
    PreviousMonthlyAverageOfDailyMaxTemperature,
    PreviousMonthlyAverageOfDailyMinTemperature,
)

index_name = "obs_raw_history_obs_time_idx"
index_table_name = "obs_raw"
index_columns = ("history_id", "obs_time")


def _create_history_obs_time_index():
    conn = op.get_bind()
    existing_indexes = get_schema_item_names(
        conn,
        "indexes",
        table_name=index_table_name,
        schema_name=schema_name,
    )

    if index_name not in existing_indexes:
        op.create_index(
            index_name,
            index_table_name,
            index_columns,
            unique=False,
            schema=schema_name,
        )


def _drop_history_obs_time_index():
    conn = op.get_bind()
    existing_indexes = get_schema_item_names(
        conn,
        "indexes",
        table_name=index_table_name,
        schema_name=schema_name,
    )

    if index_name in existing_indexes:
        op.drop_index(
            index_name,
            table_name=index_table_name,
            schema=schema_name,
        )


existing_matviews = (
    DailyMaxTemperature,
    DailyMinTemperature,
    MonthlyTotalPrecipitation,
    MonthlyAverageOfDailyMaxTemperature,
    MonthlyAverageOfDailyMinTemperature,
)

new_matviews = (
    DiscardedObsRaw,
    DailyMaxTemperature,
    DailyMinTemperature,
    MonthlyTotalPrecipitation,
    MonthlyAverageOfDailyMaxTemperature,
    MonthlyAverageOfDailyMinTemperature,
)


def upgrade():
    op.set_role(get_su_role_name())

    # This is a ReplaceableFunction with replace=True, so the repository's
    # canonical function definition and the database definition stay aligned.
    op.create_replaceable_object(effective_day)

    for matview in reversed(existing_matviews):
        drop_matview(matview, schema=schema_name)

    for matview in new_matviews:
        create_matview(matview, schema=schema_name)

    _create_history_obs_time_index()

    op.reset_role()


def downgrade():
    op.set_role(get_su_role_name())

    for matview in reversed(new_matviews):
        drop_matview(matview, schema=schema_name)

    for matview in previous_matviews:
        create_matview(matview, schema=schema_name)

    # previous_effective_day has replace=False because it was originally used
    # for first creation. Reuse its definition but enable replacement here.
    previous_effective_day.replace = True
    op.create_replaceable_object(previous_effective_day)

    _drop_history_obs_time_index()

    op.reset_role()
