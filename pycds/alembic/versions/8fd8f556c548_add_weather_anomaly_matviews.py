"""Add weather anomaly matviews

Revision ID: 8fd8f556c548
Revises: 84b7fc2596d5
Create Date: 2020-02-04 16:53:58.084405

"""
from alembic import op
import sqlalchemy as sa
from pycds import get_schema_name
from pycds.weather_anomaly.version_8fd8f556c548 import (
    DailyMaxTemperature,
    DailyMinTemperature,
    MonthlyAverageOfDailyMaxTemperature,
    MonthlyAverageOfDailyMinTemperature,
    MonthlyTotalPrecipitation,
)
import pycds.replaceable_objects.materialized_views


# revision identifiers, used by Alembic.
revision = "8fd8f556c548"
down_revision = "84b7fc2596d5"
branch_labels = None
depends_on = None


schema_name = get_schema_name()

matviews = (
    DailyMaxTemperature,
    DailyMinTemperature,
    MonthlyAverageOfDailyMaxTemperature,
    MonthlyAverageOfDailyMinTemperature,
    MonthlyTotalPrecipitation,
)


def upgrade():
    for view in matviews:
        op.create_manual_materialized_view(view, schema=schema_name)


def downgrade():
    for view in reversed(matviews):
        op.drop_manual_materialized_view(view, schema=schema_name)
