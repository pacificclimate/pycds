"""add monthly cumulative precipitation data matview

Revision ID: 9e5ca6dc3f02
Revises: 3505750d3416
Create Date: 2024-04-15 17:26:17.878212

"""
from alembic import op
import sqlalchemy as sa

from pycds.alembic.util import create_matview, drop_matview
from pycds.context import get_su_role_name, get_schema_name

#matview to be created de novo
from pycds.orm.native_matviews.version_9e5ca6dc3f02 import (
    CumulativePrecipMonthMinmax,
    CumulativePrecipMonthStartTimestamps,
    CumulativePrecipMonthStart,
    CumulativePrecipMonthEndTimestamps,
    CumulativePrecipMonthEnd,
    CumulativePrecipPerMonth
)
# revision identifiers, used by Alembic.
revision = "9e5ca6dc3f02"
down_revision = "3505750d3416"
branch_labels = None
depends_on = None

schema_name = get_schema_name()


def upgrade():
    op.set_role(get_su_role_name())
    
    create_matview(CumulativePrecipMonthMinmax, schema=schema_name)
    create_matview(CumulativePrecipMonthStartTimestamps, schema=schema_name)
    create_matview(CumulativePrecipMonthStart, schema=schema_name)
    create_matview(CumulativePrecipMonthMEndTimestamps, schema=schema_name)
    create_matview(CumulativePrecipMonthEnd, schema=schema_name)
    create_matview(CumulativePrecipPerMonth, schema=schema_name)

    op.reset_role()


def downgrade():
    op.set_role(get_su_role_name())
    
    drop_matview(CumulativePrecipPerMonth, schema=schema_name)    
    drop_matview(CumulativePrecipMonthMinmax, schema=schema_name)
    drop_matview(CumulativePrecipMonthStart, schema=schema_name)
    drop_matview(CumulativePrecipMonthStartTimstamps, schema=schema_name)
    drop_matview(CumulativePrecipMonthEnd, schema=schema_name)
    drop_matview(CumulativePrecipMonthEndTimstamps, schema=schema_name)
    
    op.reset_role()

