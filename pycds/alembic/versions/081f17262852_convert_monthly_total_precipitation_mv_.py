"""convert monthly_total_precipitation_mv to native matview

Revision ID: 081f17262852
Revises: 3505750d3416
Create Date: 2024-04-23 13:39:39.618612

"""
import logging
from alembic import op
import sqlalchemy as sa
from pycds.alembic.util import (
    create_matview,
    drop_matview,
    grant_standard_table_privileges,
)
from pycds import get_schema_name
from pycds.database import matview_exists

from pycds.orm.native_matviews.version_081f17262852 import MonthlyTotalPrecipitation
from pycds.orm.manual_matviews.version_8fd8f556c548 import (
    MonthlyTotalPrecipitation as OldMonthlyTotalPrecipitation,
)

# revision identifiers, used by Alembic.
revision = "081f17262852"
down_revision = "3505750d3416"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic")
schema_name = get_schema_name()


def upgrade():
    engine = op.get_bind().engine
    if matview_exists(
        engine, MonthlyTotalPrecipitation.__tablename__, schema=schema_name
    ):
        logger.info(
            f"A native materialized view '{MonthlyTotalPrecipitation.__tablename__}' "
            f"already exists in the database; skipping upgrade"
        )

    else:
        # drop old "matview"-style table
        op.drop_replaceable_object(OldMonthlyTotalPrecipitation)

        # Replace with native matview
        create_matview(MonthlyTotalPrecipitation, schema=schema_name)


def downgrade():
    # Drop native matview
    drop_matview(MonthlyTotalPrecipitation, schema=schema_name)

    op.create_replaceable_object(OldMonthlyTotalPrecipitation)

    grant_standard_table_privileges(
        "monthly_total_precipitation_mv", schema=schema_name
    )
