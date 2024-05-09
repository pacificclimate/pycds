"""convert monthly_total_precipitation_mv to native matview

Revision ID: 081f17262852
Revises: 3505750d3416
Create Date: 2024-04-23 13:39:39.618612

"""
from alembic import op
import sqlalchemy as sa
from pycds.alembic.util import create_matview, drop_matview
from pycds import get_schema_name
from pycds.database import matview_exists

from pycds.orm.native_matviews.version_081f17262852 import MonthlyTotalPrecipitation

# revision identifiers, used by Alembic.
revision = "081f17262852"
down_revision = "3505750d3416"
branch_labels = None
depends_on = None

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
        op.drop_table_if_exists("monthly_total_precipitation_mv")
        print("drop table if exists")

        # Replace with native matview
        create_matview(MonthlyTotalPrecipitation, schema=schema_name)


def downgrade():
    # Drop native matview
    drop_matview(MonthlyTotalPrecipitation, schema=schema_name)

    # Replace with old "matview"-style table - contains no data
    op.create_table(
        "monthly_total_precipitation_mv",
        sa.Column("history_id", sa.Integer, nullable=False),
        sa.Column("vars_id", sa.Integer, primary_key=True),
        sa.Column("obs_month", sa.DateTime, primary_key=True),
        sa.Column("statistic", sa.Float, nullable=False),
        sa.Column("data_coverage", sa.Float),
        sa.ForeignKeyContraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"]
        ),
        sa.ForeignKeyConstraint(["vars_id"], [f"{schema_name}.meta_vars.vars_id"]),
        sa.PrimaryKeyConstraint("history_id"),
        schema=schema_name,
    )
    grant_standard_table_privileges(
        "monthly_total_precipitation_mv", schema=schema_name
    )
