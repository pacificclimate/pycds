"""Convert climo_obs_count_mv to native matview

Revision ID: 96729d6db8b3
Revises: 5c841d2c01d1
Create Date: 2023-07-14 15:35:03.045034

"""

import logging
from alembic import op
import sqlalchemy as sa
from pycds.alembic.util import (
    grant_standard_table_privileges,
    create_matview,
    create_view,
    drop_matview,
    drop_view,
)
from pycds import get_schema_name
from pycds.database import matview_exists
from pycds.orm.native_matviews.version_96729d6db8b3 import (
    ClimoObsCount as ClimoObsCountMatview,
)
from pycds.orm.views.version_522eed334c85 import (
    ClimoObsCount as OldClimoObsCountView,
)

# Import the view that will be replaced by the native matview
from pycds.orm.views.version_96729d6db8b3 import ClimoObsCount as ClimoObsCountView

# revision identifiers, used by Alembic.
revision = "96729d6db8b3"
down_revision = "5c841d2c01d1"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")
schema_name = get_schema_name()


def upgrade():
    conn = op.get_bind()
    if matview_exists(conn, ClimoObsCountMatview.__tablename__, schema=schema_name):
        logger.info(
            f"A native materialized view '{ClimoObsCountMatview.__tablename__}' "
            f"already exists in the database; skipping upgrade"
        )
    else:
        drop_view(ClimoObsCountView, schema=schema_name)
        op.drop_table_if_exists(ClimoObsCountMatview.__tablename__, schema=schema_name)
        create_matview(ClimoObsCountMatview, schema=schema_name)


def downgrade():
    drop_matview(ClimoObsCountMatview, schema=schema_name)
    op.create_table(
        "climo_obs_count_mv",
        sa.Column("count", sa.BigInteger(), nullable=True),
        sa.Column("history_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"]
        ),
        sa.PrimaryKeyConstraint("history_id"),
        schema=schema_name,
    )
    grant_standard_table_privileges("climo_obs_count_mv", schema=schema_name)
    create_view(OldClimoObsCountView, schema=schema_name)
