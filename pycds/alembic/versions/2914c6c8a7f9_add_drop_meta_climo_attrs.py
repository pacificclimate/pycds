"""Add/drop meta_climo_attrs

Revision ID: 2914c6c8a7f9
Revises: 0d99ba90c229
Create Date: 2021-05-26 11:20:52.714448

"""
from alembic import op
import sqlalchemy as sa
from pycds.context import get_schema_name


# revision identifiers, used by Alembic.
revision = "2914c6c8a7f9"
down_revision = "e688e520d265"
branch_labels = None
depends_on = None

schema_name = get_schema_name()


def upgrade():
    op.drop_table_if_exists("meta_climo_attrs", schema=schema_name)


def downgrade():
    op.create_table(
        "meta_climo_attrs",
        sa.Column("vars_id", sa.Integer(), nullable=False),
        sa.Column("station_id", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("wmo_code", sa.String(length=1), nullable=True),
        sa.Column("adjusted", sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(
            ["station_id"], [f"{schema_name}.meta_station.station_id"]
        ),
        sa.ForeignKeyConstraint(
            ["vars_id"], [f"{schema_name}.meta_vars.vars_id"]
        ),
        sa.PrimaryKeyConstraint("vars_id", "station_id", "month"),
        schema=schema_name,
    )
