"""Support multiple climo normals

Revision ID: 758be4f4ce0f
Revises: 7ab87f8fbcf4
Create Date: 2025-01-13 17:19:03.192403

"""

from alembic import op

# import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy import (
    Column,
    Enum,
    Integer,
    String,
    ForeignKey,
    DateTime,
    Float,
)
from sqlalchemy.dialects.postgresql import CITEXT, ENUM as PG_ENUM

from pycds import get_schema_name

# revision identifiers, used by Alembic.
# TODO: When version control PRs are merged, "rebase" this onto the head revision.
revision = "758be4f4ce0f"
down_revision = "8c05da87cb79"
branch_labels = None
depends_on = None


schema_name = get_schema_name()


# Note: These tables are likely to be under version control. Currently they are defined
# as not so. The idea is that they will have the conversion to VC applied after they are
# added to the database. That could be done in this migration or in a separate subsequent
# migration.

def upgrade():

    op.create_table(
        "climo_period",
        Column("climo_period_id", Integer, primary_key=True),
        Column("start_date", DateTime, nullable=False),
        Column("end_date", DateTime, nullable=False),
        schema=schema_name,
    ) 

    Enum("long-record", "composite", "prism", name="climo_station_type_enum").create(op.get_bind())

    op.create_table(
        # TODO: Columns in this table parallel those in meta_station and meta_history.
        # They differ in the following ways, which may be questioned:
        #
        # - String types do not provide lengths. All strings are ultimately variable
        #   length in PG and there seems no reason to limit them. (Except perhaps to
        #   prevent entry of erroneous values that just happen to be very long?)
        #
        # - None are nullable. In contrast, most in the model tables are.
        "climo_station",  # TODO: Revise name?
        Column("climo_station_id", Integer, primary_key=True),
        Column(
            "type", PG_ENUM("long-record", "composite", "prism", name="climo_station_type_enum", create_type=False), nullable=False
        ),
        Column("basin_id", Integer, nullable=True),
        Column("comments", String, nullable=False),
        Column(
            "climo_period_id",
            Integer,
            ForeignKey(
                f"{schema_name}.climo_period.climo_period_id"
            ),
            nullable=False,
        ),
        schema=schema_name,
    )

    Enum("base", "joint", name="climo_station_role_enum").create(op.get_bind())

    op.create_table(
        "climo_stn_x_hist",
        Column(
            "climo_station_id",
            Integer,
            ForeignKey(
                f"{schema_name}.climo_station.climo_station_id"
            ),
            primary_key=True,
        ),
        Column(
            "history_id",
            Integer,
            ForeignKey(f"{schema_name}.meta_history.history_id"),
            primary_key=True,
        ),
        Column("role", PG_ENUM("base", "joint", name="climo_station_role_enum", create_type=False), nullable=False),
        schema=schema_name,
    )

    Enum("annual", "seasonal", "monthly", name="climo_duration_enum").create(op.get_bind())

    op.create_table(
        # TODO: Columns in this table parallel those in meta_vars.
        # They differ in the following ways, which may be questioned:
        #
        # - String types do not provide lengths. All strings are ultimately variable
        #   length in PG and there seems no reason to limit them. (Except perhaps to
        #   prevent entry of erroneous values that just happen to be very long?)
        #
        # - None are nullable. In contrast, most in the model tables are.
        "climo_variable",
        Column("climo_variable_id", Integer, primary_key=True),
        Column(
            "duration", PG_ENUM("annual", "seasonal", "monthly", name="climo_duration_enum", create_type=False), nullable=False
        ),
        Column("unit", String, nullable=False),
        Column("standard_name", String, nullable=False),
        Column("display_name", String, nullable=False),
        Column("short_name", String, nullable=False),
        Column("cell_methods", String, nullable=False),
        Column("net_var_name", CITEXT(), nullable=False),
        schema=schema_name,
    )

    op.create_table(
        "climo_value",
        Column("climo_value_id", Integer, primary_key=True),
        Column("value_time", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("num_contributing_years", Integer, nullable=False),
        Column(
            "climo_variable_id",
            Integer,
            ForeignKey(
                f"{schema_name}.climo_variable.climo_variable_id"
            ),
        ),
        Column(
            "climo_station_id",
            Integer,
            ForeignKey(
                f"{schema_name}.climo_station.climo_station_id"
            ),
        ),
        schema=schema_name,
    )


def downgrade():
    op.drop_table("climo_value", schema=schema_name)
    op.drop_table("climo_variable", schema=schema_name)
    Enum("annual", "seasonal", "monthly", name="climo_duration_enum").drop(op.get_bind())
    op.drop_table("climo_stn_x_hist", schema=schema_name)
    Enum("base", "joint", name="climo_station_role_enum").drop(op.get_bind())
    op.drop_table("climo_station", schema=schema_name)
    Enum("long-record", "composite", "prism", name="climo_station_type_enum").drop(op.get_bind())
    op.drop_table("climo_period", schema=schema_name)
