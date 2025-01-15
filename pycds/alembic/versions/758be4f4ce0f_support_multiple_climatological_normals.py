"""Support multiple climatological normals

Revision ID: 758be4f4ce0f
Revises: 7ab87f8fbcf4
Create Date: 2025-01-13 17:19:03.192403

"""
from citext import CIText
from alembic import op

# import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy import (
    Column,
    Integer,
    String,
    Interval,
    ForeignKey,
    DateTime,
    Float,
)
from sqlalchemy.dialects.postgresql import ARRAY

from pycds import get_schema_name

# revision identifiers, used by Alembic.
# TODO: When version control PRs are merged, "rebase" this onto the head revision.
revision = "758be4f4ce0f"
down_revision = "7ab87f8fbcf4"
branch_labels = None
depends_on = None


schema_name = get_schema_name()


# Note: These tables are likely to be under version control. Currently they are defined
# as not so. The idea is that they will have the conversion to VC applied after they are
# added to the database. That could be done in this migration or in a separate subsequent
# migration.


def upgrade():
    op.create_table(
        # TODO: Columns in this table parallel those in meta_station and meta_history.
        # They differ in the following ways, which may be questioned:
        #
        # - String types do not provide lengths. All strings are ultimately variable
        #   length in PG and there seems no reason to limit them. (Except perhaps to
        #   prevent entry of erroneous values that just happen to be very long?)
        #
        # - None are nullable. In contrast, most in the model tables are.
        "meta_climatological_station",  # TODO: Revise name?
        Column("climatological_station_id", Integer, primary_key=True),
        Column("station_type", String, nullable=False),  # TODO: Enumeration?
        Column("basin_id", Integer, nullable=False),  # TODO: Same as basin id in SCIP?
        Column("station_name", String, nullable=False),
        Column("province", String, nullable=False),
        Column("country", String, nullable=False),
        Column("tz_offset", Interval, nullable=False),
        Column("comments", String, nullable=False),
        Column("location", Geometry("GEOMETRY", 4326), nullable=False),
        schema=schema_name,
    )

    op.create_table(
        # TODO: Name? This is a recommended pattern but not the only such.
        "meta_climatological_station_x_meta_history",
        Column(
            "climatological_station_id",
            Integer,
            ForeignKey(
                f"{schema_name}.meta_climatological_station.climatological_station_id"
            ),
            primary_key=True,
        ),
        Column(
            "history_id",
            Integer,
            ForeignKey(f"{schema_name}.meta_history.history_id"),
            primary_key=True,
        ),
        schema=schema_name,
    )

    op.create_table(
        # TODO: Columns in this table parallel those in meta_vars.
        # They differ in the following ways, which may be questioned:
        #
        # - String types do not provide lengths. All strings are ultimately variable
        #   length in PG and there seems no reason to limit them. (Except perhaps to
        #   prevent entry of erroneous values that just happen to be very long?)
        #
        # - None are nullable. In contrast, most in the model tables are.
        "meta_climatological_variable",
        Column("climatological_variable_id", Integer, primary_key=True),
        # TODO: Duration can be computed from climatology_bounds. Do this with a provided
        #  function or store in separate column (this one)?
        #  In either case, represent value as an enumeration type?
        Column("duration", String, nullable=False),
        # climatology_bounds corresponds to the attribute of the same name defined in
        # CF Metadata Standards, 7.4 Climatological Statistics
        Column("climatology_bounds", ARRAY(String, dimensions=2), nullable=False),
        Column("num_years", Integer, nullable=False),
        Column("unit", String, nullable=False),
        Column("precision", String, nullable=False),  # Type? Utility???
        Column("standard_name", String, nullable=False),
        Column("display_name", String, nullable=False),
        Column("short_name", String, nullable=False),
        Column("cell_methods", String, nullable=False),
        Column("net_var_name", CIText(), nullable=False),
        schema=schema_name,
    )

    op.create_table(
        # TODO: Columns in this table parallel those in obs_raw.
        # They differ in the following ways, which may be questioned:
        #
        # - None are nullable. In contrast, most in the model tables are.
        "climatological_value",
        Column("climatological_value_id", Integer, primary_key=True),
        Column("mod_time", DateTime, nullable=False),
        Column("datum_time", DateTime, nullable=False),
        Column("datum", Float, nullable=False),
        Column("num_contributing_years", Integer, nullable=False),
        Column(
            "climatological_variable_id",
            Integer,
            ForeignKey(
                f"{schema_name}.meta_climatological_variable.climatological_variable_id"
            ),
        ),
        schema=schema_name,
    )


def downgrade():
    op.drop_table("climatological_value", schema=schema_name)
    op.drop_table("meta_climatological_variable", schema=schema_name)
    op.drop_table("meta_climatological_station_x_meta_history", schema=schema_name)
    op.drop_table("meta_climatological_station", schema=schema_name)
