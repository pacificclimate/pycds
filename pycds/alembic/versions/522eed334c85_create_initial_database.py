"""Create initial database

Revision ID: 522eed334c85
Revises: 
Create Date: 2020-01-21 17:25:40.000843

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from pycds import get_schema_name


# revision identifiers, used by Alembic.
revision = "522eed334c85"
down_revision = None
branch_labels = None
depends_on = None


schema_name = get_schema_name()

# TODO: Verify index handling
# TODO: Add sequences


def upgrade():
    op.create_table(
        "meta_contact",
        sa.Column("contact_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("title", sa.String(), nullable=True),
        sa.Column("organization", sa.String(), nullable=True),
        sa.Column("email", sa.String(), nullable=True),
        sa.Column("phone", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("contact_id"),
        schema=schema_name,
    )
    op.create_table(
        "meta_pcic_flag",
        sa.Column("pcic_flag_id", sa.Integer(), nullable=False),
        sa.Column("flag_name", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("discard", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("pcic_flag_id"),
        schema=schema_name,
    )
    op.create_table(
        "meta_sensor",
        sa.Column("sensor_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("sensor_id"),
        schema=schema_name,
    )
    op.create_table(
        "meta_network",
        sa.Column("network_id", sa.Integer(), nullable=False),
        sa.Column("network_name", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("virtual", sa.String(length=255), nullable=True),
        sa.Column("publish", sa.Boolean(), nullable=True),
        sa.Column("col_hex", sa.String(), nullable=True),
        sa.Column("contact_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["contact_id"], [f"{schema_name}.meta_contact.contact_id"]
        ),
        sa.PrimaryKeyConstraint("network_id"),
        schema=schema_name,
    )
    op.create_table(
        "meta_native_flag",
        sa.Column("native_flag_id", sa.Integer(), nullable=False),
        sa.Column("flag_name", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("network_id", sa.Integer(), nullable=True),
        sa.Column("value", sa.String(), nullable=True),
        sa.Column("discard", sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(
            ["network_id"], [f"{schema_name}.meta_network.network_id"]
        ),
        sa.PrimaryKeyConstraint("native_flag_id"),
        sa.UniqueConstraint(
            "network_id", "value", name="meta_native_flag_unique"
        ),
        schema=schema_name,
    )
    op.create_table(
        "meta_station",
        sa.Column("station_id", sa.Integer(), nullable=False),
        sa.Column("native_id", sa.String(), nullable=True),
        sa.Column("network_id", sa.Integer(), nullable=True),
        sa.Column("min_obs_time", sa.DateTime(), nullable=True),
        sa.Column("max_obs_time", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(
            ["network_id"], [f"{schema_name}.meta_network.network_id"]
        ),
        sa.PrimaryKeyConstraint("station_id"),
        schema=schema_name,
    )
    op.create_table(
        "meta_vars",
        sa.Column("vars_id", sa.Integer(), nullable=False),
        sa.Column("net_var_name", sa.String(), nullable=True),
        sa.Column("unit", sa.String(), nullable=True),
        sa.Column("standard_name", sa.String(), nullable=True),
        sa.Column("cell_method", sa.String(), nullable=True),
        sa.Column("precision", sa.Float(), nullable=True),
        sa.Column("long_description", sa.String(), nullable=True),
        sa.Column("display_name", sa.String(), nullable=True),
        sa.Column("short_name", sa.String(), nullable=True),
        sa.Column("network_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["network_id"], [f"{schema_name}.meta_network.network_id"]
        ),
        sa.PrimaryKeyConstraint("vars_id"),
        schema=schema_name,
    )
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
    op.create_table(
        "meta_history",
        sa.Column("history_id", sa.Integer(), nullable=False),
        sa.Column("station_id", sa.Integer(), nullable=True),
        sa.Column("station_name", sa.String(), nullable=True),
        sa.Column("lon", sa.Numeric(), nullable=True),
        sa.Column("lat", sa.Numeric(), nullable=True),
        sa.Column("elev", sa.Float(), nullable=True),
        sa.Column("sdate", sa.Date(), nullable=True),
        sa.Column("edate", sa.Date(), nullable=True),
        sa.Column("tz_offset", sa.Interval(), nullable=True),
        sa.Column("province", sa.String(), nullable=True),
        sa.Column("country", sa.String(), nullable=True),
        sa.Column("comments", sa.String(length=255), nullable=True),
        sa.Column("freq", sa.String(), nullable=True),
        sa.Column("sensor_id", sa.Integer(), nullable=True),
        sa.Column(
            "the_geom", geoalchemy2.types.Geometry(srid=4326), nullable=True
        ),
        sa.ForeignKeyConstraint(
            ["sensor_id"], [f"{schema_name}.meta_sensor.sensor_id"]
        ),
        sa.ForeignKeyConstraint(
            ["station_id"], [f"{schema_name}.meta_station.station_id"]
        ),
        sa.PrimaryKeyConstraint("history_id"),
        schema=schema_name,
    )
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
    op.create_table(
        "collapsed_vars_mv",
        sa.Column("history_id", sa.Integer(), nullable=False),
        sa.Column("vars", sa.String(), nullable=True),
        sa.Column("display_names", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"]
        ),
        sa.PrimaryKeyConstraint("history_id"),
        schema=schema_name,
    )
    op.create_table(
        "obs_count_per_month_history_mv",
        sa.Column("count", sa.Integer(), nullable=True),
        sa.Column("date_trunc", sa.DateTime(), nullable=False),
        sa.Column("history_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"]
        ),
        sa.PrimaryKeyConstraint("date_trunc", "history_id"),
        schema=schema_name,
    )
    op.create_table(
        "obs_derived_values",
        sa.Column("obs_derived_value_id", sa.Integer(), nullable=False),
        sa.Column("value_time", sa.DateTime(), nullable=True),
        sa.Column("mod_time", sa.DateTime(), nullable=False),
        sa.Column("datum", sa.Float(), nullable=True),
        sa.Column("vars_id", sa.Integer(), nullable=True),
        sa.Column("history_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"]
        ),
        sa.ForeignKeyConstraint(
            ["vars_id"], [f"{schema_name}.meta_vars.vars_id"]
        ),
        sa.PrimaryKeyConstraint("obs_derived_value_id"),
        sa.UniqueConstraint(
            "value_time",
            "history_id",
            "vars_id",
            name="obs_derived_value_time_place_variable_unique",
        ),
        schema=schema_name,
    )
    op.create_table(
        "obs_raw",
        sa.Column("obs_raw_id", sa.BigInteger(), nullable=False),
        sa.Column("obs_time", sa.DateTime(), nullable=True),
        sa.Column("mod_time", sa.DateTime(), nullable=False),
        sa.Column("datum", sa.Float(), nullable=True),
        sa.Column("vars_id", sa.Integer(), nullable=True),
        sa.Column("history_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"]
        ),
        sa.ForeignKeyConstraint(
            ["vars_id"], [f"{schema_name}.meta_vars.vars_id"]
        ),
        sa.PrimaryKeyConstraint("obs_raw_id"),
        sa.UniqueConstraint(
            "obs_time",
            "history_id",
            "vars_id",
            name="time_place_variable_unique",
        ),
        schema=schema_name,
    )
    op.create_table(
        "station_obs_stats_mv",
        sa.Column("station_id", sa.Integer(), nullable=False),
        sa.Column("history_id", sa.Integer(), nullable=True),
        sa.Column("min_obs_time", sa.DateTime(), nullable=True),
        sa.Column("max_obs_time", sa.DateTime(), nullable=True),
        sa.Column("obs_count", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"]
        ),
        sa.ForeignKeyConstraint(
            ["station_id"], [f"{schema_name}.meta_station.station_id"]
        ),
        schema=schema_name,
    )
    op.create_table(
        "vars_per_history_mv",
        sa.Column("history_id", sa.Integer(), nullable=False),
        sa.Column("vars_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"]
        ),
        sa.ForeignKeyConstraint(
            ["vars_id"], [f"{schema_name}.meta_vars.vars_id"]
        ),
        sa.PrimaryKeyConstraint("history_id", "vars_id"),
        schema=schema_name,
    )
    op.create_table(
        "obs_raw_native_flags",
        sa.Column("obs_raw_id", sa.BigInteger(), nullable=True),
        sa.Column("native_flag_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["native_flag_id"],
            [f"{schema_name}.meta_native_flag.native_flag_id"],
        ),
        sa.ForeignKeyConstraint(
            ["obs_raw_id"], [f"{schema_name}.obs_raw.obs_raw_id"]
        ),
        sa.UniqueConstraint(
            "obs_raw_id", "native_flag_id", name="obs_raw_native_flag_unique"
        ),
        schema=schema_name,
    )
    with op.batch_alter_table(
        "obs_raw_native_flags", schema=schema_name
    ) as batch_op:
        batch_op.create_index("flag_index", ["obs_raw_id"], unique=False)

    op.create_table(
        "obs_raw_pcic_flags",
        sa.Column("obs_raw_id", sa.BigInteger(), nullable=True),
        sa.Column("pcic_flag_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["obs_raw_id"], [f"{schema_name}.obs_raw.obs_raw_id"]
        ),
        sa.ForeignKeyConstraint(
            ["pcic_flag_id"], [f"{schema_name}.meta_pcic_flag.pcic_flag_id"]
        ),
        sa.UniqueConstraint(
            "obs_raw_id", "pcic_flag_id", name="obs_raw_pcic_flag_unique"
        ),
        schema=schema_name,
    )
    with op.batch_alter_table(
        "obs_raw_pcic_flags", schema=schema_name
    ) as batch_op:
        batch_op.create_index("pcic_flag_index", ["obs_raw_id"], unique=False)

    op.create_table(
        "time_bounds",
        sa.Column("obs_raw_id", sa.Integer(), nullable=False),
        sa.Column("start", sa.DateTime(), nullable=True),
        sa.Column("end", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(
            ["obs_raw_id"], [f"{schema_name}.obs_raw.obs_raw_id"]
        ),
        sa.PrimaryKeyConstraint("obs_raw_id"),
        schema=schema_name,
    )


def downgrade():
    op.drop_table("time_bounds", schema=schema_name)
    with op.batch_alter_table(
        "obs_raw_pcic_flags", schema=schema_name
    ) as batch_op:
        batch_op.drop_index("pcic_flag_index")

    op.drop_table("obs_raw_pcic_flags", schema=schema_name)
    with op.batch_alter_table(
        "obs_raw_native_flags", schema=schema_name
    ) as batch_op:
        batch_op.drop_index("flag_index")

    op.drop_table("obs_raw_native_flags", schema=schema_name)
    op.drop_table("vars_per_history_mv", schema=schema_name)
    op.drop_table("station_obs_stats_mv", schema=schema_name)
    op.drop_table("obs_raw", schema=schema_name)
    op.drop_table("obs_derived_values", schema=schema_name)
    op.drop_table("obs_count_per_month_history_mv", schema=schema_name)
    op.drop_table("collapsed_vars_mv", schema=schema_name)
    op.drop_table("climo_obs_count_mv", schema=schema_name)
    op.drop_table("meta_history", schema=schema_name)
    op.drop_table("meta_climo_attrs", schema=schema_name)
    op.drop_table("meta_vars", schema=schema_name)
    op.drop_table("meta_station", schema=schema_name)
    op.drop_table("meta_native_flag", schema=schema_name)
    op.drop_table("meta_network", schema=schema_name)
    op.drop_table("meta_sensor", schema=schema_name)
    op.drop_table("meta_pcic_flag", schema=schema_name)
    op.drop_table("meta_contact", schema=schema_name)
