"""Add VarsPerHistory native matview

Revision ID: 7a3b247c577b
Revises: 8fd8f556c548
Create Date: 2021-01-18 17:06:14.064487

"""
from alembic import op
import sqlalchemy as sa
from pycds import get_schema_name
from pycds.materialized_views import VarsPerHistory

# revision identifiers, used by Alembic.
revision = "7a3b247c577b"
down_revision = "8fd8f556c548"
branch_labels = None
depends_on = None


def upgrade():
    schema_name = get_schema_name()
    op.drop_table("vars_per_history_mv", schema=schema_name)
    op.create_native_materialized_view(VarsPerHistory, schema=schema_name)


def downgrade():
    schema_name = get_schema_name()
    op.drop_native_materialized_view(VarsPerHistory, schema=schema_name)
    op.create_table(
        "vars_per_history_mv",
        sa.Column("history_id", sa.Integer(), nullable=False),
        sa.Column("vars_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"],
        ),
        sa.ForeignKeyConstraint(["vars_id"], [f"{schema_name}.meta_vars.vars_id"],),
        sa.PrimaryKeyConstraint("history_id", "vars_id"),
        schema=schema_name,
    )
