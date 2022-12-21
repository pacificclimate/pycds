"""Add change history

Revision ID: 96841303c39c
Revises: 879f0efa125f
Create Date: 2022-12-20 11:22:48.187932

"""
from alembic import op
import sqlalchemy as sa
from pycds import get_cxhx_schema_name, get_schema_name


# revision identifiers, used by Alembic.
revision = "96841303c39c"
down_revision = "879f0efa125f"
branch_labels = None
depends_on = None


schema_name = get_schema_name()
cxhx_schema_name = get_cxhx_schema_name()


def upgrade():
    op.create_table(
        "obs_raw_cxhx",
        sa.Column("obs_raw_cxhx_id", sa.Integer(), nullable=False),
        sa.Column("obs_raw_id", sa.BigInteger(), nullable=False),
        sa.Column("obs_time", sa.DateTime(), nullable=True),
        sa.Column("mod_time", sa.DateTime(), nullable=False),
        sa.Column("datum", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ["obs_raw_id"], [f"{schema_name}.obs_raw.obs_raw_id"]
        ),
        sa.PrimaryKeyConstraint("obs_raw_cxhx_id"),
        schema=cxhx_schema_name,
    )


def downgrade():
    op.drop_table("obs_raw_native_flags", schema=cxhx_schema_name)