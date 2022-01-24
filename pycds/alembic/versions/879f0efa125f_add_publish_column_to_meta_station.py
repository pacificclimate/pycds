"""add publish column to meta station

Revision ID: 879f0efa125f
Revises: 0d99ba90c229
Create Date: 2022-01-13 09:33:05.636350

"""
from alembic import op
import sqlalchemy as sa
from pycds.context import get_schema_name

# revision identifiers, used by Alembic.
revision = "879f0efa125f"
down_revision = "3d50ec832e47"
branch_labels = None
depends_on = None

schema_name = get_schema_name()

def upgrade():
    op.add_column(
        "meta_station",
        sa.Column("publish", sa.Boolean(), default=True, nullable=False),
        schema=schema_name
    )


def downgrade():
    op.drop_column("meta_station", "publish", schema=schema_name)
 