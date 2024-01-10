"""Fix function getstationvariabletable

Revision ID: efde19ea4f52
Revises: 6cb393f711c3
Create Date: 2024-01-09 16:22:39.469951

"""
from alembic import op
import sqlalchemy as sa

from pycds.orm.functions.version_4a2f1879293a import (
    getstationvariabletable as old_getstationvariabletable,
)
from pycds.orm.functions.version_efde19ea4f52 import (
    getstationvariabletable as new_getstationvariabletable,
)

# revision identifiers, used by Alembic.
revision = "efde19ea4f52"
down_revision = "6cb393f711c3"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_replaceable_object(old_getstationvariabletable)
    op.create_replaceable_object(new_getstationvariabletable)


def downgrade():
    op.drop_replaceable_object(new_getstationvariabletable)
    op.create_replaceable_object(old_getstationvariabletable)
