"""Add function variable_tags

Revision ID: 83896ee79b06
Revises: 879f0efa125f
Create Date: 2023-06-28 10:12:38.733792

"""

from alembic import op
from pycds.orm.functions.version_83896ee79b06 import variable_tags

# revision identifiers, used by Alembic.
revision = "83896ee79b06"
down_revision = "879f0efa125f"
branch_labels = None
depends_on = None


def upgrade():
    op.create_replaceable_object(variable_tags)


def downgrade():
    op.drop_replaceable_object(variable_tags)
