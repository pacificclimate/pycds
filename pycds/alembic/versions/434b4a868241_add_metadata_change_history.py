"""Add metadata change history

Revision ID: 434b4a868241
Revises: 081f17262852
Create Date: 2024-08-16 15:47:28.844217

"""
from alembic import op
import sqlalchemy as sa
from pycds.context import get_su_role_name
from pycds.orm.trigger_functions.version_434b4a868241 import update_metadata


# revision identifiers, used by Alembic.
revision = "434b4a868241"
down_revision = "081f17262852"
branch_labels = None
depends_on = None


def upgrade():
    op.set_role(get_su_role_name())
    op.create_replaceable_object(update_metadata)
    op.reset_role()


def downgrade():
    op.drop_replaceable_object(update_metadata)
