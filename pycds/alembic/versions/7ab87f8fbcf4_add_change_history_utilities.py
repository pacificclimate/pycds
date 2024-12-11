"""Add change history utilities

Add all utilities needed to implement change history tracking. These
utilities are functions and trigger functions installed in the schema.

Implementing change history tracking on any given table(s) will be done
in a separate migration.

Revision ID: 7ab87f8fbcf4
Revises: 081f17262852
Create Date: 2024-12-10 15:45:19.379892

"""
from alembic import op

from pycds.orm.functions.version_7ab87f8fbcf4 import (
    hxtk_collection_name_from_hx,
    hxtk_hx_table_name,
    hxtk_hx_id_name,
)
from pycds.orm.trigger_functions.version_7ab87f8fbcf4 import (
    hxtk_primary_ops_to_hx,
    hxtk_primary_control_hx_cols,
    hxtk_add_foreign_hx_keys,
)

# revision identifiers, used by Alembic.
revision = "7ab87f8fbcf4"
down_revision = "081f17262852"
branch_labels = None
depends_on = None


functions = (
    # Ordinary functions
    hxtk_collection_name_from_hx,
    hxtk_hx_table_name,
    hxtk_hx_id_name,
    # Trigger functions
    hxtk_primary_control_hx_cols,
    hxtk_primary_ops_to_hx,
    hxtk_add_foreign_hx_keys,
)


def upgrade():
    for f in functions:
        op.create_replaceable_object(f)


def downgrade():
    for f in reversed(functions):
        op.drop_replaceable_object(f)
