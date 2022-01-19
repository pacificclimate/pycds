"""add not null constraint on meta vars

Revision ID: 7b139906ac46
Revises: 3d50ec832e47
Create Date: 2022-01-17 14:21:37.975335

"""
from alembic import op
import sqlalchemy as sa
from pycds import get_schema_name

# revision identifiers, used by Alembic.
revision = "7b139906ac46"
down_revision = "3d50ec832e47"
branch_labels = None
depends_on = None

schema_name = get_schema_name()
table_name = "meta_vars"

def update_table(
    var,
    replacement,
):
    op.execute(f"UPDATE {schema_name}.{table_name} SET {var}={replacement} WHERE {var} IS null")

def regress_table(
    var,
    replacable,
):
    op.execute(f"UPDATE {schema_name}.{table_name} SET {var}=NULL WHERE {var} LIKE {replacable}")

def upgrade():
    update_table("cell_method", "'foo: bar'")
    update_table("standard_name", "'foo_bar'")
    update_table("display_name", "'foo bar'")
    op.alter_column("meta_vars", "cell_method", nullable=False, schema=schema_name,)
    op.alter_column("meta_vars", "standard_name", nullable=False, schema=schema_name,)
    op.alter_column("meta_vars", "display_name", nullable=False, schema=schema_name,)

def downgrade():
    op.alter_column("meta_vars", "display_name", nullable=True, schema=schema_name,)
    op.alter_column("meta_vars", "standard_name", nullable=True, schema=schema_name,)
    op.alter_column("meta_vars", "cell_method", nullable=True, schema=schema_name,)
    regress_table("display_name", "'foo bar'")
    regress_table("standard_name", "'foo_bar'")
    regress_table("cell_method", "'foo: bar'")
