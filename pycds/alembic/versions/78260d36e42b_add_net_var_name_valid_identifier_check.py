"""Add net_var_name valid identifier check constraint

Revision ID: 78260d36e42b
Revises: bb2a222a1d4a
Create Date: 2024-01-04 23:22:47.992791

"""
from alembic import op
from pycds import get_schema_name

# revision identifiers, used by Alembic.
revision = "78260d36e42b"
down_revision = "bb2a222a1d4a"
branch_labels = None
depends_on = None

schema_name = get_schema_name()
table_name = "meta_vars"
constraint_name = "ck_net_var_name_valid_identifier"


def update_table():
    op.execute(
        f"""UPDATE {schema_name}.{table_name} SET net_var_name=regexp_replace(net_var_name, '\\W', '_', 'g') WHERE net_var_name ~ '\\W';"""
    )


def regress_table():
    # kept for convention.
    # basically impossible to reverse as we can't know what has been removed and what was an underscore already
    op.noop()


def upgrade():
    update_table()
    op.create_check_constraint(
        constraint_name, table_name, """net_var_name !~ '\\W'""", schema=schema_name
    )
    pass


def downgrade():
    op.drop_constraint(constraint_name, table_name, schema=schema_name, type_="check")
    # regress_table()
    pass
