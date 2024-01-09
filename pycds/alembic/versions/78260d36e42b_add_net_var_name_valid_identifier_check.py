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


# Updates existing table data to conform to the new constraint. Because this table is small it is written for
# readability over pure performance doing each replacement seperately.
def update_table():
    op.execute(
        f"""
        CREATE EXTENSION unaccent;

        -- strip diacritics from characters to make what we can valid ascii
        UPDATE crmp.meta_vars SET net_var_name=unaccent(net_var_name);
        -- replace first characters that aren't letters or underscores with an underscore
        UPDATE crmp.meta_vars SET net_var_name=regexp_replace(net_var_name, '^[^a-zA-Z_]', '_', 'g');
        -- replace all other whitespace & non valid characters
        UPDATE crmp.meta_vars SET net_var_name=regexp_replace(net_var_name, '[^a-zA-Z0-9_$]', '_', 'g');
        """
    )


def regress_table():
    # kept for convention.
    # basically impossible to reverse as we can't know what has been removed and what was an underscore already
    pass


def upgrade():
    update_table()
    op.create_check_constraint(
        constraint_name,
        table_name,
        "net_var_name ~ '^[a-zA-Z_][a-zA-Z0-9_$]*$'",
        schema=schema_name,
    )


def downgrade():
    op.drop_constraint(constraint_name, table_name, schema=schema_name, type_="check")
    regress_table()
