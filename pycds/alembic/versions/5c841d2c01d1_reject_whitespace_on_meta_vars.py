"""Updates text columns in meta_var to strip newlines and add constraints
preventing further whitespace from being added.

Revision ID: 5c841d2c01d1
Revises: 6cb393f711c3
Create Date: 2024-01-10 00:06:47.471826

"""
from alembic import op
import sqlalchemy as sa
from pycds import get_schema_name
from pycds.orm.tables import no_newline_ck_name, no_newline_ck_check


# revision identifiers, used by Alembic.
revision = "5c841d2c01d1"
down_revision = "6cb393f711c3"
branch_labels = None
depends_on = None


schema_name = get_schema_name()
table_name = "meta_vars"
columns_to_apply = [
    "unit",
    "standard_name",
    "cell_method",
    "display_name",
    "short_name",
    "long_description",
]
# existing newlines will be replaced with a single space.
replace_value = " "


# This migration applies the same check and update operation to multiple columns. We can reduce some of this repetition
# by defining some string templates that apply the column


def strip_newlines_update_template(column):
    return f"""
        UPDATE {schema_name}.{table_name} SET {column} = regexp_replace({column}, '[\r\n]+', '{replace_value}', 'g') 
        WHERE {column} ~ '[\r\n]+';
    """


def update_table():
    update_statement = "\n".join(
        list(map(strip_newlines_update_template, columns_to_apply))
    )
    op.execute(update_statement)


def regress_table():
    # The data change here is not reversible due to a loss of specific information
    pass


def upgrade():
    update_table()
    for column in columns_to_apply:
        op.create_check_constraint(
            no_newline_ck_name(column),
            table_name,
            no_newline_ck_check(column),
            schema=schema_name,
        )


def downgrade():
    for column in columns_to_apply:
        op.drop_constraint(
            no_newline_ck_name(column),
            table_name,
            schema=schema_name,
            type_="check",
        )
    regress_table()
