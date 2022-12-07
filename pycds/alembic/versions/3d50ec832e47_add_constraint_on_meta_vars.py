"""add constraint on meta vars

Revision ID: 3d50ec832e47
Revises: 0d99ba90c229
Create Date: 2022-01-12 13:11:12.480424

"""
from alembic import op
import sqlalchemy as sa
import citext as ci
from pycds.orm.views.version_84b7fc2596d5 import ObsWithFlags
from pycds import get_schema_name, get_su_role_name

# revision identifiers, used by Alembic.
revision = "3d50ec832e47"
down_revision = "0d99ba90c229"
branch_labels = None
depends_on = None

schema_name = get_schema_name()


def upgrade():
    op.set_role(get_su_role_name())
    op.execute("CREATE EXTENSION IF NOT EXISTS citext")
    op.reset_role()

    op.drop_replaceable_object(ObsWithFlags)
    op.alter_column(
        "meta_vars",
        "net_var_name",
        type_=ci.CIText(),
        schema=schema_name,
    )
    op.create_unique_constraint(
        "network_variable_name_unique",
        "meta_vars",
        ["network_id", "net_var_name"],
        schema=schema_name,
    )
    op.create_replaceable_object(ObsWithFlags)


def downgrade():
    op.drop_replaceable_object(ObsWithFlags)
    op.drop_constraint(
        "network_variable_name_unique",
        "meta_vars",
        schema=schema_name,
        type_="unique",
    )
    op.alter_column(
        "meta_vars",
        "net_var_name",
        type_=sa.String(),
        schema=schema_name,
    )
    op.create_replaceable_object(ObsWithFlags)

    op.set_role(get_su_role_name())
    op.execute("DROP EXTENSION IF EXISTS citext")
    op.reset_role()
