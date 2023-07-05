"""Add function variable_tags

Revision ID: 83896ee79b06
Revises: 879f0efa125f
Create Date: 2023-06-28 10:12:38.733792

"""
from alembic import op
from pycds.context import get_schema_name
from pycds.alembic.extensions.replaceable_objects import ReplaceableStoredProcedure

# revision identifiers, used by Alembic.
revision = "83896ee79b06"
down_revision = "879f0efa125f"
branch_labels = None
depends_on = None


schema_name = get_schema_name()


variable_tags = ReplaceableStoredProcedure(
    f"variable_tags(var {schema_name}.meta_vars)",
    f"""
    RETURNS text[]
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    AS $BODY$
     SELECT CASE
         WHEN var.net_var_name ~ 'Climatology' THEN array['climatology']
         ELSE array['observation']
        END;
    $BODY$
    """,
    replace=True,
    schema=schema_name,
)


def upgrade():
    op.create_replaceable_object(variable_tags)


def downgrade():
    op.drop_replaceable_object(variable_tags)
