from pycds.context import get_schema_name
from pycds.alembic.extensions.replaceable_objects import ReplaceableFunction


schema_name = get_schema_name()


variable_tags = ReplaceableFunction(
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


