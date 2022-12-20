"""
Trigger definitions for revision xxx.
"""

from pycds.alembic.extensions.replaceable_objects import (
    ReplaceableStoredProcedure, ReplaceableTrigger,
)
from pycds.context import get_schema_name, get_cxhx_schema_name


schema_name = get_schema_name()
cxhx_schema_name = get_cxhx_schema_name()


obs_raw_cxhx_log = ReplaceableStoredProcedure(
    identifier="""obs_raw_cxhx_log()""",
    definition=f"""
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF (NEW.time IS DISTINCT FROM OLD.time) OR (NEW.datum IS DISTINCT FROM OLD.datum) THEN
                INSERT INTO {cxhx_schema_name}.obs_raw_cxhx (obs_raw_id, mod_time, time, datum)
                VALUES (OLD.obs_raw_id, OLD.mod_time, OLD.time, OLD.datum);
            END IF;
        END;
        $$
    """,
    schema=cxhx_schema_name,
)


obs_raw_cxhx_trigger = ReplaceableTrigger(
    identifier="obs_raw_cxhx_trigger",
    definition=f"""
        BEFORE UPDATE ON {schema_name}.obs_raw
        FOR EACH ROW
        EXECUTE PROCEDURE {cxhx_schema_name}.obs_raw_cxhx_log()
    """,
    schema=cxhx_schema_name,
)
