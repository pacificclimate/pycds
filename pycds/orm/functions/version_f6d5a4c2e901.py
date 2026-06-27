"""
Function definitions introduced by migration f6d5a4c2e901.

Only effective_day is changed in this version. Its implementation is retained;
the function is declared immutable and parallel safe so PostgreSQL may run
daily-extrema aggregation plans in parallel.
"""

from pycds.alembic.extensions.replaceable_objects import ReplaceableFunction
from pycds.context import get_schema_name


schema_name = get_schema_name()


effective_day = ReplaceableFunction(
    """
    effective_day(
        obs_time timestamp without time zone,
        extremum character varying,
        freq character varying DEFAULT ''::character varying
    )
    """,
    """
    RETURNS timestamp without time zone
    AS $BODY$
    DECLARE
        offs INTERVAL;
        hour INTEGER;
    BEGIN
        offs := '0'::INTERVAL;

        IF extremum = 'max' THEN
            hour := date_part('hour', obs_time);

            IF freq = '12-hourly' AND hour >= 12 THEN
                offs = '1 day'::INTERVAL;
            END IF;
        ELSE
            offs = '0'::INTERVAL;
        END IF;

        RETURN date_trunc('day', obs_time) + offs;
    END;
    $BODY$
    LANGUAGE plpgsql
    IMMUTABLE
    PARALLEL SAFE
    COST 100;
    """,
    schema=schema_name,
    replace=True,
)
