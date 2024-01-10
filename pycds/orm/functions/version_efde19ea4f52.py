from pycds.context import get_schema_name
from pycds.alembic.extensions.replaceable_objects import ReplaceableFunction


schema_name = get_schema_name()


# Returns the text of a SELECT statement for a table containing the values
# of all the variables reported by the specified station, filtered by whether
# the variables are climatological or non-climatological. The rows of the
# resulting table contain the following columns:
#
#   `obs_time`
#       for each variable reported by the station:
#       `   datum` of observation for this variable at `obs_time`
#           (`NULL` if no observation for this variable at `obs_time`)
#               AS `net_var_name`
#
# This would be better done in application code with SQLAlchemy, but it likely
# predates its introduction. Consider such replacement.
#
# NOTE: Production code: This function is called by functions
# `query_one_station` and `query_one_station_climo` .
getstationvariabletable = ReplaceableFunction(
    """
    getstationvariabletable(
        station_id integer,
        climo boolean)
    """,
    f"""
    RETURNS text
    LANGUAGE 'plpython3u'
    COST 100
    VOLATILE PARALLEL UNSAFE
    AS $BODY$
        query = (
            "SELECT vars_id, net_var_name " 
            "FROM meta_vars JOIN meta_station ON meta_vars.network_id = meta_station.network_id " 
            "WHERE ARRAY['" + ("climatology" if climo else "observation") + "'::text] <@ variable_tags(meta_vars.*) " 
            "AND station_id = " + str(station_id)
            + " ORDER BY net_var_name" 
        )
        data = plpy.execute(query)
        bits = [ 'obs_time' ] + [ ("MAX(CASE WHEN vars_id=" + str(x['vars_id']) + " THEN datum END) as " + x['net_var_name']) for x in data ]
        vars_ids = [ str(x['vars_id']) for x in data ]
        hid_query = "SELECT history_id FROM meta_history WHERE station_id=" + str(station_id)
        hid_data = plpy.execute(hid_query)
        hid_clauses = [ "history_id = " + str(x['history_id']) for x in hid_data ]
        return "SELECT "+ ",".join(bits) + " from obs_raw WHERE (" + " OR ".join(hid_clauses) + ") AND vars_id IN (" + ",".join(vars_ids) + ") GROUP BY obs_time ORDER BY obs_time" 
    $BODY$;
    """,
    schema=schema_name,
)
