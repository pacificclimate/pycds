"""create functions

Revision ID: 4a2f1879293a
Revises: 522eed334c85
Create Date: 2020-01-28 16:43:12.112378

"""
from alembic import op
import sqlalchemy as sa
from pycds.context import (get_schema_name, get_su_role_name)
from pycds.alembic.extensions.replaceable_objects import ReplaceableStoredProcedure 
import pycds.alembic.extensions.operation_plugins


schema_name = get_schema_name()

# revision identifiers, used by Alembic.
revision = "4a2f1879293a"
down_revision = "522eed334c85"
branch_labels = None
depends_on = None


closest_stns_within_threshold = ReplaceableStoredProcedure(
    """
    closest_stns_within_threshold(
        IN x numeric,
        IN y numeric,
        IN thres integer
    )""",
    f"""
    RETURNS TABLE(
        history_id integer,
        lat numeric,
        lon numeric,
        dist double precision
    ) AS
    $BODY$

    DECLARE
        mystr TEXT;
    BEGIN
        mystr = 'WITH stns_in_thresh AS (
        SELECT history_id, lat, lon,
            Geography(ST_Transform(the_geom,4326)) as p_existing,
            Geography(ST_SetSRID(ST_MakePoint('|| X ||','|| Y ||'),4326)) as p_new
        FROM {schema_name}.meta_history
        WHERE the_geom && ST_Buffer(Geography(ST_SetSRID(ST_MakePoint('|| X || ','|| Y ||'),4326)),'|| thres ||')
        )
        SELECT history_id, lat, lon, ST_Distance(p_existing,p_new) as dist
        FROM stns_in_thresh
        ORDER BY dist';
        RETURN QUERY EXECUTE mystr;
    END;
    $BODY$
      LANGUAGE plpgsql VOLATILE SECURITY DEFINER
      COST 100
      ROWS 1000;
    """,
    schema=schema_name,
)

daily_ts = ReplaceableStoredProcedure(
    """
    daily_ts(
        IN station_id integer,
        IN vars_id integer,
        IN percent_obs real,
        OUT daily_time timestamp without time zone,
        OUT daily_mean real,
        OUT percent_obs_available real,
        OUT daily_count integer)
    """,
    f"""
    RETURNS SETOF record AS
    $BODY$
    DECLARE
    BEGIN
        RAISE DEBUG 'Running daily_ts "%" "%" "%"', station_id, vars_id, percent_obs;
        FOR daily_time, daily_mean, daily_count IN EXECUTE
            'SELECT date_trunc(''day'', obs_time) as obs_time_trunc, avg(datum) as obs_datum, count(datum) as obs_count FROM {schema_name}.obs_raw WHERE station_id = ' || station_id || ' AND vars_id = ' || vars_id || ' GROUP BY obs_time_trunc ORDER BY obs_time_trunc'
        LOOP
            RAISE DEBUG 'In loop, Row: "%" "%" "%"', daily_time, daily_mean, daily_count;
            percent_obs_available := daily_count / 24.0;
                IF percent_obs_available >= percent_obs THEN
               RAISE DEBUG 'Conditional is TRUE';
               RETURN NEXT;
            END IF;
        END LOOP;
        RETURN;
            
    END;
    $BODY$
      LANGUAGE plpgsql VOLATILE
      COST 100
      ROWS 1000;
    """,
    schema=schema_name,
)


# Return the number of days in the month of the given date.
daysinmonth = ReplaceableStoredProcedure(
    """
    daysinmonth(d timestamp)
    """,
    """
    RETURNS double precision AS
    $BODY$
    SELECT EXTRACT(DAY FROM CAST(date_trunc('month', d) + interval '1 month'
    - interval '1 day' as timestamp));
    $BODY$
      LANGUAGE sql VOLATILE
      COST 100;
    """,
    schema=schema_name,
)


# Execute function `query_one_station`. This function does not appear to be
# in use in any production code. Possibly it is in use ad-hoc.
do_query_one_station = ReplaceableStoredProcedure(
    """
    do_query_one_station(station_id integer)
    """,
    f"""
    RETURNS refcursor AS
    $BODY$
    DECLARE
        query text;
        result refcursor := 'result';
    BEGIN
        query := {schema_name}.query_one_station(station_id);
        OPEN result NO SCROLL FOR EXECUTE query;
        RETURN result;
    END;
    $BODY$
      LANGUAGE plpgsql VOLATILE
      COST 100;
    """,
    schema=schema_name,
)


# Returns the day that should be used (the effective day) for computing daily
# temperature extrema. It maps the actual observation day to an effective day
# that causes it to be aggregated within the appropriate 24-hour period.
#
# Effective day depends on the extremum being computed (max, min) and the
# observation frequency (and, technically, the network, but for now it seems
# that 12-hour frequency is only used in a single network).
#
# For maximum temperature:
#   For 1-hour and daily frequency:
#       the effective day is day of observation
#   For 12-hour frequency:
#       nominal reporting times are 0700 and 1600 local; we use noon (1200)
#       to divide morning from afternoon reports the period over which the
#       max should be taken is from noon the day before to noon of the given
#       day the effective day is advanced by one day for afternoon observations
#
# For minimum temperature:
#   effective day does not depend on observation frequency; it is always the
#   day of observation
effective_day = ReplaceableStoredProcedure(
    """
    effective_day(
        obs_time timestamp without time zone,
        extremum character varying,
        freq character varying DEFAULT ''::character varying)
    """,
    """
    RETURNS timestamp without time zone AS
    $BODY$
    DECLARE
      offs INTERVAL; -- 'offset' is a reserved word
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
      LANGUAGE plpgsql VOLATILE
      COST 100;
    """,
    schema=schema_name,
)


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
getstationvariabletable = ReplaceableStoredProcedure(
    """
    getstationvariabletable(
        station_id integer,
        climo boolean)
    """,
    f"""
    RETURNS text AS
    $BODY$
        query = "SELECT vars_id, net_var_name FROM {schema_name}.meta_vars NATURAL JOIN {schema_name}.meta_station WHERE cell_method " + ("" if climo else "!") + "~ '(within|over)' AND station_id = " + str(station_id) + ' ORDER BY net_var_name'
        data = plpy.execute(query)
        bits = [ 'obs_time' ] + [ ("MAX(CASE WHEN vars_id=" + str(x['vars_id']) + " THEN datum END) as " + x['net_var_name']) for x in data ]
        vars_ids = [ str(x['vars_id']) for x in data ]
    
        hid_query = "SELECT history_id FROM {schema_name}.meta_history WHERE station_id=" + str(station_id)
        hid_data = plpy.execute(hid_query)
        hid_clauses = [ "history_id = " + str(x['history_id']) for x in hid_data ]
    
        return "SELECT "+ ",".join(bits) + " from {schema_name}.obs_raw WHERE (" + " OR ".join(hid_clauses) + ") AND vars_id IN (" + ",".join(vars_ids) + ") GROUP BY obs_time ORDER BY obs_time"
    $BODY$
      LANGUAGE plpythonu VOLATILE
      COST 100;
    """,
    schema=schema_name,
)


# Returns the last day of the month, as a date, of the month of the input date.
lastdateofmonth = ReplaceableStoredProcedure(
    """
    lastdateofmonth(date)
    """,
    """
    RETURNS date AS
    $BODY$
    SELECT CAST(date_trunc('month', $1) + interval '1 month' - interval '1 day' as date);
    $BODY$
      LANGUAGE sql VOLATILE
      COST 100;
    """,
    schema=schema_name,
)


monthly_ts = ReplaceableStoredProcedure(
    """
    monthly_ts(
        IN station_id integer,
        IN vars_id integer,
        IN percent_obs real,
        OUT monthly_time timestamp without time zone,
        OUT monthly_mean real,
        OUT percent_obs_available real,
        OUT monthly_count integer)
    """,
    f"""
    RETURNS SETOF record AS
    $BODY$
    DECLARE
        the_month date;
    BEGIN
        RAISE DEBUG 'Running monthly_ts "%" "%" "%"', station_id, vars_id, percent_obs;
        FOR monthly_time, monthly_mean, monthly_count IN EXECUTE
            'SELECT date_trunc(''month'', obs_time) as obs_time_trunc, avg(datum) as obs_datum, count(datum) as obs_count FROM {schema_name}.obs_raw WHERE station_id = ' || station_id || ' AND vars_id = ' || vars_id || ' GROUP BY obs_time_trunc ORDER BY obs_time_trunc'
        LOOP
            RAISE DEBUG 'In loop, Row: "%" "%" "%"', monthly_time, monthly_mean, monthly_count;
            the_month := CAST(monthly_time AS date);
            percent_obs_available := monthly_count / ({schema_name}.DaysInMonth(the_month));
                IF percent_obs_available >= percent_obs THEN
               RAISE DEBUG 'Conditional is TRUE';
               RETURN NEXT;
            END IF;
        END LOOP;
        RETURN;
    END;
    $BODY$
      LANGUAGE plpgsql VOLATILE
      COST 100
      ROWS 1000;
    """,
    schema=schema_name,
)


# Returns a row set containing the values of all the non-climatological
# variables reported by the specified station. For the definition of the
# row set, see function `getStationVariableTable`.
# NOTE: Production code: This function is called by the PDP PCDS backend.
query_one_station = ReplaceableStoredProcedure(
    """
    query_one_station(station_id integer)
    """,
    f"""
    RETURNS text AS
    $BODY$
        stn_query = "SELECT * FROM {schema_name}.getStationVariableTable(" + str(station_id) + ", false)"
        data = plpy.execute(stn_query)
        #plpy.warning(data)
        return data[0]['getstationvariabletable']
    $BODY$
      LANGUAGE plpythonu VOLATILE
      COST 100;
    """,
    schema=schema_name,
)


# Returns a row set containing the values of all the climatological
# variables reported by the specified station. For the definition of the
# row set, see function `getStationVariableTable`.
# NOTE: Production code: This function is called by the PDP PCDS backend.
query_one_station_climo = ReplaceableStoredProcedure(
    """
    query_one_station_climo(station_id integer)
    """,
    f"""
    RETURNS text AS
    $BODY$
        stn_query = "SELECT * FROM {schema_name}.getStationVariableTable(" + str(station_id) + ", true)"
        data = plpy.execute(stn_query)
        #plpy.warning(data)
        return data[0]['getstationvariabletable']
    $BODY$
      LANGUAGE plpythonu VOLATILE
      COST 100;
    """,
    schema=schema_name,
)


season = ReplaceableStoredProcedure(
    """
    season(d timestamp without time zone)
    """,
    """
    RETURNS date AS
    $BODY$
    DECLARE
        m DOUBLE PRECISION;
    BEGIN
        m := date_part('month', d);
        CASE m
            WHEN 12, 1, 2 THEN RETURN ('' || extract(year from d) || '-01-15')::DATE;
            WHEN 3, 4, 5 THEN RETURN ('' || extract(year from d) || '-04-15')::DATE;
            WHEN 6, 7, 8 THEN RETURN ('' || extract(year from d) || '-07-15')::DATE;
            WHEN 9, 10, 11 THEN RETURN ('' || extract(year from d) || '-10-15')::DATE;
        END CASE;
    END
    $BODY$
      LANGUAGE plpgsql VOLATILE
      COST 100;
    """,
    schema=schema_name,
)


updatesdateedate = ReplaceableStoredProcedure(
    """
    updatesdateedate()
    """,
    f"""
    RETURNS void AS
    $BODY$
    DECLARE
        sid  integer;
        min_date timestamp without time zone;
        max_date timestamp without time zone;
    BEGIN
        FOR sid IN SELECT DISTINCT station_id FROM {schema_name}.meta_history
        LOOP
            SELECT min(obs_time), max(obs_time) INTO min_date, max_date
            FROM {schema_name}.obs_raw NATURAL JOIN {schema_name}.meta_history 
            WHERE station_id = sid;
    
            UPDATE {schema_name}.meta_station
            SET (min_obs_time, max_obs_time) = (min_date, max_date)
            WHERE station_id = sid;
    END LOOP;
    RETURN;
    END;
    $BODY$
      LANGUAGE plpgsql VOLATILE
      COST 100;
    """,
    schema=schema_name,
)


stored_procedures = (
    closest_stns_within_threshold,
    daily_ts,
    daysinmonth,
    do_query_one_station,
    effective_day,
    getstationvariabletable,
    lastdateofmonth,
    monthly_ts,
    query_one_station,
    query_one_station_climo,
    season,
    updatesdateedate,
)


def upgrade():
    # In order to create stored procedures using untrusted languages (we use
    # `plpythonu`), the user needs superuser privileges. This is achieved by
    # temporarily setting the role to a superuser role name that is externally
    # granted to the user only for the period when database migrations are
    # performed.
    op.set_role(get_su_role_name())
    for sp in stored_procedures:
        op.create_replaceable_object(sp)
    op.reset_role()


def downgrade():
    for sp in stored_procedures:
        op.drop_replaceable_object(sp)
