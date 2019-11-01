"""
This module defines SQL functions in the database.

SQLAlchemy does not directly support functions. However, it does support
arbitrary DDL statements, which are the usual route for actions that are not
directly supported, such as defining functions.
See https://docs.sqlalchemy.org/en/13/core/ddl

It's not clear how best to manage the creation of these functions. There are
at least the following options:

1. (As originally done for `effective_day`.) Add an event listener for
   event `before_create`, and execute `CREATE OR REPLACE fn ...` on that event.
   Question is where to place this. Probably not buried in a place where
   views and tables are defined, as originally. See
   https://docs.sqlalchemy.org/en/13/core/events.html#sqlalchemy.events.DDLEvents.before_create

2. Use Alembic (not yet introduced, but it will be soon). See Alembic Cookbook,
   Replaceable Objects:
   https://alembic.sqlalchemy.org/en/latest/cookbook.html#replaceable-objects
   for a way to proceed. This also has some bearing on the creation of views
   and matviews. It is probably the best way forward.

3. By script invoked from the command line. This has some advantages (amongst
   them, simplicity), but it places the functions outside of the migration
   path, which in some cases might be undesirable (e.g., in case a function
   depends on a particular table or view).
"""

from sqlalchemy.schema import DDL


# Define DDL statements for each function. In order to define a function
# in the database, its statement must be executed.
#
# Only a few of these functions are properly documented. The remainder were
# copied from the original CRMP database, where there is little or no
# documentation. Caveat emptor.

closest_stns_within_threshold = DDL('''
    CREATE OR REPLACE FUNCTION closest_stns_within_threshold(
        IN x numeric,
        IN y numeric,
        IN thres integer)
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
        SELECT history_id, lat, lon, Geography(ST_Transform(the_geom,4326)) as p_existing, Geography(ST_SetSRID(ST_MakePoint('|| X ||','|| Y ||'),4326)) as p_new
        FROM meta_history
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
''')


daily_ts = DDL('''
    CREATE OR REPLACE FUNCTION daily_ts(
        IN station_id integer,
        IN vars_id integer,
        IN percent_obs real,
        OUT daily_time timestamp without time zone,
        OUT daily_mean real,
        OUT percent_obs_available real,
        OUT daily_count integer)
      RETURNS SETOF record AS
    $BODY$
    DECLARE
    BEGIN
        RAISE DEBUG 'Running daily_ts "%" "%" "%"', station_id, vars_id, percent_obs;
        FOR daily_time, daily_mean, daily_count IN EXECUTE
            E'SELECT date_trunc(\'day\', obs_time) as obs_time_trunc, avg(datum) as obs_datum, count(datum) as obs_count FROM obs_raw WHERE station_id = ' || station_id || ' AND vars_id = ' || vars_id || ' GROUP BY obs_time_trunc ORDER BY obs_time_trunc'
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
''')


daysinmonth = DDL('''
    CREATE OR REPLACE FUNCTION daysinmonth(date)
      RETURNS double precision AS
    $BODY$
    SELECT EXTRACT(DAY FROM CAST(date_trunc('month', $1) + interval '1 month'
    - interval '1 day' as timestamp));
    $BODY$
      LANGUAGE sql VOLATILE
      COST 100;
''')


do_query_one_station = DDL('''
    CREATE OR REPLACE FUNCTION do_query_one_station(station_id integer)
      RETURNS refcursor AS
    $BODY$
    DECLARE
        query text;
        result refcursor := 'result';
    BEGIN
        query := query_one_station(station_id);
        OPEN result NO SCROLL FOR EXECUTE query;
        RETURN result;
    END;
    $BODY$
      LANGUAGE plpgsql VOLATILE
      COST 100;
''')


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
effective_day = DDL('''
    CREATE OR REPLACE FUNCTION effective_day(
        obs_time timestamp without time zone,
        extremum character varying,
        freq character varying DEFAULT ''::character varying)
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
  ''')


getstationvariabletable = DDL('''
    CREATE OR REPLACE FUNCTION getstationvariabletable(
        station_id integer,
        climo boolean)
      RETURNS text AS
    $BODY$
        query = "SELECT vars_id, net_var_name FROM meta_vars NATURAL JOIN meta_station WHERE cell_method " + ("" if climo else "!") + "~ '(within|over)' AND station_id = " + str(station_id) + ' ORDER BY net_var_name'
        data = plpy.execute(query)
        bits = [ 'obs_time' ] + [ ("MAX(CASE WHEN vars_id=" + str(x['vars_id']) + " THEN datum END) as " + x['net_var_name']) for x in data ]
        vars_ids = [ str(x['vars_id']) for x in data ]
    
        hid_query = "SELECT history_id FROM meta_history WHERE station_id=" + str(station_id)
        hid_data = plpy.execute(hid_query)
        hid_clauses = [ "history_id = " + str(x['history_id']) for x in hid_data ]
    
        return "SELECT "+ ",".join(bits) + " from obs_raw WHERE (" + " OR ".join(hid_clauses) + ") AND vars_id IN (" + ",".join(vars_ids) + ") GROUP BY obs_time ORDER BY obs_time"
    $BODY$
      LANGUAGE plpythonu VOLATILE
      COST 100;
''')


# Returns the last day of the month, as a date, of the month of the input date.
lastdateofmonth = DDL('''
    CREATE OR REPLACE FUNCTION lastdateofmonth(date)
      RETURNS date AS
    $BODY$
    SELECT CAST(date_trunc('month', $1) + interval '1 month' - interval '1 day' as date);
    $BODY$
      LANGUAGE sql VOLATILE
      COST 100;
''')


monthly_ts = DDL('''
    CREATE OR REPLACE FUNCTION monthly_ts(
        IN station_id integer,
        IN vars_id integer,
        IN percent_obs real,
        OUT monthly_time timestamp without time zone,
        OUT monthly_mean real,
        OUT percent_obs_available real,
        OUT monthly_count integer)
      RETURNS SETOF record AS
    $BODY$
    DECLARE
        the_month date;
    BEGIN
        RAISE DEBUG 'Running monthly_ts "%" "%" "%"', station_id, vars_id, percent_obs;
        FOR monthly_time, monthly_mean, monthly_count IN EXECUTE
            E'SELECT date_trunc(\'month\', obs_time) as obs_time_trunc, avg(datum) as obs_datum, count(datum) as obs_count FROM obs_raw WHERE station_id = ' || station_id || ' AND vars_id = ' || vars_id || ' GROUP BY obs_time_trunc ORDER BY obs_time_trunc'
        LOOP
            RAISE DEBUG 'In loop, Row: "%" "%" "%"', monthly_time, monthly_mean, monthly_count;
            the_month := CAST(monthly_time AS date);
            percent_obs_available := monthly_count / (DaysInMonth(the_month));
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
''')


query_one_station = DDL('''
    CREATE OR REPLACE FUNCTION query_one_station(station_id integer)
      RETURNS text AS
    $BODY$
        stn_query = "SELECT * FROM crmp.getStationVariableTable(" + str(station_id) + ", false)"
        data = plpy.execute(stn_query)
        #plpy.warning(data)
        return data[0]['getstationvariabletable']
    $BODY$
      LANGUAGE plpythonu VOLATILE
      COST 100;
''')


query_one_station_climo = DDL('''
    CREATE OR REPLACE FUNCTION query_one_station_climo(station_id integer)
      RETURNS text AS
    $BODY$
        stn_query = "SELECT * FROM crmp.getStationVariableTable(" + str(station_id) + ", true)"
        data = plpy.execute(stn_query)
        #plpy.warning(data)
        return data[0]['getstationvariabletable']
    $BODY$
      LANGUAGE plpythonu VOLATILE
      COST 100;
''')


season = DDL('''
    CREATE OR REPLACE FUNCTION season(d timestamp without time zone)
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
''')


updatesdateedate = DDL('''
    CREATE OR REPLACE FUNCTION updatesdateedate()
      RETURNS void AS
    $BODY$
    DECLARE
        sid  integer;
        min_date timestamp without time zone;
        max_date timestamp without time zone;
    BEGIN
        FOR sid IN SELECT DISTINCT station_id FROM meta_history
        LOOP
            SELECT min(obs_time), max(obs_time) INTO min_date, max_date
                   FROM obs_raw NATURAL JOIN meta_history WHERE station_id = sid;
    
            UPDATE meta_station
                   SET (min_obs_time, max_obs_time) = (min_date, max_date)
                   WHERE station_id = sid;
    END LOOP;
    RETURN;
    END;
    $BODY$
      LANGUAGE plpgsql VOLATILE
      COST 100;
''')
