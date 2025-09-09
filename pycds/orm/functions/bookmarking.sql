-----------------
CREATE OR REPLACE FUNCTION hxtk_make_query_latest_undeleted_hx_ids(
    collection_name text,
    collection_id_name text,
    where_condition text = 'TRUE'
)
    RETURNS text
    LANGUAGE 'plpgsql'
AS
$BODY$
    -- This function returns text containing a query that returns the history id's for the LU records,
    -- given a collection spec and a where condition.
DECLARE
    hx_table_name text := hxtk_hx_table_name(collection_name);
    hx_id_name text := hxtk_hx_id_name(collection_name);
    q text := format(
        'SELECT hx.%1$I AS %1$I, max(hx.%2$I) AS %2$I ' ||
        'FROM %3$I hx ' ||
        'WHERE %4$s ' ||
        'GROUP BY hx.%1$I ' ||
        'HAVING NOT bool_or(hx.deleted) ',
        collection_id_name,
        hx_id_name,
        hx_table_name,
        where_condition
              );
BEGIN
    --     RAISE NOTICE '%', q;
    RETURN q;
END;
$BODY$;


DO LANGUAGE 'plpgsql'
$$
    BEGIN
        RAISE NOTICE 'hxtk_make_query_latest_undeleted_hx_ids: %',
            hxtk_make_query_latest_undeleted_hx_ids('meta_network', 'network_id');
    END;
$$;



-----------------
CREATE OR REPLACE FUNCTION hxtk_get_latest_undeleted_hx_ids(
    collection_name text,
    collection_id_name text,
    where_condition text = 'true'
)
    RETURNS SETOF RECORD
    LANGUAGE 'plpgsql'
AS
$BODY$
    -- This function returns the history id's for the LU records, given a collection and a where condition.
BEGIN
    RETURN QUERY EXECUTE hxtk_make_query_latest_undeleted_hx_ids(
        collection_name, collection_id_name, where_condition);
END;
$BODY$;


DO LANGUAGE 'plpgsql'
$$
    DECLARE
        r record;
    BEGIN
        RAISE NOTICE '(network_id, meta_network_hx_id)';
        FOR r IN
            SELECT *
            FROM
                hxtk_get_latest_undeleted_hx_ids('meta_network', 'network_id')
                    AS t(network_id int, meta_network_hx_id int)
            ORDER BY network_id
        LOOP
            RAISE NOTICE '%', r;
        END LOOP;
    END ;
$$;



-----------------
CREATE OR REPLACE FUNCTION hxtk_get_latest_undeleted_hx_records(
    collection_name text,
    collection_id_name text,
    where_condition text = 'true'
)
    RETURNS SETOF RECORD
    LANGUAGE 'plpgsql'
AS
$BODY$
    -- This function returns the full history records for the LU records, given a collection and a where condition.
DECLARE
    hx_table_name text := hxtk_hx_table_name(collection_name);
    hx_id_name text := hxtk_hx_id_name(collection_name);
    hx_ids_query text := hxtk_make_query_latest_undeleted_hx_ids(
        collection_name, collection_id_name, where_condition);
BEGIN
    RETURN QUERY EXECUTE
        format(
            'SELECT * FROM %1$I WHERE %2$I IN (SELECT t.%2$I FROM (%3$s) AS t)',
            hx_table_name, hx_id_name, hx_ids_query
        );
END;
$BODY$;


DO LANGUAGE 'plpgsql'
$$
    DECLARE
        r record;
    BEGIN
        RAISE NOTICE 'LU(meta_network_hx)';
        RAISE NOTICE '(meta_network_hx_id, network_id, network_name)';
        FOR r IN
            SELECT
                meta_network_hx_id,
                network_id,
                network_name
            FROM
                hxtk_get_latest_undeleted_hx_records('meta_network', 'network_id')
                    AS t(network_id integer,
                         network_name varchar(255),
                         description varchar(255),
                         virtual varchar(255),
                         publish boolean,
                         col_hex varchar(7),
                         contact_id integer,
                         mod_time timestamp,
                         mod_user varchar(64),
                         deleted boolean,
                         meta_network_hx_id integer)
        LOOP
            RAISE NOTICE '%', r;
        END LOOP;
    END ;
$$;



-----------------
-- Can this and should this be used as a column in table bookmark_associations?
CREATE TYPE history_tuple AS (
    obs_raw_hx_id bigint,
    meta_history_hx_id int,
    meta_station_hx_id int,
    meta_network_hx_id int,
    meta_vars_hx_id int
);


CREATE OR REPLACE FUNCTION hxtk_current_hx_tuple()
    RETURNS history_tuple
    LANGUAGE 'plpgsql'
AS
$BODY$
    -- This function returns the current history tuple.
DECLARE
    result history_tuple;
BEGIN
    SELECT
            (SELECT max(obs_raw_hx.obs_raw_hx_id) FROM obs_raw_hx) AS obs_raw_hx_id,
        (SELECT max(meta_history_hx.meta_history_hx_id) FROM meta_history_hx) AS meta_history_hx_id,
        (SELECT max(meta_station_hx.meta_station_hx_id) FROM meta_station_hx) AS meta_station_hx_id,
        (SELECT max(meta_network_hx.meta_network_hx_id) FROM meta_network_hx) AS meta_network_hx_id,
            (SELECT max(meta_vars_hx.meta_vars_hx_id) FROM meta_vars_hx) AS meta_vars_hx_id
    INTO STRICT result;
    RETURN result;
END;
$BODY$;


DO LANGUAGE 'plpgsql'
$$
    BEGIN
        RAISE NOTICE '(obs_raw_hx_id, meta_history_hx_id, meta_station_hx_id int, meta_network_hx_id int, meta_vars_hx_id int)';
        RAISE NOTICE '%', hxtk_current_hx_tuple();
    END ;
$$;



-----------------
CREATE OR REPLACE FUNCTION hxtk_is_valid_history_tuple(hx_tuple history_tuple)
    RETURNS boolean
    LANGUAGE 'plpgsql'
AS
$BODY$
    -- This function returns true if and only if the provided history tuple implies a valid
    -- history subset. See documentation for definitions of validity.
DECLARE
    obs_raw_ok boolean;
    meta_history_ok boolean;
    meta_station_ok boolean;
    meta_network_ok boolean;
    meta_vars_ok boolean;
    t_start timestamp;
    t_end timestamp;
BEGIN
    RAISE NOTICE 'hxtk_is_valid_history_tuple %', hx_tuple;

    IF hx_tuple IS NULL THEN
        RETURN FALSE;
    END IF;

    -- If this tuple is the current point in history, we know it is valid.
    IF
        hx_tuple = hxtk_current_hx_tuple()
    THEN
        RAISE NOTICE '= current hx tuple';
        RETURN TRUE;
    END IF;

    -- Warning: The query against `obs_raw_hx` could take a long time.

    -- This query is likely more efficient if the correct indexes exist and are selected by the query planner.
    -- TODO: Determine which query is best.

    RAISE NOTICE 'Checking in detail';
    t_start := clock_timestamp();
    RAISE NOTICE 'Starting obs_raw_hx at %', t_start;
    SELECT
        max(obs_raw_hx.meta_history_hx_id) <= hx_tuple.meta_history_hx_id AND
        max(obs_raw_hx.meta_vars_hx_id) <= hx_tuple.meta_vars_hx_id
    FROM
        obs_raw_hx
    WHERE
        obs_raw_hx.obs_raw_hx_id <= hx_tuple.obs_raw_hx_id
    INTO STRICT obs_raw_ok;
    t_end := clock_timestamp();
    RAISE NOTICE 'Finished obs_raw_hx at %; time elapsed %', t_end, t_end - t_start;

    SELECT
        max(meta_history_hx.meta_station_hx_id) <= hx_tuple.meta_station_hx_id
    FROM
        meta_history_hx
    WHERE
        meta_history_hx.meta_history_hx_id <= hx_tuple.meta_history_hx_id
    INTO STRICT meta_history_ok;

    SELECT
        max(meta_station_hx.meta_network_hx_id) <= hx_tuple.meta_network_hx_id
    FROM
        meta_station_hx
    WHERE
        meta_station_hx.meta_station_hx_id <= hx_tuple.meta_station_hx_id
    INTO STRICT meta_station_ok;

    SELECT
        max(meta_vars_hx.meta_network_hx_id) <= hx_tuple.meta_network_hx_id
    FROM
        meta_vars_hx
    WHERE
        meta_vars_hx.meta_vars_hx_id <= hx_tuple.meta_vars_hx_id
    INTO STRICT meta_vars_ok;

    RETURN obs_raw_ok AND meta_history_ok AND meta_station_ok AND meta_network_ok AND meta_vars_ok;

    -- This is probably a slower query. If used, reformulate as a series of separate queries like above.
--     RETURN QUERY SELECT
--                      -- It's tempting to DRY up the sub-queries below into a generic make-query function,
--                      -- but it's probably more work than it's worth. Advantage would be getting the pattern
--                      -- right just once, and easy extension to additional cases.
--                      (SELECT
--                           bool_and(
--                               obs_raw_hx.meta_history_hx_id <= hx_tuple.meta_history_hx_id AND
--                               obs_raw_hx.meta_vars_hx_id <= hx_tuple.meta_vars_hx_id
--                           )
--                       FROM
--                           obs_raw_hx
--                       WHERE
--                           obs_raw_hx.obs_raw_hx_id <= hx_tuple.obs_raw_hx_id)
--                          AND (SELECT
--                                   bool_and(meta_history_hx.meta_station_hx_id <= hx_tuple.meta_station_hx_id)
--                               FROM
--                                   meta_history_hx
--                               WHERE
--                                   meta_history_hx.meta_history_hx_id <= hx_tuple.meta_history_hx_id)
--                          AND (SELECT
--                                   bool_and(meta_station_hx.meta_network_hx_id <= hx_tuple.meta_network_hx_id)
--                               FROM
--                                   meta_station_hx
--                               WHERE
--                                   meta_station_hx.meta_station_hx_id <= hx_tuple.meta_station_hx_id)
--                          AND (SELECT
--                                   bool_and(meta_vars_hx.meta_network_hx_id <= hx_tuple.meta_network_hx_id)
--                               FROM
--                                   meta_vars_hx
--                               WHERE
--                                   meta_vars_hx.meta_vars_hx_id <= hx_tuple.meta_vars_hx_id);

END;
$BODY$;


DO LANGUAGE 'plpgsql'
$$
    DECLARE
        curr_hx_tuple history_tuple := hxtk_current_hx_tuple();
        bad_hx_tuple history_tuple :=
            (curr_hx_tuple.obs_raw_hx_id,
             curr_hx_tuple.meta_history_hx_id - 1,
             curr_hx_tuple.meta_station_hx_id - 1,
             curr_hx_tuple.meta_network_hx_id - 1,
             curr_hx_tuple.meta_vars_hx_id - 1
                )::history_tuple;
    BEGIN
        RAISE NOTICE 'current: %', hxtk_is_valid_history_tuple(curr_hx_tuple);
        RAISE NOTICE 'bad: %', hxtk_is_valid_history_tuple(bad_hx_tuple);
    END ;
$$;


-----------------
CREATE TABLE IF NOT EXISTS bookmark_labels (
    bookmark_label_id int PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    network_id int REFERENCES meta_network (network_id),
    label text NOT NULL,
    comment text,
    mod_time timestamp NOT NULL DEFAULT now(),
    mod_user text NOT NULL DEFAULT current_user
);


CREATE TABLE IF NOT EXISTS bookmark_associations (
    bookmark_association_id int PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    bookmark_label_id int REFERENCES bookmark_labels (bookmark_label_id),
    role text NOT NULL, -- Should be enum type
    bracket_begin_id int REFERENCES bookmark_associations (bookmark_association_id),
    comment text,
    hx_tuple history_tuple NOT NULL DEFAULT hxtk_current_hx_tuple(),
    mod_time timestamp NOT NULL DEFAULT now(),
    mod_user text NOT NULL DEFAULT current_user
);


-- Trigger functions

CREATE OR REPLACE FUNCTION hxtk_validate_history_tuple()
    RETURNS trigger
    LANGUAGE 'plpgsql'
AS
$BODY$
    -- This trigger function validates the history tuple provided to table bookmark_associations.
    --
    -- Usage:
    -- CREATE TRIGGER t100_validate_history_tuple
    --      BEFORE INSERT OR UPDATE
    --      ON bookmark_associations
    -- EXECUTE PROCEDURE hxtk_validate_history_tuple()
DECLARE
BEGIN
    RAISE NOTICE 'hxtk_validate_history_tuple: NEW = %', NEW;
    IF NOT hxtk_is_valid_history_tuple(NEW.hx_tuple) THEN
        RAISE 'Invalid history tuple';
    END IF;
    RETURN NEW;
END;
$BODY$;


CREATE OR REPLACE FUNCTION hxtk_bm_check_bracket_end()
    RETURNS trigger
    LANGUAGE 'plpgsql'
AS
$BODY$
    -- This trigger function checks that a bracket-end insert to table references an open
    -- bracket (bracket_begin_id), or if it references no bracket and there is only one
    -- open one, it provides that value.
    --
    -- Usage:
    -- CREATE TRIGGER t200_check_bracket_end
    --      BEFORE INSERT OR UPDATE
    --      ON bookmark_associations
    -- EXECUTE PROCEDURE hxtk_check_bm_bracket_end()
DECLARE
    bracket_begin_q text :=
        format(
            'SELECT count(*), max(bookmark_association_id) FROM %I.%I ' ||
            'WHERE role = ''bracket_begin''',
            TG_TABLE_SCHEMA, TG_TABLE_NAME
        );
    bracket_end_q text :=
        format(
            'SELECT count(*) FROM %I.%I ' ||
            'WHERE role = ''bracket_end'' ' ||
            '   AND bracket_end_id = NEW.bracket_begin_id',
            TG_TABLE_SCHEMA, TG_TABLE_NAME
        );
    max_ba_id int;
    ba_id_count int;
BEGIN
    IF NEW.bracket_begin_id IS NULL THEN
        -- Check if we have one open bracket. Use it if so.
        EXECUTE bracket_begin_q INTO STRICT ba_id_count, max_ba_id;
        IF ba_id_count = 1 THEN
            NEW.bracket_begin_id := max_ba_id;
        ELSE
            RAISE EXCEPTION 'bracket_begin_id is unspecified, and there are % > 1 open brackets.', ba_id_count;
        END IF;
    ELSE
        -- Check whether this bracket is open. Error if it is not.
        EXECUTE bracket_end_q INTO ba_id_count;
        IF ba_id_count > 0 THEN
            RAISE EXCEPTION 'The bracket with id % is already closed.', NEW.bracket_begin_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$BODY$;


DROP TRIGGER IF EXISTS t100_validate_history_tuple ON bookmark_associations;
CREATE TRIGGER t100_validate_history_tuple
    BEFORE INSERT OR UPDATE
    ON bookmark_associations
    FOR EACH ROW
EXECUTE PROCEDURE hxtk_validate_history_tuple();


DROP TRIGGER IF EXISTS t200_check_bracket_end ON bookmark_associations;
CREATE TRIGGER t200_check_bracket_end
    BEFORE INSERT OR UPDATE
    ON bookmark_associations
    FOR EACH ROW
    WHEN ( NEW.role = 'bracket_end' )
EXECUTE PROCEDURE hxtk_bm_check_bracket_end();


-----------------
BEGIN; -- begin transaction for tests; roll back at end
DO LANGUAGE 'plpgsql'
$$
    DECLARE
        bookmark_alpha int;
        nw_id int;
        open_br_id int;
        br_hx_tuple history_tuple;
        close_br_begin_id int;
        r record;
        test_id int;
    BEGIN
        -- Create a bookmark label
        INSERT INTO bookmark_labels(network_id, label)
        VALUES (34, 'Alpha')
        RETURNING bookmark_label_id
            INTO STRICT bookmark_alpha;

        RAISE NOTICE 'bookmark_labels';
        FOR r IN SELECT * FROM bookmark_labels
        LOOP
            RAISE NOTICE '%', r;
        END LOOP;

        -- Create a singleton bookmark at current history point.
        RAISE NOTICE 'Create singleton bookmark';
        INSERT INTO bookmark_associations(bookmark_label_id, role)
        VALUES (bookmark_alpha, 'singleton');

        RAISE NOTICE 'bookmark_associations';
        FOR r IN SELECT * FROM bookmark_associations
        LOOP
            RAISE NOTICE '%', r;
        END LOOP;

        -- TEST 1: Bracket with manually provided values.
        test_id = 1;
        RAISE NOTICE 'TEST % - BEGIN', test_id;

        -- Open a bracket bookmark at current history point.
        RAISE NOTICE 'TEST % - Open bracket', test_id;
        INSERT INTO bookmark_associations(bookmark_label_id, role, hx_tuple)
        VALUES (bookmark_alpha, 'bookmark_begin', hxtk_current_hx_tuple())
        RETURNING bookmark_association_id
            INTO STRICT open_br_id;

        -- Insert some gunk.
        RAISE NOTICE 'TEST % - Add new network', test_id;
        INSERT INTO meta_network(network_name)
        VALUES ('Rod Test 1')
        RETURNING network_id INTO STRICT nw_id;

        -- Close the open bookmark at current history point.
        RAISE NOTICE 'TEST % - Close bracket', test_id;
        INSERT INTO bookmark_associations(bookmark_label_id, role, bracket_begin_id)
        VALUES (bookmark_alpha, 'bookmark_end', open_br_id)
        RETURNING hx_tuple
            INTO STRICT r;
        RAISE NOTICE 'Close returns: %', r;
        IF r.hx_tuple != hxtk_current_hx_tuple() THEN
            RAISE 'Close bracket: Hx tuple % is not current', br_hx_tuple;
        END IF;

        RAISE NOTICE 'TEST % - END', test_id;
        -- END TEST 1

        -- TEST 2: Bracket with auto-provided values.
        test_id := 2;
        RAISE NOTICE 'TEST % - BEGIN', test_id;

        -- Open a bracket bookmark at current history point.
        RAISE NOTICE 'TEST % - Open bracket', test_id;
        INSERT INTO bookmark_associations(bookmark_label_id, role)
        VALUES (bookmark_alpha, 'bookmark_begin')
        RETURNING bookmark_association_id, hx_tuple
            INTO STRICT r;
        open_br_id := r.bookmark_association_id;
        IF r.hx_tuple != hxtk_current_hx_tuple() THEN
            RAISE 'Open bracket: Hx tuple % is not current', br_hx_tuple;
        END IF;

        -- Insert some gunk.
        RAISE NOTICE 'TEST % - Add new network', test_id;
        INSERT INTO meta_network(network_name)
        VALUES ('Rod Test 1')
        RETURNING network_id INTO STRICT nw_id;

        -- Close the open bookmark at current history point.
        -- Let the tf supply the bookmark-begin id.
        RAISE NOTICE 'TEST % - Close bracket', test_id;
        INSERT INTO bookmark_associations(bookmark_label_id, role)
        VALUES (bookmark_alpha, 'bookmark_end')
        RETURNING bracket_begin_id, hx_tuple
            INTO STRICT r;
        IF r.hx_tuple != hxtk_current_hx_tuple() THEN
            RAISE 'Close bracket: Hx tuple % is not current', br_hx_tuple;
        END IF;
        IF r.bracket_begin_id != open_br_id THEN
            RAISE 'Close bracket: Expected bracket_begin_id = %, got %', open_br_id, close_br_begin_id;
        END IF;

        RAISE NOTICE 'TEST % - END', test_id;
        -- END TEST 2

    END;
$$;
ROLLBACK;
