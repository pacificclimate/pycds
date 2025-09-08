CREATE OR REPLACE FUNCTION hxtk_make_query_latest_undeleted_hx_ids(
    collection_name text,
    collection_id_name text,
    where_condition text = 'true'
)
    RETURNS SETOF RECORD
    LANGUAGE 'plpgsql'
AS
$BODY$
    -- This function returns text containing a query that returns the history id's for the LU records,
    -- given a collection spec and a where condition.
DECLARE
    hx_table_name text := hxtk_hx_table_name(collection_name);
    hx_id_name text := hxtk_hx_id_name(collection_name);
    q text := format(
        'SELECT hx.%1$I, max(hx.%2$I) ' ||
        'FROM %3$I hx ' ||
        'WHERE %4$I ' ||
        'GROUP BY hx.%1$I ' ||
        'HAVING NOT bool_or(hx.deleted) ',
        collection_id_name,
        hx_id_name,
        hx_table_name,
        where_condition
              );
BEGIN
    RAISE NOTICE '%', q;
    RETURN q;
END;
$BODY$;


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
    RETURN QUERY EXECUTE format('SELECT * FROM %I WHERE %I IN (%s)', hx_table_name, hx_id_name, hx_ids_query);
END;
$BODY$;


CREATE OR REPLACE FUNCTION hxtk_is_valid_history_tuple(hx_tuple history_tuple)
    RETURNS boolean
    LANGUAGE 'plpgsql'
AS
$BODY$
    -- This function returns true if and only if the provided history tuple implies a valid
    -- history subset. See documentation for definitions of validity.
BEGIN
    -- If this tuple is the current point in history, we know it is valid.
    IF
        hx_tuple.obs_raw_hx_id = (SELECT max(obs_raw_hx.obs_raw_hx_id) FROM obs_raw_hx) AND
        hx_tuple.meta_history_hx_id = (SELECT max(meta_history_hx.meta_history_hx_id) FROM meta_history_hx) AND
        hx_tuple.meta_station_hx_id = (SELECT max(meta_station_hx.meta_station_hx_id) FROM meta_station_hx) AND
        hx_tuple.meta_network_hx_id = (SELECT max(meta_network_hx.meta_network_hx_id) FROM meta_network_hx) AND
        hx_tuple.meta_vars_hx_id = (SELECT max(meta_vars_hx.meta_vars_hx_id) FROM meta_vars_hx)
    THEN
        RETURN TRUE;
    END IF;

    -- This query ANDs together sub-queries for each table.
    -- Warning: The query against `obs_raw_hx` could take a long time.
    RETURN QUERY SELECT
                     -- It's tempting to DRY up the sub-queries below into a generic make-query function,
                     -- but it's probably more work than it's worth. Advantage would be getting the pattern
                     -- right just once, and easy extension to additional cases.
                     (SELECT
                          bool_and(
                              obs_raw_hx.meta_history_hx_id <= hx_tuple.meta_history_hx_id AND
                              obs_raw_hx.meta_vars_hx_id <= hx_tuple.meta_vars_hx_id
                          )
                      FROM
                          obs_raw_hx
                      WHERE
                          obs_raw_hx.obs_raw_hx_id <= hx_tuple.obs_raw_hx_id)
                         AND (SELECT
                                  bool_and(meta_history_hx.meta_station_hx_id <= hx_tuple.meta_station_hx_id)
                              FROM
                                  meta_history_hx
                              WHERE
                                  meta_history_hx.meta_history_hx_id <= hx_tuple.meta_history_hx_id)
                         AND (SELECT
                                  bool_and(meta_station_hx.meta_network_hx_id <= hx_tuple.meta_network_hx_id)
                              FROM
                                  meta_station_hx
                              WHERE
                                  meta_station_hx.meta_station_hx_id <= hx_tuple.meta_station_hx_id)
                         AND (SELECT
                                  bool_and(meta_vars_hx.meta_network_hx_id <= hx_tuple.meta_network_hx_id)
                              FROM
                                  meta_vars_hx
                              WHERE
                                  meta_vars_hx.meta_vars_hx_id <= hx_tuple.meta_vars_hx_id);

    -- The above query can be reformulated as follows. This may be much more efficient if the correct indexes exist
    -- and are selected by the query planner.
    -- TODO: Determine which query is best.
--     RETURN QUERY SELECT
--         (SELECT
--              max(obs_raw_hx.meta_history_hx_id) <= hx_tuple.meta_history_hx_id AND
--              max(obs_raw_hx.meta_vars_hx_id) <= hx_tuple.meta_vars_hx_id
--          FROM
--              obs_raw_hx
--          WHERE
--              obs_raw_hx.obs_raw_hx_id <= hx_tuple.obs_raw_hx_id)
--             AND
--         (SELECT
--              max(meta_history_hx.meta_station_hx_id) <= hx_tuple.meta_station_hx_id
--          FROM
--              meta_history_hx
--          WHERE
--              meta_history_hx.meta_history_hx_id <= hx_tuple.meta_history_hx_id)
--             AND
--         (SELECT
--              max(meta_station_hx.meta_network_hx_id) <= hx_tuple.meta_network_hx_id
--          FROM
--              meta_station_hx
--          WHERE
--              meta_station_hx.meta_station_hx_id <= hx_tuple.meta_station_hx_id)
--             AND
--         (SELECT
--              max(meta_vars_hx.meta_network_hx_id) <= hx_tuple.meta_network_hx_id
--          FROM
--              meta_vars_hx
--          WHERE
--              meta_vars_hx.meta_vars_hx_id <= hx_tuple.meta_vars_hx_id);
END;
$BODY$;


-- Can this and should this be used as a column in table bookmark_associations?
CREATE TYPE history_tuple AS (
    obs_raw_hx_id bigint,
    meta_history_hx_id int,
    meta_station_hx_id int,
    meta_network_hx_id int,
    meta_vars_hx_id int
);


CREATE TABLE IF NOT EXISTS bookmark_labels (
    bookmark_label_id int PRIMARY KEY ,
    network_id int REFERENCES meta_network(network_id),
    label text NOT NULL,
    comment text,
    mod_time timestamp NOT NULL DEFAULT now(),
    mod_user text NOT NULL DEFAULT current_user
);


CREATE TABLE IF NOT EXISTS bookmark_associations (
    bookmark_association_id int PRIMARY KEY,
    bookmark_label_id int REFERENCES bookmark_labels(bookmark_label_id),
    role text NOT NULL , -- Should be enum type
    bracket_begin_id int REFERENCES bookmark_associations(bookmark_association_id),
    comment text,
    hx_tuple history_tuple,
    mod_time timestamp NOT NULL DEFAULT now(),
    mod_user text NOT NULL DEFAULT current_user
);


-- Example usages

DO $$
    BEGIN ;
    INSERT INTO
    COMMIT ;
$$;


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
    IF NOT hxtk_is_valid_history_tuple(NEW.hx_tuple) THEN
        RAISE 'Invalid history tuple';
    END IF;
END;
$BODY$;


CREATE OR REPLACE FUNCTION hxtk_check_bracket_end()
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
    -- EXECUTE PROCEDURE hxtk_check_bracket_end()
DECLARE
    max_ba_id int;
    ba_id_count int;
BEGIN
    IF NEW.bracket_begin_id IS NULL THEN
        -- Check if we have one open bracket. Use it if so.
        SELECT
            max(bookmark_association_id),
            count(*)
        INTO STRICT max_ba_id, ba_id_count
        FROM
            bookmark_associations
        WHERE
            role = 'bracket_begin';
        IF ba_id_count = 1 THEN
            NEW.bracket_begin_id := max_ba_id;
        ELSE
            RAISE EXCEPTION 'bracket_begin_id is null, and there are % open brackets.', ba_id_count;
        END IF;
    ELSE
        -- Check whether this bracket is open. Error if it is not.
        SELECT
            count(*)
        INTO ba_id_count
        FROM
            bookmark_associations
        WHERE
            role = 'bracket_end'
            AND bracket_end_id = NEW.bracket_begin_id;
        IF ba_id_count > 0 THEN
            RAISE EXCEPTION 'The bracket with id % is already closed.';
        END IF;
    END IF;
END;
$BODY$;


CREATE OR REPLACE FUNCTION hxtk_()
    RETURNS text -- FIXME
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
BEGIN
END;
$BODY$;


---- Template
CREATE OR REPLACE FUNCTION hxtk_()
    RETURNS text -- FIXME
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
BEGIN
END;
$BODY$;


