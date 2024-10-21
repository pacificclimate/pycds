# Utility functions

Eventually these should all go into schema `public`.
They should have a unique, short prefix (e.g., `mdhx_` to their names for this).

## Naming

```postgresql
CREATE OR REPLACE FUNCTION metadata_view_name(collection_name text)
RETURNS text
    LANGUAGE 'plpgsql'
AS $$
    BEGIN 
        RETURN collection_name;
    END;
$$;

CREATE OR REPLACE FUNCTION metadata_history_table_name(collection_name text)
RETURNS text
    LANGUAGE 'plpgsql'
AS $$
    BEGIN 
        RETURN collection_name || '_hx';
    END;
$$;

CREATE OR REPLACE FUNCTION metadata_history_id_name(collection_name text)
RETURNS text
    LANGUAGE 'plpgsql'
AS $$
    BEGIN
        RETURN collection_name || '_hx_id';
    END;
$$;

CREATE OR REPLACE FUNCTION metadata_history_id_seq_name(collection_name text)
RETURNS text
    LANGUAGE 'plpgsql'
AS $$
    BEGIN
        RETURN collection_name || '_hx_id_seq';
    END;
$$;
```

## Mark delete in progress - temp table and queries

```postgresql
CREATE TYPE mark_delete_in_progress_type AS (
    base_collection_name text,
    base_metadata_id_name text,
    base_metadata_id int
);

CREATE OR REPLACE PROCEDURE create_table_mark_delete_in_progress()
    LANGUAGE plpgsql
AS $$
    BEGIN 
        CREATE TEMPORARY TABLE IF NOT EXISTS mark_delete_in_progress(
            base_collection_name text, 
            base_metadata_id_name text, 
            base_metadata_id int
        ) ON COMMIT DELETE ROWS;
        CALL test_print_table('mark_delete_in_progress');
    END;
$$;

CREATE OR REPLACE PROCEDURE set_mark_delete_in_progress(
    this_collection_name text, this_metadata_id_name text, this_metadata_id int
)
    LANGUAGE plpgsql
AS $$
    BEGIN
        INSERT INTO mark_delete_in_progress VALUES (
            this_collection_name, this_metadata_id_name, this_metadata_id
        );
    END;
$$;

CREATE OR REPLACE PROCEDURE unset_mark_delete_in_progress(
    this_collection_name text, this_metadata_id_name text, this_metadata_id int
)
    LANGUAGE plpgsql
AS $$
    DECLARE 
        n int;
    BEGIN
        WITH deleted AS (
            DELETE FROM mark_delete_in_progress mdip WHERE
                base_collection_name = this_collection_name
                AND base_metadata_id_name = this_metadata_id_name
                AND base_metadata_id = this_metadata_id
            RETURNING *
        ) SELECT count(*) INTO n;
        IF n != 1 THEN
            RAISE EXCEPTION 'Internal error: Expected mdip to contain 1 row, found %', n;
        END IF;
    END;
$$;

CREATE OR REPLACE FUNCTION get_mark_delete_in_progress()
    RETURNS mark_delete_in_progress_type
    LANGUAGE plpgsql
AS $$
    DECLARE
        result mark_delete_in_progress_type;
    BEGIN
        BEGIN
            SELECT * FROM mark_delete_in_progress STRICT INTO result;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                -- TODO: Can this be reduced to RETURN (NULL, NULL, NULL); ?
                SELECT NULL, NULL, NULL INTO result;
                RETURN result;
            WHEN TOO_MANY_ROWS THEN
                RAISE EXCEPTION 'Internal error: too many rows in mark_delete_in_progress';
        END;
        RETURN result;
    END;
$$;


```

## Other



