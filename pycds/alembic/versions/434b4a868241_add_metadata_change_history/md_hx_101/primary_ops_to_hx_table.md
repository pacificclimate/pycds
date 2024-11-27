# Operations on primary table to history table

```postgresql
CREATE OR REPLACE FUNCTION mdhx_primary_control_hx_cols()
    RETURNS trigger
    LANGUAGE plpgsql
    PARALLEL UNSAFE
AS
$BODY$
    -- This trigger function controls the values of the history columns, namely
    -- mod_time and creator. This is necessary to prevent them from being carried 
    -- forward from the previous state or from being set explicitly by the user.
    -- This trigger function should be called by a BEFORE trigger on the primary table.
BEGIN
    IF tg_op = 'INSERT' OR tg_op = 'UPDATE' THEN
        NEW.mod_time = now();
        NEW.creator = current_user;
        RETURN NEW;
    ELSIF tg_op = 'DELETE' THEN
        OLD.mod_time = now();
        OLD.creator = current_user;
        RETURN OLD;
    END IF;
END;
$BODY$;
```

```postgresql
CREATE OR REPLACE FUNCTION mdhx_primary_ops_to_hx()
    RETURNS trigger
    LANGUAGE plpgsql
    PARALLEL UNSAFE
AS
$BODY$
    -- This trigger function inserts a new record into the corresponding history table
    -- when a primary table receives an insert, update or delete operation. 
    -- This trigger function is called AFTER a primary table operation. Therefore it sees
    -- the final values for NEW/OLD; in particular, any values set in a BEFORE trigger.
DECLARE
    -- Values from special variables
    this_collection_name text := tg_table_name;

    -- Other values
    this_history_table_name text := mdhx_hx_table_name(this_collection_name);
    this_metadata_history_id_seq_name text := mdhx_hx_id_seq_name(this_collection_name);
    new_metadata_hx_id integer;
    
    insert_stmt text := format(
        -- values: (NEW/OLD, new_metadata_hx_id, deleted)
        'INSERT INTO %I VALUES ($1.*, $2, $3)',
        this_history_table_name
    );
BEGIN
    SELECT NEXTVAL(this_metadata_history_id_seq_name::regclass) INTO new_metadata_hx_id;
    IF tg_op = 'INSERT' OR tg_op = 'UPDATE' THEN
        EXECUTE insert_stmt USING NEW, new_metadata_hx_id, FALSE;
        RETURN NEW;  -- Ignored in an AFTER trigger
    ELSIF tg_op = 'DELETE' THEN
        EXECUTE insert_stmt USING OLD, new_metadata_hx_id, TRUE;
        RETURN OLD;  -- Ignored in an AFTER trigger
    END IF;
END;
$BODY$;
```


