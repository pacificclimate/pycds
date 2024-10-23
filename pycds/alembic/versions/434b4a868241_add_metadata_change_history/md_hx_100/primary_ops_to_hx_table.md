# Operations on primary table to history table

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
    -- the final values for NEW/OLD; in particular, any default values.
DECLARE
    -- Values from special variables
    this_collection_name text := tg_table_name;

    -- Other values
    this_history_table_name text := mdhx_hx_table_name(this_collection_name);
    this_metadata_history_id_seq_name text := mdhx_hx_id_seq_name(this_collection_name);
    new_metadata_hx_id integer;
    
    insert_stmt text := format(
        'INSERT INTO %I VALUES ($1, $2, $3.*)',
        this_history_table_name
    );
BEGIN
    SELECT NEXTVAL(this_metadata_history_id_seq_name::regclass) INTO new_metadata_hx_id;
    IF tg_op = 'INSERT' OR tg_op = 'UPDATE' THEN
        CALL mdhx_set_history_attrs(NEW);
        EXECUTE insert_stmt USING new_metadata_hx_id, FALSE, NEW;
        RETURN NEW;  -- Ignored in an AFTER trigger
    ELSIF tg_op = 'DELETE' THEN
        CALL mdhx_set_history_attrs(OLD);
        EXECUTE insert_stmt USING new_metadata_hx_id, TRUE, OLD;
        RETURN OLD;  -- Ignored in an AFTER trigger
    END IF;
END;
$BODY$;
```
