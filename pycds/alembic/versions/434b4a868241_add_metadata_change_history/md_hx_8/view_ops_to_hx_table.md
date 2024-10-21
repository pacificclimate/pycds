## View operations to hx table

```postgresql
CREATE OR REPLACE FUNCTION view_ops_to_hx_table()
    RETURNS trigger
    LANGUAGE plpgsql
    PARALLEL UNSAFE
AS
$BODY$
-- This trigger function inserts the appropriate new record into the corresponding 
-- history table when a view receives an insert, update or delete operation.
DECLARE
    -- Trigger function arguments from CREATE TRIGGER statement, provided by the special 
    -- variable tg_argv.
    this_collection_name text := tg_argv[0];
    this_metadata_id_name text := tg_argv[1];  -- Metadata id column name can be arbitrary; not determined by view/table name.

    -- Values derived from trigger function args
    this_metadata_history_id_seq_name text := metadata_history_id_seq_name
                                              (this_collection_name);
 
    -- String containing INSERT statement to be executed in body of function with EXECUTE 
    -- statement. Here we derive the history table name from the view name. The $n params 
    -- are substituted later by the USING parameters of the EXECUTE statement.
    -- The first two arguments are <metadata history id> and <deleted>. The third 
    -- argument is a record, which is spread (note * in expression $3.*) to provide the 
    -- remainder of the VALUES columns.
    insert_stmt text := format('INSERT INTO %I_hx VALUES ($1, $2, $3.*)', this_collection_name);
 
    old_metadata_id integer;
    new_metadata_hx_id integer;
BEGIN
    SELECT NEXTVAL(this_metadata_history_id_seq_name::regclass) INTO new_metadata_hx_id;
    IF tg_op = 'INSERT' THEN
        -- Create a completely new metadata item. Its metadata id is the new metadata 
        -- history id.
        NEW.mod_time = now();
        NEW.creator = current_user;
        -- Update the NEW record with the value of the new metadata id, targeted at 
        -- the column with name metadata_id_name. The easiest way to do that is to 
        -- use hstore.
        NEW := NEW #= hstore(this_metadata_id_name, new_metadata_hx_id::text);
        EXECUTE insert_stmt USING new_metadata_hx_id, FALSE, NEW;
        RETURN NEW;
    ELSIF tg_op = 'UPDATE' THEN
        -- Update an existing metadata item. Its metadata id is preserved (copied forward  
       -- from OLD).
        NEW.mod_time = now();
        NEW.creator = current_user;
        -- Update the NEW record with the value of the old metadata id, targeted at 
        -- the column with name this_metadata_id_name. We use hstore once to extract the 
        -- appropriate value from OLD, and a second time to update NEW.
        old_metadata_id := hstore(OLD) -> this_metadata_id_name;
        NEW := NEW #= hstore(this_metadata_id_name, old_metadata_id::text);
        EXECUTE insert_stmt USING new_metadata_hx_id, FALSE, NEW;
        RETURN NEW;
    ELSIF tg_op = 'DELETE' THEN
        -- Set deleted flag. All old metadata info is preserved.
        OLD.mod_time = now();
        OLD.creator = current_user;
        -- No tricky updating of OLD or NEW required!
        EXECUTE insert_stmt USING new_metadata_hx_id, TRUE, OLD;
        RETURN NULL;
    END IF;
END;
$BODY$;
```