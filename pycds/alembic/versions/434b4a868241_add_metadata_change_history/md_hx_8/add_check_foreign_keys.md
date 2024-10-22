## Hx table: Fill in history id's; referential integrity for referenced items

TODO: Rename better

```postgresql
CREATE OR REPLACE FUNCTION mdhx_add_check_foreign_keys() 
   RETURNS trigger
    LANGUAGE plpgsql
    PARALLEL UNSAFE 
AS
$BODY$
    -- This trigger function updates the NEW record with the history id corresponding to 
    -- the metadata id for each foreign key. FK correspondences are specified in the
    -- trigger function arg. They must be specified in the same order they appear in 
    -- the view and history table.
    -- 
    -- This TF also enforces "forward" referential integrity: It detects and flags the 
    -- case where a referenced foreign metadata item is marked deleted (and this is not 
    -- a mark-deleted operation being inserted).
    DECLARE 
        -- Trigger function arguments
       foreign_keys                text[][] := tg_argv[0];
       -- Specifies metadata history table info for each foreign key in this table.
       -- Content: Array (in order of FK occurrence in this hx table) of 
       -- array[foreign_collection_name, foreign_metadata_id]
        
       -- Special values
       this_collection_name text := mdhx_collection_name_from_hx(tg_table_name);
        
       fk_item                     text[];
       fk_metadata_collection_name text;
       fk_metadata_id_name         text;
       fk_metadata_history_id_name text;
       fk_metadata_history_table_name text;
       fk_query                    text;
       fk_metadata_id      integer;
       fk_metadata_history_id      integer;
       fk_deleted                  boolean;
       mdip mdhx_mark_delete_in_progress_type;
    BEGIN
        RAISE NOTICE 'BEGIN %', tg_name;
        -- Check whether this is a re-insertion of a trial deletion. 
        -- If so, don't intervene.
        CALL mdhx_create_table_mark_delete_in_progress();
        SELECT * FROM mdhx_get_mark_delete_in_progress() INTO
            mdip;
        IF mdip IS NOT NULL THEN
            RAISE NOTICE 'Re-insertion, %', mdip;
            ASSERT mdip.base_collection_name = this_collection_name, 
                'Mark delete is in progress. ' ||
                'Re-insertion should only occur for base collection "%", ' ||
                'but this is collection "%".', 
                mdip.base_collection_name, this_collection_name;
            -- This is a re-insertion. Just do it.
            RETURN NEW;
        END IF;
        
        -- Add foreign key values for referenced foreign metadata items. If no such 
        -- value exists, or it exists but the corresponding item has been marked deleted, 
        -- that is a referential integrity error.
       IF foreign_keys IS NOT NULL THEN
          FOREACH fk_item SLICE 1 IN ARRAY foreign_keys
             LOOP
                  RAISE NOTICE 'fk_item = %', fk_item;
                  fk_metadata_collection_name := fk_item[1];
                  fk_metadata_id_name := fk_item[2];
                  fk_metadata_history_id_name := 
                      mdhx_hx_id_name(fk_metadata_collection_name);
                  fk_metadata_history_table_name := 
                      mdhx_hx_table_name(fk_metadata_collection_name);
                  fk_metadata_id := (hstore(NEW) -> fk_metadata_id_name)::integer;
                  -- Extract the foreign metadata history id corresponding to the metadata
                  -- id in this record. This will be the foreign key for this record.
                  fk_query := format(
                        'SELECT DISTINCT ON (%1$I) %2$I, deleted ' ||
                        'FROM %3$I WHERE %1$I = $1 ' ||
                        'ORDER BY %1$I, %2$I DESC',
                        fk_metadata_id_name, 
                        fk_metadata_history_id_name,
                        fk_metadata_history_table_name
                  );
                  RAISE NOTICE 'fk_query = %', fk_query;
                  BEGIN
                     EXECUTE fk_query
                        INTO STRICT fk_metadata_history_id, fk_deleted
                        USING fk_metadata_id;
                  EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        -- No such item: Referential integrity error.
                        RAISE EXCEPTION 'Metadata collection % contains no item with 
                        id %', fk_metadata_collection_name, fk_metadata_id;
                    WHEN TOO_MANY_ROWS THEN
                      RAISE EXCEPTION 'Internal error: too many rows';
                  END;
                  RAISE NOTICE 'FK: % = %, deleted = %', fk_metadata_id_name, 
                     fk_metadata_history_id, fk_deleted;
                  -- TODO: Should we raise an exception even if NEW.deleted is true? 
                  --  Is it a ref integ error to mark an item deleted when an item
                  --  it references is marked deleted? Could this even happen? (No: 
                  --  that would raise a ref integrity error when the item in question 
                  --  was attempted to mark deleted.) Seems that NEW.deleted should 
                  --  always be false, given we catch re-insertions at the beginning.
                  IF not NEW.deleted AND fk_deleted THEN
                      -- The foreign item has been marked deleted: 
                      -- referential integrity error. 
                     RAISE EXCEPTION 
                         'Error for collection "%":'
                         'Referenced metadata item in collection "%", id = "%" '
                         'is deleted', 
                        tg_table_name, fk_metadata_collection_name, fk_metadata_id;
                  END IF;
                  -- Update NEW with it.
                  NEW := NEW #= hstore(fk_metadata_history_id_name,
                                       fk_metadata_history_id::text);
                  RAISE NOTICE 'NEW(after fk update) = %', NEW;
             END LOOP;
       END IF;
        RAISE NOTICE 'END %',tg_name;
       RETURN NEW;
    END;
$BODY$;
```