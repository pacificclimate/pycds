## Hx table: Referential integrity on item delete

Overview:
1. We exploit the built-in FK mechanism to check whether a mark-delete would be OK.
    1. We define FK relations with ON DELETE CASCADE.
    2. We do a trial deletion of all hx records associated with the metadata id
       proposed to be marked deleted.
    3. These deletions cascade to the dependent (referencing) tables.
    4. If a referencing item still uses the item to be deleted, this is a referential
       integrity error. Otherwise, it's not, and the deletion can proceed.
2. We only need to check one level deep (immediate FK refs). We do not need further
   delete cascade, even though it is possible by permitting the deletion to proceed.
3. We must actually delete the item records to trigger the cascade. But we do not need
   and indeed should disallow deleting any other history records.
4. The records deleted (to trigger cascade) must be re-inserted into the history table
   afterwards.

How to indicate trial deletion in progress?

Temp table trial_deletion_in_progress acts as a global variable. A record in it  
indicates a trial deletion is in progress. No records in it indicates no trial
deletion in progress.

Therefore:

1. If there is NO mark-delete in progress:
    1. Start one: insert a record in the temp table.
    2. Perform a trial deletion.
2. If there IS a mark-delete in progress
    1. Avoid treating the re-insertion as a true mark-delete. That is done by checking
       the temp table for a record, and (supplementarily) whether this collection is
       the same one as in the temp table record. Don't do a trial deletion. Just insert
       the record.
    2. Allow the deletion *of the base record* to go through by returning OLD, and ...
    2. Re-insert it after.
4. Do not allow deletion of any non-base record.

```postgresql
CREATE OR REPLACE FUNCTION mdhx_mark_delete_enforce_ref_integ_before()
    -- This trigger function detects a "mark item as deleted" operation. When it
    -- detects such, it performs a trial deletion of all history records with the same
    -- metadata id. The trial deletion may raise an error in the deletion cascade, or it 
    -- may complete without error. 
   RETURNS trigger
    LANGUAGE plpgsql
    PARALLEL UNSAFE
AS
$BODY$
    DECLARE
        -- Trigger function arguments
        this_metadata_id_name text := tg_argv[0];

        -- Values from special variables
        this_history_table_name text := tg_table_name;
        this_collection_name text := mdhx_collection_name_from_hx(tg_table_name);
        
        this_metadata_id int;

        -- Table mark_delete_in_progress
        mdip_item record;
        base_collection_name text;
        base_metadata_id_name text;
        base_metadata_id int;

        trial_delete text := format(
            'DELETE FROM %I WHERE %I = $1', this_history_table_name, this_metadata_id_name
        );

        uses_base_metadata_item text;
        in_use boolean;
        
    BEGIN
        -- This temporary table functions as a collection of global variables via which 
        -- we enable the nested (recursive) trigger functions call to: 
        -- 1. determine whether it is handling the base case or a nested case 
        --    (due to this function issuing a query which triggers itself again)
        -- 2. access data about the base case, namely the base table name, base metadata 
        --    id name, and base metadata id value.
        -- 
        -- The following information about is essential to understanding how 
        -- this trigger function works and why it is correct:
        --  
        -- A temp table created with ON COMMIT DELETE ROWS behaves as if the 
        -- rows it contains are private to the session or transaction in 
        -- which they are inserted. The table itself is not visible to other sessions,
        -- although it is visible to other transactions within the session. 
        -- 
        -- Creating two temp tables with the same name in different sessions is permitted
        -- and behaves as if they are separate tables. 
        -- 
        -- A trigger function executes within a transaction. Therefore, any rows it has 
        -- inserted in a temp table created with ON COMMIT DELETE ROWS are deleted when
        -- it returns (the transaction is committed) or raises an exception (the 
        -- transaction is rolled back).
        -- 
        -- Finally, a trigger function triggered by the action of another trigger 
        -- function (including itself) executes within the transaction context of 
        -- the "parent" (triggering) trigger function.
        -- 
        -- Therefore:
        -- 1. Rows in the temp table are private to the execution transaction of the 
        --    trigger function that creates them.
        -- 2. Those rows are visible within the (nested) execution transaction of any
        --    trigger function triggered by an action of a earlier trigger function.
        -- 3. This trigger function is correct so long as concurrent trigger function
        --    executions do not occur. This is prevented by labelling the function 
        --    PARALLEL UNSAFE, as the PostgreSQL documentation requires (see 
        --    https://www.postgresql.org/docs/current/parallel-safety.html).

        CALL mdhx_create_table_mark_delete_in_progress();

        IF tg_op = 'INSERT' AND NOT NEW.deleted THEN
            -- This insertion is not of interest here. Allow to continue.
            RETURN NEW;
        ELSIF tg_op = 'INSERT' AND NEW.deleted THEN
            RAISE NOTICE '%: Mark-deleted op detected, NEW = %', this_collection_name, NEW;
            -- Perform trial DELETE operation on all history records related to this 
            -- metadata item. Deletions cascade, so built-in foreign key integrity 
            -- checking does the hardest part of the work for us, which is knowing all
            -- the FK relations to this table.
            this_metadata_id := hstore(NEW) -> this_metadata_id_name;

            SELECT * FROM mdhx_get_mark_delete_in_progress() INTO
                base_collection_name, base_metadata_id_name, base_metadata_id;
            
            IF base_collection_name IS NOT NULL THEN
                -- TODO: Implicitly this should only occur when 
                --  base_collection_name = this_collection_name. Check it.
                RAISE NOTICE '%: Mark-deleted op: Re-insertion', this_collection_name;
                -- This is a re-insertion. Just do it.
                RETURN NEW;
            END IF;
            
            
            -- Mark this deletion in progress
            CALL mdhx_set_mark_delete_in_progress(
                this_collection_name, this_metadata_id_name, this_metadata_id
             );
            -- TODO: Explicitly remove record added here instead of relying on 
            --  ON COMMIT DELETE ROWS.
            
            -- Perform trial deletion
            -- There are two possible outcomes:
            -- 1. Referential integrity checking performed in the DELETE branch 
            --    below raises an exception. This exception is allowed to bubble up to 
            --    user level, and this item is not marked as deleted.
            -- 2. The trial DELETE does not raise an exception. We are free to mark
            --    this item as deleted.
            RAISE NOTICE '%: Mark-deleted op: Performing trial deletion',
                this_collection_name;
            -- TODO: Catch error, remove mdip record, re-raise. DONE.
            BEGIN
                EXECUTE trial_delete USING this_metadata_id;
            EXCEPTION
                WHEN OTHERS THEN
                    RAISE NOTICE '%: Mark-deleted op: Trial deletion failed',
                        this_collection_name;
                    CALL mdhx_unset_mark_delete_in_progress(
                        this_collection_name, this_metadata_id_name, this_metadata_id
                     );
                    RAISE;
            END;
            RAISE NOTICE '%: Mark-deleted op: Trial deletion succeeded', 
                this_collection_name;
            -- If we got this far, there was no referential integrity error. Mark item as 
            -- deleted.
            -- TODO: Remove record from mdip. DONE
            CALL mdhx_unset_mark_delete_in_progress(
                this_collection_name, this_metadata_id_name, this_metadata_id
             );
            RETURN NEW;
        ELSIF tg_op = 'DELETE' THEN
            RAISE NOTICE '%: DELETE detected, OLD = %', this_collection_name, OLD;
            -- If trial deletion in progress check whether item is marked deleted.
            -- Don't do the check if this is the base collection.
            SELECT * FROM mdhx_get_mark_delete_in_progress() INTO
                base_collection_name, base_metadata_id_name, base_metadata_id;
            
            IF base_collection_name IS NULL THEN
                RAISE EXCEPTION 'Error: This table is append-only. DELETE attempted.';
            END IF;
                
            IF base_collection_name = this_collection_name THEN
                -- Allow deletion of base record. This causes the delete cascade we need.
                -- The record will be reinserted by the AFTER trigger.
                RETURN OLD;
            ELSE
                -- TODO: Remove ELSE wrapper
               -- This is not the base collection. Therefore, check whether this 
               -- metadata item still uses the base metadata item (it is not marked 
               -- as deleted and its latest state refers to the base item).
                this_metadata_id := hstore(OLD) -> this_metadata_id_name;
                -- TODO: Use ORDER BY ..., hx_id DESC
                uses_base_metadata_item := format(
                    'SELECT DISTINCT ON (%1$I) not deleted and (%3$I = $2)'
                        'FROM %2$I WHERE %1$I = $1 '
                        'ORDER BY %1$I, create_time DESC',
                    this_metadata_id_name,
                    this_history_table_name,
                    base_metadata_id_name
                );
                EXECUTE uses_base_metadata_item INTO in_use 
                    USING this_metadata_id, base_metadata_id;
                IF in_use THEN
                    -- This signals a referential integrity error.
                    -- Raising an exception also cancels the DELETE operation.
                    RAISE EXCEPTION 
                        'Error: Metadata item in collection "%" with "%" = % '
                        'uses item to be deleted',
                        this_collection_name, this_metadata_id_name, this_metadata_id
                    ;
                END IF;
                -- Prevent actual deletion of this history record.
                RETURN NULL;
            END IF;
        -- TODO: Extract to separate trigger function.
        ELSIF tg_op = 'UPDATE' THEN
            RAISE EXCEPTION 'Error: This table is append-only. UPDATE attempted.';
        END IF;
    END;
$BODY$;
```

```postgresql
CREATE OR REPLACE FUNCTION mdhx_mark_delete_enforce_ref_integ_after()
    -- This trigger function "rolls back" a trial deletion is by reinserting the deleted 
    -- record. It is a necessary companion to TF mark_delete_enforce_ref_integ_before()
   RETURNS trigger
    LANGUAGE plpgsql
    PARALLEL UNSAFE
AS
$BODY$
    DECLARE
        -- Values from special variables
        history_table_name text := tg_table_name;
        collection_name text := mdhx_collection_name_from_hx(history_table_name);
    BEGIN
        RAISE NOTICE '% After DELETE: re-inserting deleted record %',
            collection_name, OLD;
        EXECUTE format(
            'INSERT INTO %I VALUES ($1.*)', history_table_name
        ) USING OLD;
        RETURN NULL; -- Return value is ignored in AFTER triggers.
    END;
$BODY$;
```