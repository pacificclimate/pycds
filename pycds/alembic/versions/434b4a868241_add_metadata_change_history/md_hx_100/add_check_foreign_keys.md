## Hx table: Fill in history id's

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
DECLARE
    -- Trigger function arguments
    foreign_keys text[][] := tg_argv[0];
    -- Specifies metadata history table info for each foreign key in this table.
    -- Content: Array (in order of FK occurrence in this hx table) of 
    -- array[foreign_collection_name, foreign_metadata_id]

    -- Special values
    this_collection_name text := mdhx_collection_name_from_hx(tg_table_name);
    fk_item text[];
    fk_metadata_collection_name text;
    fk_metadata_id_name text;
    fk_metadata_history_id_name text;
    fk_metadata_history_table_name text;
    fk_query text;
    fk_metadata_id integer;
    fk_metadata_history_id integer;
BEGIN
    RAISE NOTICE 'BEGIN %', tg_name;
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
                        'SELECT DISTINCT ON (%1$I) %2$I ' ||
                        'FROM %3$I WHERE %1$I = $1 ' ||
                        'ORDER BY %1$I, %2$I DESC',
                        fk_metadata_id_name,
                        fk_metadata_history_id_name,
                        fk_metadata_history_table_name
                            );
                RAISE NOTICE 'fk_query = %', fk_query;
                -- TODO: Remove exception catches -- neither should occur, and if they do
                --  we should let them bubble up.
                BEGIN
                    EXECUTE fk_query
                        INTO STRICT fk_metadata_history_id
                        USING fk_metadata_id;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        -- No such item: Referential integrity error. 
                        -- This should never happen: We get here when a valid operation
                        -- on the primary table has been performed (PG would raise an 
                        -- error otherwise). Therefore the foreign item must exist in 
                        -- its primary table and therefore also in its history table. 
                        -- Therefore this query must always succeed.
                        RAISE EXCEPTION
                            'Metadata collection % contains no item with id %',
                            fk_metadata_collection_name, fk_metadata_id;
                    WHEN TOO_MANY_ROWS THEN
                        RAISE EXCEPTION 'Internal error: too many rows';
                END;
                RAISE NOTICE 'FK: % = %', fk_metadata_id_name, fk_metadata_history_id;
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