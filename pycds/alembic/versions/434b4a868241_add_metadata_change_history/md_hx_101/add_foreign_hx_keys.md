## Hx table: Fill in foreign history id's

```postgresql
CREATE OR REPLACE FUNCTION mdhx_add_foreign_hx_keys()
    RETURNS trigger
    LANGUAGE plpgsql
    PARALLEL UNSAFE
AS
$BODY$
    -- Foreign keys come in pairs: In a primary table, they refer to another primary 
    -- table. In a history table, they refer to the corresponding history table. For 
    -- any given primary table FK, there are in general many history table FKs 
    -- corresponding to it, one for each history of the item. For any given newly 
    -- inserted history record, the desired history FK is the latest one in the set 
    -- selected by the primary FK.
    -- 
    -- This trigger function updates the NEW record with the latest history FK 
    -- corresponding to each primary FK in the primary table. The corresponding primary
    -- table names and primary FKs are passed in as arguments to the trigger function.
DECLARE
    -- Trigger function arguments
    foreign_keys text[][] := tg_argv[0];
    -- Specifies metadata history table info for each primary foreign key in this table.
    -- Content: Array (in order of FK occurrence in this hx table) of 
    -- array[foreign_collection_name, foreign_metadata_id]. 

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

                -- Extract the most recent foreign metadata history id corresponding to 
                -- the foreign metadata id in this record. This will be the foreign 
                -- key to use.
                fk_query := format(
                        'SELECT max(%2$I) ' ||
                        'FROM %3$I ' ||
                        'WHERE %1$I = $1 ',
                        fk_metadata_id_name,
                        fk_metadata_history_id_name,
                        fk_metadata_history_table_name
                );
                RAISE NOTICE 'fk_query = %', fk_query;
                EXECUTE fk_query
                    INTO STRICT fk_metadata_history_id
                    USING fk_metadata_id;
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