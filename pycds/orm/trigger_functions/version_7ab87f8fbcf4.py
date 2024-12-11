"""
Define trigger functions for change history tracking.

These trigger functions maintain the contents of the history table that tracks each
primary table. Triggers calling these functions are established on the primary and
history table.

To understand the trigger functions, it is necessary to understand the overall setup of
the history tracking tables.

Table setup
===========

For each meta/data table (called the primary table) that has history tracking, the
following things are true:

* The primary table is a slightly modified version of the existing table. Its name is
  the same, its existing columns are in the same order and position, and it continues
  to provide the main user-facing interface to the meta/data. It also contains a couple
  of supplementary columns that expose history information.

* The history table contains all the columns of the primary table, plus several history
  -specific columns. It is append-only, and contains records of every past and present
  state of the meta/data.
    * Each row of the history table represents a single state (in time) of a single
      metadata item.
    * The primary table and history table both store the contents of the most recent
      items. That is, the history table duplicates contents of the primary table.

* Triggers attached to the primary intercept each INSERT, UPDATE and DELETE operation
  and append appropriate record(s) to the history table.

Trigger functions
=================

As noted above, triggers are defined on each primary table to convert insert, update, and
delete operations on the primary to inserts (only) on the history table.

We define generic trigger functions that work for all the different meta/data tables,
rather defining a separate trigger function for each table. Parametrization of trigger
functions allows us to do that.

It's important to keep in mind that there are two id's in play in the history table:

* The *history* id, which is a unique identifier for the history record. It is provided
  by a corresponding sequence.

* The *primary* or *metadata* id, which identifies a single item in the collection,
  but which can have many different history records for it, each with a different
  timestamp.
  The combination of history id and timestamp is unique. (That pair could be used as
  the primary key, but implementation is simpler if we tolerate a slight
  lack of normalization and use an independent primary key column.)

Naming conventions for tables, history id columns, and sequences enable us to write
simpler, more self-configuring trigger functions. These conventions are:

* The primary table, history table, history id column, and history id sequence
  must be named as follows:

    * Primary table: ``<collection_name>`` (the original table name, e.g., ``meta_network``)
    * History table: ``<collection_name>_hx``
    * History id column: ``<collection_name>_hx_id``
    * History id sequence: ``<history_table_name>_<history_id_name>_seq``
      (i.e., the default name for automatically created primary key sequences)

* The primary table is extended with the following columns (mainly for the convenience
  of the user):

  * ``mod_time``: most recent modification time of this record
  * ``mod_user``: user rolename who most recently modified this record

* The history table columns must be defined as follows, in this order:

    * Primary table columns (including ``mod_time`` and ``mod_user``).

    * History maintenance columns

        * ``deleted``: flag indicating if this record was deleted
        * history id
        * For each foreign key in the primary table to another primary (history-tracked)
          table:
            * A foreign key to the corresponding history table

Other notes:

* One of the trigger functions sets ``mod_time`` and ``mod_user`` in operations on the
  primary table so that they cannot be set inaccurately by a user.
* To do some of the manipulations we require on the ``NEW``/``OLD`` records, we must
  access their contents by attribute name. By far the easiest way to do this in
  ``pgplsql`` is to use the ``hstore`` extension. Its syntax and usage are slightly
  gnarly, but it is very helpful. For more details on ``hstore``, see PG documentation .
* The current triggers are record-level, and the trigger functions written accordingly.
  The performance of these triggers / functions may be able to be improved by converting
  them to statement-level. This will be somewhat more complicated, particularly for the
  code that fills in the history table foreign keys.
"""

from pycds.alembic.extensions.replaceable_objects import ReplaceableFunction
from pycds.context import get_schema_name


schema_name = get_schema_name()




hxtk_primary_control_hx_cols = ReplaceableFunction(
    """
hxtk_primary_control_hx_cols()
    """,
    f"""
-- CREATE OR REPLACE FUNCTION crmp.hxtk_primary_control_hx_cols()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
    -- This trigger function controls the values of the history columns, namely
    -- mod_time and creator. This is necessary to prevent them from being carried 
    -- forward from the previous state or from being set explicitly by the user.
    -- This trigger function should be called by a BEFORE trigger on the primary table.
BEGIN
    NEW.mod_time = now();
    NEW.creator = current_user;
    RETURN NEW;
END;
$BODY$
    """,
    schema=schema_name,
)


hxtk_primary_ops_to_hx = ReplaceableFunction(
    """
hxtk_primary_ops_to_hx()
    """,
    f"""
-- CREATE OR REPLACE FUNCTION hxtk_primary_ops_to_hx()
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
    this_history_table_name text := hxtk_hx_table_name(this_collection_name);
    new_metadata_hx_id integer;
    
    insert_stmt text := format(
        -- values: (NEW/OLD, deleted, new_metadata_hx_id)
        'INSERT INTO %I VALUES ($1.*, $2, DEFAULT)',
        this_history_table_name
    );
BEGIN
    IF tg_op = 'INSERT' OR tg_op = 'UPDATE' THEN
        -- NEW.mod_time, NEW.mod_user maintained in hxtk_primary_control_hx_cols
        EXECUTE insert_stmt USING NEW, FALSE;
    ELSIF tg_op = 'DELETE' THEN
        OLD.mod_time = now();
        OLD.mod_user = current_user;        
        EXECUTE insert_stmt USING OLD, TRUE;
    END IF;
    RETURN NULL;  -- Ignored in an AFTER trigger
END;
$BODY$
    """,
    schema=schema_name,
)


hxtk_add_foreign_hx_keys = ReplaceableFunction(
    """
hxtk_add_foreign_hx_keys()
    """,
    f"""
-- CREATE OR REPLACE FUNCTION hxtk_add_foreign_hx_keys()
    RETURNS trigger
    LANGUAGE plpgsql
    PARALLEL UNSAFE
AS
$BODY$
    -- This trigger function updates the NEW record with the latest history FK 
    -- corresponding to each primary FK in the primary table. The corresponding primary
    -- table names and primary FKs are passed in as arguments to the trigger function.
    -- 
    -- Foreign keys come in pairs: In a primary table, they refer to another primary 
    -- table. In a history table, they refer to the corresponding history table. For 
    -- any given primary table FK, there are in general many history table FKs 
    -- corresponding to it, one for each history of the item. For any given newly 
    -- inserted history record, the desired history FK is the latest one in the set 
    -- selected by the primary FK.
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
    -- Add foreign key values for referenced foreign metadata items. If no such 
    -- value exists, or it exists but the corresponding item has been marked deleted, 
    -- that is a referential integrity error.
    IF foreign_keys IS NOT NULL THEN
        FOREACH fk_item SLICE 1 IN ARRAY foreign_keys
            LOOP
                fk_metadata_collection_name := fk_item[1];
                fk_metadata_id_name := fk_item[2];
                fk_metadata_history_id_name :=
                        hxtk_hx_id_name(fk_metadata_collection_name);
                fk_metadata_history_table_name :=
                        hxtk_hx_table_name(fk_metadata_collection_name);
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
                EXECUTE fk_query
                    INTO STRICT fk_metadata_history_id
                    USING fk_metadata_id;
                -- Update NEW with it.
                NEW := NEW #= hstore(fk_metadata_history_id_name,
                                     fk_metadata_history_id::text);
            END LOOP;
    END IF;
    RETURN NEW;
END;
$BODY$    
    """,
    schema=schema_name,
)
