"""
Define trigger function ``update_metadata_generic()``.

This trigger function is the core of the metadata change history tracking code. It is used by triggers on the metadata
views that form the user-facing interface of the metadata. This trigger function (plus the appropriate CREATE
TRIGGER statements) cause the views to have a table-like interface supporting INSERT, UPDATE and DELETE operations.

View-History table setup
========================

For each metadata "table", a coordinated view, sequence, and table are defined:

* The view provides the user-facing interface to the "table". Triggers are attached to the view to handle INSERT,
  UPDATE and DELETE operations.
* Behind the metadata view is the metadata history table.
* The history table is append-only, and contains records of every past and present state of the metadata.

    * Each row of the history table represents a single state (in time) of a single metadata item.
    * The view selects the latest metadata item from history table. Thus the user sees only the latest version of each metadata item.

* The sequence provides the unique primary key for the history table.

Trigger function for metadata views
===================================

As noted above, triggers are defined on each metadata view to convert insert, update, and delete operations on the
view to inserts (only) on the history table.

We define one generic trigger function that will work for all the different metadata tables, rather than having to
define a separate trigger function for each table. The parametrization of trigger functions allows us to do that.

It's important to keep in mind that there are two id's in play in the history table:

* The *metadata history* id, which is a unique identifier for the record. It is provided by a corresponding sequence.
* The *metadata* id, which identifies a single metadata item, but which can have many timestamped history records for
  it. The combination of metadata id and timestamp is unique. (This pair could in theory replace the history id, but
  implementation is much simpler if we tolerate this slight lack of normalization.)

The secret sauce is in the INSERT statement that the trigger function prepares and executes. It works only if the
following conditions are met:

* The view, history table, and sequence, must be named as follows:

    * View: ``<view_name>``
    * History table: ``<view_name>_hx``
    * Sequence: ``<view_name>_hx_id_seq``

* The history table columns must be defined as follows, in this order:

    * history maintenance columns

        * metadata history id
        * deleted flag
        * create time (a.k.a. mod time)
        * creator (id of user causing this record to be created)

    * metadata columns proper

        * metadata id (e.g., network_id, vars_id, station_id, history_id in existing metadata tables)
        * metadata-specific columns, which vary by metadata table

* View column *names* may be changed by the view definition,
  but the view must consist of exactly the following columns in the following order:

    * create time (a.k.a. mod time)
    * creator (id of user causing this record to be created)
    * metadata id
    * metadata-specific columns, which vary by metadata table, in the same order as the history table

The INSERT statement is prepared and executed in two phases:

* First the view name is injected into the string to form the history table name.
* Later, column arguments are substituted when it is EXECUTEd.
* The final result is an INSERT statement of the following form::

    INSERT INTO <history table> VALUES <metadata history id> <deleted> <create time> <creator> <metadata id> <metadata-specific columns...>

* The first two columns are provided explicitly, as parameters ``$1``, ``$2``.
* The remaining columns are provided by spreading either the NEW or the OLD record, depending on what operation caused
  the trigger. The whole record is passed as parameter ``$3``.
* We have to update the following information in the ``NEW``/``OLD`` record before it can be passed to the EXECUTE
  statement:

    * create time
    * creator
    * metadata id – in the case of a newly created metadata item, it is the same as the history id; in the case of an
      existing metadata item, it is brought forward from the existing metadata item

To do some of the manipulations we require (see prev item) on the ``NEW``/``OLD`` records, we must access their contents by
attribute name (specifically, metadata id column name). By far the easiest way to do this in ``pgplsql`` is to use the
``hstore`` extension. Its syntax and usage are slightly gnarly, but it is very helpful. For more details on
``hstore``, see PG documentation .
"""
from pycds.alembic.extensions.replaceable_objects import ReplaceableFunction
from pycds.context import get_schema_name


schema_name = get_schema_name()


update_metadata = ReplaceableFunction(
    """
    update_metadata()
    """,
    f"""
        RETURNS trigger
        LANGUAGE 'plpgsql'
        COST 100
        VOLATILE NOT LEAKPROOF
    AS $BODY$
    DECLARE
        -- Trigger function arguments from CREATE TRIGGER statement, provided 
        -- via the special variable tg_argv.
        view_name text := tg_argv[0];
        metadata_id_name text := tg_argv[1];
        
        -- Name of history id sequence, derived from view name.
        hx_id_seq_name text := format('%I_hx_id_seq', view_name);
        
        -- String containing INSERT statement to be executed in body of function with EXECUTE statement.
        -- Here we derive the history table name from the view name. The $n params are here just another string.
        -- They are substituted by the USING parameters of the EXECUTE statement.
        -- The first two arguments are <metadata history id> and <deleted>. The third argument is a record, which is
        -- spread (note * in expression $3.*) to form the remainder of the VALUES columns.   
        insert_stmt text := format('INSERT INTO %I_hx VALUES ($1, $2, $3.*)', view_name);
        
        old_metadata_id integer;
        metadata_hx_id integer;
    BEGIN
        SELECT NEXTVAL(hx_id_seq_name::regclass) INTO metadata_hx_id;
        IF tg_op = 'INSERT' THEN
            -- Create a completely new metadata item. Its metadata id is new and is the same as the new metadata 
            -- history id.
            NEW.mod_time = now();
            NEW.creator = current_user;
            -- We need to update the NEW record with the value of the new metadata id, targeted at the column
            -- with name metadata_id_name. The easiest way to do that is to use hstore. The hstore syntax used here
            -- is not at all intuitive ... there may be a clearer syntax we can substitute.            
            NEW := NEW #= hstore(metadata_id_name, metadata_hx_id::text);
            EXECUTE insert_stmt USING metadata_hx_id, FALSE, NEW;
            RETURN NEW;
        ELSIF tg_op = 'UPDATE' THEN
            -- Update an existing metadata item. Its metadata_id is preserved (copied from OLD).
            NEW.mod_time = now();
            NEW.creator = current_user;
            old_metadata_id := hstore(OLD) -> metadata_id_name;
            -- We need to update the NEW record with the value of the old metadata id, targeted at the column
            -- with name metadata_id_name. The easiest way to do that is to use hstore. We use hstore once to
            -- extract the appropriate value from the OLD record, and a second time to update the NEW record.
            old_metadata_id := hstore(OLD) -> metadata_id_name;
            NEW := NEW #= hstore(metadata_id_name, old_metadata_id::text);
            EXECUTE insert_stmt USING metadata_hx_id, FALSE, NEW;
            RETURN NEW;
        ELSIF tg_op = 'DELETE' THEN
            -- Set deleted flag. All old metadata info is preserved.
            OLD.mod_time = now();
            OLD.creator = current_user;
            -- No tricky updating of OLD or NEW required in this case.
            EXECUTE insert_stmt USING metadata_hx_id, TRUE, OLD;
            RETURN NULL;
        END IF;
    END;
    $BODY$;
    """,
    schema=schema_name,
)
