"""
This implementation represents change history more parsimoniously than does module
crmprtd.orm.basic, but still follows the same pattern outlined in
https://www.cybertec-postgresql.com/en/tracking-changes-in-postgresql/.
"""
import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, text, BigInteger,
)

from pycds import Base, get_schema_name
from pycds.alembic.extensions.replaceable_objects import ReplaceableStoredProcedure

schema_name = get_schema_name


class UpdateHistory(Base):
    """
    Record a change to an existing record. Notes:
    1. Each record in this table records a single update to a single column.
    2. It records the row and column identifiers, plus the *old* (pre-update) value of
        the column.
    3. The column value is cast to text for storage. There is no "Any" type in Postgres.
    """
    __tablename__ = "update_history_1"

    id = Column("update_history_id", Integer, primary_key=True)
    change_time = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    change_agent = Column(String, nullable=False, server_default=text("current_user"))
    schema_name = Column(String, nullable=False)   # Necessary?
    table_name = Column(String, nullable=False)
    # row_id is effectively a foreign key into an arbitrary table. Is there a better or
    # more explicit way to represent that?
    # BigInteger to accommodate obs_raw.
    row_id = Column(BigInteger, nullable=False)
    column_name = Column(String, nullable=False)
    column_val = Column(String, nullable=False)


# Naive triggers

obs_raw_log_update_history = ReplaceableStoredProcedure(
    # This is a naive and un-DRY trigger function used to attach history tracking to the
    # table obs_raw.
    identifier="obs_raw_log_update_history()",
    definition=f"""
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF (NEW.datum IS DISTINCT FROM OLD.datum) THEN
                INSERT INTO {schema_name}.update_history_1(
                    schema_name, table_name, row_id, column_name, column_value
                )
                VALUES (
                    -- Note we record OLD value; this is a record of what was 
                    -- superseded, and when when.
                    TG_TABLE_SCHEMA, TG_RELNAME, NEW.obs_raw_id, 'datum', OLD.datum::text
                )
            END IF;
            -- Etc. as required for other columns to be tracked. A bit tedious, not
            -- DRY, but simple.   
            RETURN NEW;  
        END;
        $$
    """,
    schema=schema_name,
)


# Create a trigger to track changes to table obs_raw.
# TODO: Copy extension ReplaceableTrigger from branch change-history.
obs_raw_log_update_history_trigger = ReplaceableTrigger(
    identifier="obs_raw_log_update_history_1_trigger",
    definition=f"""
        BEFORE UPDATE ON {schema_name}.obs_raw
        FOR EACH ROW
        EXECUTE PROCEDURE {schema_name}.obs_raw_log_update_history()
    """,
    schema=schema_name,
)


# A generic trigger function

generic_log_update_history_1 = ReplaceableStoredProcedure(
    # This trigger function is generic to all change history triggers for all tables.
    # To specialize it for each table, it takes two arguments:
    #
    #   TG_ARGV[0]: row_id_column_name: Name of column in table that specifies row
    #       (typically the "id" column of the table). Note: This does not support
    #       multi-column primary keys, and it assumes that the id is an integer.
    #
    #   TG_ARGV[1]: column_names: Array of column names to be recorded on each update.
    #
    # The arguments are specified when the trigger is created for a specific table.
    #
    # An update that affects multiple columns will create one update history record per
    # column. It will ease subsequent queries to timestamp them all with a common time,
    # i.e., not to rely on the default value, which will differ slightly for each record
    # inserted. This is part of the code in
    identifier="generic_log_update_history_1()",
    definition=f"""
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$
        DECLARE
            -- Arguments
            row_id_column_name text;
            column_names text[];
            
            -- Other variables
            change_time timestamp;
            row_id bigint;
            column_name text;
            old_column_value text;
            new_column_value text;
            
        BEGIN
            row_id_column_name := TG_ARGV[0];
            column_names := TG_ARGV[1];
            EXECUTE 'SELECT NOW()' INTO change_time;
            -- These object accessor statements could be factored out as function. 
            -- But fairly verbosely.
            EXECUTE 'SELECT OLD.' || row_id_column_name || INTO row_id;
            FOREACH column_name IN ARRAY column_names LOOP
                EXECUTE 'SELECT OLD.' || column_name || '::text' INTO old_column_value;
                EXECUTE 'SELECT NEW.' || column_name || '::text' INTO new_column_value;
                IF new_column_value IS DISTINCT FROM old_column_value THEN
                    INSERT INTO {schema_name}.update_history_1(
                        -- Probably have to quote column names because vars of 
                        -- same name in procedure.
                        schema_name, table_name, change_time, row_id, column_name, column_value
                    )
                    VALUES (
                        -- Note we record OLD value; this is a record of what was 
                        -- superseded, when. The new value is in the table.
                        TG_TABLE_SCHEMA, TG_RELNAME, change_time, row_id, column_name, old_column_value
                    )
                END IF;
            END LOOP;
            RETURN NEW;  
        END;
        $$
    """,
    schema=schema_name,
)


# Create a trigger to track changes to table obs_raw.
# TODO: Copy extension ReplaceableTrigger from branch change-history.
obs_raw_change_history_0_trigger = ReplaceableTrigger(
    identifier="obs_raw_log_update_history_1_trigger",
    definition=f"""
        BEFORE UPDATE ON {schema_name}.obs_raw
        FOR EACH ROW
        EXECUTE PROCEDURE {schema_name}.generic_log_update_history_1(
            'obs_raw_id', '{"datum"}'
        )
    """,
    schema=schema_name,
)
