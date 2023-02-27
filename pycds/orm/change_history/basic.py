"""
This module implements change history using exactly the same table and columns
as in https://www.cybertec-postgresql.com/en/tracking-changes-in-postgresql/.

Columns ChangeHistory.new_val and ChangeHistory.old_val contain a lot of redundancy.
This will potentially cause a lot of storage use when changes are made to the Obs
table.
"""
import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    JSON,
    text,
)

from pycds import Base, get_schema_name
from pycds.alembic.extensions.replaceable_objects import ReplaceableStoredProcedure

schema_name = get_schema_name


class ChangeHistory(Base):
    __tablename__ = "change_history_0"

    id = Column("change_history_id", Integer, primary_key=True)
    change_time = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    change_agent = Column(String, nullable=False, server_default=text("current_user"))
    schema_name = Column(String, nullable=False)  # Necessary?
    table_name = Column(String, nullable=False)
    operation = Column(String, nullable=False)
    old_val = Column(JSON, nullable=False)
    new_val = Column(JSON, nullable=False)


log_change_history = ReplaceableStoredProcedure(
    # Class ChangeHistory needs a trigger function like this one. It is common to all
    # triggers that use this implementation. See trigger below for an example.
    identifier="log_change_history()",
    definition=f"""
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                INSERT INTO {schema_name}.change_history_0 (schema_name, table_name, operation, new_val)
                    VALUES (TG_TABLE_SCHEMA, TG_RELNAME, TG_OP, row_to_json(NEW));
                RETURN NEW;
            ELSIF TG_OP = 'UPDATE' THEN
                INSERT INTO {schema_name}.change_history_0 (schema_name, table_name, operation, new_val, old_val)
                    VALUES (TG_TABLE_SCHEMA, TG_RELNAME, TG_OP, row_to_json(NEW), row_to_json(OLD));
                RETURN NEW;
            ELSIF TG_OP = 'DELETE' THEN
                INSERT INTO {schema_name}.change_history_0 (schema_name, table_name, operation, old_val)
                        VALUES (TG_TABLE_SCHEMA, TG_RELNAME, TG_OP, row_to_json(OLD));
                RETURN OLD;
            END IF;        
        END;
        $$
    """,
    schema=schema_name,
)


# Create a trigger to track changes to table obs_raw.
# TODO: Copy extension ReplaceableTrigger from branch change-history.
obs_raw_change_history_0_trigger = ReplaceableTrigger(
    identifier="obs_raw_change_history_0_trigger",
    definition=f"""
        BEFORE INSERT OR UPDATE OR DELETE ON {schema_name}.obs_raw
        FOR EACH ROW
        EXECUTE PROCEDURE {schema_name}.log_change_history()
    """,
    schema=schema_name,
)
