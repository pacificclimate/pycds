"""
Define utility database functions for change history tracking.
"""

from pycds.alembic.extensions.replaceable_objects import ReplaceableFunction
from pycds.context import get_schema_name


schema_name = get_schema_name()


hxtk_collection_name_from_hx = ReplaceableFunction(
    """
hxtk_collection_name_from_hx(hx_table_name text)
    """,
    f"""
    RETURNS text
    LANGUAGE 'plpgsql'
AS $$
    -- Returns the collection name given the history table name.
BEGIN
    RETURN regexp_replace(hx_table_name, '_hx$', '');
END;
$$    
    """,
    schema=schema_name,
)


hxtk_hx_table_name = ReplaceableFunction(
    """
hxtk_hx_table_name(collection_name text)
    """,
    f"""
RETURNS text
    LANGUAGE 'plpgsql'
AS $$
    -- Returns the history table name given the collection name.
    BEGIN 
        RETURN collection_name || '_hx';
    END;
$$
    """,
    schema=schema_name,
)


hxtk_hx_id_name = ReplaceableFunction(
    """
hxtk_hx_id_name(collection_name text)
    """,
    f"""
RETURNS text
    LANGUAGE 'plpgsql'
AS $$
    -- Returns the history id name given the collection name.
    BEGIN
        RETURN collection_name || '_hx_id';
    END;
$$    
    """,
    schema=schema_name,
)
