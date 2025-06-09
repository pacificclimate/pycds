import logging
from typing import List, Union, Tuple

from sqlalchemy.sql.ddl import DDLElement
from alembic import op
from pycds.context import get_standard_table_privileges


def create_view(obj, schema=None, grant_privs=True):
    create_view_or_matview(
        obj, schema=schema, grant_privs=grant_privs, create_indexes=False
    )


def create_matview(obj, **kwargs):
    create_view_or_matview(obj, **kwargs)


def create_view_or_matview(obj, schema=None, grant_privs=True, create_indexes=True):
    """Create a view or matview, and optionally create the indexes associated with it."""
    if schema == None:
        logging.warning(
            f"Schema name not set while attempting to create {obj}, this may result in an error if search paths are not set correctly or during testing."
        )
    op.create_replaceable_object(obj, schema=schema)
    if grant_privs:
        grant_standard_table_privileges(obj, schema=schema)
    if create_indexes:
        for index in obj.__table__.indexes:
            op.create_index(
                index_name=index.name,
                table_name=index.table.name,
                columns=[col.name for col in index.columns],
                unique=index.unique,
                schema=schema,
            )


def drop_view(obj, schema=None):
    drop_view_or_matview(obj, schema=schema, drop_indexes=False)


def drop_matview(obj, **kwargs):
    drop_view_or_matview(obj, **kwargs)


def drop_view_or_matview(obj, schema=None, drop_indexes=True):
    """Drop a view or matview, and optionally drop the indexes associated with it."""
    if schema == None:
        logging.warn(
            f"Schema name not set while attempting to drop {obj}, this may result in an error if search paths are not set correctly or during testing."
        )
    if drop_indexes:
        for index in obj.__table__.indexes:
            op.drop_index(
                index_name=index.name,
                table_name=index.table.name,
                schema=schema,
                if_exists=True,
            )
    op.drop_replaceable_object(obj, schema=schema)


def grant_standard_table_privileges(
    obj: Union[DDLElement, str],
    role_privileges: List[Tuple[str, Tuple[str]]] = get_standard_table_privileges(),
    schema: str = None,
) -> None:
    """Grant standard privileges on a table-like object (table, view, matview).

    :param obj: Table-like ORM object or string identifying the object permissions
        are to be granted on.
    :param role_privileges: Iterable of pairs `(role, permissions)`, where `role`
        is a string containing the role name, and `permissions` is an iterable of
        permissions (strings) to be granted on the object to `role`.
    :param schema: Name of schema in which object exists.
    """
    obj_name = obj if isinstance(obj, str) else obj.__tablename__
    for role, privileges in role_privileges:
        op.grant_table_privileges(privileges, obj_name, role, schema=schema)
