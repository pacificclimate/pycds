from typing import List, Union, Tuple

from sqlalchemy.sql.ddl import DDLElement
from alembic import op


def create_view_or_matview(obj, schema=None, grant_privs=True, create_indexes=True):
    """Drop a matview, and optionally create the indexes associated with it. """
    # Create the matview
    op.create_replaceable_object(obj, schema=schema)
    if grant_privs:
        grant_standard_table_privileges(obj, schema=schema)
    if create_indexes:
        # Create any indices on the matview
        for index in obj.__table__.indexes:
            op.create_index(
                index_name=index.name,
                table_name=index.table.name,
                columns=[col.name for col in index.columns],
                unique=index.unique,
                schema=schema,
            )


def drop_view_or_matview(obj, schema=None, drop_indexes=True):
    """Drop a matview, and optionally drop the indexes associated with it. """
    if drop_indexes:
        # Drop any indexes on the matview
        for index in obj.__table__.indexes:
            print(f"### dropping {index.name} on {schema}.{index.table.name}")
            op.drop_index(
                index_name=index.name,
                table_name=index.table.name,
                schema=schema,
            )
    # Drop the matview
    op.drop_replaceable_object(obj, schema=schema)



def grant_standard_table_privileges(
    obj: Union[DDLElement, str],
    role_privileges: List[Tuple[str, Tuple[str]]] = (
        ("inspector", ("SELECT",)),
        ("viewer", ("SELECT",)),
        ("steward", ("ALL",)),
    ),
    schema: str = None,
) -> None:
    """Grant standard privileges on a table-like object (table, view, table_object).

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
