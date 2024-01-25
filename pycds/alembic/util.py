from typing import List, Union, Tuple

from sqlalchemy.sql.ddl import DDLElement

from alembic import op


def grant_standard_table_privileges(
    obj: Union[DDLElement, str],
    role_privileges: List[Tuple[str, Tuple[str]]] = (
        ("inspector", ("SELECT",)),
        ("viewer", ("SELECT",)),
        ("steward", ("ALL",)),
    ),
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
