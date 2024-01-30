"""
This package defines SQLAlchemy DDLElement constructs that represent DDL
statements not built in to SQLAlchemy. Effectively, it adds new "commands" to
SQLAlchemy on a par with those such as `sqlalchemy.schema.CreateSchema`.

The definition pattern is:
- Define a subclass of `DDLElement` that represents the command.
- Define a compilation (implementation) of the command.
"""
from ..ddl_extensions.function import CreateFunction, DropFunction
from ..ddl_extensions.grant import GrantTablePrivileges
from ..ddl_extensions.materialized_view import (
    CreateMaterializedView,
    DropMaterializedView,
    RefreshMaterializedView,
)
from ..ddl_extensions.role import SetRole, ResetRole
from ..ddl_extensions.view import CreateView, DropView
