"""
This package provides Alembic plugins that add to the operations available in
migrations.

Plugins are broken out into modules specific to the database object they affect.
This should make the pattern of plugin implementation a little clearer and easier
to add to.

In brief, a plugin has 2 parts:
- an operation that is registered with Alembic; a subclass of MigrateOperation
- an implementation of the operation; a method that executes a SQL command

For more information on Alembic operation plugins, see
https://alembic.sqlalchemy.org/en/latest/api/operations.html#operation-plugins
"""

# Imports are required to register the new operations.
from .simple_operations import (
    constraint,
    index,
    role,
    table,
)
from ..operation_plugins import replaceable_object_operations

# from ..operation_plugins  reversible_operation
