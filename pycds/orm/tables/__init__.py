"""
ORM declarations for tables.

This module provides access to table ORM definitions, with support for
retrieving definitions at specific migration revisions for testing purposes.

By default, imports from this module provide the current (head) version of tables.
Tests can set a specific revision using set_global_table_version() before importing.
"""

from pycds.orm.versioning import (
    get_global_table_version,
)

# Check if a specific version has been requested
_requested_version = get_global_table_version()

if _requested_version is None:
    # No specific version requested - import from (current/head)
    from .version_33179b5ae85a import *
else:
    # Specific version requested - import from that version module
    import importlib

    _version_module = importlib.import_module(
        f"pycds.orm.tables.version_{_requested_version}"
    )

    # Import all public members from the version module
    for _name in dir(_version_module):
        if not _name.startswith("_"):
            globals()[_name] = getattr(_version_module, _name)

    del importlib, _version_module, _name
