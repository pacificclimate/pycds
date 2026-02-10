"""
ORM table versioning system.

Provides access to table ORM definitions at specific migration revisions.
This allows tests to use the correct schema version when testing migrations.
"""

import importlib
import sys
from typing import Optional


class TableVersionManager:
    """
    Manages versioned ORM table definitions.

    Usage:
        # Get tables at specific revision
        tables = get_tables_at_revision("a59d64cf16ca")
        Network = tables.Network
        NetworkHistory = tables.NetworkHistory

        # Or use current version (default)
        tables = get_tables_at_revision()  # Returns current version
        Network = tables.Network
    """

    def __init__(self, revision: Optional[str] = None):
        self.revision = revision
        self._module = None

    def _load_module(self):
        """Lazily load the versioned module."""
        if self._module is None:
            if self.revision is None:
                # Import current/head version from head version module
                self._module = importlib.import_module("pycds.orm.tables")
            else:
                # Import specific version
                module_name = f"pycds.orm.tables.version_{self.revision}"
                self._module = importlib.import_module(module_name)
        return self._module

    def __getattr__(self, name):
        """Dynamically access classes from the versioned module."""
        module = self._load_module()
        try:
            return getattr(module, name)
        except AttributeError:
            raise AttributeError(
                f"Class '{name}' not found in tables module "
                f"{'at current version' if self.revision is None else f'at revision {self.revision}'}"
            )


def get_tables_at_revision(revision: Optional[str] = None) -> TableVersionManager:
    """
    Get ORM table classes at a specific migration revision.

    Args:
        revision: Alembic revision ID (e.g., "a59d64cf16ca").
                 If None, returns current version.

    Returns:
        TableVersionManager that provides access to versioned table classes.

    Examples:
        # Get tables at old revision (before network_display_name)
        tables = get_tables_at_revision("a59d64cf16ca")
        Network = tables.Network
        assert not hasattr(Network, 'display_name')

        # Get current tables
        tables = get_tables_at_revision()
        Network = tables.Network
        assert hasattr(Network, 'display_name')
    """
    return TableVersionManager(revision)


# Convenience for setting a global version context
_global_version: Optional[str] = None


def set_global_table_version(revision: Optional[str] = None):
    """
    Set the global table version that will be used by default.

    This is useful for test fixtures that need to ensure all table
    references use a specific version.

    IMPORTANT: This clears the module cache for pycds.orm.tables and related
    modules to ensure the new version is loaded on next import.

    Args:
        revision: Alembic revision ID, or None for current version.
    """
    global _global_version
    _global_version = revision

    # Clear module cache for tables/views/matviews modules to force reload with new version
    # We need to clear modules that import from pycds.orm.tables (like main pycds module)
    # but NOT modules that don't depend on tables (like pycds.database, pycds.context, etc.)
    # since they may have been imported by test files and clearing them breaks references.
    modules_to_clear = [
        key
        for key in sys.modules.keys()
        if (
            key.startswith("pycds.orm.tables")
            or key.startswith("pycds.orm.views")
            or key.startswith("pycds.orm.native_matviews")
            or key.startswith("pycds.orm.manual_matviews")
            or key == "pycds"
        )
    ]
    for module_name in modules_to_clear:
        del sys.modules[module_name]


def get_global_table_version() -> Optional[str]:
    """Get the currently set global table version."""
    return _global_version


def get_default_tables() -> TableVersionManager:
    """
    Get tables using the global version if set, otherwise current version.

    This respects the global version set by set_global_table_version().
    """
    return TableVersionManager(_global_version)
