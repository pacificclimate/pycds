"""
This module defines classes for defining replaceable objects.

Replaceable objects represent database entities that, unlike tables, cannot be
mutated, and must instead be replaced (dropped, re-created) as a whole in order
to update them.

We use a variation of the
[replaceable object recipe](https://alembic.sqlalchemy.org/en/latest/cookbook.html#replaceable-objects)
in the Alembic Cookbook.

Each type of replaceable object needs a representation. That representation
is quite flexible, but it must provide the following two methods:

- `create()`: Returns a DDL element (or string) containing SQL instructions that
  create the object.
- `drop()`: Returns a DDL element (or string) containing SQL instructions that
  drop the object.

For replaceable objects whose representation is a *class* (e.g., views,
matviews), the API must be implemented as class methods. For replaceable
objects whose representation an *object*, they are instance methods.

Note how we factor out the create and drop instructions as SQLAlchemy DDL
commands, which we add as extensions elsewhere.
"""

from pycds.util import snake_case, ddl_escape
from pycds.sqlalchemy.ddl_extensions import (
    CreateView,
    DropView,
    CreateMaterializedView,
    DropMaterializedView,
    RefreshMaterializedView,
    CreateStoredProcedure,
    DropStoredProcedure,
)


# Replaceable objects represented by ORM classes
#
# Note: SQLAlchemy documentation often refers to classes that supply extra
# features in ORM classes as "mixins", but we reserve that terminology for
# classes that actually affect the mapping proper, not just adding an API
# properties or methods for other use.


class ReplaceableOrmClass:
    """
    Base class for replaceable objects which are also ORM classes (e.g., views).
    All replaceable objects must provide methods `create` and `drop`.
    Methods `base_name` and `qualified_name` support implementations
    of `create` and `drop`.
    """

    @classmethod
    def create(cls):
        raise NotImplementedError()

    @classmethod
    def drop(cls):
        raise NotImplementedError()

    @classmethod
    def base_name(cls):
        raise NotImplementedError()

    @classmethod
    def qualified_name(cls):
        prefix = "" if cls.metadata.schema is None else cls.metadata.schema + "."
        return prefix + cls.base_name()


# Views


class ReplaceableView(ReplaceableOrmClass):
    """
    Parent class for replaceable views. These are also ORM classes, and
    this class should be one of the parent classes of the ORM class definition,
    thus:

    ```
    class MyView(Base, ReplaceableView):
        ...
    ```
    """

    @classmethod
    def create(cls):
        return CreateView(cls.qualified_name(), cls.__selectable__)

    @classmethod
    def drop(cls):
        return DropView(cls.qualified_name(), cls.__selectable__)

    # TODO: For SQLAlchemy 1.4/2.0, replace with
    # @declared_attr
    # def __tablename__(cls):
    #   return snake_case(cls.__name__) + '_v'
    @classmethod
    def base_name(cls):
        try:
            return cls.__tablename__
        except AttributeError:
            return snake_case(cls.__name__) + "_v"


# Native materialized views


class ReplaceableNativeMatview(ReplaceableOrmClass):
    """
    Parent class for replaceable native materialized views. These are also ORM
    classes, and this class should be one of the parent classes of the ORM
    class definition, thus:

        class MyMatview(Base, ReplaceableNativeMatview):
            ...
    """

    @classmethod
    def create(cls):
        return CreateMaterializedView(cls.qualified_name(), cls.__selectable__)

    @classmethod
    def drop(cls):
        return DropMaterializedView(cls.qualified_name(), cls.__selectable__)

    @classmethod
    def refresh(cls):
        return RefreshMaterializedView(cls.qualified_name(), cls.__selectable__)

    @classmethod
    def base_name(cls):
        try:
            return cls.__tablename__
        except AttributeError:
            return snake_case(cls.__name__) + "_mv"


# Manual materialized views


class ReplaceableManualMatview(ReplaceableOrmClass):
    """
    Parent class for replaceable Manual materialized views. These are also ORM
    classes, and this class should be one of the parent classes of the ORM
    class definition, thus:

        class MyMatview(Base, ReplaceableManualMatview):
            ...
    """

    @classmethod
    def create(cls):
        return CreateMaterializedView(
            cls.qualified_name(), cls.__selectable__, type_="manual"
        )

    @classmethod
    def drop(cls):
        return DropMaterializedView(
            cls.qualified_name(), cls.__selectable__, type_="manual"
        )

    @classmethod
    def refresh(cls):
        return RefreshMaterializedView(
            cls.qualified_name(), cls.__selectable__, type_="manual"
        )

    @classmethod
    def base_name(cls):
        try:
            return cls.__tablename__
        except AttributeError:
            return snake_case(cls.__name__) + "_mv"


# Replaceable objects represented by objects (not classes)


class ReplaceableObject:
    """A very simple way to represent a named object."""

    def __init__(self, identifier, definition, schema=None):
        self.identifier = identifier
        self.definition = definition
        self.schema = schema

    def qualified_name(self):
        prefix = "" if self.schema is None else f"{self.schema}."
        return f"{prefix}{self.identifier}"

    def create(self):
        raise NotImplementedError()

    def drop(self):
        raise NotImplementedError()


# Replaceable stored procedure


class ReplaceableFunction(ReplaceableObject):
    def __init__(self, identifier, definition, schema=None, escape=True, replace=False):
        # DDL statements substitute special % expressions, and literal '%' must
        # be escaped as '%%'. This is the default behaviour of this class. This
        # is of particular concern for stored procedures as some of the
        # languages themselves use %-substitution.
        super().__init__(identifier, definition, schema)
        self.escape = escape
        self.replace = replace

    def create(self):
        definition = ddl_escape(self.definition) if self.escape else self.definition
        return CreateStoredProcedure(
            self.qualified_name(), definition=definition, replace=self.replace
        )

    def drop(self):
        return DropStoredProcedure(self.qualified_name())
