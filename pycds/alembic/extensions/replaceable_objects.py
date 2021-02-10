"""
This module defines base classes for defining replaceable objects. SQLAlchemy
documentation often refers to classes intended to supply extra features in
ORM classes as "mixins", but we prefer to reserve that terminology for classes
that actually affect the mapping proper, not just adding an API for other use.

TBD: Add references to documentation.
"""

from pycds.util import snake_case
from pycds.sqlalchemy.ddl_extensions import (
    CreateView,
    DropView,
    CreateMaterializedView,
    DropMaterializedView,
    RefreshMaterializedView,
)


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
        prefix = '' if cls.metadata.schema is None \
            else cls.metadata.schema + '.'
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
            return snake_case(cls.__name__) + '_v'


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
        return CreateMaterializedView(
            cls.qualified_name(), cls.__selectable__
        )

    @classmethod
    def drop(cls):
        return DropMaterializedView(
            cls.qualified_name(), cls.__selectable__
        )

    @classmethod
    def refresh(cls):
        return RefreshMaterializedView(
            cls.qualified_name(), cls.__selectable__
        )

    @classmethod
    def base_name(cls):
        try:
            return cls.__tablename__
        except AttributeError:
            return snake_case(cls.__name__) + '_mv'


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
            return snake_case(cls.__name__) + '_mv'
