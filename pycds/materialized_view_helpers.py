"""Materialized view creation tools

SQLAlchemy does not have out-of-the-box support for views or materialized views.
This module adds materialized view functionality.

For info on materialized view creation in SQLAlchemy see:
http://www.jeffwidman.com/blog/847/using-sqlalchemy-to-create-and-manage-postgresql-materialized-views/
but note this depends on flask. Essence is the same as a view.
For an explanation of why we use `append_column` instead of `_make_proxy`, see. This is the core stuff
for materialized views in SQLAlchemy, from its author:
https://bitbucket.org/zzzeek/sqlalchemy/issues/3616/calling-index-on-a-materialized-view

The current version of these helpers creates and updates regular tables instead
of true materialized views. This enables these helpers to be used in with
Postgresql versions < 9.3 (when materialized views were introduced).
To restore the true materialized view functionality, revert the merge commit
for the branch "fake-materialized-views".

TODO: Clean this up a bit. It's not clear the SQLAlchemy commands
  `CreateMaterializedView`, etc. are necessary, since the selectable can be
  compiled by invoking `selectable.compile(...)`. In that case, they can be
  removed and their usage replaced by the simpler functions
  `create_materialized_view` etc.

TODO:
"""

from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy import event
from sqlalchemy import Table, Column, MetaData
from sqlalchemy.ext.declarative import declared_attr
from pycds.view_helpers import snake_case


# SQLAlchemy "commands"


class MaterializedViewCommand(DDLElement):
    def __init__(self, name, selectable=None):
        self.name = name
        self.selectable = selectable


class CreateManualMaterializedView(MaterializedViewCommand):
    pass


class DropManualMaterializedView(MaterializedViewCommand):
    pass


class RefreshManualMaterializedView(MaterializedViewCommand):
    pass


class CreateNativeMaterializedView(MaterializedViewCommand):
    pass


class DropNativeMaterializedView(MaterializedViewCommand):
    pass


class RefreshNativeMaterializedView(MaterializedViewCommand):
    def __init__(self, name, concurrently=False):
        super().__init__(name)
        self.concurrently = concurrently


# SQL implementation (compilation) of commands
# Note: Functions `create_materialized_view` and `drop_materialized_view`
# are also used to implement Alembic matview operations. Hence their existence.

def create_manual_materialized_view(name, body):
    return f"CREATE TABLE {name} AS {body}"


@compiler.compiles(CreateManualMaterializedView)
def compile(element, compiler, **kw):
    return create_manual_materialized_view(
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True),
    )


def drop_manual_materialized_view(name):
    return f"DROP TABLE {name}"


@compiler.compiles(DropManualMaterializedView)
def compile(element, compiler, **kw):
    return drop_manual_materialized_view(element.name)


@compiler.compiles(RefreshManualMaterializedView)
def compile(element, compiler, **kw):
    return 'TRUNCATE TABLE {0}; INSERT INTO {0} {1}'.format(
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True))


@compiler.compiles(CreateNativeMaterializedView)
def compile(element, compiler, **kw):
    return 'CREATE MATERIALIZED VIEW {} AS {}'.format(
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True))


@compiler.compiles(DropNativeMaterializedView)
def compile(element, compiler, **kw):
    return 'DROP MATERIALIZED VIEW {}'.format(element.name)


@compiler.compiles(RefreshNativeMaterializedView)
def compile(element, compiler, **kw):
    return 'REFRESH MATERIALIZED VIEW {} {}'.format(
        ('CONCURRENTLY' if element.concurrently else ''), element.name)


# Implementation of materialized view in declarative base style.

# TODO: Extend this to handle native matviews:
#   - Rename compiler classes etc. to "manual matview"
#   - Add new compiler classes for native matview
#   - Split MaterializedViewMixin into a base and two derived classes,
#       ManualMatviewMixin and NativeMatviewMixin, differing in the defs for
#       class methods create, drop, refresh
#   - Deploy appropriately in code that uses it.

class MaterializedViewMixinBase:
    """
    Base class for mixins for ORM classes that are materialized views.

    Defines the two key attributes of an ORM class, `__table__` and
    `__mapper_args__`, based on the values of class attributes `__selectable__`
    and `__primary_key__`.

    Usage:

        class Thing(Base, MaterializedViewMixin):
            __selectable__ = <SQLAlchemy selectable>
            __primary_key__ = ['primary', 'key', 'columns']

    __selectable__ must be assigned a SQLAlchemy selectable, which is any
        SQLAlchemy object from which rows can be selected. This could be be
        the result of a sqlalchemy.orm.query expression, a
        `sqlalchemy.sql.select` expression, or a `sqlalchemy.sql.text`
        expression. For details, see
        http://docs.sqlalchemy.org/en/latest/core/selectable.html
        .
        The columns returned by __selectable__ are used to construct a table
        that represents the materialized view in the ORM. In the database,
        this table is actually a materialized view (which is very much like a
        table). In the ORM it is a table object, since SQLAlchemy does not
        have a separate native materialized view object (which is why these
        helpers have to be defined).

    __primary_key__ is an optional (see below) array of the names of the
        columns that from the primary key of the view. This array is used to
        construct the standard SQLAlchemy declarative attribute
        `__mapper_args__`, specifically its 'primary_key' component.
        Construction of __mapper_args__ is very simple, and it may at some
        point become desirable to make it more sophisticated.
        For now it is adequate to the need. __primary_key__ is optional and
        may be omitted if __selectable__ already defines primary keys.
        It must be defined otherwise (e.g., text selectables with anonymous
        columns; see tests).


    To create a materialized view in the database:
        Thing.create()

    To drop a materialized view from the database:
        Thing.drop()

    To refresh a materialized view in the database:
        Thing.refresh()

    TODO: Remove methods `create` and `drop`? View management is now handled
      through Alembic migrations, so these methods are redundant and unlikely
      to be used. Remove corresponding tests.
    """

    @declared_attr
    def __table__(cls):
        t = Table(cls.base_viewname(), cls.metadata)
        for c in cls.__selectable__.c:
            t.append_column(Column(c.name, c.type, primary_key=c.primary_key))

        # Not sure if this will work, but it reproduces the setting above ...
        # This event listener causes indexes defined for the materialized view
        # to be created after the matview table is created by calling
        # `metadata.create_all()`.
        # However, this is no the only way to create this table, and so it
        # may be necessary to add index creation code to method create() below.
        @event.listens_for(cls.metadata, "after_create")
        def create_indexes(target, connection, **kw):
            for idx in t.indexes:
                idx.create(connection)

        return t

    @declared_attr
    def __mapper_args__(cls):
        # the return value should instead be merged into other `__mapper_args__`
        # declared for the class ...
        try:
            return {
                'primary_key': [cls.__table__.c[col]
                                for col in cls.__primary_key__]
            }
        except AttributeError:
            return {}

    @classmethod
    def base_viewname(cls):
        try:
            return cls.__viewname__
        except AttributeError:
            return snake_case(cls.__name__) + '_mv'

    @classmethod
    def qualfied_viewname(cls):
        prefix = '' if cls.metadata.schema is None \
            else cls.metadata.schema + '.'
        return prefix + cls.base_viewname()

    @classmethod
    def create(cls, sesh):
        raise NotImplementedError

    @classmethod
    def drop(cls, sesh):
        raise NotImplementedError

    @classmethod
    def refresh(cls, sesh):
        raise NotImplementedError


class ManualMaterializedViewMixin(MaterializedViewMixinBase):
    @classmethod
    def create(cls, sesh):
        return sesh.execute(CreateManualMaterializedView(
            cls.qualfied_viewname(), cls.__selectable__)
        )
        # TODO: Add index creation code here?

    @classmethod
    def drop(cls, sesh):
        return sesh.execute(DropManualMaterializedView(cls.qualfied_viewname()))

    @classmethod
    def refresh(cls, sesh):
        return sesh.execute(RefreshManualMaterializedView(
            cls.qualfied_viewname(), cls.__selectable__)
        )


class NativeMaterializedViewMixin(MaterializedViewMixinBase):
    @classmethod
    def create(cls, sesh):
        return sesh.execute(
            CreateNativeMaterializedView(
                cls.qualfied_viewname(), cls.__selectable__
            )
        )
        # TODO: Add index creation code here?

    @classmethod
    def drop(cls, sesh):
        return sesh.execute(DropNativeMaterializedView(cls.qualfied_viewname()))

    @classmethod
    def refresh(cls, sesh, concurrently=False):
        return sesh.execute(RefreshNativeMaterializedView(
            cls.qualfied_viewname(), concurrently)
        )
