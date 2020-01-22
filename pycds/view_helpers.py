"""View creation tools

SQLAlchemy does not have out-of-the-box support for views or materialized views.
This module adds view functionality.

For details on view creation in SQLAlchemy see:
https://github.com/sqlalchemy/sqlalchemy/wiki/Views
http://stackoverflow.com/questions/9766940/how-to-create-an-sql-view-with-sqlalchemy
https://gist.github.com/techniq/5174412

See also the docstring in pycds/materialized_view_helpers.py for additional
information. In particular, note that we use the materialized-view style
implementation based on `sqlalchemy.Table`, not the one advised in
https://github.com/sqlalchemy/sqlalchemy/wiki/Views based on
`sqlalchemy.sql.table`. The implementation chosen makes it easy to place a view
in schema, but it has the following disadvantage:

WARNING: Views should be declared using a declarative base separate from the
PyCDS tables declarative base `pycds.Base`. (Typically the views declarative
base will use the same schema name as the PyCDS tables declarative base.)

Why? If a view is declared using the PyCDS tables declarative base, then
`pycds.Base.metadata.create_all()` causes an error as the view's intermediary
table is (incorrectly) created.

NOTE: It may be possible to implement views differently so that the issue with
declarative bases does not arise, but the exact form of that implementation
is not clear at the moment. This will do.
"""

import re
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy import Table, Column
from sqlalchemy.sql import table
from sqlalchemy.ext.declarative import declared_attr


class CreateView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable


class DropView(DDLElement):
    def __init__(self, name):
        self.name = name


@compiler.compiles(CreateView)
def compile(element, compiler, **kw):
    return 'CREATE VIEW {} AS {}'.format(
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True)
    )


@compiler.compiles(DropView)
def compile(element, compiler, **kw):
    return 'DROP VIEW {}'.format(element.name)


def snake_case(ident):
    """Return a snake-case version of a camel-case identifier, e.g., MyBigDeal -> my_big_deal.
    Courtesy of http://stackoverflow.com/a/12867228"""
    a = re.compile('((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))')
    return a.sub(r'_\1', ident).lower()


class ViewMixin(object):
    """Mixin for ORM classes that are views.

    This mixin defines the two key attributes of an ORM class, __table__ and __mapper_args__,
    based on the values of class attributes __selectable__ and __primary_key__ that the inheriting class defines.

    Usage:

        class Thing(Base, ViewMixin):
            __viewname__ = 'things'
            __selectable__ = <SQLAlchemy selectable>
            __primary_key__ = ['primary', 'key', 'columns']

    __viewname__ is an optional name for the view. If this attribute is not
        defined, a value is constructed by converting the class name to snake
        case and suffixing '_v'.

    __selectable__ must be assigned a SQLAlchemy selectable, which is any SQLAlchemy object from which rows can be
        selected. This could be be the result of a sqlalchemy.orm.query expression, a sqlalchemy.sql.select
        expression, or a sqlalchemy.sql.text expression. See http://docs.sqlalchemy.org/en/latest/core/selectable.html
        for details.
        The columns returned by __selectable__ are used to construct a proxy table that represents the view in the
        ORM. In the database, this table is actually a view. In the ORM it is a table object, since SQLAlchemy
        does not have a separate native view object (which is why these helpers have to be defined).

    __primary_key__ is an optional (see below) array of the names of the columns that from the primary key of the view.
        This array is used to construct the standard SQLAlchemy declarative attribute __mapper_args__,
        specifically its 'primary_key' component. Construction of __mapper_args__ is very simple, and it may at
        some point become desirable to make it more sophisticated. For now it is adequate to the need.
        __primary_key__ is optional and may be omitted if __selectable__ already defines primary keys. It must
        be defined otherwise (e.g., text selectables with anonymous columns; see tests).

    To create a view in the database:
        Base.metadata.create_all()
    or
        Thing.create()

    To drop a view from the database:
        Base.metadata.drop_all()
    or
        Thing.drop()

    """

    @declared_attr
    def __table__(cls):
        t = Table(cls.base_viewname(), cls.metadata)
        for c in cls.__selectable__.c:
            t.append_column(Column(c.name, c.type, primary_key=c.primary_key))
        return t

    @declared_attr
    def __mapper_args__(cls):
        # the return value should instead be merged into other __mapper_args__ declared for the class ...
        try:
            return {'primary_key': [cls.__table__.c[col] for col in cls.__primary_key__]}
        except AttributeError:
            return {}

    @classmethod
    def base_viewname(cls):
        try:
            return cls.__viewname__
        except AttributeError:
            return snake_case(cls.__name__)

    @classmethod
    def qualfied_viewname(cls):
        prefix = '' if cls.metadata.schema is None \
            else cls.metadata.schema + '.'
        return prefix + cls.base_viewname()

    @classmethod
    def create(cls, sesh):
        return sesh.execute(CreateView(
            cls.qualfied_viewname(), cls.__selectable__)
        )

    @classmethod
    def drop(cls, sesh):
        return sesh.execute(DropView(cls.qualfied_viewname()))
