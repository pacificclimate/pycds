"""
Declarative base factory for all views and matviews.

Things which are not actually tables but which are mapped as tables in the ORM
(namely, views and matviews), cannot share the same base with tables because
invoking `Base.metadata.create_all()` creates all defined tables, including
ones for those things which aren't, which causes errors. Therefore, we use a
separate base for views and matviews.

Only one definition is permitted for each named object (e.g., table) associated with a
given metadata object. Therefore, we must use a separate declarative base for multiple
definitions of an object with the same name. Hence the factory below, which is used
to DRY up the manufacture of independent bases.
"""

from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base
from pycds.context import get_schema_name


def make_declarative_base():
    return declarative_base(metadata=MetaData(schema=get_schema_name()))
