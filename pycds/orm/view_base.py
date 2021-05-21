"""
Common base for all views and matviews.

Things which are not actually tables but which are mapped as tables in the ORM
(namely, views and matviews), cannot share the same base with tables because
invoking `Base.metadata.create_all()` creates all defined tables, including
ones for those things which aren't, which causes errors. Therefore, we use a
separate base for views. In future, it's possible that we will add an event
listener (to `Base.metadata`) that creates the views proper. But see the
caveat below.

Sharing a common base will probably not work if multiple versions of the same
view are defined *and* `Base.metadata.create_all()` is invoked (with the
appropriate listeners to ensure that the appropriate objects are created).
This would probably cause errors as multiple instances of a thing with
the same name are attempted to be created. Even this might be
avoided by clever programming, but we aren't that clever this afternoon.
"""
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from pycds.context import get_schema_name

Base = declarative_base(metadata=MetaData(schema=get_schema_name()))
