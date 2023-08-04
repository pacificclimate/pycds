# SQLAlchemy extensions

## Introduction

SQLAlchemy provides constructs for the SQL statements it needs to emit, which is to say the kinds of operations it supports, such as [CREATE TABLE](https://docs.sqlalchemy.org/en/20/core/ddl.html#sqlalchemy.schema.CreateTable).

Such items are called data definition language (DDL) constructs. Existing DDL constructs are [documented](https://docs.sqlalchemy.org/en/14/core/ddl.html).

SQLAlchemy doesn't have DDL constructs for all the SQL statements we'd like to emit. For example, it does not have DDL constructs for CREATE FUNCTION or SET ROLE. If we wish to use those statements without repeating code wherever we do so, and risking inconsistencies, we have to define them ourselves.

Therefore, we would like to define new DDL constructs for ourselves. By "define new ones", we do not mean emitting ad-hoc SQL literals, as shown in [Custom DDL](https://docs.sqlalchemy.org/en/14/core/ddl.html#custom-ddl). Those make it impossible to write consistent, DRY code. We mean instead creating fully fledged constructs parallel to [CreateTable](https://docs.sqlalchemy.org/en/14/core/ddl.html#sqlalchemy.schema.CreateTable), [DropTable](https://docs.sqlalchemy.org/en/14/core/ddl.html#sqlalchemy.schema.DropTable), etc.

SQLAlchemy documentation contains a guide to [Custom SQL Constructs and Compilation Extension](https://docs.sqlalchemy.org/en/14/core/compiler.html#). It provides quite a bit of information, but only a little on creating one's own DDL construct, under the unhelpful heading [Dialect-specific compilation rules](https://docs.sqlalchemy.org/en/14/core/compiler.html#dialect-specific-compilation-rules).

For greater insight, it is worth examining SQLAlchemy [source code for existing DDL elements](https://github.com/sqlalchemy/sqlalchemy/blob/rel_1_4/lib/sqlalchemy/sql/ddl.py).

## DDL extensions defined in PyCDS

We have created several DDL extensions. They are all defined in package [`pycds.sqlalchemy.ddl_extensions`](../../pycds/pycds/sqlalchemy/ddl_extensions). Extensions are partitioned into modules that address a single type of object or operation, e.g., operations on views.

The basic pattern is as follows. This is pretty much identical to the pattern shown in [Dialect-specific compilation rules](https://docs.sqlalchemy.org/en/14/core/compiler.html#dialect-specific-compilation-rules).
```python
from sqlalchemy.schema import DDLElement
from sqlalchemy.ext import compiler

class MyCommand(DDLElement):
    def __init__(self, arg..., kwarg=...):
        ...

@compiler.compiles(MyCommand)
def compile_my_command(element, compiler, **kw):
    return "..."
```

Note that the commands in any given module (e.g., view) often define and use a common subclass of `DDLElement` to avoid repetitive code.
