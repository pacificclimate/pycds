# Alembic extensions

**IMPORTANT NOTE**: Alembic documentation seems only to be available for the latest release. This project currently uses a non-latest release, 1.5.8. Therefore, links to (latest) Alembic documentation may point at information that does not precisely match the code in this project. In particular, some code here may seem redundant now that Alembic has filled some holes present in ver 1.5.8.

## Introduction

To support writing migrations, Alembic provides methods, called _operations_, that emit SQL statements needed to implement a migration. For example, there is the Alembic operation [`create_table`](https://alembic.sqlalchemy.org/en/latest/ops.html#alembic.operations.Operations.create_table), which creates a table. In migrations, operations are available by importing `alembic.op`. For example:

```python
from alembic import op

...


def upgrade():
    op.create_table(
        ...
    )

...
```

Built-in Alembic operations are limited to those supported by SQLAlchemy, e.g., CREATE TABLE. But we would like to use other operations as part of migrations.

Fortunately, Alembic provides a framework for defining new operations which are then available just like the out-of-the-box operations. It uses a plug-in architecture that any user can invoke.

## Alembic operation extensions defined in PyCDS

PyCDS defines a handful of operation extensions, which can be divided into two categories:
- Simple extensions: 
  - Provide simple new operations such as SET ROLE, or extended functionality such as IF NOT EXISTS clause in a create operations.
  - Use a minimum of extra mechanisms. Typically invoke a DDL construct, usually one of [our own](./sqlalchemy-extensions.md).
- Replaceable object extensions: 
  - Provide extensions for managing "replaceable objects" such as functions and views.
  - Use a considerably more complex set of extra mechanisms we supply in PyCDS.

### Simple extensions

Simple extensions provide simple new operations such as SET ROLE, or extended functionality such as an IF NOT EXISTS clause in a create operation. These are defined in the package `pycds.alembic.extensions.operation_plugins.simple_operations`.

Simple extensions also demonstrate the basic pattern for registering a new operation plugin in Alembic. Such a registration has two parts:
- Define and register an operation class.
- Define and register an implementation for the operation class.

The Alembic documentation for [Operation Plugins](https://alembic.sqlalchemy.org/en/latest/api/operations.html#operation-plugins) provides a clear example. For an even simpler example, let's look at our implementation of the `set_role` operation, reproduced below:

```python
from alembic.operations import Operations, MigrateOperation


@Operations.register_operation("set_role")
class SetRoleOp(MigrateOperation):
  """SET ROLE operation"""

  def __init__(self, role_name):
    self.role_name = role_name

  @classmethod
  def set_role(cls, operations, name, **kw):
    """Issue a SET ROLE command."""
    return operations.invoke(cls(name))


@Operations.implementation_for(SetRoleOp)
def implement_set_role(operations, operation):
  # TODO: Refactor into a DDL extension.
  operations.execute(f"SET ROLE '{operation.role_name}'")
```

Note that this example, sadly, emits an ad-hoc literal rather than using a properly defined [custom DDL construct](./sqlalchemy-extensions.md) (which would be named `SetRole`).

### Replaceable object extensions

Although relatively simple in outline, the detailed implementation of these objects and operations on them can be hard to understand. This section is intended to guide you in understanding them and adding new extensions as necessary following the same patterns.

Replaceable object extensions are defined with three component parts:
1. Replaceable objects
   - not operations proper
   - instead, classes of objects suitable to be processed by replaceable object operations
   - specific instances are defined in the ORM
2. Reversible operation: base class for replaceable object operations
3. Replaceable object operations
   - subclasses of reversible operations
   - accept replaceable objects as arguments

#### Replaceable objects

Module `pycds/alembic/extensions/replaceable_objects`.

This module defines classes for creating replaceable objects. A replaceable object is an instance of one of these classes.

A replaceable object represents a database entity that, unlike a table, cannot be mutated in place, and must instead be replaced (dropped, re-created) as a whole in order to update it. Examples of such objects are functions and views. Classes corresponding to these objects are defined in this module.

To implement these classes, we use a variation of the [replaceable object recipe](https://alembic.sqlalchemy.org/en/latest/cookbook.html#replaceable-objects) in the Alembic Cookbook.

We factor out the create and drop instructions as SQLAlchemy DDL commands, which we define elsewhere as [DDL extensions](sqlalchemy-extensions.md).

#### Reversible operation

Module `pycds/alembic/extensions/operation_plugins/reversible_operation`.

This module defines `ReversibleOperation`, a base class for reversible Alembic migration operations. It is a subclass of `MigrateOperation`.

A reversible operation is one capable of emitting create and drop instructions for an object, and of "reversing" the creation (or dropping) of such an object.

Reversal is required because such an operation must invoke the appropriate drop/create operation on the old object before invoking the create/drop operation on the new object in order to replace one with the other in a migration. It does this by accessing other migration scripts so that it can use different (previous or later) versions, enabling an object from one revision to be replaced by its version from another revision.

#### Replaceable object operations

Module `pycds/alembic/extensions/operation_plugins/replaceable_object_operations`.

This module defines Alembic operation plugins that add operations for managing replaceable objects. These operations are _reversible_ operations -- subclasses of `ReversibleOperation`, which in turn is a subclass of the Alembic base class `MigrationOperation`.

All replaceable objects conform to the same API, that is, are instances of a subclass of `ReplaceableObject`. 

Therefore, we do not need to specialize the operations for each different kind of replaceable object (view, matview, function). Instead, we have three generic operations (`create_replaceable_object`, `drop_replaceable_object`, `replace_replaceable_object`) that manage all types of replaceable object.

#### An example

Let's consider how SQL functions are managed by Alembic using this setup.

1. Replaceable object:
   - Module `pycds/alembic/extensions/replaceable_objects` defines a class `ReplaceableFunction`, which is a subclass of `ReplaceableObject`. 
   - In the ORM, an instance of `ReplaceableFunction` is defined for each function managed. Let's call one such instance `example_function`.

2. Replaceable object operations: 
   - Module `pycds/alembic/extensions/operation_plugins/replaceable_object_operations` registers new operations `create_replaceable_object`, `drop_replaceable_object`, `replace_replaceable_object`, which are available on the object `alembic.op`.
   - This is universal to all replaceable objects, and no changes need be made to it to use it for any given replaceable object.

3. Usage:
   - A migration that adds `example_function` to the database.
   - the upgrade operation invokes `alembic.op.create_replaceable_object(example_function)`.
   - The downgrade operation invokes `alembic.op.drop_replaceable_object(example_function)`.

#### Defining and using a new type of replaceable object

Let's suppose we want to start managing a new type of replaceable database object. Call it a "gronk".

The process to add gronks to Alembic management is as follows:

1. Define [new DDL constructs](sqlalchemy-extensions.md).
   - Define DDL constructs that emit SQL statements for creating and dropping gronks, in a new module named `pycds.sqlalchemy.ddl_extensions.gronk`. 
   - Name these classes `CreateGronk` and `DropGronk`.
2. Define a new replaceable object class 
   - Define a new class `ReplaceableGronk` in module `pycds/alembic/extensions/replaceable_objects`. 
   - Follow the pattern for other such replaceable objects. 
   - The class's `create` and `drop` operations will return instances of DDL commands `CreateGronk` and `DropGronk`.
3. Define new instances of `ReplaceableGronk` as required in the ORM. 
   - Use them in migrations as targets of the (already available) operations `create_replaceable_object`, `drop_replaceable_object`, `replace_replaceable_object`.

