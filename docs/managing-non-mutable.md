# Managing non-mutable database content

## Table of contents

- [Introduction](#introduction)
- [Replaceable object definitions](#replaceable-object-definitions)
    - [Example: Replaceable views](#example-replaceable-views)
- [Alembic migration operations for replaceable objects](#alembic-migration-operations-for-replaceable-objects)

## Introduction

Database tables can be mutated "in place": for example, columns can be added, 
dropped, or renamed.

Other items, including stored procedures and views, are not mutable.
When a change to them is required they must be replaced in their entirety, 
i.e., dropped and recreated. Such objects are called replaceable objects, 
and PyCDS manages them accordingly.

All important changes to a CRMP/PCDS database are now managed by Alembic 
migrations, including changes to replaceable objects. (Older versions of 
PyCDS did not use Alembic, and a different, now deprecated mechanism was 
used to make changes.)

We use a variation of the 
[replaceable object recipe](https://alembic.sqlalchemy.org/en/latest/cookbook.html#replaceable-objects) 
in the Alembic Cookbook. Because it is somewhat complicated, we outline it
here. Similar explanations are provided in the comments to actual code.

## Replaceable object definitions

Each type of replaceable object needs a representation. That representation
is quite flexible, but it must provide the following two methods:

- `create()`: Returns a DDL object or string containing SQL instructions that
  create the object.
- `drop()`: Returns a DDL object or string containing SQL instructions that
  drop the object.

For replaceable objects whose representation is a *class* (e.g., views,
matviews), the API must be implemented as class methods.

```python
class ReplaceableOrmClass:
    @classmethod
    def create(cls):
        raise NotImplementedError()    

    @classmethod
    def drop(cls):
        raise NotImplementedError()    
```

### Example: Replaceable views

Let's look at how we represent a view which is a replaceable object.

First, we define a class that factors out what is common to replaceable
views, namely the create and drop operations.

```python
class ReplaceableView(ReplaceableOrmClass):
    @classmethod
    def create(cls):
        return ...

    @classmethod
    def drop(cls):
        return ...
```

Now we define a specific view, using both the SQLAlchemy ORM and the 
replaceable view superclass we defined above. The resulting class gives us:

- A SQLAlchemy ORM class that maps (a table) onto the view.
- A definition of the view proper (attribute `__definition__`).
- An implementation of view create/drop operations by inheritance from the
  class `View`.

```python
from pycds import Base  # maybe can't use same base as real tables?

class MyView(Base, ReplaceableView):
    """
    This class does two things:
    - Defines an ORM class mapping a table onto the view.
    - Provides the view's definition
    """
    __tablename__ = "my_view"

    # ... declare ORM columns mapping to view ...

    # Definition must match declared columns
    __definition__ = "SELECT foo FROM bar"  # or a SQLAlchemy selectable
```

## Alembic migration operations for replaceable objects

Next we extend Alembic to process replaceable objects. The following
code extends the Alembic Operations API with operations to create, drop, and
replace replaceable objects. The end result is to add these operations to the
Alembic `op.*` namespace used in migration scripts.

```python
from alembic.operations import Operations, MigrateOperation


class ReversibleOperation(MigrateOperation):
    """
    Base for Alembic migration operations capable of emitting create and drop 
    instructions for a replaceable object and of "reversing" the creation 
    (or removal) of a replaceable object by accessing other migration files 
    (scripts) in order to refer to a "previous" version of an object. 
    It does this so it can invoke the appropriate drop/create operation on 
    the old object before invoking the create/drop operation on the new in 
    order to replace one with the other.

    The "target" of a reversible operation is a replaceable object. Therefore 
    the reversible operation has to know how to invoke the compilation of the 
    create and drop operations for the target objects. It does this by deriving 
    specific reversible operations from this base operation for each kind of 
    replaceable object (target).
    """

    def __init__(self, target):
        self.target = target

     # ... methods that implement generic reversibility  ...
```

Since all replaceable objects conform to the same API, we do not need 
to specialize the operations for each different kind (view, matview,
stored procedure), and we can have a just 3 operations (create, drop, replace)
for all the different types of replaceable objects.

```python
@Operations.register_operation("create_replaceable_object", "invoke_for_target")
@Operations.register_operation("replace_replaceable_object", "replace")
class CreateReplaceableObjectOp(ReversibleOperation):
    """
    Class representing a reversible create operation for a replaceable object.
    This class also requires an implementation to make it executable.
    """
    def reverse(self):
        return DropReplaceableObjectOp(self.target)


@Operations.register_operation("drop_replaceable_object", "invoke_for_target")
class DropReplaceableObjectOp(ReversibleOperation):
    """
    Class representing a reversible drop operation for a replaceable object.
    This class also requires an implementation to make it executable.
    """
    def reverse(self):
        return CreateReplaceableObjectOp(self.target)
```

The replaceable object itself supplies the SQL instructions that create or
drop the object. The Alembic create and drop operations need only execute 
those instructions.

```python
@Operations.implementation_for(CreateReplaceableObjectOp)
def create_replaceable_object(operations, operation):
    operations.execute(operation.target.create())


@Operations.implementation_for(DropReplaceableObjectOp)
def drop_replaceable_object(operations, operation):
    operations.execute(operation.target.drop())     
```

With the representation of replaceable objects and the implementation of
Alembic operations on them defined, we can make use of those objects and
operations in an Alembic migration script.

```python
revision = '123'
down_revision = '000'
branch_labels = None
depends_on = None

from alembic import op
from whereever.version_123 import MyView


def upgrade():
    op.create_replaceable_object(MyView)


def downgrade():
    op.drop_replaceable_object(MyView)
```

More interesting is a migration that replaces one version of a replaceable
object with another. See the Alembic Cookbook article for more details.

```python
revision = '456'
down_revision = '123'
branch_labels = None
depends_on = None

from alembic import op
# New version of MyView that replaces the old
from pycds.orm.views.version_456 import MyView   


def upgrade():
    op.replace_replaceable_object(MyView, replaces="123.MyView")


def downgrade():
    op.replace_replaceable_object(MyView, replace_with="123.MyView")
```
