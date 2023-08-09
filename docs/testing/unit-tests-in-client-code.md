# Unit tests in client code

Client projects -- codebases that install PyCDS -- need to create PyCDS
databases at specific revisions. To enable this, PyCDS includes its Alembic 
database migration content as a subpackage. Client projects need to use this
content in a specific way to create properly migrated test databases.
A template for such test code is provided in the project 
[`pycds-alembic-client`](https://github.com/pacificclimate/pycds-alembic-client).
