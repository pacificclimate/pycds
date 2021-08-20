![Python CI](https://github.com/pacificclimate/pycds/workflows/Python%20CI/badge.svg)
![Pypi Publishing](https://github.com/pacificclimate/pycds/workflows/Pypi%20Publishing/badge.svg)

# PyCDS

PyCDS is a Python package that provides an
[Object Relational Mapping (ORM)](http://en.wikipedia.org/wiki/Object-relational_mapping)
layer for accessing meteorological observations stored in a relational 
database in a standard database model, referred to variously as a CRMP 
database or a PCDS database.

This package also uses [Alembic](https://alembic.sqlalchemy.org/) to manage 
database creation and migration. For details, 
see [Database maintenance](docs/database-maintenance.md).

This type of database (PCDS/CRMP schema) is currently used at 
PCIC to store BC's long-term weather archive, the Provincial Climate Data 
Set (PCDS), and the Northern Climate Database (dbnorth). For details, see 
[Background](docs/background.md).

With this package, one can recreate the database schema in a
[PostgreSQL](http://www.postgresql.org) or [SQLite](http://www.sqlite.org)
database and/or use the package as an object mapper for programmatic database 
access. PyCDS uses [SQLAlchemy](http://www.sqlalchemy.org) to provide the 
ORM layer.

## Documentation

- [Background](docs/background.md)
- [Installation](docs/installation.md)
- [ORM contents and usage](docs/orm.md)
- [Database maintenance](docs/database-maintenance.md)
- [Managing non-mutable database content](docs/managing-non-mutable.md)
- Testing
    - [Project unit tests](docs/project-unit-tests.md)
    - [Test migrations with a test database](docs/test-migrations.md)
    - [Unit tests in client code](docs/unit-tests-in-client-code.md)
