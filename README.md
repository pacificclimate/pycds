![Python CI](https://github.com/pacificclimate/pycds/workflows/Python%20CI/badge.svg)
![Pypi Publishing](https://github.com/pacificclimate/pycds/workflows/Pypi%20Publishing/badge.svg)

# PyCDS

PyCDS is a Python package that provides an
[Object Relational Mapping (ORM)](http://en.wikipedia.org/wiki/Object-relational_mapping)
layer for accessing meteorological observations stored in a relational 
database in a standard database model, referred to variously as a CRMP 
database or a PCDS database.

This package uses [Alembic](https://alembic.sqlalchemy.org/) to manage 
database creation and migration. For details, 
see [Database creation and migration](docs/database-maintenance.md).

This type of database (PCDS/CRMP schema) is currently used at 
PCIC to store BC's long-term weather archive, the Provincial Climate Data 
Set (PCDS), and the Northern Climate Database (Metnorth). For details, see 
[Background](docs/background.md).

With this package, one can recreate the database schema in a
[PostgreSQL](http://www.postgresql.org) or [SQLite](http://www.sqlite.org)
database and/or use the package as an object mapper for programmatic database 
access. PyCDS uses [SQLAlchemy](http://www.sqlalchemy.org) to provide the 
ORM layer.

## Documentation

- [Background](docs/background.md)
- [Installation](docs/installation.md)
- [Scripts](docs/scripts.md)
- [ORM contents and usage](docs/orm.md)
- [Database creation and migration](docs/database-maintenance.md)
- [Managing non-mutable database content](docs/managing-non-mutable.md)
- Testing
    - [Project unit tests](docs/project-unit-tests.md)
    - [Test migrations with a test database](docs/test-migrations.md)
    - [Unit tests in client code](docs/unit-tests-in-client-code.md)

## Releasing

### Note

Below we describe incrementing the package version manually. We could consider 
using the command 
[`poetry version`](https://python-poetry.org/docs/cli/#version) 
instead to get Poetry to do it for us.

## Production release

1. Modify `tool.poetry.version` in `pyproject.toml`: First remove any suffix
   to the version number, as our convention is to reserve those for test builds
   (e.g., `1.2.3` is a release build, `1.2.3.dev7` is a test build).
   Then increment the release build version.
1. Summarize release changes in `NEWS.md`
1. Commit these changes, then tag the release
   ```bash
   git add pyproject.toml NEWS.md
   git commit -m"Bump to version X.Y.Z"
   git tag -a -m"X.Y.Z" X.Y.Z
   git push --follow-tags
   ```
1. Our GitHub Action `pypi-publish.yml` will build and release the package 
   on our PyPI server.

### Dev/test release

The process is very similar to a production release, but uses a different
version number convention, and omits any notice in NEWS.md.

1. Modify `tool.poetry.version` in `pyproject.toml`: Add or increment the suffix
   in the pattern `.devN`, where N is any number of numeric digits (e.g., `1.2.3.dev11`).
   Our convention is to reserve those for test releases
   (e.g., `1.2.3` is a release build, `1.2.3.dev11` is a test build).
2. Commit changes and tag the release:
   ```bash
   git add pyproject.toml
   git commit -m"Create test version X.Y.Z.devN"
   git tag -a -m"X.Y.Z.devN" X.Y.Z.devN
   git push --follow-tags
   ```
1. Our GitHub Action `pypi-publish.yml` will build and release the package
   on our PyPI server.
