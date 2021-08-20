# ORM contents and usage

PyCDS defines the following contents of a CRMP/PCDS database:

# Table of contents

- [Schema name](#schema-name)
- [Tables](#tables)
- [Stored procedures](#stored-procedures)
- [Views](#views)
- [Materialized views](#materialized-views)

## Schema name

All contents of a PyCDS database are defined in a named schema.

SQLAlchemy documentation [recommends against](https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#remote-schema-table-introspection-and-postgresql-search-path)
using the `search_path` to establish schema name:

> keep the `search_path` variable set to its default of `public`, name schemas other than `public`
explicitly within `Table` definitions.

Unfortunately, SQLAlchemy does not make it easy to set the schema name at run-time.
Since SQLAlchemy ORMs are defined declaratively, not procedurally, the schema name must be determined at
"declare time" (i.e., when SQLAlchemy table classes are processed), which is done most conveniently
by fetching the name from an environment variable. Once classes are declared, the schema name
cannot be (re)set.

**The schema name is specified by the environment variable `PYCDS_SCHEMA_NAME`.**

If `PYCDS_SCHEMA_NAME` is not specified, the default value of `crmp` is used,
making this backward compatible with all existing code. The function
`pycds.get_schema_name()` returns this value.

**IMPORTANT:** `PYCDS_SCHEMA_NAME` must agree with the name of the schema targeted in
an existing database, or with the intended name for the creation of a new database.

In the case of an existing database, a mismatch between `PYCDS_SCHEMA_NAME` and the
actual database schema name will cause operations to
fail with errors of the form "could not find object X in schema Y".

## Tables

The tables defined in PyCDS are all those found in a standard CRMP database.

Tables are defined using the SQLAlchemy declarative base method, in the root `pycds` module.

The declarative base for the tables, `pycds.Base`, automatically receives the schema name returned
`pycds.get_schema_name()`. The schema name can only be
modified by specifying the environment variable `PYCDS_SCHEMA_NAME` in the execution environment of the code.

A table in the database is represented by the class `pycds.<Table>`, where `<Table>` is the name of
the ORM object corresponding to the table in the database. The ORM name and the table name are different but
clearly related. For example, the database table `meta_stations` is represented by the ORM class
`pycds.Station`.
Column names within tables bear a similarly close but not always identical relationship.

To map onto tables already defined in a database, execute

```python
pycds.Base.metadata.create_all(engine)
```

where `engine` is a SQLAlchemy database engine.

## Stored procedures

A stored procedure is a replaceable object 
(see [Managing non-mutable database content](managing-non-mutable.md)), 
and all such objects are managed
via Alembic migrations. A migration may add, drop, or change a stored procedure.
The stored procedures present in one
migration version of the database can be different than those in another version.

Because of this, it is not
straightforward to get from PyCDS the list of stored procedures that will be present in
any given version of a database. The easiest way to do that is to examine a database at the
migration version of interest.

TODO: Build a tool for this. Existing infrastructure will make this easy.
Alternative: Extract SP definitions to a version-structured sub-module as for views,
and maintain its `__init__.py`.

A stored procedure in the database is is accessed in SQLAlchemy through the standard
`sqlalchemy.func` mechanism in queries.

## Views

A view is a replaceable object 
(see [Managing non-mutable database content](managing-non-mutable.md)), 
and all such objects are managed via
Alembic migrations. A migration may add, drop, or change a view. The views present in one
migration version of the database can be different than those in another version.

To enable Alembic to work properly, it is necessary retain all
versions of views, not just the latest. The views defined in migration
version `<version>` are stored in the module `pycds.utility_views.version_<version>`
in this directory. The most recent version of each view (frozen in a given release of PyCDS)
is exported by `pycds.utility_views`.

Views are defined using the SQLAlchemy declarative base method, based on an extension to SQLAlchemy
implemented in `pycds/view_helpers.py`.
The declarative base for views, `pycds.utility_views.Base`, automatically receives the schema name returned
`pycds.get_schema_name()`. The schema name can only be
modified by specifying the environment variable `PYCDS_SCHEMA_NAME` in the execution environment of the code.

A view in the database is represented by the class `pycds.utility_views.<View>`, where `<View>` is the name of
the ORM object corresponding to the view in the database. The ORM name and the view name are different but
clearly related. For example, the database view `crmp_network_geoserver` is represented by the ORM class
`pycds.views.CrmpNetworkGeoserver`.
Column names within views bear a similarly close but not always identical relationship.


## Materialized views

A materialized view is a replaceable object
(see [Managing non-mutable database content](managing-non-mutable.md)),
and all such objects are managed via
Alembic migrations. A migration may add, drop, or change a materialized view. The materialized views present in one
migration version of the database can be different than those in another version.

A selection of materialized views is defined in module `pycds.weather_anomaly`. These views support the PCIC
Weather Anomaly application. At present, _all_ predefined materialized views are for the Weather Anomaly application.
Additional materialized views, possibly for other purposes, may be added in future.

To enable Alembic to work properly, it is necessary retain all
versions of materialized views, not just the latest. The materialized views defined in migration
version `<version>` are stored in the module `pycds.weather_anomaly.version_<version>`
in this directory. The most recent version of each view (frozen in a given release of PyCDS)
is exported by `pycds.weather_anomaly`.

Materialized views are defined using the SQLAlchemy declarative base method, based on an extension to SQLAlchemy
in `pycds/materialized_view_helpers.py`. This extension does not at the moment support native materialized views,
which appeared in PostgreSQL 9.3. Instead, materialized views are at present ordinary database tables, and their
contents are maintained by the ORM matview function `refresh()` (see below).
A future release of PyCDS will support native matviews.

The declarative base for materialized views, `pycds.weather_anomaly.Base`, automatically receives the schema name
returned `pycds.get_schema_name()`. The schema name can only be
modified by specifying the environment variable `PYCDS_SCHEMA_NAME` in the execution environment of the code.

A predefined materialized view in the database is represented by the class `pycds.weather_anomaly.<Matview>`,
where `<Matview>` is the name of
the ORM object corresponding to the materialized view in the database.
The ORM name and the materialized view name are different but clearly related.
For example, the database materialized view `daily_max_temperature_mv` is represented by the ORM class
`pycds.weather_anomaly.DailyMaxTemperature`.
Column names within materialized views bear a similarly close but not always identical relationship.

To refresh a materialized view, execute code like the following:

```python
pycds.weather_anomaly.<Matview>.refresh(session)
```

